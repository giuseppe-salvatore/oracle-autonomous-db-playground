import argparse
from contextlib import ExitStack
import multiprocessing as mp
import time
from datetime import datetime

from oracledb.exceptions import DatabaseError

from lib.database.factory import create_data_source
from lib.utils.cli import install_error_handler
from lib.database.oracle.dbimpl import OracleDB
from lib.utils.logger import log

STAGING_TABLE = "minute_bars_repl"
TARGET_TABLE = "minute_bars"
BACKUP_TABLE = "minute_bars_backup"
MAX_RETRIES = 5
RETRYABLE_ORACLE_CODE = "ORA-30036"


def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description="Safely migrate SQLite bars to Oracle."
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="replace minute_bars after staging data passes validation",
    )
    parser.add_argument(
        "--workers", type=int, default=min(mp.cpu_count(), 8), help="parallel workers"
    )
    parser.add_argument("symbols", nargs="*", help="symbols to migrate (default: all)")
    options = parser.parse_args(args)
    if options.workers < 1:
        parser.error("--workers must be at least 1")
    return options


def connect():
    oracle_impl = OracleDB()
    oracle_impl.connect()
    return oracle_impl


def _table_exists(oracle_impl, table_name):
    return (
        oracle_impl.execute_sql(
            "SELECT COUNT(*) FROM user_tables WHERE table_name = :table_name",
            {"table_name": table_name.upper()},
        )[0][0]
        > 0
    )


def prepare_staging_table(oracle_impl):
    if _table_exists(oracle_impl, STAGING_TABLE):
        raise RuntimeError(
            "Staging table minute_bars_repl already exists; it was left untouched. "
            "Review or rename it before starting another migration."
        )
    with create_data_source("OracleDB") as datasource:
        datasource.create_table(STAGING_TABLE)


def migrate_symbol(symbol):
    started = datetime.now()
    with (
        create_data_source("OracleDB") as oracle_datasource,
        create_data_source("SQLite3DB") as sqlite_datasource,
    ):
        rows = sqlite_datasource.get_raw_data_for_symbol(symbol)
        converted_rows = [
            (
                row[0],
                datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S"),
                row[2],
                row[3],
                row[4],
                row[5],
                row[6],
            )
            for row in rows
        ]

        for attempt in range(MAX_RETRIES + 1):
            try:
                oracle_datasource.insert_raw_data_for_symbol_in_table(
                    STAGING_TABLE, converted_rows
                )
                break
            except DatabaseError as error:
                error_text = str(error)
                if "ORA-00001" in error_text:
                    raise RuntimeError(
                        "Duplicate rows rejected while migrating symbol {!r}".format(
                            symbol
                        )
                    ) from error
                if RETRYABLE_ORACLE_CODE not in error_text or attempt == MAX_RETRIES:
                    raise RuntimeError(
                        "Migration failed for symbol {!r} after {} attempt(s): {}".format(
                            symbol, attempt + 1, error_text
                        )
                    ) from error
                delay = 2**attempt
                log.warning(
                    "Retryable Oracle error for %s; retry %s/%s in %s seconds",
                    symbol,
                    attempt + 1,
                    MAX_RETRIES,
                    delay,
                )
                time.sleep(delay)

        elapsed = str(datetime.now() - started).split(".", 2)[0]
        log.info("Migrated %s rows for %s in %s", len(rows), symbol, elapsed)
        return symbol, len(rows)


def validate_staging(oracle_impl, sqlite_datasource, symbols):
    mismatches = []
    for symbol in symbols:
        source_count = sqlite_datasource.count_minute_bars(symbol)[0][0]
        staging_count = oracle_impl.execute_sql(
            "SELECT COUNT(*) FROM {} WHERE symbol = :symbol".format(STAGING_TABLE),
            {"symbol": symbol},
        )[0][0]
        if source_count != staging_count:
            mismatches.append((symbol, source_count, staging_count))
    if mismatches:
        raise RuntimeError(
            "Staging row count mismatch (symbol, source, staged): {}".format(mismatches)
        )


def replace_target(oracle_impl):
    target_exists = _table_exists(oracle_impl, TARGET_TABLE)
    backup_exists = _table_exists(oracle_impl, BACKUP_TABLE)
    if target_exists and backup_exists:
        raise RuntimeError(
            "Cannot replace minute_bars: minute_bars_backup already exists; "
            "preserve or rename it before retrying"
        )

    if target_exists:
        oracle_impl.execute_ddl(
            "ALTER TABLE {} RENAME TO {}".format(TARGET_TABLE, BACKUP_TABLE)
        )
    try:
        oracle_impl.execute_ddl(
            """CREATE TABLE minute_bars (
                symbol VARCHAR2(6) NOT NULL,
                datetime DATE NOT NULL,
                open NUMBER NOT NULL,
                close NUMBER NOT NULL,
                high NUMBER NOT NULL,
                low NUMBER NOT NULL,
                volume NUMBER NOT NULL,
                PRIMARY KEY (symbol, datetime))"""
        )
        oracle_impl.execute_ddl(
            """INSERT INTO minute_bars
                (symbol, datetime, open, close, high, low, volume)
                SELECT symbol, time, open, close, high, low, volume
                FROM minute_bars_repl"""
        )
        staged_count = oracle_impl.execute_sql(
            "SELECT COUNT(*) FROM {}".format(STAGING_TABLE)
        )[0][0]
        target_count = oracle_impl.execute_sql(
            "SELECT COUNT(*) FROM {}".format(TARGET_TABLE)
        )[0][0]
        if staged_count != target_count:
            raise RuntimeError(
                "Replacement row count mismatch: staged {}, target {}".format(
                    staged_count, target_count
                )
            )
        oracle_impl.execute_ddl("DROP TABLE {} PURGE".format(STAGING_TABLE))
    except Exception:
        if _table_exists(oracle_impl, TARGET_TABLE):
            oracle_impl.execute_ddl("DROP TABLE {} PURGE".format(TARGET_TABLE))
        if target_exists and _table_exists(oracle_impl, BACKUP_TABLE):
            oracle_impl.execute_ddl(
                "ALTER TABLE {} RENAME TO {}".format(BACKUP_TABLE, TARGET_TABLE)
            )
        raise


def run_migration(options):
    with ExitStack() as stack:
        sqlite_datasource = stack.enter_context(create_data_source("SQLite3DB"))
        source_symbols = {row[0] for row in sqlite_datasource.get_all_symbols()}
        symbols = options.symbols or sorted(source_symbols)
        if len(symbols) != len(set(symbols)):
            raise ValueError("Each symbol may be listed only once")
        unknown_symbols = set(symbols) - source_symbols
        if unknown_symbols:
            raise ValueError(
                "Symbols were not found in SQLite: {}".format(
                    ", ".join(sorted(unknown_symbols))
                )
            )
        if options.replace and (not source_symbols or set(symbols) != source_symbols):
            raise ValueError(
                "--replace requires migrating every source symbol; run without --replace "
                "to stage a partial migration safely"
            )
        oracle_impl = connect()
        stack.callback(oracle_impl.close)
        prepare_staging_table(oracle_impl)

        try:
            with mp.Pool(processes=options.workers) as pool:
                pool.map(migrate_symbol, symbols)
            validate_staging(oracle_impl, sqlite_datasource, symbols)
            if options.replace:
                replace_target(oracle_impl)
                log.info(
                    "Validated staging table replaced minute_bars; prior target retained as backup"
                )
            else:
                log.info(
                    "Validated data remains in %s; minute_bars was not changed",
                    STAGING_TABLE,
                )
        except Exception:
            log.error("Migration failed; existing minute_bars was left untouched")
            raise


def main(args=None):
    run_migration(parse_args(args))


if __name__ == "__main__":
    install_error_handler("migrate_to_oracle")
    main()
