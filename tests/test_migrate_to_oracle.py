from datetime import datetime

import pytest
from oracledb.exceptions import DatabaseError

import migrate_to_oracle as migration


def test_parse_args_supports_safe_defaults_and_controls():
    options = migration.parse_args(["--workers", "2", "--replace", "AAPL"])

    assert options.workers == 2
    assert options.replace is True
    assert options.symbols == ["AAPL"]


def test_parse_args_rejects_zero_workers():
    with pytest.raises(SystemExit):
        migration.parse_args(["--workers", "0"])


def test_replace_refuses_partial_source_symbol_list(monkeypatch):
    class SQLite:
        def get_all_symbols(self):
            return [("AAPL",), ("MSFT",)]

    monkeypatch.setattr(migration, "StockMarketDataSource", lambda _name: SQLite())
    monkeypatch.setattr(
        migration,
        "connect",
        lambda: (_ for _ in ()).throw(AssertionError("Oracle must not connect")),
    )

    with pytest.raises(ValueError, match="every source symbol"):
        migration.run_migration(migration.parse_args(["--replace", "AAPL"]))


def test_prepare_staging_refuses_to_drop_existing_table():
    class ExistingStage:
        def execute_sql(self, *_args):
            return [(1,)]

        def execute_ddl(self, *_args):
            raise AssertionError("existing staging table must not be changed")

    with pytest.raises(RuntimeError, match="already exists"):
        migration.prepare_staging_table(ExistingStage())


def test_validate_staging_rejects_row_count_mismatch():
    class Oracle:
        def execute_sql(self, *_args):
            return [(1,)]

    class SQLite:
        def count_minute_bars(self, _symbol):
            return [(2,)]

    with pytest.raises(RuntimeError, match="row count mismatch"):
        migration.validate_staging(Oracle(), SQLite(), ["AAPL"])


def test_replace_target_restores_old_table_if_copy_fails():
    class Oracle:
        def __init__(self):
            self.tables = {"MINUTE_BARS", "MINUTE_BARS_REPL"}

        def execute_sql(self, _query, parameters):
            if parameters:
                return [(int(parameters["table_name"] in self.tables),)]
            return [(2,)]

        def execute_ddl(self, statement):
            if statement.startswith("ALTER TABLE"):
                tokens = statement.split()
                table, new_name = tokens[2].upper(), tokens[5].upper()
                self.tables.remove(table)
                self.tables.add(new_name)
            elif statement.startswith("CREATE TABLE"):
                self.tables.add("MINUTE_BARS")
            elif statement.startswith("INSERT INTO"):
                raise RuntimeError("simulated copy failure")
            elif statement.startswith("DROP TABLE"):
                self.tables.remove(statement.split()[2].upper())

    oracle = Oracle()

    with pytest.raises(RuntimeError, match="simulated copy failure"):
        migration.replace_target(oracle)

    assert oracle.tables == {"MINUTE_BARS", "MINUTE_BARS_REPL"}


def test_replace_target_copies_to_compatible_schema_and_keeps_backup():
    class Oracle:
        def __init__(self):
            self.tables = {"MINUTE_BARS", "MINUTE_BARS_REPL"}
            self.statements = []

        def execute_sql(self, query, parameters=None):
            if parameters:
                return [(int(parameters["table_name"] in self.tables),)]
            return [(2,)]

        def execute_ddl(self, statement):
            self.statements.append(statement)
            if statement.startswith("ALTER TABLE"):
                tokens = statement.split()
                self.tables.remove(tokens[2].upper())
                self.tables.add(tokens[5].upper())
            elif statement.startswith("CREATE TABLE"):
                self.tables.add("MINUTE_BARS")
            elif statement.startswith("DROP TABLE"):
                self.tables.remove(statement.split()[2].upper())

    oracle = Oracle()
    migration.replace_target(oracle)

    assert oracle.tables == {"MINUTE_BARS", "MINUTE_BARS_BACKUP"}
    copy_statement = next(
        statement for statement in oracle.statements if statement.startswith("INSERT")
    )
    assert "datetime" in copy_statement
    assert "SELECT symbol, time" in copy_statement


def _install_fake_datasources(monkeypatch, insert):
    class OracleSource:
        def insert_raw_data_for_symbol_in_table(self, table, rows):
            assert table == migration.STAGING_TABLE
            assert rows[0][1] == datetime(2024, 1, 2, 3, 4, 5)
            insert()

    class SQLiteSource:
        def get_raw_data_for_symbol(self, _symbol):
            return [("AAPL", "2024-01-02 03:04:05", 1, 2, 3, 0, 4)]

    monkeypatch.setattr(
        migration,
        "StockMarketDataSource",
        lambda name: OracleSource() if name == "OracleDB" else SQLiteSource(),
    )


def test_migrate_symbol_retries_transient_error_then_succeeds(monkeypatch):
    attempts = []

    def insert():
        attempts.append(1)
        if len(attempts) == 1:
            raise DatabaseError("ORA-30036: unable to extend")

    _install_fake_datasources(monkeypatch, insert)
    delays = []
    monkeypatch.setattr(migration.time, "sleep", delays.append)

    assert migration.migrate_symbol("AAPL") == ("AAPL", 1)
    assert delays == [1]


def test_migrate_symbol_stops_after_retry_limit(monkeypatch):
    _install_fake_datasources(
        monkeypatch, lambda: (_ for _ in ()).throw(DatabaseError("ORA-30036: full"))
    )
    monkeypatch.setattr(migration.time, "sleep", lambda _delay: None)

    with pytest.raises(RuntimeError, match=r"after 6 attempt\(s\)"):
        migration.migrate_symbol("AAPL")


def test_migrate_symbol_reports_duplicates(monkeypatch):
    _install_fake_datasources(
        monkeypatch,
        lambda: (_ for _ in ()).throw(DatabaseError("ORA-00001: duplicate")),
    )

    with pytest.raises(RuntimeError, match="Duplicate rows rejected"):
        migration.migrate_symbol("AAPL")
