# Note you can find more info and documentation here:
# https://oracle.github.io/python-oracledb/samples/tutorial/Python-and-Oracle-Database-The-New-Wave-of-Scripting.html
# https://apexapps.oracle.com/pls/apex/r/dbpm/livelabs/view-workshop?wid=3482

import sys
import time
import multiprocessing as mp


from datetime import datetime
from lib.utils.logger import log
from oracledb.exceptions import DatabaseError
from lib.database.oracle.dbimpl import OracleDB
from lib.database.datasource import StockMarketDataSource

# Just making a connection to the db

drop_minute_bars_table = """
        begin
            execute immediate 'drop table minute_bars';
            exception when others then if sqlcode <> -942 then raise;
            end if;
        end;"""

create_minute_bars_table = """
        create table minute_bars (
            symbol      VARCHAR2(6) NOT NULL,
            datetime    DATE        NOT NULL,
            open        NUMBER      NOT NULL,
            close       NUMBER      NOT NULL,
            high        NUMBER      NOT NULL,
            low         NUMBER      NOT NULL,
            volume      NUMBER      NOT NULL,
            PRIMARY KEY (symbol, datetime)) """


insert_data_in_minute_bars_table = """
        insert into minute_bars
            (symbol, datetime, open, close, high, low, volume)
            values(:1, :2, :3, :4, :5, :6, :7)"""

count_rows_in_minute_bars_table = """
        select count(*) from minute_bars"""

# Insert some data
start_count = 0
end_count = 2
entires = 100000


def connect():
    oracle_impl = OracleDB()
    oracle_impl.connect()
    conn = oracle_impl.get_connection()
    return conn, oracle_impl


def delete_minute_bar_table(oracle_impl):
    log.info("Deleting minute_bars table")
    oracle_impl.execute_ddl(drop_minute_bars_table)


def create_minute_bar_table(oracle_impl):
    log.info("Create minute_bars table")
    oracle_impl.execute_ddl(create_minute_bars_table)


def migrate_symbol(symbol):

    start_migration_process = datetime.now()
    oracleDatasource = StockMarketDataSource("OracleDB")
    sqliteDatasource = StockMarketDataSource("SQLite3DB")

    raw_data = sqliteDatasource.get_raw_data_for_symbol(symbol)
    data_length = len(raw_data)

    raw_data_out = []
    for i in range(0, data_length):
        raw_data_out.append((
            raw_data[i][0],
            datetime.strptime(raw_data[i][1], "%Y-%m-%d %H:%M:%S"),
            raw_data[i][2],
            raw_data[i][3],
            raw_data[i][4],
            raw_data[i][5],
            raw_data[i][6],
        ))

    completed = False
    while (not completed):
        try:
            oracleDatasource.insert_raw_data_for_symbol(raw_data_out)
            completed = True
        except DatabaseError as dbError:
            error_str = str(dbError)
            print(error_str)

            if "ORA-30036" in error_str:
                log.warning(
                    """Exception Handler: DatabaseError ORA-30036... will wait 10 secs and retry""")
                time.sleep(10.0)
            elif "ORA-00001" in error_str:
                log.warning("""Exception Handler:
                            DatabaseError ORA-30001
                            will return and proceed to next batch""")
                completed = True
            else:
                log.error(
                    """Exception Handler: DatabaseError but not ORA-30036 will exit as I think this is critical""")
                sys.exit(1)
        except Exception:
            log.error("Exception Handler: I don't know what to do")
            sys.exit(1)

    log.info("Migrating {} which has {} entries took {}".format(
        symbol,
        data_length,
        str(datetime.now() - start_migration_process).split('.', 2)[0]))

    return [symbol, data_length]


def initialize_multiprocessing_pool():
    max_processes = mp.cpu_count()
    max_load = 8
    log.info("Available parallel execution processing : {}".format(max_processes))
    log.info("Max user-defined parallel load          : {}".format(max_load))
    actual_load = min(mp.cpu_count(), max_load)
    log.info(f"NOTE: we are using the min of the above which is {actual_load}")

    start = time.time()
    log.info(
        "Initializing a multi-process pool with {} parallel processes".format(actual_load))

    return mp.Pool(min(mp.cpu_count(), max_load))


def main():

    ops_start_time = datetime.now()
    pool = initialize_multiprocessing_pool()

    # Initial db setup
    conn, oracle_impl = connect()
    delete_minute_bar_table(oracle_impl=oracle_impl)
    create_minute_bar_table(oracle_impl=oracle_impl)
    sqliteDatasource = StockMarketDataSource("SQLite3DB")

    start_fetching = datetime.now()
    target_symbols = []
    if len(sys.argv) == 1:
        all_sym = sqliteDatasource.get_all_symbols()
        for elem in all_sym:
            target_symbols.append(elem[0])
    else:
        for i in range(1, len(sys.argv)):
            target_symbols.append(sys.argv[i])

    start_migration_process = datetime.now()

    res = []

    print(target_symbols)

    for symbol in target_symbols:
        if type(symbol) != str:
            print("This is a problem")
            print(symbol)
        res.append(pool.apply_async(
            migrate_symbol,
            args=(symbol,)
        )
        )

    data_count = {}
    for r in res:
        result = r.get()
        symbol = result[0]
        data_length = result[1]
        data_count[symbol] = data_length

    pool.close()
    pool.join()

    end_migration_process = datetime.now()

    print("Fetching symbol time: {}".format(
        str(start_migration_process - start_fetching).split('.', 2)[0]))
    print("Migration time      : {}".format(
        str(end_migration_process - start_migration_process).split('.', 2)[0]))
    print("Total migration time: {}".format(
        str(end_migration_process - start_fetching).split('.', 2)[0]))


if __name__ == "__main__":

    main()
