# Note you can find more info and documentation here:
# https://oracle.github.io/python-oracledb/samples/tutorial/Python-and-Oracle-Database-The-New-Wave-of-Scripting.html
# https://apexapps.oracle.com/pls/apex/r/dbpm/livelabs/view-workshop?wid=3482

import sys
import time

from datetime import datetime
from oracledb.exceptions import DatabaseError
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


insert_data_in_minute_bars_repl_teable = """
        insert into minute_bars
            (symbol, datetime, open, close, high, low, volume)
            values(:1, :2, :3, :4, :5, :6, :7)"""

count_rows_in_minute_bars_table = """
        select count(*) from minute_bars"""

# Insert some data
start_count = 0
end_count = 2
entires = 100000

ops_start_time = datetime.utcnow()

sqliteDatasource = StockMarketDataSource("SQLite3DB")

start_fetching = datetime.utcnow()

ret = []

if len(sys.argv) == 1:
    all_sym = sqliteDatasource.get_all_symbols()
    for elem in all_sym:
        ret.append(elem[0])
else:
    for i in range(1, len(sys.argv)):
        ret.append(sys.argv[i])


start_migration_process = datetime.utcnow()
datacount = {}
for elem in ret:
    symbol = elem
    raw_data = sqliteDatasource.get_raw_data_for_symbol(symbol)
    datacount[symbol] = len(raw_data)
    print("Migrating {} that has {} entries".format(symbol, datacount[symbol]))

    raw_data_out = []
    for i in range(0, len(raw_data)):
        raw_data_out.append((
            raw_data[i][1],
            datetime.strptime(raw_data[i][2], "%Y-%m-%d %H:%M:%S"),
            raw_data[i][3],
            raw_data[i][4],
            raw_data[i][5],
            raw_data[i][6],
            raw_data[i][7],
        ))
    try:
        sqliteDatasource.insert_raw_data_for_symbol_in_table(
            "minute_bars_repl", raw_data_out)
    except DatabaseError as dbError:
        error_str = str(dbError)
        print(error_str)

        if "ORA-30036" in error_str:
            print("""Expection Handler:
                        DatabaseError ORA-30036
                        will wait 10 secs and retry""")
            time.sleep(10.0)
        elif "ORA-00001" in error_str:
            print("""Expection Handler:
                        DatabaseError ORA-30001
                        will return and proceed to next batch""")
        else:
            print("""Expection Handler:
                        DatabaseError but not ORA-30036
                        will exit""")
            sys.exit(1)
    # except Exception as e:
    #    print("Expection Handler: I don't know what to do")
    #    print(e)
    #    sys.exit(1)

end_migration_process = datetime.utcnow()

print("Fetching symbol time: {}".format(
    str(start_migration_process - start_fetching).split('.', 2)[0]))
print("Migration time      : {}".format(
    str(end_migration_process - start_migration_process).split('.', 2)[0]))
print("Total migration time: {}".format(
    str(end_migration_process - start_fetching).split('.', 2)[0]))
