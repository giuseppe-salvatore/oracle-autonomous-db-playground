# Note you can find more info and documentation here:
# https://oracle.github.io/python-oracledb/samples/tutorial/Python-and-Oracle-Database-The-New-Wave-of-Scripting.html
# https://apexapps.oracle.com/pls/apex/r/dbpm/livelabs/view-workshop?wid=3482

import sys
import time
import random

from datetime import datetime
from datetime import timedelta
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
            PRIMARY KEY (symbol, time))"""


insert_data_in_minute_bars_table = """
        insert into minute_bars
            (symbol, time, open, close, high, low, volume)
            values(:1, :2, :3, :4, :5, :6, :7)"""

count_rows_in_minute_bars_table = """
        select count(*) from minute_bars"""

# Insert some data
start_count = 0
end_count = 2
entires = 100000


def generate_bars():

    rows = []
    symbols = ["TSLA", "AAPL", "MSFT", "CICCA"]
    now = datetime.utcnow()

    start = start_count*100*100*100
    end = start + entires

    print("Inserting indexes from {} to {}".format(start, end))
    for i in range(start, end):
        random_open = random.randint(500, 50000)
        random_max = random.randint(
            random_open, int(random_open + random_open * 10 / 100))
        random_min = random.randint(
            int(random_open - random_open * 10 / 100), random_open)
        random_close = random.randint(random_min, random_max)
        volume = random.randint(500, 100000000)
        rows.append((
            i,
            symbols[i % 4],
            now + timedelta(minutes=i),
            random_open/100,
            random_close/100,
            random_max/100,
            random_min/100,
            volume
        ))

    stop_generating = datetime.now()
    print("Generated {} entires".format(len(rows)))
    print("Time to generate {}".format(
        str(stop_generating - now).split('.', 2)[0]))
    return rows


def get_data_from_minute_bars(connection):
    with connection.cursor() as cursor:
        count = 0
        initial_threshold = 100000
        current_threshold = initial_threshold
        for _ in cursor.execute("""
                    SELECT *
                    FROM minute_bars
                    WHERE symbol=\'TSLA\'"""):
            count = count + 1
            if count == current_threshold:
                print("count is {}".format(count))
                current_threshold = current_threshold + initial_threshold
        print("{} total number of rows".format(count))


def count_rows_in_minute_bars(conn):

    with conn.cursor() as cursor:
        for row in cursor.execute('select count(*) from minute_bars'):
            print(row)


ops_start_time = datetime.utcnow()
oracle_impl = OracleDB()
oracle_impl.connect()
conn = oracle_impl.get_connection()

print("Deleting minute_bars table")
oracle_impl.execute_ddl(drop_minute_bars_table)

print("Create minute_bars table")
oracle_impl.execute_ddl(create_minute_bars_table)


oracleDatasource = StockMarketDataSource("OracleDB")
sqliteDatasource = StockMarketDataSource("SQLite3DB")
# #ret = oracleDatasource.get_all_symbols()
# #ret2 = sqliteDatasource.get_all_symbols()

datacount = {}
symbol = "AAPL"
raw_data = sqliteDatasource.get_raw_data_for_symbol(symbol)
datacount[symbol] = len(raw_data)
print("{} has {} entries".format(symbol, datacount[symbol]))

raw_data_out = []
for i in range(0, len(raw_data)):
    raw_data_out.append((
        raw_data[i][0],
        raw_data[i][1],
        datetime.strptime(raw_data[i][2], "%Y-%m-%d %H:%M:%S"),
        raw_data[i][3],
        raw_data[i][4],
        raw_data[i][5],
        raw_data[i][6],
        raw_data[i][7],
    ))


try:
    oracleDatasource.insert_raw_data_for_symbol(raw_data_out)
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
except Exception:
    print("Expection Handler: I don't know what to do")
    sys.exit(1)
