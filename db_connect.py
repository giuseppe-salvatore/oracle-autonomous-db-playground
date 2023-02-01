# Note you can find more info and documentation here:
# https://oracle.github.io/python-oracledb/samples/tutorial/Python-and-Oracle-Database-The-New-Wave-of-Scripting.html
# https://apexapps.oracle.com/pls/apex/r/dbpm/livelabs/view-workshop?wid=3482

import sys
import time
import random
import asyncio

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
            id     NUMBER      PRIMARY KEY,
            symbol VARCHAR2(6) NOT NULL,
            time   DATE        NOT NULL,
            open   NUMBER      NOT NULL,
            close  NUMBER      NOT NULL,
            high   NUMBER      NOT NULL,
            low    NUMBER      NOT NULL,
            volume NUMBER      NOT NULL,
            UNIQUE (symbol, time))"""


insert_data_in_minute_bars_table = """
        insert into minute_bars
            (id, symbol, time, open, close, high, low, volume)
            values(:1, :2, :3, :4, :5, :6, :7, :8)"""

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

        # print("{} total number of rows".format(len(cursor.execute('select * from minute_bars'))))
        count = 0
        initial_threshold = 100000
        current_threshold = initial_threshold
        for _ in cursor.execute('select * from minute_bars where symbol=\'TSLA\''):
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


# for i in range(0, 1):
#     rows = generate_bars()
#     success = False

#     start_pushing = datetime.utcnow()
#     while not success:
#         try:
#             oracle_impl.execute_many_queries(
#                 insert_data_in_minute_bars_table, rows)
#             success = True
#         except DatabaseError as dbError:
#             error_str = str(dbError)
#             print(error_str)

#             if "ORA-30036" in error_str:
#                 print("""Expection Handler:
#                          DatabaseError ORA-30036
#                          will wait 10 secs and retry""")
#                 time.sleep(10.0)
#             elif "ORA-00001" in error_str:
#                 print("""Expection Handler:
#                          DatabaseError ORA-30001
#                          will return and proceed to next batch""")
#                 break
#             else:
#                 print("""Expection Handler:
#                          DatabaseError but not ORA-30036
#                          will exit""")
#                 sys.exit(1)
#         except Exception:
#             print("Expection Handler: I don't know what to do")
#             sys.exit(1)

#     stop_pushing = datetime.now()
#     print("Time to push {}".format(
#         str(stop_pushing - start_pushing).split('.', 2)[0]))

#     ret = oracle_impl.execute_sql(count_rows_in_minute_bars_table)
#     print("Row count = {}".format(ret.pop()[0]))
#     start_count = start_count + 2
#     end_count = end_count + 2
#     ops_partial_time = datetime.utcnow()
#     print("----> Partial execution time after {} rounds: {}".format(
#         i, str(datetime.utcnow() - ops_start_time).split('.', 2)[0]))

# print("----> Total execution time: {}"
#       .format(str(datetime.utcnow() - ops_start_time).split('.', 2)[0]))


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


# sum = 1
# for elem in ret2:
#     symbol = elem[0]
#     if symbol == "AAPL":
#         raw_data = sqliteDatasource.get_raw_data_for_symbol(symbol)
#         datacount[symbol] = len(raw_data)
#         print("{} has {} entries".format(symbol, datacount[symbol]))
#         sum = sum + datacount[symbol]
# print(len(ret2))
# print(sum)
# async def print_data(symbol, datasource):
#     start = datetime.utcnow()
#     ret2 = datasource.get_minute_bars_for_symbol(symbol)
#     end = datetime.utcnow()
#     print("----> Execution time for {}: {}"
#           .format(symbol, str(datetime.utcnow() - start).split('.', 2)[0]))
#     print("----> Start time: {}".format(start))
#     print("----> End time  : {}".format(end))
#     # for elem in ret2:
#     #     print("{}, {}, {}, {}, {}, {}, {}"
#     #           .format(elem[1], elem[2], elem[3], elem[4], elem[5], elem[6], elem[7]))
#     print("Size of data {}".format(len(ret2)))


# async def print_all():
#     futures = [print_data(item[0], StockMarketDataSource("OracleDB"))
#                for item in ret]
#     await asyncio.gather(*futures)

# asyncio.run(print_all())
