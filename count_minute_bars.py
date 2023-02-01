# Note you can find more info and documentation here:
# https://oracle.github.io/python-oracledb/samples/tutorial/Python-and-Oracle-Database-The-New-Wave-of-Scripting.html
# https://apexapps.oracle.com/pls/apex/r/dbpm/livelabs/view-workshop?wid=3482

import sys
from lib.database.datasource import StockMarketDataSource

available_db = ["OracleDB", "SQLite3DB"]

if __name__ == "__main__":
    db = sys.argv[1]
    symbol = None

    if len(sys.argv) == 3:
        symbol = sys.argv[2]

    if db in available_db:
        datasource = StockMarketDataSource(db)
        ret = datasource.count_minute_bars(symbol)
        print(
            "There are {} bars available in the selected db".format(ret[0][0]))
    else:
        print("Database connection only availble to {}".format(available_db))
