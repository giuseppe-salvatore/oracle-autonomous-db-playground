# Note you can find more info and documentation here:
# https://oracle.github.io/python-oracledb/samples/tutorial/Python-and-Oracle-Database-The-New-Wave-of-Scripting.html
# https://apexapps.oracle.com/pls/apex/r/dbpm/livelabs/view-workshop?wid=3482

import argparse
from lib.database.factory import create_data_source
from lib.utils.cli import install_error_handler

available_db = ["OracleDB", "SQLite3DB"]

if __name__ == "__main__":
    install_error_handler("count_minute_bars")
    parser = argparse.ArgumentParser(description="Count minute bars.")
    parser.add_argument("db", choices=available_db)
    parser.add_argument("symbol", nargs="?")
    args = parser.parse_args()
    db, symbol = args.db, args.symbol

    if db in available_db:
        with create_data_source(db) as datasource:
            ret = datasource.count_minute_bars(symbol)
            print("There are {} bars available in the selected db".format(ret[0][0]))
    else:
        print("Database connection only availble to {}".format(available_db))
