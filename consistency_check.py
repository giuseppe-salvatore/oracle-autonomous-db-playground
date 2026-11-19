# Note you can find more info and documentation here:
# https://oracle.github.io/python-oracledb/samples/tutorial/Python-and-Oracle-Database-The-New-Wave-of-Scripting.html
# https://apexapps.oracle.com/pls/apex/r/dbpm/livelabs/view-workshop?wid=3482

import argparse
from datetime import datetime
from lib.utils.time import time_diff, to_userfriendly_str
from lib.database.factory import create_data_source
from lib.utils.cli import install_error_handler


available_db = ["OracleDB", "SQLite3DB"]

if __name__ == "__main__":
    install_error_handler("consistency_check")
    argparse.ArgumentParser(
        description="Compare Oracle and SQLite data counts."
    ).parse_args()
    start = datetime.utcnow()

    with (
        create_data_source("OracleDB") as oracle_ds,
        create_data_source("SQLite3DB") as sqlite_ds,
    ):
        print("Performing consistency check on symbols...")

        oracle_symbols = oracle_ds.get_all_symbols()
        sqlite_symbols = sqlite_ds.get_all_symbols()

        print("There are {} symbols in the Oracle Database".format(len(oracle_symbols)))
        print("There are {} symbols in the SQLite Database".format(len(sqlite_symbols)))

        missing_count = 0
        if len(oracle_symbols) != len(sqlite_symbols):
            for sqlite_sym in sqlite_symbols:
                if sqlite_sym not in oracle_symbols:
                    print("{} not present in Oracle Database".format(sqlite_sym))
                    missing_count = missing_count + 1

        if missing_count == 0:
            print("No mismatch between symbols!")

        print("Performing consistency check on entries...")
        oracle_entries_count = oracle_ds.count_minute_bars()[0][0]
        sqlite_entries_count = sqlite_ds.count_minute_bars()[0][0]
        print("There are {} bars in the Oracle Database".format(oracle_entries_count))
        print("There are {} bars in the SQLite Database".format(sqlite_entries_count))
        if oracle_entries_count != sqlite_entries_count:
            print(
                "Numbers are different: SQLite ({}) vs Oracle ({})".format(
                    sqlite_entries_count, oracle_entries_count
                )
            )
        else:
            print("Numbers are matching!")

        print(
            "Consistency check took: {}".format(
                to_userfriendly_str(time_diff(start, datetime.utcnow()))
            )
        )
