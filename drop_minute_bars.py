import argparse
from lib.database.factory import create_data_source
from lib.utils.cli import install_error_handler

available_db = ["OracleDB"]

if __name__ == "__main__":
    install_error_handler("drop_minute_bars")
    parser = argparse.ArgumentParser(description="Drop the Oracle minute bars table.")
    parser.add_argument("db", choices=available_db)
    db = parser.parse_args().db

    if db in available_db:
        with create_data_source(db) as datasource:
            ret = datasource.drop_table("minute_bars")
    else:
        raise Exception(f"Unknown database {db}")
