import argparse
from lib.database.factory import create_data_source
from lib.utils.cli import install_error_handler

available_db = ["OracleDB"]

if __name__ == "__main__":
    install_error_handler("create_minute_bars")
    parser = argparse.ArgumentParser(description="Create the Oracle minute bars table.")
    parser.add_argument("db", choices=available_db)
    db = parser.parse_args().db

    if db in available_db:
        datasource = create_data_source(db)
        ret = datasource.create_table("minute_bars")
    else:
        raise Exception(f"Unknown database {db}")
