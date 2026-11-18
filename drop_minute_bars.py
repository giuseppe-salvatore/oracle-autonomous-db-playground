import argparse
from lib.database.datasource import StockMarketDataSource
from lib.utils.cli import install_error_handler

available_db = ["OracleDB"]

if __name__ == "__main__":
    install_error_handler("drop_minute_bars")
    parser = argparse.ArgumentParser(description="Drop the Oracle minute bars table.")
    parser.add_argument("db", choices=available_db)
    db = parser.parse_args().db

    if db in available_db:
        datasource = StockMarketDataSource(db)
        ret = datasource.drop_table("minute_bars")
    else:
        raise Exception(f"Unknown database {db}")
