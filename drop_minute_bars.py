import argparse
from lib.database.datasource import StockMarketDataSource

available_db = ["OracleDB"]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Drop the Oracle minute bars table.")
    parser.add_argument("db", choices=available_db)
    db = parser.parse_args().db

    if db in available_db:
        datasource = StockMarketDataSource(db)
        ret = datasource.drop_table("minute_bars")
    else:
        raise Exception(f"Unknown database {db}")
