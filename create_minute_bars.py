import sys
from lib.database.datasource import StockMarketDataSource

available_db = ["OracleDB"]

if __name__ == "__main__":
    db = sys.argv[1]
    symbol = None

    if db in available_db:
        datasource = StockMarketDataSource(db)
        ret = datasource.create_table('minute_bars')
    else:
        raise Exception(f"Unknown database {db}")
