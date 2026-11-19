from lib.database.datasource import StockMarketDataSource


def create_data_source(name):
    if name == "OracleDB":
        from lib.database.oracle.dbimpl import OracleDB

        database = OracleDB()
    elif name == "SQLite3DB":
        from lib.database.sqlite.dbimpl import SQLite3DB

        database = SQLite3DB()
    else:
        raise ModuleNotFoundError("Cannot find database {}".format(name))
    return StockMarketDataSource(database)
