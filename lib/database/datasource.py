

class StockMarketDataSource():

    def __init__(self, datasource):

        if datasource == "OracleDB":
            from lib.database.oracle.dbimpl import OracleDB
            self.db = OracleDB()
        elif datasource == "SQLite3DB":
            from lib.database.sqlite.dbimpl import SQLite3DB
            self.db = SQLite3DB()
        else:
            raise ModuleNotFoundError(
                "Cannot find database {}".format(datasource))
        self.db.connect()

    # Used to get all the symbols available in the datasource
    def get_all_symbols(self):
        return self.db.execute_sql("SELECT DISTINCT symbol FROM minute_bars")

    def count_minute_bars(self, symbol=None):
        base = "SELECT COUNT(*) FROM minute_bars"
        if symbol is not None:
            base += " WHERE symbol=\'{}\'".format(symbol)
        return self.db.execute_sql(base)

    def get_raw_data_for_symbol(self, symbol: str):
        return self.db.execute_sql("""SELECT *
                                      FROM minute_bars
                                      WHERE symbol=\'{}\'""".format(symbol))

    def insert_raw_data_for_symbol(self, data: list):
        self.db.execute_many_queries("""INSERT into minute_bars
            (symbol, datetime, open, close, high, low, volume)
            values(:1, :2, :3, :4, :5, :6, :7)""", data)

    def insert_raw_data_for_symbol_in_table(self, table: str, data: list):
        self.db.execute_many_queries("""INSERT INTO minute_bars_repl
            (symbol, time, open, close, high, low, volume)
            values(:1, :2, :3, :4, :5, :6, :7)""", data)

    def drop_table(self, table_name: str) -> None:
        self.db.execute_ddl("""DROP TABLE {}
                            """.format(table_name))

    def create_table(self, table_name: str) -> None:
        self.db.execute_ddl("""CREATE TABLE {} (
                                symbol      VARCHAR2(6) NOT NULL,
                                datetime    DATE        NOT NULL,
                                open        NUMBER      NOT NULL,
                                close       NUMBER      NOT NULL,
                                high        NUMBER      NOT NULL,
                                low         NUMBER      NOT NULL,
                                volume      NUMBER      NOT NULL,
                                PRIMARY KEY (symbol, datetime))
                                """.format(table_name))
