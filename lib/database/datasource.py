class StockMarketDataSource:
    ALLOWED_TABLES = {"minute_bars", "minute_bars_repl"}

    def __init__(self, database):
        self.db = database

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.db.close()

    def _ensure_connected(self):
        if not hasattr(self.db, "connection") or self.db.connection is None:
            self.db.connect()

    # Used to get all the symbols available in the datasource
    def get_all_symbols(self):
        self._ensure_connected()
        return self.db.execute_sql("SELECT DISTINCT symbol FROM minute_bars")

    def count_minute_bars(self, symbol=None):
        self._ensure_connected()
        query = "SELECT COUNT(*) FROM minute_bars"
        parameters = None
        if symbol is not None:
            query += " WHERE symbol = :symbol"
            parameters = {"symbol": symbol}
        return self.db.execute_sql(query, parameters)

    def get_raw_data_for_symbol(self, symbol: str):
        self._ensure_connected()
        return self.db.execute_sql(
            "SELECT * FROM minute_bars WHERE symbol = :symbol",
            {"symbol": symbol},
        )

    def insert_raw_data_for_symbol(self, data: list):
        self._ensure_connected()
        self.db.execute_many_queries(
            """INSERT into minute_bars
            (symbol, datetime, open, close, high, low, volume)
            values(:1, :2, :3, :4, :5, :6, :7)""",
            data,
        )

    def insert_raw_data_for_symbol_in_table(self, table: str, data: list):
        table = self._validate_table_name(table)
        self._ensure_connected()
        time_column = "datetime" if table == "minute_bars" else "time"
        self.db.execute_many_queries(
            """INSERT INTO {}
            (symbol, {}, open, close, high, low, volume)
            values(:1, :2, :3, :4, :5, :6, :7)""".format(table, time_column),
            data,
        )

    def drop_table(self, table_name: str) -> None:
        table_name = self._validate_table_name(table_name)
        self._ensure_connected()
        self.db.execute_ddl("DROP TABLE {}".format(table_name))

    def create_table(self, table_name: str) -> None:
        table_name = self._validate_table_name(table_name)
        self._ensure_connected()
        time_column = "datetime" if table_name == "minute_bars" else "time"
        self.db.execute_ddl(
            """CREATE TABLE {} (
                                symbol      VARCHAR2(6) NOT NULL,
                                {}          DATE        NOT NULL,
                                open        NUMBER      NOT NULL,
                                close       NUMBER      NOT NULL,
                                high        NUMBER      NOT NULL,
                                low         NUMBER      NOT NULL,
                                volume      NUMBER      NOT NULL,
                                PRIMARY KEY (symbol, {}))
                                """.format(table_name, time_column, time_column)
        )

    @classmethod
    def _validate_table_name(cls, table_name: str) -> str:
        if not isinstance(table_name, str) or table_name not in cls.ALLOWED_TABLES:
            raise ValueError(
                "Unsupported table name {!r}; allowed tables: {}".format(
                    table_name, ", ".join(sorted(cls.ALLOWED_TABLES))
                )
            )
        return table_name
