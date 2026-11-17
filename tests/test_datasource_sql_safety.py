import sqlite3

import pytest

from lib.database.datasource import StockMarketDataSource
from lib.database.sqlite.dbimpl import SQLite3DB


@pytest.fixture
def datasource():
    database = SQLite3DB.__new__(SQLite3DB)
    database.connection = sqlite3.connect(":memory:")
    database.connection.execute("CREATE TABLE minute_bars (symbol TEXT, datetime TEXT)")
    database.connection.executemany(
        "INSERT INTO minute_bars (symbol, datetime) VALUES (?, ?)",
        [("AAPL", "2024-01-01"), ("AAPL' OR 1=1 --", "2024-01-02")],
    )
    source = StockMarketDataSource.__new__(StockMarketDataSource)
    source.db = database
    yield source
    database.connection.close()


def test_symbol_queries_treat_sql_fragments_as_data(datasource):
    symbol = "AAPL' OR 1=1 --"

    assert datasource.count_minute_bars(symbol) == [(1,)]
    assert datasource.get_raw_data_for_symbol(symbol) == [(symbol, "2024-01-02")]


@pytest.mark.parametrize("operation", ["create_table", "drop_table", "insert"])
def test_table_operations_reject_names_outside_allowlist(operation):
    class DatabaseMustNotBeCalled:
        def __getattr__(self, name):
            raise AssertionError("database called before table validation")

    source = StockMarketDataSource.__new__(StockMarketDataSource)
    source.db = DatabaseMustNotBeCalled()

    with pytest.raises(ValueError, match="Unsupported table name"):
        if operation == "create_table":
            source.create_table("minute_bars; DROP TABLE users")
        elif operation == "drop_table":
            source.drop_table("minute_bars; DROP TABLE users")
        else:
            source.insert_raw_data_for_symbol_in_table(
                "minute_bars; DROP TABLE users", []
            )
