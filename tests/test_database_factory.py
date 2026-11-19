from unittest.mock import Mock

import pytest

from lib.database.datasource import StockMarketDataSource
from lib.database.factory import create_data_source


def test_datasource_uses_injected_database():
    database = Mock()
    database.connection = object()
    database.execute_sql.return_value = [(3,)]
    source = StockMarketDataSource(database)

    assert source.count_minute_bars("AAPL") == [(3,)]
    database.execute_sql.assert_called_once_with(
        "SELECT COUNT(*) FROM minute_bars WHERE symbol = :symbol", {"symbol": "AAPL"}
    )


@pytest.mark.parametrize("name", ["OracleDB", "SQLite3DB"])
def test_factory_selects_requested_database(monkeypatch, name):
    module = "oracle" if name == "OracleDB" else "sqlite"
    factory = Mock()
    monkeypatch.setattr(f"lib.database.{module}.dbimpl.{name}", factory)
    assert create_data_source(name).db is factory.return_value


def test_factory_rejects_unknown_database():
    with pytest.raises(ModuleNotFoundError, match="Unknown"):
        create_data_source("Unknown")
