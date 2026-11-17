import sqlite3

from lib.database.datasource import StockMarketDataSource
from lib.database.oracle.dbimpl import OracleDB
from lib.database.sqlite.dbimpl import SQLite3DB


def make_source(connection):
    database = SQLite3DB.__new__(SQLite3DB)
    database.connection = connection
    source = StockMarketDataSource.__new__(StockMarketDataSource)
    source.db = database
    return source


def test_sqlite_insert_count_create_and_drop_operations():
    connection = sqlite3.connect(":memory:")
    source = make_source(connection)
    try:
        source.create_table("minute_bars")
        rows = [("AAPL", "2024-01-02", 10, 11, 12, 9, 100)]
        source.insert_raw_data_for_symbol(rows)

        assert source.count_minute_bars("AAPL") == [(1,)]
        assert source.get_raw_data_for_symbol("AAPL") == [
            ("AAPL", "2024-01-02", 10, 11, 12, 9, 100)
        ]

        source.drop_table("minute_bars")
        assert (
            connection.execute(
                "SELECT name FROM sqlite_master WHERE name = 'minute_bars'"
            ).fetchall()
            == []
        )
    finally:
        connection.close()


def test_oracle_connect_uses_mtls_wallet_settings(monkeypatch):
    oracle = OracleDB.__new__(OracleDB)
    oracle.user = "test-user"
    oracle.password = "test-password"
    oracle.dsn_tls = "test_tls_dsn"
    oracle.config_dir = "/tmp/test-wallet"
    oracle.wallet_location = "/tmp/test-wallet"
    oracle.wallet_password = "test-wallet-password"
    expected_connection = object()

    def fake_connect(**kwargs):
        calls.update(kwargs)
        return expected_connection

    calls = {}
    monkeypatch.setattr("lib.database.oracle.dbimpl.oracledb.connect", fake_connect)
    oracle.connect()

    assert oracle.connection is expected_connection
    assert calls == {
        "user": "test-user",
        "password": "test-password",
        "dsn": "test_tls_dsn",
        "config_dir": "/tmp/test-wallet",
        "wallet_location": "/tmp/test-wallet",
        "wallet_password": "test-wallet-password",
    }
