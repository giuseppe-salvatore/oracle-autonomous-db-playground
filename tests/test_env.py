import pytest

from lib.utils.env import read_environment_variable


def test_read_environment_variable_returns_configured_value(monkeypatch):
    monkeypatch.setenv("TEST_VALUE", "configured")

    assert read_environment_variable("TEST_VALUE") == "configured"


def test_read_environment_variable_rejects_missing_value(monkeypatch):
    monkeypatch.delenv("TEST_VALUE", raising=False)

    with pytest.raises(ImportError, match="TEST_VALUE"):
        read_environment_variable("TEST_VALUE")
