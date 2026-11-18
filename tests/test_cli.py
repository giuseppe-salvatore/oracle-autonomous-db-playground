import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "script",
    [
        "count_minute_bars.py",
        "get_bars.py",
        "create_minute_bars.py",
        "drop_minute_bars.py",
        "consistency_check.py",
        "migrate_to_oracle.py",
        "migrate_to_new_table.py",
        "db_connect.py",
    ],
)
def test_help_succeeds_without_database_credentials(script):
    result = subprocess.run(
        [sys.executable, str(ROOT / script), "--help"], capture_output=True, text=True
    )
    assert result.returncode == 0
    assert "usage:" in result.stdout


def test_invalid_database_returns_usage_error():
    result = subprocess.run(
        [sys.executable, str(ROOT / "count_minute_bars.py"), "invalid"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2


def test_missing_configuration_reports_structured_failure():
    env = os.environ.copy()
    env.pop("SQLITE_DB_FILE", None)
    result = subprocess.run(
        [sys.executable, str(ROOT / "count_minute_bars.py"), "SQLite3DB"],
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert (
        "event=command_failed command=count_minute_bars error_type=ImportError"
        in result.stderr
    )
