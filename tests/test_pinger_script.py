import os
import fcntl
import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.fixture
def pinger_environment(tmp_path):
    project = tmp_path / "project"
    scripts = project / "scripts"
    scripts.mkdir(parents=True)
    (project / ".venv" / "bin").mkdir(parents=True)
    (project / ".venv" / "bin" / "activate").write_text("")
    (project / ".env.local").write_text("export NTFY_TOPIC=test-topic-only\n")
    (project / "count_minute_bars.py").write_text("# test stub\n")
    shutil.copy(
        Path(__file__).parents[1] / "scripts" / "run_count_minute_bars.sh",
        scripts / "run_count_minute_bars.sh",
    )

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    python = fake_bin / "python"
    python.write_text(
        "#!/usr/bin/env bash\n"
        'printf \'%s\\n\' "$@" > "$PYTHON_ARGS_FILE"\n'
        "printf '%s' \"$PYTHON_STDOUT\"\n"
        "printf '%s' \"$PYTHON_STDERR\" >&2\n"
        'exit "${PYTHON_EXIT:-0}"\n'
    )
    python.chmod(0o755)
    curl = fake_bin / "curl"
    curl.write_text(
        "#!/usr/bin/env bash\n"
        'printf \'%s\\n\' "$@" > "$CURL_ARGS_FILE"\n'
        'exit "${CURL_EXIT:-0}"\n'
    )
    curl.chmod(0o755)

    home = tmp_path / "home"
    (home / "workspace").mkdir(parents=True)
    env = os.environ.copy()
    env.update(
        {
            "HOME": str(home),
            "PATH": "{}:{}".format(fake_bin, env["PATH"]),
            "NTFY_TOPIC": "test-topic-only",
            "PYTHON_ARGS_FILE": str(tmp_path / "python-args.txt"),
            "CURL_ARGS_FILE": str(tmp_path / "curl-args.txt"),
            "PYTHON_STDOUT": "pinger output\n",
            "PYTHON_STDERR": "",
            "PYTHON_EXIT": "0",
            "CURL_EXIT": "0",
        }
    )
    return scripts / "run_count_minute_bars.sh", home, env, tmp_path


def test_pinger_success_captures_logs_without_notification(pinger_environment):
    script, home, env, tmp_path = pinger_environment

    result = subprocess.run(
        ["bash", str(script), "SQLite3DB", "AAPL"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "pinger output" in result.stdout
    assert (home / "workspace" / "oracle-pinger-AAPL-output.log").read_text() == (
        "pinger output\n"
    )
    assert not (tmp_path / "curl-args.txt").exists()
    assert (tmp_path / "python-args.txt").read_text().splitlines()[-2:] == [
        "SQLite3DB",
        "AAPL",
    ]


def test_pinger_failure_notifies_topic_but_keeps_original_exit_code(
    pinger_environment,
):
    script, _home, env, tmp_path = pinger_environment
    env.update(
        {
            "PYTHON_EXIT": "7",
            "PYTHON_STDERR": "database failed\n",
            "CURL_EXIT": "22",
        }
    )

    result = subprocess.run(
        ["bash", str(script), "OracleDB", "AAPL"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 7
    assert "Failed to send pinger notification" in result.stderr
    request_args = (tmp_path / "curl-args.txt").read_text()
    assert "https://ntfy.sh/test-topic-only" in request_args
    assert "Exit code: 7" in request_args
    assert "database failed" in result.stdout


@pytest.mark.parametrize("db,symbol", [("bad", "AAPL"), ("OracleDB", "../oops")])
def test_pinger_rejects_invalid_arguments(pinger_environment, db, symbol):
    script, _home, env, tmp_path = pinger_environment
    result = subprocess.run(
        ["bash", str(script), db, symbol], env=env, capture_output=True, text=True
    )
    assert result.returncode == 2
    assert not (tmp_path / "python-args.txt").exists()


def test_pinger_refuses_concurrent_run(pinger_environment):
    script, home, env, tmp_path = pinger_environment
    lock = home / "workspace" / "oracle-pinger-AAPL.lock"
    with lock.open("w") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = subprocess.run(
            ["bash", str(script), "OracleDB", "AAPL"],
            env=env,
            capture_output=True,
            text=True,
        )
    assert result.returncode == 1
    assert "already running" in result.stderr
    assert not (tmp_path / "python-args.txt").exists()
