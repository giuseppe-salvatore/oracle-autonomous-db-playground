# Oracle and SQLite stock data playground

Tools to query and compare stock minute bars in SQLite and Oracle Autonomous Database, and migrate SQLite data to Oracle.

## Setup

Python 3.11 is used for the locked dependencies and continuous integration.

```bash
make prepare
make install
source .venv/bin/activate
source .env.local
```

Keep `.env.local` and wallet files private. The `.local` suffix is ignored by Git.
Set and export these environment variables in `.env.local`, using your own values:

- `SQLITE_DB_FILE`: path to the SQLite database.
- `ORACLE_ADB_USER` and `ORACLE_ADB_PASSWORD`: Oracle database credentials.
- `ORACLE_ADB_TLS_CONNECTION_STRING`: Oracle connection string.
- `ORACLE_ADB_WALLET_DIR`: directory containing the downloaded Oracle wallet and network configuration.
- `ORACLE_ADB_WALLET_PASSWORD`: wallet password.
- `NTFY_TOPIC`: notification topic used by the Raspberry Pi pinger.

Oracle connections use the wallet for mutual TLS. Obtain the wallet and connection details from your Oracle Cloud database configuration; do not commit them.

## Commands

Run a command with `--help` for its arguments. Examples:

```bash
python count_minute_bars.py SQLite3DB AAPL
python get_bars.py SQLite3DB AAPL
python consistency_check.py
python migrate_to_oracle.py --workers 2
bash scripts/run_count_minute_bars.sh OracleDB AAPL
```

The Oracle migration stages data in `minute_bars_repl` and checks row counts. It changes the target only with `--replace`, which requires every source symbol and retains an existing target as `minute_bars_backup`. Existing staging or backup tables can block a run; inspect them before removing or renaming them. Oracle operations and recovery should be verified against a test database first.

`drop_minute_bars.py` drops the Oracle table. The legacy `db_connect.py` demo also drops and recreates it; use with care. `migrate_to_new_table.py` is a legacy SQLite copy script.

Run local checks with `make check` after `make prepare`; it installs development dependencies and requires ShellCheck. `make verify`, `make test`, and `make audit` run individual checks.

## Troubleshooting

- Missing environment variable: export it in `.env.local`, then source that file.
- Oracle wallet or connection failure: check the wallet directory, password, connection string, network access and database configuration.
- Pinger notification failure: check `NTFY_TOPIC` and network access; the script reports notification errors to stderr.

## Updating Python dependencies

Edit `requirements.in` or `requirements-dev.in`, install `uv`, then regenerate both locks:

```bash
uv pip compile --python-version 3.11 --generate-hashes --no-emit-index-url requirements.in -o requirements.txt
uv pip compile --python-version 3.11 --generate-hashes --no-emit-index-url requirements-dev.in -o requirements-dev.txt
```

Review both generated files together and verify them on each target platform before deploying.
