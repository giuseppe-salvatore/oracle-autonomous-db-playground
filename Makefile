SHELL := /bin/bash

prepare: .venv/bin/activate 
	@echo "Prepare"
	python3 -m pip install virtualenv
	python3 -m venv .venv/

install:
	source .venv/bin/activate && \
	python -m pip install --upgrade pip && \
	python -m pip install -r requirements.txt

install-dev: install
	source .venv/bin/activate && \
	python -m pip install -r requirements-dev.txt

format: install-dev
	source .venv/bin/activate && \
	ruff format .

verify: install-dev
	source .venv/bin/activate && \
	ruff format --check . && \
	ruff check .

test: install-dev
	source .venv/bin/activate && \
	pytest

audit: install-dev
	source .venv/bin/activate && \
	pip-audit -r requirements.txt

shellcheck:
	shellcheck scripts/*.sh

run-consistency-check : install
	bash scripts/run_consistency_check.sh 

run-bar-count : install
	bash scripts/run_count_minute_bars.sh OracleDB SPY

run-bar-count-local : install
	bash scripts/run_count_minute_bars.sh SQLite3DB SPY

execute-migration-to-oracle : install
	source .venv/bin/activate && \
	source .env.local && \
	export SQLITE_DB_FILE=/home/gr4ce/workspace/personal/algo-trading/data/stock_data.alpaca.db && \
	python -m migrate_to_oracle

.PHONY: audit check clean format install install-dev shellcheck test verify

check: install-dev
	source .venv/bin/activate && \
	ruff format --check . && \
	ruff check . && \
	pytest && \
	pip-audit -r requirements.txt
	shellcheck scripts/*.sh

clean :
	rm -rf .pytest_cache/ .venv/ test-reports/ .pytest_cache/
