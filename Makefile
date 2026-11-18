SHELL := /bin/bash

prepare:
	@echo "Prepare"
	@if ! command -v python3 >/dev/null 2>&1; then \
		echo "python3 is required but not installed."; \
		exit 1; \
	fi
	@if ! python3 -c "import venv" >/dev/null 2>&1; then \
		if ! command -v apt-get >/dev/null 2>&1; then \
			echo "python3-venv is missing and apt-get is unavailable."; \
			echo "Install python3-venv manually and re-run make prepare."; \
			exit 1; \
		fi; \
		echo "python3-venv is missing. Installing with apt-get..."; \
		if [ "$$(id -u)" -eq 0 ]; then \
			apt-get update && apt-get install -y python3-venv; \
		elif command -v sudo >/dev/null 2>&1; then \
			sudo apt-get update && sudo apt-get install -y python3-venv; \
		else \
			echo "Need root or sudo to install python3-venv via apt-get."; \
			exit 1; \
		fi; \
	fi
	python3 -m venv .venv/

install:
	source .venv/bin/activate && \
	python -m pip install --upgrade pip && \
	python -m pip install --require-hashes -r requirements.txt

install-dev: install
	source .venv/bin/activate && \
	python -m pip install --require-hashes -r requirements-dev.txt

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

install-quiet:
	@$(MAKE) --no-print-directory install >/dev/null

run-bar-count : install-quiet
	bash scripts/run_count_minute_bars.sh OracleDB SPY

run-bar-count-local : install-quiet
	bash scripts/run_count_minute_bars.sh SQLite3DB SPY

execute-migration-to-oracle : install
	source .venv/bin/activate && \
	source .env.local && \
	export SQLITE_DB_FILE=/home/gr4ce/workspace/personal/algo-trading/data/stock_data.alpaca.db && \
	python -m migrate_to_oracle

.PHONY: audit check clean format install install-dev install-quiet shellcheck test verify

check: install-dev
	source .venv/bin/activate && \
	ruff format --check . && \
	ruff check . && \
	pytest && \
	pip-audit -r requirements.txt
	shellcheck scripts/*.sh

clean :
	rm -rf .pytest_cache/ .venv/ test-reports/ .pytest_cache/
