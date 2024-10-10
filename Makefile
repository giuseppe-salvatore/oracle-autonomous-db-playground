SHELL := /bin/bash

prepare: .venv/bin/activate 
	@echo "Prepare"
	python3 -m pip install virtualenv
	python3 -m venv .venv/

install:
	source .venv/bin/activate && \
	python -m pip install --upgrade pip && \
	python -m pip install -r requirements.txt

format: install
	source .venv/bin/activate && \
	for target in `find  -name "*.py" -not -path "./.venv/*"`; do autopep8 -i $$target; done

verify: install
	source .venv/bin/activate && \
	flake8 . --extend-exclude=dist,build,.venv --show-source --statistics

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

.PHONY: clean
clean :
	rm -rf .pytest_cache/ .venv/ test-reports/ .pytest_cache/
