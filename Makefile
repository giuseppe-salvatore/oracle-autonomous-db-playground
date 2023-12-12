SHELL := /bin/bash

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

.PHONY: clean
clean :
	rm -rf .pytest_cache/ .venv/ test-reports/ .pytest_cache/
	rm .tmp/*