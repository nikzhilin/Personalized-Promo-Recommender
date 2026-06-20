.PHONY: install-dev lint test validate-data

PYTHON ?= python3
DATA_DIR ?= data/raw
FEATURE_CUTOFF ?= 2019-03-01T00:00:00

install-dev:
	$(PYTHON) -m pip install -e '.[dev]'

lint:
	$(PYTHON) -m ruff check .

test:
	$(PYTHON) -m pytest

validate-data:
	$(PYTHON) -m spark_jobs.validate_raw_data \
		--data-dir $(DATA_DIR) \
		--feature-cutoff $(FEATURE_CUTOFF)

