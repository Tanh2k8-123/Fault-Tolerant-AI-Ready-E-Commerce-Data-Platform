.DEFAULT_GOAL := up
PYTHON ?= python
COMPOSE ?= docker compose
DATA_DIR ?= data
ROWS ?= 10000
REVIEWS ?= 1000
SEED ?= 42
SHARD_SIZE ?= 250000

.PHONY: up down gen-small upload dbt-build backfill benchmark test test-py lint

up:
	$(COMPOSE) up --build --detach --wait

down:
	$(COMPOSE) down

gen-small:
	$(PYTHON) src/ingestion/chaos_generator.py --rows $(ROWS) --reviews $(REVIEWS) --seed $(SEED) --shard-size $(SHARD_SIZE) --output "$(DATA_DIR)/raw"

# Upload and dbt commands use the project .env with the active Python environment.
upload:
	$(PYTHON) -m dotenv run -- $(PYTHON) src/ingestion/upload_to_minio.py --source-dir "$(DATA_DIR)"

dbt-build:
	$(PYTHON) -m dotenv run -- dbt build --project-dir dbt_project --profiles-dir dbt_project

# Example: make backfill START_DATE=2026-01-01 END_DATE=2026-01-03
backfill:
	$(if $(strip $(START_DATE)),,$(error Set START_DATE=YYYY-MM-DD))
	$(if $(strip $(END_DATE)),,$(error Set END_DATE=YYYY-MM-DD))
	$(COMPOSE) exec -T airflow airflow dags backfill ecommerce_daily --start-date "$(START_DATE)" --end-date "$(END_DATE)" --rerun-failed-tasks

test: test-py

REPORT_MONTH ?= 2026-01-01
benchmark:
	$(PYTHON) benchmarks/run_benchmark.py --month "$(REPORT_MONTH)"

test-py:
	$(PYTHON) -m pytest -q tests/

lint:
	$(PYTHON) -m ruff check .
ifneq ($(wildcard dbt_project/models),)
	$(PYTHON) -m sqlfluff lint --config dbt_project/.sqlfluff dbt_project/models
endif
