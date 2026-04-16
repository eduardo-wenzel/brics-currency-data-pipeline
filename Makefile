SHELL := /bin/sh

.PHONY: up up-s3 up-airflow down logs logs-s3 logs-airflow run run-s3 ps pgadmin pipeline test lint dbt-test dbt-debug dbt-run dbt-build

up:
	./scripts/docker.sh up

up-s3:
	./scripts/docker.sh up-s3

up-airflow:
	./scripts/docker.sh up-airflow

down:
	./scripts/docker.sh down

logs:
	./scripts/docker.sh logs

logs-s3:
	./scripts/docker.sh logs-s3

logs-airflow:
	./scripts/docker.sh logs-airflow

run:
	./scripts/docker.sh run

run-s3:
	./scripts/docker.sh run-s3

ps:
	./scripts/docker.sh ps

pgadmin:
	./scripts/docker.sh pgadmin

pipeline:
	./scripts/run_pipeline.sh

test:
	.conda/python.exe -m pytest

lint:
	.conda/python.exe -m ruff check .

dbt-test:
	./scripts/dbt.sh test

dbt-debug:
	./scripts/dbt.sh debug

dbt-run:
	./scripts/dbt.sh run

dbt-build:
	./scripts/dbt.sh build
