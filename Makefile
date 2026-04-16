SHELL := /bin/sh

DEFAULT_GOAL := help

.PHONY: help up up-s3 up-airflow down logs logs-s3 logs-airflow run run-s3 ps pgadmin pipeline test lint dbt-test dbt-debug dbt-run dbt-build

help:
	@printf "%s\n" \
	"Targets disponiveis:" \
	"  make up            # sobe postgres" \
	"  make run           # executa o pipeline no container app" \
	"  make up-s3         # sobe o modo S3-only" \
	"  make up-airflow    # sobe a stack do Airflow" \
	"  make logs          # acompanha logs do app" \
	"  make down          # derruba os servicos" \
	"  make pipeline      # executa o pipeline localmente" \
	"  make test          # roda pytest" \
	"  make lint          # roda ruff" \
	"  make dbt-test      # roda dbt test"

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
