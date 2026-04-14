#!/usr/bin/env sh

set -eu

ACTION="${1:-up}"

case "$ACTION" in
  build)
    docker compose build app app-s3
    ;;
  up)
    docker compose up --build -d postgres
    ;;
  up-s3)
    docker compose --profile s3 up --build -d app-s3
    ;;
  up-airflow)
    docker compose --profile airflow up --build -d postgres airflow-init airflow-webserver airflow-scheduler
    ;;
  down)
    docker compose --profile s3 --profile admin --profile airflow down
    ;;
  logs)
    docker compose logs -f app
    ;;
  logs-s3)
    docker compose --profile s3 logs -f app-s3
    ;;
  logs-airflow)
    docker compose --profile airflow logs -f airflow-webserver airflow-scheduler
    ;;
  run)
    docker compose run --rm app
    ;;
  run-s3)
    docker compose --profile s3 run --rm app-s3
    ;;
  ps)
    docker compose --profile s3 --profile admin --profile airflow ps
    ;;
  pgadmin)
    docker compose --profile admin up -d pgadmin
    ;;
  *)
    echo "Uso: ./scripts/docker.sh {build|up|up-s3|up-airflow|down|logs|logs-s3|logs-airflow|run|run-s3|ps|pgadmin}" >&2
    exit 1
    ;;
esac
