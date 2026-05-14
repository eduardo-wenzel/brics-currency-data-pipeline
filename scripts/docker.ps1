param(
    [Parameter(Position = 0)]
    [ValidateSet('build', 'up', 'up-s3', 'up-airflow', 'down', 'logs', 'logs-s3', 'logs-airflow', 'logs-metabase', 'run', 'run-s3', 'ps', 'pgadmin', 'metabase')]
    [string]$Action = 'up'
)

$ErrorActionPreference = 'Stop'

switch ($Action) {
    'build' {
        docker compose build app app-s3
        break
    }
    'up' {
        docker compose up --build -d postgres
        break
    }
    'up-s3' {
        docker compose --profile s3 up --build -d app-s3
        break
    }
    'up-airflow' {
        docker compose --profile airflow up --build -d postgres airflow-init airflow-webserver airflow-scheduler
        break
    }
    'down' {
        docker compose --profile s3 --profile admin --profile airflow --profile bi down
        break
    }
    'logs' {
        docker compose logs -f app
        break
    }
    'logs-s3' {
        docker compose --profile s3 logs -f app-s3
        break
    }
    'logs-airflow' {
        docker compose --profile airflow logs -f airflow-webserver airflow-scheduler
        break
    }
    'logs-metabase' {
        docker compose --profile bi logs -f metabase
        break
    }
    'run' {
        docker compose run --rm app
        break
    }
    'run-s3' {
        docker compose --profile s3 run --rm app-s3
        break
    }
    'ps' {
        docker compose --profile s3 --profile admin --profile airflow --profile bi ps
        break
    }
    'pgadmin' {
        docker compose --profile admin up -d pgadmin
        break
    }
    'metabase' {
        docker compose --profile bi up -d postgres metabase
        break
    }
}
