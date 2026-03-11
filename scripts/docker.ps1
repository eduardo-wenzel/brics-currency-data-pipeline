param(
    [Parameter(Position = 0)]
    [ValidateSet('build', 'up', 'up-s3', 'down', 'logs', 'logs-s3', 'run', 'run-s3', 'ps', 'pgadmin')]
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
    'down' {
        docker compose --profile s3 --profile admin down
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
    'run' {
        docker compose run --rm app
        break
    }
    'run-s3' {
        docker compose --profile s3 run --rm app-s3
        break
    }
    'ps' {
        docker compose --profile s3 --profile admin ps
        break
    }
    'pgadmin' {
        docker compose --profile admin up -d pgadmin
        break
    }
}
