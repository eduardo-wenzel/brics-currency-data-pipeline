param(
    [Parameter(Position = 0)]
    [ValidateSet('build', 'up', 'down', 'logs', 'run', 'ps', 'pgadmin')]
    [string]$Action = 'up'
)

$ErrorActionPreference = 'Stop'

switch ($Action) {
    'build' {
        docker compose build
        break
    }
    'up' {
        docker compose up --build -d postgres
        break
    }
    'down' {
        docker compose down
        break
    }
    'logs' {
        docker compose logs -f app
        break
    }
    'run' {
        docker compose run --rm app
        break
    }
    'ps' {
        docker compose ps
        break
    }
    'pgadmin' {
        docker compose --profile admin up -d pgadmin
        break
    }
}
