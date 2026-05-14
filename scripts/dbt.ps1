param(
    [Parameter(Position = 0)]
    [ValidateSet('test', 'debug', 'parse', 'deps', 'run', 'build')]
    [string]$Action = 'test'
)

$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $projectRoot '.env'
$profilesDir = Join-Path $projectRoot 'dbt'
$projectDir = $profilesDir

function Set-DotEnvVariables {
    param([string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        throw "Arquivo .env nao encontrado em $Path. Crie-o a partir de .env.example antes de executar o dbt."
    }

    Get-Content -LiteralPath $Path | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith('#')) {
            return
        }

        $parts = $line -split '=', 2
        if ($parts.Count -ne 2) {
            return
        }

        $name = $parts[0].Trim()
        $value = $parts[1].Trim()
        Set-Item -Path "Env:$name" -Value $value
    }
}

function Assert-RequiredVariables {
    $required = @('PG_HOST', 'PG_DATABASE', 'PG_USER', 'PG_PASSWORD', 'PG_PORT')
    $missing = foreach ($variableName in $required) {
        $value = [Environment]::GetEnvironmentVariable($variableName)
        if ([string]::IsNullOrWhiteSpace($value)) {
            $variableName
        }
    }

    if ($missing) {
        throw "Variaveis ausentes para o dbt: $($missing -join ', '). Preencha o .env e tente novamente."
    }
}

function Get-DbtCommand {
    $candidates = @(
        (Join-Path $projectRoot '.conda\Scripts\dbt.exe'),
        (Get-Command dbt -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source -ErrorAction SilentlyContinue)
    ) | Where-Object { $_ }

    foreach ($candidate in $candidates) {
        if (Test-Path -LiteralPath $candidate) {
            return $candidate
        }
    }

    throw "Executavel do dbt nao encontrado. Ative um ambiente com dbt instalado ou instale as dependencias de dev."
}

function Test-LocalPythonSupportsDbt {
    $python = Join-Path $projectRoot '.conda\python.exe'
    if (-not (Test-Path -LiteralPath $python)) {
        return $true
    }

    $version = & $python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
    return ([version]$version -lt [version]'3.14')
}

function Invoke-DbtDocker {
    & docker compose --profile dbt run --rm dbt $Action --project-dir /app/dbt --profiles-dir /app/dbt
}

Set-DotEnvVariables -Path $envFile
Assert-RequiredVariables

if (-not (Test-LocalPythonSupportsDbt)) {
    Write-Host "Python local 3.14+ detectado; executando dbt em container Python 3.12."
    Invoke-DbtDocker
    exit $LASTEXITCODE
}

$dbt = Get-DbtCommand
& $dbt $Action --project-dir $projectDir --profiles-dir $profilesDir
