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
        $envItem = Get-Item -Path "Env:$variableName" -ErrorAction SilentlyContinue
        if (-not $envItem -or [string]::IsNullOrWhiteSpace($envItem.Value)) {
            $variableName
        }
    }

    if ($missing) {
        throw "Variaveis ausentes para o dbt: $($missing -join ', '). Preencha o .env e tente novamente."
    }
}

function Get-DbtCommand {
    $candidates = @(
        (Get-Command dbt -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source -ErrorAction SilentlyContinue),
        (Join-Path $projectRoot '.conda\Scripts\dbt.exe')
    ) | Where-Object { $_ }

    foreach ($candidate in $candidates) {
        if (Test-Path -LiteralPath $candidate) {
            return $candidate
        }
    }

    throw "Executavel do dbt nao encontrado. Ative um ambiente com dbt instalado ou instale as dependencias de dev."
}

Set-DotEnvVariables -Path $envFile
Assert-RequiredVariables

$dbt = Get-DbtCommand
& $dbt $Action --project-dir $projectDir --profiles-dir $profilesDir
