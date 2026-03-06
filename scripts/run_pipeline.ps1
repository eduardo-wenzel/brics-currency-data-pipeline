param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
)

$ErrorActionPreference = "Stop"

Set-Location $ProjectRoot

$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
    throw "Python nao encontrado no PATH."
}

if (-not (Test-Path ".env")) {
    Write-Warning "Arquivo .env nao encontrado na raiz do projeto."
}

Write-Host "Executando pipeline em $ProjectRoot"
python src/pipeline.py
exit $LASTEXITCODE
