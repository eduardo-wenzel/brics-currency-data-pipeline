param(
    [string]$TaskName = "BRICS-Currency-Pipeline",
    [string]$DailyAt = "06:00"
)

$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$runScript = Join-Path $projectRoot "scripts\run_pipeline.ps1"

if (-not (Test-Path $runScript)) {
    throw "Script de execucao nao encontrado em $runScript"
}

$actionArgs = "-NoProfile -ExecutionPolicy Bypass -File `"$runScript`""
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $actionArgs -WorkingDirectory $projectRoot
$trigger = New-ScheduledTaskTrigger -Daily -At $DailyAt

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Description "Executa o pipeline de moedas BRICS diariamente" -Force | Out-Null

Write-Host "Tarefa '$TaskName' registrada para rodar diariamente as $DailyAt."
