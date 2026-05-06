$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$pullScript = Join-Path $PSScriptRoot "local_auto_pull.ps1"
$taskName = "AutoInvest Local Pull"

$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$pullScript`""

$morningTrigger = New-ScheduledTaskTrigger -Daily -At 11:30
$afternoonTrigger = New-ScheduledTaskTrigger -Daily -At 22:00

$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 20)

$userId = "$env:USERDOMAIN\$env:USERNAME"
$principal = New-ScheduledTaskPrincipal -UserId $userId -LogonType Interactive -RunLevel Limited

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger @($morningTrigger, $afternoonTrigger) `
    -Settings $settings `
    -Principal $principal `
    -Description "Pull latest GitHub Actions outputs for auto-invest at 11:30 and 22:00 every day." `
    -Force | Out-Null

Write-Output "registered task: $taskName"
Write-Output "repo: $repoRoot"
Write-Output "script: $pullScript"
Write-Output "schedule: every day 11:30, 22:00"
