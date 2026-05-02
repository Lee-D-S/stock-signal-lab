param(
    [string]$Branch = "main"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$logDir = Join-Path $repoRoot ".local"
$logPath = Join-Path $logDir "local_auto_pull.log"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

function Write-Log {
    param([string]$Message)
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') $Message"
    Add-Content -Path $logPath -Value $line -Encoding UTF8
    Write-Output $line
}

Set-Location $repoRoot
Write-Log "start repo=$repoRoot branch=$Branch"

$dirty = git status --porcelain
if ($LASTEXITCODE -ne 0) {
    Write-Log "git status failed"
    exit 1
}

if ($dirty) {
    Write-Log "skip: working tree has local changes"
    $dirty | ForEach-Object { Write-Log "dirty: $_" }
    exit 0
}

git fetch origin $Branch
if ($LASTEXITCODE -ne 0) {
    Write-Log "git fetch failed"
    exit 1
}

$local = git rev-parse HEAD
$remote = git rev-parse "origin/$Branch"
$base = git merge-base HEAD "origin/$Branch"

if ($local -eq $remote) {
    Write-Log "up-to-date: $local"
    exit 0
}

if ($local -eq $base) {
    git merge --ff-only "origin/$Branch"
    if ($LASTEXITCODE -ne 0) {
        Write-Log "fast-forward merge failed"
        exit 1
    }
    $newHead = git rev-parse HEAD
    Write-Log "updated: $local -> $newHead"
    exit 0
}

Write-Log "skip: local branch diverged from origin/$Branch"
Write-Log "local=$local remote=$remote base=$base"
exit 0
