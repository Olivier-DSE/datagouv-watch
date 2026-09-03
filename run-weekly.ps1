# Runs the weekly data.gouv.fr watch locally (data.gouv.fr blocks Anthropic's
# cloud sandbox IPs mid-TLS-handshake, so the download can't happen from the
# cloud routine). This script only downloads, classifies, and commits the
# digest to GitHub; the cloud routine "datagouv-watch-weekly" picks it up
# shortly after and pushes it into the dashboard's database.
#
# Invoked by a Windows Scheduled Task; safe to run manually too.

$ErrorActionPreference = "Stop"

$repoDir = "C:\Users\PERFORM2235\datagouv-watch"
$logDir = Join-Path $repoDir "run-logs"

Set-Location $repoDir

if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
}
$timestamp = Get-Date -Format "yyyy-MM-dd_HHmmss"
$logFile = Join-Path $logDir "run_$timestamp.log"

function Log($msg) {
    $msg | Tee-Object -FilePath $logFile -Append
}

Log "=== Weekly data.gouv.fr watch: starting $timestamp ==="

try {
    Log "--- git pull ---"
    git pull --ff-only 2>&1 | ForEach-Object { Log $_ }

    Log "--- running pipeline.py ---"
    python pipeline.py --download --digest-json digest_latest.json 2>&1 | ForEach-Object { Log $_ }
    if ($LASTEXITCODE -ne 0) {
        throw "pipeline.py exited with code $LASTEXITCODE"
    }

    Log "--- committing digest_latest.json ---"
    git add digest_latest.json
    $status = git status --porcelain digest_latest.json
    if ($status) {
        git commit -m "Weekly digest $timestamp"
        git push
        Log "Pushed updated digest_latest.json."
    } else {
        Log "digest_latest.json unchanged, nothing to commit."
    }

    Log "=== Finished OK: $(Get-Date -Format 'yyyy-MM-dd_HHmmss') ==="
} catch {
    Log "!!! FAILED: $($_.Exception.Message)"
    Log "No commit/push made for a failed run — GitHub still holds the last good digest."
    exit 1
}
