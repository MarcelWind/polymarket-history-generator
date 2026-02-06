param(
    [string]$DestDir = ".\data"
)

Set-StrictMode -Version Latest

# Default connection settings (can be overridden with env vars)
$Port = if ($env:PORT) { [int]$env:PORT } else { 49152 }
$User = if ($env:USER) { $env:USER } else { 'aegir' }
$HostName = if ($env:HOST) { $env:HOST } else { '141.227.131.249' }
$RemoteDir = if ($env:REMOTE_DIR) { $env:REMOTE_DIR } else { '/home/restricted/polybot/polymarket-history-generator/data' }

function Show-Usage {
        @"
Usage: .\scripts\fetch_results.ps1 [-DestDir <path>]

Copies the remote directory:
    ${User}@${HostName}:${RemoteDir}

to the local destination directory (default: .\data).

You can override connection settings with environment variables:
    USER, HOST, PORT, REMOTE_DIR

Examples:
    .\scripts\fetch_results.ps1
    .\scripts\fetch_results.ps1 -DestDir C:\temp\results
    # Override inline in PowerShell:
    $env:USER='alice'; $env:HOST='1.2.3.4'; $env:PORT='2222'; $env:REMOTE_DIR='/path/to/data'; .\scripts\fetch_results.ps1 -DestDir C:\tmp\results
"@
}

if ($args -contains '-h' -or $args -contains '--help') {
    Show-Usage
    exit 0
}

# Ensure destination exists
if (-not (Test-Path -LiteralPath $DestDir)) {
    New-Item -ItemType Directory -Path $DestDir -Force | Out-Null
}

Write-Host "Copying from ${User}@${HostName}:${RemoteDir} -> ${DestDir}"

# Helper to run a command and return success
function Run-Command([string[]]$cmd) {
    try {
        & $cmd[0] @($cmd[1..($cmd.Length-1)])
        return $true
    } catch {
        return $false
    }
}

# Try rsync
$rsync = Get-Command rsync -ErrorAction SilentlyContinue
if ($rsync) {
    Write-Host "Using rsync over ssh (port $Port)"
    $args = @('-avz', '-e', "ssh -p $Port", "$($User)@$($HostName):$RemoteDir/", "$DestDir/")
    & rsync @args
    Write-Host 'Done.'
    exit 0
}

# Try scp (OpenSSH)
$scp = Get-Command scp -ErrorAction SilentlyContinue
if ($scp) {
    Write-Host "Using scp (OpenSSH) on port $Port"
    & scp -r -P $Port "$($User)@$($HostName):$RemoteDir" "$DestDir"
    Write-Host 'Done.'
    exit 0
}

# Try pscp (PuTTY)
$pscp = Get-Command pscp -ErrorAction SilentlyContinue
if ($pscp) {
    Write-Host "Using pscp (PuTTY) on port $Port"
    & pscp -r -P $Port "$($User)@$($HostName):$RemoteDir" "$DestDir"
    Write-Host 'Done.'
    exit 0
}

Write-Host "No suitable copy tool (rsync, scp, or pscp) found on PATH." -ForegroundColor Yellow
Write-Host "Install OpenSSH (scp), rsync (e.g., via WSL or msys), or PuTTY (pscp) and try again." -ForegroundColor Yellow
Show-Usage
exit 1
