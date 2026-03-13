$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$serverUrl = "http://localhost:3000/api/health"
$serverProcess = $null
$startedServerHere = $false

function Test-ServerReady {
  param(
    [string]$Url
  )

  try {
    $response = Invoke-RestMethod -Uri $Url -TimeoutSec 5
    return [bool]$response.ok
  } catch {
    return $false
  }
}

Set-Location $projectRoot

if (-not (Test-ServerReady -Url $serverUrl)) {
  $stdout = Join-Path $projectRoot "server.out"
  $stderr = Join-Path $projectRoot "server.err"
  $serverProcess = Start-Process -FilePath "node" -ArgumentList "server.js" -WorkingDirectory $projectRoot -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru
  $startedServerHere = $true

  $ready = $false
  for ($i = 0; $i -lt 20; $i++) {
    Start-Sleep -Seconds 2
    if (Test-ServerReady -Url $serverUrl) {
      $ready = $true
      break
    }
  }

  if (-not $ready) {
    if ($serverProcess -and -not $serverProcess.HasExited) {
      Stop-Process -Id $serverProcess.Id -Force
    }
    throw "Не удалось поднять localhost:3000 для daily collect."
  }
}

try {
  & node "scripts/daily-collect-history.js"
  if ($LASTEXITCODE -ne 0) {
    throw "Daily collect завершился с ошибкой."
  }
} finally {
  if ($startedServerHere -and $serverProcess -and -not $serverProcess.HasExited) {
    Stop-Process -Id $serverProcess.Id -Force
  }
}
