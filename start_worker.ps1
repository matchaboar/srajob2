#!/usr/bin/env pwsh

param(
    [switch]$ForceScrapeAll = $false,
    [switch]$UseProd = $false,
    [string]$EnvFile = ""
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

# Avoid hardlink/symlink issues on Windows filesystems when uv manages the venv
if (-not $env:UV_LINK_MODE) {
    $env:UV_LINK_MODE = "copy"
}
$env:UV_NO_PROGRESS = "1"
$env:PATH = "$HOME/.cargo/bin;$env:PATH"
# Prefer a stable Python that has prebuilt wheels (helps tiktoken, xxhash, etc.)
if (-not $env:UV_PYTHON) {
    $env:UV_PYTHON = "3.13"
}
$ProgressPreference = "SilentlyContinue"
if ($PSStyle.PSObject.Properties.Name -contains "Progress" -and $PSStyle.Progress.PSObject.Properties.Name -contains "View") {
    # PowerShell 7.4+ only supports Minimal/Classic; fall back to Minimal if None is unavailable
    $progressViewNames = [enum]::GetNames([System.Management.Automation.ProgressView])
    $targetView = if ($progressViewNames -contains "None") {
        [System.Management.Automation.ProgressView]::None
    } else {
        [System.Management.Automation.ProgressView]::Minimal
    }
    $PSStyle.Progress.View = $targetView
}

function Assert-LastExit([string]$step) {
    if ($LASTEXITCODE -ne 0) {
        throw "$step failed (exit $LASTEXITCODE)"
    }
}

# Tear down a broken venv created on a different platform (e.g., lib64 symlink)
function Reset-StaleVenv {
    $lib64 = Join-Path ".venv" "lib64"
    if (Test-Path $lib64) {
        try {
            Write-Host "Removing stale .venv (lib64 link) to let uv recreate it..."
            Remove-Item -Recurse -Force ".venv" -ErrorAction Stop
        } catch {
            Write-Warning "Failed to remove .venv: $($_.Exception.Message)"
        }
    }
}

function Load-DotEnv($path, [bool]$Override = $false) {
    if (-not (Test-Path $path)) { return }
    Get-Content $path | ForEach-Object {
        if ($_ -match "^\s*#" -or $_.Trim() -eq "") { return }
        if ($_ -match "^\s*([^=]+)=(.*)$") {
            $key = $matches[1].Trim()
            $val = $matches[2]
            $existing = [Environment]::GetEnvironmentVariable($key)
            $shouldSet = $Override -or [string]::IsNullOrEmpty($existing)
            if (-not [string]::IsNullOrEmpty($key) -and $shouldSet) {
                [Environment]::SetEnvironmentVariable($key, $val)
            }
        }
    }
}

function Test-TemporalPort {
    param(
        [string]$TargetHost = "127.0.0.1",
        [int]$Port = 7233,
        [int]$TimeoutMs = 1500
    )
    try {
        $client = New-Object System.Net.Sockets.TcpClient
        $iar = $client.BeginConnect($TargetHost, $Port, $null, $null)
        $connected = $iar.AsyncWaitHandle.WaitOne($TimeoutMs, $false)
        if ($connected -and $client.Connected) {
            $client.EndConnect($iar)
            $client.Close()
            return $true
        }
    } catch {}
    return $false
}

function Start-TemporaliteContainer {
    param(
        [string]$Cmd,
        [bool]$IsPodman,
        [int]$TemporalPort,
        [int]$TemporalUiPort,
        [string]$TemporalContainerName,
        [string]$TemporalImageName,
        [string]$TemporalDockerfile,
        [string]$TemporalDockerContext,
        [string]$TemporalComposeFile
    )

    # Try to start existing container; if it fails (e.g., port already bound), recreate it
    $exists = $false
    try {
        $inspect = & $Cmd inspect $TemporalContainerName 2>&1
        if ($LASTEXITCODE -eq 0) {
            $exists = $true
        }
    } catch {}

    if ($exists) {
        try {
            Write-Host "Container '$TemporalContainerName' already exists. Starting it..."
            & $Cmd start $TemporalContainerName
            Assert-LastExit "Starting temporalite container"
            return
        } catch {
            Write-Warning "Failed to start existing 'temporalite' (likely stale port forward). Recreating..."
            try {
                & $Cmd rm -f $TemporalContainerName
                Assert-LastExit "Removing temporalite container"
            } catch {
                throw "Could not remove stale temporalite container: $($_.Exception.Message)"
            }
        }
    }

    Write-Host "Creating and starting 'temporalite' container..."
    if ($IsPodman) {
        # Build image if missing, then run directly to avoid docker-compose dependency
        podman build -t $TemporalImageName -f $TemporalDockerfile $TemporalDockerContext
        Assert-LastExit "podman build temporal-dev"
        podman run -d --name $TemporalContainerName -p ${TemporalPort}:${TemporalPort} -p ${TemporalUiPort}:${TemporalUiPort} $TemporalImageName
        Assert-LastExit "podman run temporalite"
    } else {
        docker-compose -f $TemporalComposeFile up -d
        Assert-LastExit "docker-compose up"
    }
}

function Start-WorkerProcess {
    param(
        [string]$ErrorLogPath,
        [string]$TemporalAddress,
        [string]$TemporalNamespace
    )

    $env:TEMPORAL_ADDRESS = $TemporalAddress
    $env:TEMPORAL_NAMESPACE = $TemporalNamespace

    $workerArgs = @("run", "python", "-u", "-m", "job_scrape_application.workflows.worker")
    $proc = Start-Process -FilePath "uv" -ArgumentList $workerArgs -NoNewWindow -PassThru -RedirectStandardError $ErrorLogPath
    if (-not $proc) {
        throw "Failed to start worker process."
    }
    $script:WorkerProcId = $proc.Id
    return $proc
}

function Start-WorkerMain {
    $errorLogPath = Join-Path "logs" "worker-errors.log"
    if (-not (Test-Path (Split-Path $errorLogPath -Parent))) {
        New-Item -ItemType Directory -Force -Path (Split-Path $errorLogPath -Parent) | Out-Null
    }
    if (Test-Path $errorLogPath) {
        Remove-Item $errorLogPath -Force -ErrorAction SilentlyContinue
    }

    # Background watcher to surface error count without flooding stdout
    $script:ErrorWatcher = Start-Job -ArgumentList $errorLogPath -ScriptBlock {
        param($logPath)
        $count = 0
        while ($true) {
            if (Test-Path $logPath) {
                $newCount = (Get-Content $logPath -ErrorAction SilentlyContinue | Measure-Object -Line).Lines
                if ($newCount -ne $count) {
                    $count = $newCount
                    Write-Host "ERRORS: $count" -ForegroundColor Red
                }
            }
            Start-Sleep -Seconds 2
        }
    }

    # Core configuration (env overrides respected)
    $envFilePath = if ($EnvFile) {
        $EnvFile
    } elseif ($UseProd -and (Test-Path ".env.production")) {
        ".env.production"
    } else {
        ".env"
    }

    Write-Host "Loading environment from $envFilePath" -ForegroundColor Cyan
    $overrideEnv = $UseProd -or -not [string]::IsNullOrEmpty($EnvFile)
    Load-DotEnv $envFilePath -Override:$overrideEnv

    $TemporalAddress = if ($env:TEMPORAL_ADDRESS) { $env:TEMPORAL_ADDRESS } else { "127.0.0.1:7233" }
    $TemporalNamespace = if ($env:TEMPORAL_NAMESPACE) { $env:TEMPORAL_NAMESPACE } else { "default" }
    $ConvexUrl = $env:CONVEX_HTTP_URL
    $TemporalHost = ($TemporalAddress -split ":")[0]
    $TemporalPort = 7233
    if ($TemporalAddress -match ":(\d+)$") {
        $TemporalPort = [int]$matches[1]
    }
    $TemporalUiPort = 8233
    $TemporalContainerName = "temporalite"
    $TemporalImageName = "temporal-dev:local"
    $TemporalDockerfile = "docker/temporal/Dockerfile.temporal-dev"
    $TemporalDockerContext = "docker/temporal"
    $TemporalComposeFile = "docker/temporal/docker-compose.yml"

    # Check for Podman or Docker
    $cmd = "docker"
    $isPodman = $false

    if (Get-Command "podman" -ErrorAction SilentlyContinue) {
        $cmd = "podman"
        $isPodman = $true
        Write-Host "Podman detected."

        # Check if Podman machine is running (avoid terminating on non-zero exits)
        $podmanInfoExit = 0
        try {
            $podmanInfo = & podman info 2>&1
            $podmanInfoExit = $LASTEXITCODE
        } catch {
            $podmanInfo = $_.Exception.Message
            $podmanInfoExit = 1
        }

        if ($podmanInfoExit -ne 0) {
            Write-Host "Podman machine does not appear to be running. Attempting to start..."

            $podmanStartExit = 0
            try {
                & podman machine start 2>&1
                $podmanStartExit = $LASTEXITCODE
            } catch {
                $podmanStartExit = 1
            }

            if ($podmanStartExit -ne 0) {
                Write-Error "Failed to start podman machine. Please start it manually."
                exit 1
            }

            Write-Host "Waiting for Podman machine to initialize (15s)..."
            Start-Sleep -Seconds 15
        }
    } elseif (Get-Command "docker" -ErrorAction SilentlyContinue) {
        # Check if Docker daemon is running
        $dockerInfo = docker info 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Docker is not running. Please start Docker Desktop and try again."
            exit 1
        }
    } else {
        Write-Error "Neither Docker nor Podman found. Please install one of them."
        exit 1
    }

    # Start Temporal Server (skip if something is already listening on the Temporal port)
    Write-Host "Starting Temporal Server..."
    $temporalListening = Test-TemporalPort -TargetHost "127.0.0.1" -Port $TemporalPort
    if (-not $temporalListening) {
        $temporalListening = Test-TemporalPort -TargetHost "localhost" -Port $TemporalPort
    }

    if ($temporalListening) {
        Write-Host "Port $TemporalPort already reachable; assuming Temporal is running. Skipping container start."
    } else {
        Start-TemporaliteContainer -Cmd $cmd -IsPodman:$isPodman -TemporalPort $TemporalPort -TemporalUiPort $TemporalUiPort -TemporalContainerName $TemporalContainerName -TemporalImageName $TemporalImageName -TemporalDockerfile $TemporalDockerfile -TemporalDockerContext $TemporalDockerContext -TemporalComposeFile $TemporalComposeFile
    }

    # Wait for Temporal Port
    Write-Host "Waiting for Temporal Server to be ready on port $TemporalPort..."
    $maxRetries = 30
    $retryCount = 0
    $connected = $false

    while (-not $connected -and $retryCount -lt $maxRetries) {
        $connected = Test-TemporalPort -TargetHost "127.0.0.1" -Port $TemporalPort -TimeoutMs 1000
        if ($connected) {
            Write-Host "Temporal Server is ready!"
            break
        }
        Write-Host "Waiting for port $TemporalPort... ($($retryCount+1)/$maxRetries)"
        Start-Sleep -Seconds 2
        $retryCount++
    }

    if (-not $connected) {
        Write-Warning "Could not connect to localhost:$TemporalPort. The worker might fail if the server isn't reachable."
        Write-Warning "If using Podman, ensure port forwarding is configured correctly (e.g., 'podman machine set --rootful' or checking port mapping)."
    }

    Reset-StaleVenv

    if ($ForceScrapeAll) {
        if (-not $env:CONVEX_HTTP_URL) {
            Write-Warning "CONVEX_HTTP_URL is not set; cannot reset sites for forced scrape."
        } else {
            Write-Host "Resetting active sites to force a fresh scrape on first run..."
            try {
                Invoke-WebRequest -Method POST -Uri "$($env:CONVEX_HTTP_URL.TrimEnd('/'))/api/sites/reset" -ContentType "application/json" -Body "{}" | Out-Null
                Write-Host "Site reset request sent."
            } catch {
                Write-Warning "Failed to reset sites for forced scrape: $_"
            }
        }
    }

    Write-Host "Ensuring scrape schedule exists (every 5 minutes)..."
    $maxScheduleAttempts = 5
    for ($i = 1; $i -le $maxScheduleAttempts; $i++) {
        uv run python -m job_scrape_application.workflows.create_schedule
        if ($LASTEXITCODE -eq 0) {
            break
        }
        if ($i -lt $maxScheduleAttempts) {
            Write-Warning "Create schedule failed (exit $LASTEXITCODE). Retrying in 4s... [$i/$maxScheduleAttempts]"
            Start-Sleep -Seconds 4
        }
    }
    Assert-LastExit "Create/update Temporal schedule"

    if ($ForceScrapeAll) {
        Write-Host "Triggering schedule once for immediate scrape..."
        uv run python -m job_scrape_application.workflows.trigger_schedule
        Assert-LastExit "Trigger schedule once"
    }

    # Clear any stale progress bars from uv before showing live worker logs
    Clear-Host

    Write-Host "Starting Worker..."
    if ($ConvexUrl) {
        Write-Host "Using CONVEX_HTTP_URL=$ConvexUrl" -ForegroundColor Green
    } else {
        Write-Warning "CONVEX_HTTP_URL is not set. Worker will fail to reach Convex."
    }

    # Spawn worker as a child process so Ctrl+C can force-kill it immediately.
    $script:WorkerProcId = $null
    $cancelSub = Register-EngineEvent -SourceIdentifier ConsoleCancelEvent -Action {
        if ($script:ErrorWatcher) {
            Stop-Job $script:ErrorWatcher -ErrorAction SilentlyContinue | Out-Null
        }
        if ($script:WorkerProcId) {
            Stop-Process -Id $script:WorkerProcId -Force -ErrorAction SilentlyContinue
        }
    }

    $workerProcess = Start-WorkerProcess -ErrorLogPath $errorLogPath -TemporalAddress $TemporalAddress -TemporalNamespace $TemporalNamespace
    Write-Host "Press Ctrl+R to restart the worker instantly." -ForegroundColor Yellow
    try {
        while ($true) {
            if ($workerProcess.HasExited) {
                $exitCode = $workerProcess.ExitCode
                break
            }
            if ([Console]::KeyAvailable) {
                $key = [Console]::ReadKey($true)
                if (($key.Modifiers -band [ConsoleModifiers]::Control) -and $key.Key -eq "R") {
                    Write-Host "Ctrl+R detected: restarting worker..." -ForegroundColor Yellow
                    try {
                        Stop-Process -Id $workerProcess.Id -Force -ErrorAction SilentlyContinue
                        Wait-Process -Id $workerProcess.Id -ErrorAction SilentlyContinue
                    } catch {}
                    $workerProcess = Start-WorkerProcess -ErrorLogPath $errorLogPath -TemporalAddress $TemporalAddress -TemporalNamespace $TemporalNamespace
                    continue
                }
            }
            Start-Sleep -Milliseconds 200
        }
    } finally {
        if ($cancelSub) {
            Unregister-Event -SubscriptionId $cancelSub.Id -ErrorAction SilentlyContinue
        }
    }

    if ($ErrorWatcher) {
        Stop-Job $ErrorWatcher -ErrorAction SilentlyContinue | Out-Null
        Remove-Job $ErrorWatcher -Force | Out-Null
    }

    if ($exitCode -ne 0) {
        throw "Worker exited unexpectedly (exit $exitCode). See $errorLogPath for details."
    }
}

if ($env:SKIP_START_WORKER_MAIN -ne "1") {
    Start-WorkerMain
}
