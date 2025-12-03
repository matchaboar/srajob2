#!/usr/bin/env pwsh

param(
    [switch]$ForceScrapeAll = $false,
    [switch]$UseProd = $false,
    [string]$EnvFile = ""
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$script:TemporalContainerStartedByScript = $false
$script:TemporalContainerName = ""
$script:TemporalCmd = ""
$script:TemporalUsingPodman = $false
$script:CancelRequested = $false
$script:WorkerProcess = $null
$script:ShutdownStopwatch = $null
$script:ShutdownHandled = $false
$script:ErrorWatcher = $null
$script:ErrorWatcherCount = 0

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

function Invoke-LoggedCommand {
    param(
        [string]$StepName,
        [scriptblock]$Action,
        [int]$TimeoutSeconds = 10
    )

    if (-not $Action) {
        throw "No command specified for $StepName"
    }

    $scriptText = ($Action.ToString()).Trim()
    Write-Host ("[preflight] {0} starting (timeout={1}s)" -f $StepName, $TimeoutSeconds) -ForegroundColor Cyan
    Write-Host ("[preflight] {0} command: {1}" -f $StepName, $scriptText) -ForegroundColor DarkGray
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $capturedLines = New-Object System.Collections.Generic.List[string]

    $job = Start-Job -ScriptBlock {
        param($cmdText)
        $ErrorActionPreference = "Continue"
        $PSNativeCommandUseErrorActionPreference = $false
        $sb = [scriptblock]::Create($cmdText)
        $exception = $null
        try {
            & $sb
        } catch {
            $exception = $_
        }
        $code = if ($LASTEXITCODE -ne $null) { $LASTEXITCODE } else { 0 }
        return @{
            "__exitCode" = $code
            "__exception" = $exception
        }
    } -ArgumentList $scriptText

    $completed = Wait-Job -Job $job -Timeout $TimeoutSeconds
    if (-not $completed) {
        try { Stop-Job -Job $job -Force | Out-Null } catch {}
        try { Remove-Job -Job $job -Force -ErrorAction SilentlyContinue | Out-Null } catch {}
        throw "[preflight] $StepName timed out after ${TimeoutSeconds}s"
    }

    $jobErrors = @()
    $priorEap = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = Receive-Job -Job $job -Keep -ErrorAction Continue -ErrorVariable jobErrors
    } finally {
        $ErrorActionPreference = $priorEap
    }
    $state = $job.State
    $childJob = $null
    if ($job.ChildJobs.Count -gt 0) {
        $childJob = $job.ChildJobs[0]
    }
    $reason = if ($childJob) { $childJob.JobStateInfo.Reason } else { $null }
    try { Remove-Job -Job $job -Force -ErrorAction SilentlyContinue | Out-Null } catch {}

    $exitCode = 0
    $exceptionRecord = $null
    foreach ($line in $output) {
        if ($line -is [hashtable] -and $line.ContainsKey("__exitCode")) {
            $exitCode = [int]$line["__exitCode"]
            if ($line.ContainsKey("__exception") -and $line["__exception"]) {
                $exceptionRecord = $line["__exception"]
            }
            continue
        }
        if ($null -ne $line) {
            $lineText = [string]$line
            $capturedLines.Add($lineText) | Out-Null
            Write-Host ("[preflight][{0}] {1}" -f $StepName, $lineText)
        }
    }
    foreach ($err in $jobErrors) {
        if ($null -eq $err) { continue }
        $errText = $err.ToString()
        if ($errText) {
            $capturedLines.Add($errText) | Out-Null
            Write-Host ("[preflight][{0}] {1}" -f $StepName, $errText) -ForegroundColor Red
        }
    }

    $stopwatch.Stop()
    $duration = [math]::Round($stopwatch.Elapsed.TotalSeconds, 2)

    if ($exceptionRecord) {
        $excMessage = if ($exceptionRecord.Exception) { $exceptionRecord.Exception.Message } else { $exceptionRecord.ToString() }
        Write-Host "[preflight] $StepName raised an exception: $excMessage" -ForegroundColor Red
        try {
            $exceptionText = $exceptionRecord | Out-String
            if ($exceptionText) {
                Write-Host "[preflight] $StepName exception details:" -ForegroundColor DarkRed
                Write-Host $exceptionText
            }
        } catch {}
        if ($capturedLines.Count -gt 0) {
            Write-Host "[preflight] $StepName output before failure:" -ForegroundColor Yellow
            foreach ($l in $capturedLines) { Write-Host ("[preflight][{0}] {1}" -f $StepName, $l) }
        }
        throw "[preflight] $StepName failed: $excMessage"
    }
    if ($reason) {
        Write-Host "[preflight] $StepName failed with exception: $($reason.Message)" -ForegroundColor Red
        if ($capturedLines.Count -gt 0) {
            Write-Host "[preflight] $StepName output before failure:" -ForegroundColor Yellow
            foreach ($l in $capturedLines) { Write-Host ("[preflight][{0}] {1}" -f $StepName, $l) }
        }
        throw "[preflight] $StepName failed: $($reason.Message)"
    }
    if ($state -ne "Completed") {
        Write-Host "[preflight] $StepName did not complete (state $state)" -ForegroundColor Red
        if ($capturedLines.Count -eq 0) {
            Write-Host "[preflight] $StepName had no captured output." -ForegroundColor Yellow
        }
        throw "[preflight] $StepName did not complete (state $state)"
    }
    if ($exitCode -ne 0) {
        if ($capturedLines.Count -eq 0) {
            Write-Host "[preflight] $StepName produced no output before failing." -ForegroundColor Yellow
        } else {
            Write-Host "[preflight] $StepName output before failure:" -ForegroundColor Yellow
            foreach ($l in $capturedLines) { Write-Host ("[preflight][{0}] {1}" -f $StepName, $l) }
        }
        throw "[preflight] $StepName exited with code $exitCode (duration ${duration}s)"
    }

    return @{
        Duration = $duration
        ExitCode = $exitCode
        Output = $capturedLines
    }
}

function Start-ErrorWatcher([string]$LogPath) {
    Stop-ErrorWatcher
    if (-not $LogPath) { return }
    $logDir = Split-Path -Parent $LogPath
    if (-not $logDir) { $logDir = "." }
    if (-not (Test-Path $logDir)) {
        New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    }

    $fullPath = $LogPath
    try {
        $resolvedDir = Resolve-Path -LiteralPath $logDir -ErrorAction Stop
        $fullPath = Join-Path $resolvedDir.ProviderPath (Split-Path -Leaf $LogPath)
    } catch {}

    $script:ErrorWatcherCount = 0
    $script:ErrorWatcher = [pscustomobject]@{
        Path = $fullPath
        LastLength = -1
        LastWrite = [datetime]::MinValue
        Count = 0
        IntervalMs = 750
        Pattern = "(?i)(\berror\b|exception|traceback|critical|fatal)"
        Stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    }

    Invoke-ErrorWatcherTick -Force
}

function Invoke-ErrorWatcherTick {
    param(
        [switch]$Force
    )

    if (-not $script:ErrorWatcher) { return }
    $state = $script:ErrorWatcher
    if (-not $state.Stopwatch) { return }

    if (-not $Force -and $state.Stopwatch.ElapsedMilliseconds -lt $state.IntervalMs) {
        return
    }

    $state.Stopwatch.Restart()

    try {
        if (-not (Test-Path -LiteralPath $state.Path)) { return }
        $info = Get-Item -LiteralPath $state.Path -ErrorAction SilentlyContinue
        if (-not $info) { return }
        if (-not $Force -and $info.Length -eq $state.LastLength -and $info.LastWriteTime -eq $state.LastWrite) {
            return
        }

        $state.LastLength = $info.Length
        $state.LastWrite = $info.LastWriteTime
        $newCount = (Get-Content -LiteralPath $state.Path -ErrorAction SilentlyContinue | Where-Object { $_ -match $state.Pattern } | Measure-Object).Count
        if ($newCount -ne $state.Count) {
            $state.Count = $newCount
            $script:ErrorWatcherCount = $newCount
            Write-Host ("ERRORS: {0}" -f $newCount) -ForegroundColor Red
        }
    } catch {}
}

function Stop-ErrorWatcher {
    if (-not $script:ErrorWatcher) { return }
    $script:ErrorWatcherCount = 0
    $script:ErrorWatcher = $null
}

function Run-PreflightChecks {
    if ($env:SKIP_PREFLIGHT_CHECKS -eq "1") {
        Write-Host "[preflight] SKIP_PREFLIGHT_CHECKS=1 set; skipping preflight checks." -ForegroundColor Yellow
        return
    }

    Write-Host "=== Running preflight checks ===" -ForegroundColor Cyan
    Reset-StaleVenv

    $steps = @(
        @{ Name = "ruff"; Timeout = 10; Block = { uvx ruff check job_scrape_application } },
        @{ Name = "pytest"; Timeout = 30; Block = { uv run pytest } }
    )

    Write-Host "[preflight] Pending checks:" -ForegroundColor DarkGray
    foreach ($step in $steps) {
        Write-Host ("[ ] {0}" -f $step.Name) -ForegroundColor DarkGray
    }

    foreach ($step in $steps) {
        Write-Host ("[ ] {0} (running...)" -f $step.Name) -ForegroundColor Yellow
        try {
            $result = Invoke-LoggedCommand -StepName $step.Name -TimeoutSeconds $step.Timeout -Action $step.Block
            $duration = if ($result -and $result.ContainsKey("Duration")) { $result["Duration"] } else { $null }
            if ($duration -ne $null) {
                Write-Host ("[x] {0} passed in {1}s" -f $step.Name, $duration) -ForegroundColor Green
            } else {
                Write-Host ("[x] {0} passed" -f $step.Name) -ForegroundColor Green
            }
        } catch {
            Write-Host ("[!] {0} failed: {1}" -f $step.Name, $_.Exception.Message) -ForegroundColor Red
            throw
        }
    }

    Write-Host "[x] All preflight checks passed" -ForegroundColor Green
    Write-Host "=== Preflight checks completed ===" -ForegroundColor Cyan
}

function Load-DotEnv($path, [bool]$Override = $false, [hashtable]$SourceMap = $null) {
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
                if ($SourceMap) {
                    $SourceMap[$key] = $path
                }
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
            return $true
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

    return $true
}

function Stop-TemporalContainer {
    param(
        [string]$Cmd,
        [string]$Name,
        [bool]$IsPodman = $false
    )

    if (-not $Cmd -or -not $Name) { return }

    $containerTool = if ($IsPodman) { "podman" } else { $Cmd }
    Write-Host "[shutdown] Requesting stop for container '$Name' via $containerTool..." -ForegroundColor Yellow

    $stopExit = $null
    $stopOutput = ""
    try {
        $stopOutput = & $Cmd stop $Name 2>&1
        $stopExit = $LASTEXITCODE
    } catch {
        $stopExit = 1
        $stopOutput = $_.Exception.Message
    }
    if ($stopExit -eq 0) {
        Write-Host "[shutdown] Container '$Name' stopped via $containerTool." -ForegroundColor Yellow
    } else {
        Write-Warning "[shutdown] $containerTool stop failed (exit $stopExit): $stopOutput"
    }

    $rmExit = $null
    $rmOutput = ""
    try {
        $rmOutput = & $Cmd rm -f $Name 2>&1
        $rmExit = $LASTEXITCODE
    } catch {
        $rmExit = 1
        $rmOutput = $_.Exception.Message
    }
    if ($rmExit -eq 0) {
        Write-Host "[shutdown] Container '$Name' removed via $containerTool." -ForegroundColor Yellow
    } else {
        Write-Warning "[shutdown] $containerTool rm failed (exit $rmExit): $rmOutput"
    }
}

function Stop-WorkerAndContainer {
    param(
        $WorkerProcess,
        [switch]$SkipContainer = $false,
        [System.Diagnostics.Stopwatch]$Timer = $null,
        [string]$Reason = "shutdown"
    )

    if ($script:ShutdownHandled) {
        if ($Timer -and $Timer.IsRunning) {
            $Timer.Stop()
            Write-Host ("[shutdown] Shutdown timer already handled; elapsed {0}s" -f [math]::Round($Timer.Elapsed.TotalSeconds, 2)) -ForegroundColor Yellow
        }
        return
    }

    if ($Timer -and -not $Timer.IsRunning) {
        $Timer.Start()
    }

    $reasonText = if ($Reason) { " (reason=$Reason)" } else { "" }
    Write-Host "[shutdown] Stopping worker and related resources...$reasonText" -ForegroundColor Yellow

    if ($script:ErrorWatcher) {
        Write-Host "[shutdown] Stopping error watcher" -ForegroundColor Yellow
        Stop-ErrorWatcher
    }

    if ($WorkerProcess -and -not $WorkerProcess.HasExited) {
        Write-Host "[shutdown] Killing worker process pid=$($WorkerProcess.Id)" -ForegroundColor Yellow
        try { Stop-Process -Id $WorkerProcess.Id -Force -ErrorAction SilentlyContinue } catch {}
        try {
            Wait-Process -Id $WorkerProcess.Id -Timeout 5 -ErrorAction SilentlyContinue
        } catch {}
        if (-not $WorkerProcess.HasExited) {
            Write-Warning "[shutdown] Worker still running after kill attempt; stopping any detected worker children."
        }
        Stop-ExistingWorkers
    }

    if (-not $SkipContainer -and $script:TemporalContainerStartedByScript) {
        Write-Host "[shutdown] Stopping temporal container $($script:TemporalContainerName) via $($script:TemporalCmd)" -ForegroundColor Yellow
        Stop-TemporalContainer -Cmd $script:TemporalCmd -Name $script:TemporalContainerName -IsPodman:$script:TemporalUsingPodman
        $script:TemporalContainerStartedByScript = $false
    } elseif (-not $SkipContainer -and $script:TemporalCmd -and $script:TemporalContainerName) {
        Write-Host "[shutdown] Temporal container stop skipped (not started by this script)." -ForegroundColor Yellow
    }

    if ($Timer) {
        $Timer.Stop()
        Write-Host ("[shutdown] Shutdown duration: {0}s" -f [math]::Round($Timer.Elapsed.TotalSeconds, 2)) -ForegroundColor Yellow
    }

    $script:ShutdownHandled = $true
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

function Stop-ExistingWorkers {
    try {
        if ($IsWindows) {
            $procs = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
                $_.CommandLine -match "job_scrape_application\.workflows\.worker"
            }
            foreach ($p in $procs) {
                try {
                    Write-Host "[preflight] Stopping stale worker pid=$($p.ProcessId)" -ForegroundColor Yellow
                    Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
                } catch {
                    Write-Warning "Failed to stop stale worker pid=$($p.ProcessId): $($_.Exception.Message)"
                }
            }
            return
        }

        $psPath = "/bin/ps"
        if (-not (Test-Path $psPath)) { $psPath = "/usr/bin/ps" }
        if (-not (Test-Path $psPath)) { return }

        $psOutput = & $psPath -eo pid,args 2>$null
        foreach ($line in $psOutput) {
            if ($line -match "^\s*(\d+)\s+.*job_scrape_application\.workflows\.worker") {
                $pid = [int]$matches[1]
                try {
                    Write-Host "[preflight] Stopping stale worker pid=$pid" -ForegroundColor Yellow
                    Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
                } catch {
                    Write-Warning "Failed to stop stale worker pid=$($pid): $($_.Exception.Message)"
                }
            }
        }
    } catch {
        Write-Warning "Unable to enumerate existing workers: $($_.Exception.Message)"
    }
}

function Start-WorkerMain {
    $errorLogPath = Join-Path "logs" "worker-errors.log"
    if (-not (Test-Path (Split-Path $errorLogPath -Parent))) {
        New-Item -ItemType Directory -Force -Path (Split-Path $errorLogPath -Parent) | Out-Null
    }
    if (Test-Path $errorLogPath) {
        Remove-Item $errorLogPath -Force -ErrorAction SilentlyContinue
    }

    # Lightweight watcher to surface error count without flooding stdout
    Start-ErrorWatcher -LogPath $errorLogPath

    # Core configuration (env overrides respected)
    $envSourceMap = @{}
    $envLoadOrder = @()
    $environmentLabel = if ($UseProd) { "Production" } else { "Development" }

    $resolveEnvPath = {
        param([string]$relativePath)
        $wdCandidate = Join-Path (Get-Location) $relativePath
        if (Test-Path $wdCandidate) {
            return (Resolve-Path $wdCandidate).ProviderPath
        }
        if ($PSScriptRoot) {
            $scriptCandidate = Join-Path $PSScriptRoot $relativePath
            if (Test-Path $scriptCandidate) {
                return (Resolve-Path $scriptCandidate).ProviderPath
            }
        }
        return $wdCandidate
    }

    $defaultEnvPath = & $resolveEnvPath ".env"
    $prodEnvPath = & $resolveEnvPath "job_board_application/.env.production"

    if ($EnvFile) {
        $environmentLabel = "Custom"
        $envLoadOrder += @{ Path = (& $resolveEnvPath $EnvFile); Override = $true; Label = "Custom env file" }
    } elseif ($UseProd) {
        $envLoadOrder += @{ Path = $defaultEnvPath; Override = $false; Label = "Development defaults (.env)" }
        if (Test-Path $prodEnvPath) {
            $envLoadOrder += @{ Path = $prodEnvPath; Override = $true; Label = "Production overrides (job_board_application/.env.production)" }
        } else {
            Write-Warning "Production env file not found at $prodEnvPath; falling back to .env for missing keys."
        }
    } else {
        $envLoadOrder += @{ Path = $defaultEnvPath; Override = $false; Label = "Development (.env)" }
    }

    Write-Host ("Environment mode: {0}" -f $environmentLabel) -ForegroundColor Cyan
    foreach ($envEntry in $envLoadOrder) {
        if (-not (Test-Path $envEntry.Path)) {
            Write-Warning ("Env file not found: {0}" -f $envEntry.Path)
            continue
        }
        Write-Host ("Loading {0}: {1}" -f $envEntry.Label, $envEntry.Path) -ForegroundColor DarkCyan
        Load-DotEnv $envEntry.Path -Override:$envEntry.Override -SourceMap:$envSourceMap
    }

    $TemporalAddress = if ($env:TEMPORAL_ADDRESS) { $env:TEMPORAL_ADDRESS } else { "127.0.0.1:7233" }
    $TemporalNamespace = if ($env:TEMPORAL_NAMESPACE) { $env:TEMPORAL_NAMESPACE } else { "default" }
    $ConvexUrl = $env:CONVEX_HTTP_URL
    $convexSourcePath = if ($envSourceMap.ContainsKey("CONVEX_HTTP_URL")) { $envSourceMap["CONVEX_HTTP_URL"] } else { "existing environment" }
    $convexSourceLabel = switch ($convexSourcePath) {
        { $_ -eq $prodEnvPath } { "production env ($convexSourcePath)" ; break }
        { $_ -eq $defaultEnvPath } { "development env ($convexSourcePath)" ; break }
        default { $convexSourcePath }
    }
    if ($ConvexUrl) {
        $convexColor = if ($UseProd -and $convexSourcePath -eq $prodEnvPath) { "Green" } elseif ($UseProd) { "Yellow" } else { "Green" }
        Write-Host ("CONVEX_HTTP_URL from {0}: {1}" -f $convexSourceLabel, $ConvexUrl) -ForegroundColor $convexColor
    } else {
        Write-Host "CONVEX_HTTP_URL is not set after loading environment files." -ForegroundColor Red
    }

    # Ensure any old worker processes from previous runs are terminated
    Stop-ExistingWorkers

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

    Write-Host "[preflight] Running checks before starting services..." -ForegroundColor Cyan
    Run-PreflightChecks

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
                $started = Start-TemporaliteContainer -Cmd $cmd -IsPodman:$isPodman -TemporalPort $TemporalPort -TemporalUiPort $TemporalUiPort -TemporalContainerName $TemporalContainerName -TemporalImageName $TemporalImageName -TemporalDockerfile $TemporalDockerfile -TemporalDockerContext $TemporalDockerContext -TemporalComposeFile $TemporalComposeFile
                if ($started) {
                    $script:TemporalContainerStartedByScript = $true
                    $script:TemporalCmd = $cmd
                    $script:TemporalContainerName = $TemporalContainerName
                    $script:TemporalUsingPodman = $isPodman
                    Write-Host "[startup] Started Temporal container $TemporalContainerName via $cmd" -ForegroundColor Cyan
                }
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
    $workerProcess = Start-WorkerProcess -ErrorLogPath $errorLogPath -TemporalAddress $TemporalAddress -TemporalNamespace $TemporalNamespace
    $script:WorkerProcess = $workerProcess
    $cancelSub = Register-EngineEvent -SourceIdentifier ConsoleCancelEvent -Action {
        Write-Host "[signal] Ctrl+C received; beginning shutdown..." -ForegroundColor Red
        $script:CancelRequested = $true
        if (-not $script:ShutdownStopwatch) {
            $script:ShutdownStopwatch = [System.Diagnostics.Stopwatch]::StartNew()
        } else {
            $script:ShutdownStopwatch.Restart()
        }
        $timer = $script:ShutdownStopwatch
        Stop-WorkerAndContainer -WorkerProcess $script:WorkerProcess -Timer $timer -Reason "Ctrl+C"
        if ($timer -and $timer.IsRunning) {
            $timer.Stop()
        }
        if ($timer) {
            Write-Host ("[signal] Ctrl+C shutdown finished in {0}s" -f [math]::Round($timer.Elapsed.TotalSeconds, 2)) -ForegroundColor Yellow
        }
        Write-Host "[signal] Shutdown requested; exiting loop." -ForegroundColor Red
    }
    Write-Host "Press Ctrl+R to restart the worker instantly." -ForegroundColor Yellow
    try {
        $exitCode = $null
        while ($true) {
            if ($script:CancelRequested) { break }
            Invoke-ErrorWatcherTick
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
                    $script:WorkerProcess = $workerProcess
                    continue
                }
            }
            Start-Sleep -Milliseconds 100
        }
    } finally {
        if ($cancelSub) {
            Unregister-Event -SubscriptionId $cancelSub.Id -ErrorAction SilentlyContinue
        }
        Stop-WorkerAndContainer -WorkerProcess $workerProcess
    }

    if (-not $script:CancelRequested) {
        if ($exitCode -eq $null -and $workerProcess) {
            $exitCode = $workerProcess.ExitCode
        }
        if ($exitCode -ne 0) {
            throw "Worker exited unexpectedly (exit $exitCode). See $errorLogPath for details."
        }
    }
}

if ($env:SKIP_START_WORKER_MAIN -ne "1") {
    Start-WorkerMain
}
