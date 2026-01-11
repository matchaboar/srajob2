#!/usr/bin/env pwsh

param(
    [switch]$ForceScrapeAll = $false,
    [switch]$ResetWithinSchedule = $false,
    [switch]$UseProd = $false,
    [switch]$UseTemporal = $false,
    [string]$EnvFile = ""
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$script:StartWorkerEntryPoint = $false
$script:TemporalContainerStartedByScript = $false
$script:TemporalContainerName = ""
$script:TemporalCmd = ""
$script:TemporalUsingPodman = $false
$script:CancelRequested = $false
$script:WorkerProcess = $null
$script:WorkerProcesses = @()
$script:ShutdownStopwatch = $null
$script:ShutdownHandled = $false
$script:ErrorWatcher = $null
$script:ErrorWatcherCount = 0
$script:WorkerCountTimer = $null
$script:LastProdWorkerCheck = $null

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
$temporalMaxBytes = 10 * 1024 * 1024
if (-not $env:TEMPORAL_MAX_INCOMING_GRPC_BYTES) {
    $env:TEMPORAL_MAX_INCOMING_GRPC_BYTES = $temporalMaxBytes.ToString()
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

function Get-RuntimeConfigInt {
    param(
        [string]$Path,
        [string]$Key,
        [int]$DefaultValue
    )

    if (-not $Path -or -not (Test-Path $Path)) {
        return $DefaultValue
    }

    try {
        $pattern = "^\s*{0}\s*:\s*([0-9]+)" -f [regex]::Escape($Key)
        foreach ($line in Get-Content -LiteralPath $Path -ErrorAction Stop) {
            if ($null -eq $line) { continue }
            $trimmed = $line.Trim()
            if (-not $trimmed -or $trimmed.StartsWith("#")) { continue }
            $match = [regex]::Match($line, $pattern)
            if ($match.Success) {
                return [int]$match.Groups[1].Value
            }
        }
    } catch {
        Write-Warning "Failed to read $Key from runtime config at ${Path}: $($_.Exception.Message)"
    }

    return $DefaultValue
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
    param(
        [bool]$UseProd = $false
    )

    if ($env:SKIP_PREFLIGHT_CHECKS -eq "1") {
        Write-Host "[preflight] SKIP_PREFLIGHT_CHECKS=1 set; skipping preflight checks." -ForegroundColor Yellow
        return
    }

    Write-Host "=== Running preflight checks ===" -ForegroundColor Cyan
    Reset-StaleVenv

    $updateSiteSchedulesBlock = if ($UseProd) {
        {
            if (-not $env:CONVEX_HTTP_URL) {
                Write-Host "CONVEX_HTTP_URL not set; skipping site schedule sync."
                return
            }
            uv run agent_scripts/update_and_sync_site_schedules.py --env prod
        }
    } else {
        {
            if (-not $env:CONVEX_HTTP_URL) {
                Write-Host "CONVEX_HTTP_URL not set; skipping site schedule sync."
                return
            }
            uv run agent_scripts/update_and_sync_site_schedules.py --env dev
        }
    }

    $steps = @(
        @{ Name = "update site schedules"; Timeout = 45; Block = $updateSiteSchedulesBlock },
        @{ Name = "ruff"; Timeout = 15; Block = { uvx ruff check job_scrape_application } }
        # @{ Name = "pytest"; Timeout = 120; Block = { uv run pytest } }
    )

    foreach ($step in $steps) {
        Write-Host ("[preflight] Running {0}..." -f $step.Name) -ForegroundColor DarkGray
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
        $WorkerProcesses,
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

    if ($WorkerProcesses -and $WorkerProcesses.Count -gt 0) {
        foreach ($worker in $WorkerProcesses) {
            $proc = $worker.Process
            if ($proc -and -not $proc.HasExited) {
                Write-Host "[shutdown] Killing worker process pid=$($proc.Id) role=$($worker.Role)" -ForegroundColor Yellow
                try { Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue } catch {}
                try {
                    Wait-Process -Id $proc.Id -Timeout 5 -ErrorAction SilentlyContinue
                } catch {}
            }
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

    Stop-WorkerCountTicker
    $script:ShutdownHandled = $true
}

function Start-WorkerProcess {
    param(
        [string]$ErrorLogPath,
        [string]$TemporalAddress,
        [string]$TemporalNamespace,
        [string]$Role = "all",
        [string]$TaskQueue = "scraper-task-queue",
        [string]$JobDetailsQueue = "",
        [switch]$EnableScheduleAudit = $false
    )

    $envBlock = @{}
    foreach ($entry in Get-ChildItem Env:) {
        $envBlock[$entry.Name] = $entry.Value
    }
    $envBlock["TEMPORAL_ADDRESS"] = $TemporalAddress
    $envBlock["TEMPORAL_NAMESPACE"] = $TemporalNamespace
    $envBlock["TEMPORAL_TASK_QUEUE"] = $TaskQueue
    $envBlock["TEMPORAL_WORKER_ROLE"] = $Role
    if ($EnableScheduleAudit) {
        $envBlock["SCRAPE_WORKER_ROLE"] = "audit"
    }
    if ($JobDetailsQueue) {
        $envBlock["TEMPORAL_JOB_DETAILS_TASK_QUEUE"] = $JobDetailsQueue
    }

    $workerArgs = @("run", "python", "-u", "-m", "job_scrape_application.workflows.worker")
    $proc = Start-Process -FilePath "uv" -ArgumentList $workerArgs -NoNewWindow -PassThru -RedirectStandardError $ErrorLogPath -Environment $envBlock
    if (-not $proc) {
        throw "Failed to start worker process."
    }
    $script:WorkerProcId = $proc.Id
    return @{
        Process = $proc
        Role = $Role
        TaskQueue = $TaskQueue
        JobDetailsQueue = $JobDetailsQueue
    }
}

function Start-DbosWorkerProcess {
    param(
        [string]$ErrorLogPath,
        [switch]$WithApi = $true,
        [int]$ListingConcurrency = 2,
        [int]$DetailConcurrency = 6
    )

    $envBlock = @{}
    foreach ($entry in Get-ChildItem Env:) {
        $envBlock[$entry.Name] = $entry.Value
    }

    $workerArgs = @("run", "-m", "job_scrape_application.dbos_runtime.runner")
    if ($WithApi) {
        $workerArgs += "--with-api"
    }
    $workerArgs += @("--listing-concurrency", $ListingConcurrency)
    $workerArgs += @("--detail-concurrency", $DetailConcurrency)
    $proc = Start-Process -FilePath "uv" -ArgumentList $workerArgs -NoNewWindow -PassThru -RedirectStandardError $ErrorLogPath -Environment $envBlock
    if (-not $proc) {
        throw "Failed to start DBOS worker process."
    }
    return @{
        Process = $proc
        Role = "dbos"
        TaskQueue = "dbos"
        JobDetailsQueue = ""
    }
}

function Stop-ExistingWorkers {
    try {
        if ($IsWindows) {
            $procs = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
                $_.CommandLine -match "job_scrape_application\.(workflows\.worker|dbos_runtime\.runner)"
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
            if ($line -match "^\s*(\d+)\s+.*job_scrape_application\.(workflows\.worker|dbos_runtime\.runner)") {
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

function Get-LocalWorkerCount {
    try {
        if ($IsWindows) {
            $procs = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
                $_.CommandLine -match "job_scrape_application\.(workflows\.worker|dbos_runtime\.runner)"
            }
            return @($procs).Count
        }

        $psPath = "/bin/ps"
        if (-not (Test-Path $psPath)) { $psPath = "/usr/bin/ps" }
        if (-not (Test-Path $psPath)) { return 0 }

        $psOutput = & $psPath -eo args 2>$null
        $count = 0
        foreach ($line in $psOutput) {
            if ($line -match "job_scrape_application\.(workflows\.worker|dbos_runtime\.runner)") {
                $count += 1
            }
        }
        return $count
    } catch {
        return 0
    }
}

function Write-WorkerDebugLog {
    param(
        [string]$LogPath,
        [string]$EnvironmentLabel,
        [bool]$UseProd,
        [string]$ConvexUrl,
        [string]$TemporalAddress,
        [string]$TemporalNamespace,
        [string]$TaskQueue,
        [string]$JobDetailsQueue,
        [string]$ListingQueue
    )

    try {
        $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        $entries = New-Object System.Collections.Generic.List[string]
        $entries.Add("---- $timestamp ----") | Out-Null
        $entries.Add("Environment: $EnvironmentLabel") | Out-Null
        $entries.Add("CONVEX_HTTP_URL: $ConvexUrl") | Out-Null
        $entries.Add("TEMPORAL_ADDRESS: $TemporalAddress") | Out-Null
        $entries.Add("TEMPORAL_NAMESPACE: $TemporalNamespace") | Out-Null
        $entries.Add("TEMPORAL_TASK_QUEUE: $TaskQueue") | Out-Null
        if ($JobDetailsQueue) { $entries.Add("TEMPORAL_JOB_DETAILS_TASK_QUEUE: $JobDetailsQueue") | Out-Null }
        if ($ListingQueue) { $entries.Add("TEMPORAL_LISTING_TASK_QUEUE: $ListingQueue") | Out-Null }
        $entries.Add(("Local worker processes: {0}" -f (Get-LocalWorkerCount))) | Out-Null

        if ($UseProd -and $ConvexUrl) {
            try {
                $diag = & uv run agent_scripts/diagnose_spidercloud_stalls.py --env prod 2>$null | ConvertFrom-Json
                if ($diag.temporal) {
                    $entries.Add(("Prod activeWorkers: {0}" -f $diag.temporal.activeWorkers)) | Out-Null
                    $entries.Add(("Prod staleWorkers: {0}" -f $diag.temporal.staleWorkers)) | Out-Null
                    $queues = @($diag.temporal.taskQueues) -join ", "
                    if ($queues) { $entries.Add(("Prod taskQueues: {0}" -f $queues)) | Out-Null }
                }
            } catch {
                $entries.Add("Prod diagnostics: failed to read temporal status") | Out-Null
            }
        }

        $entries | Out-File -FilePath $LogPath -Encoding utf8 -Append
    } catch {
        Write-Warning "Failed to write debug log: $($_.Exception.Message)"
    }
}

function Start-WorkerCountTicker {
    param(
        [int]$IntervalSeconds,
        [string]$LogPath,
        [bool]$UseProd,
        [string]$ConvexUrl
    )

    if ($IntervalSeconds -le 0) { return }

    try {
        if ($script:WorkerCountTimer) {
            try { $script:WorkerCountTimer.Stop() } catch {}
            $script:WorkerCountTimer = $null
        }

        $timer = New-Object System.Timers.Timer
        $timer.Interval = $IntervalSeconds * 1000
        $timer.AutoReset = $true
        $script:LastProdWorkerCheck = $null

        $handler = {
            try {
                $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
                $entries = New-Object System.Collections.Generic.List[string]
                $entries.Add("---- $timestamp ----") | Out-Null
                $entries.Add(("Local worker processes: {0}" -f (Get-LocalWorkerCount))) | Out-Null

                if ($UseProd -and $ConvexUrl) {
                    $now = Get-Date
                    $shouldCheck = $false
                    if (-not $script:LastProdWorkerCheck) {
                        $shouldCheck = $true
                    } elseif (($now - $script:LastProdWorkerCheck).TotalMinutes -ge 5) {
                        $shouldCheck = $true
                    }

                    if ($shouldCheck) {
                        $script:LastProdWorkerCheck = $now
                        try {
                            $diag = & uv run agent_scripts/diagnose_spidercloud_stalls.py --env prod 2>$null | ConvertFrom-Json
                            if ($diag.temporal) {
                                $entries.Add(("Prod activeWorkers: {0}" -f $diag.temporal.activeWorkers)) | Out-Null
                                $entries.Add(("Prod staleWorkers: {0}" -f $diag.temporal.staleWorkers)) | Out-Null
                            }
                        } catch {
                            $entries.Add("Prod diagnostics: failed to read temporal status") | Out-Null
                        }
                    }
                }

                $entries | Out-File -FilePath $LogPath -Encoding utf8 -Append
            } catch {}
        }

        Register-ObjectEvent -InputObject $timer -EventName Elapsed -Action $handler | Out-Null
        $timer.Start()
        $script:WorkerCountTimer = $timer
    } catch {
        Write-Warning "Failed to start worker count ticker: $($_.Exception.Message)"
    }
}

function Stop-WorkerCountTicker {
    try {
        if ($script:WorkerCountTimer) {
            $script:WorkerCountTimer.Stop()
            $script:WorkerCountTimer.Dispose()
            $script:WorkerCountTimer = $null
        }
    } catch {}
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
    $temporalTaskQueue = if ($env:TEMPORAL_TASK_QUEUE) { $env:TEMPORAL_TASK_QUEUE } else { "scraper-task-queue" }
    $jobDetailsQueue = if ($env:TEMPORAL_JOB_DETAILS_TASK_QUEUE) { $env:TEMPORAL_JOB_DETAILS_TASK_QUEUE } else { "" }
    $listingQueue = if ($env:TEMPORAL_LISTING_TASK_QUEUE) { $env:TEMPORAL_LISTING_TASK_QUEUE } else { "" }
    Write-Host ("TEMPORAL_ADDRESS: {0}" -f $TemporalAddress) -ForegroundColor Cyan
    Write-Host ("TEMPORAL_NAMESPACE: {0}" -f $TemporalNamespace) -ForegroundColor Cyan
    Write-Host ("TEMPORAL_TASK_QUEUE: {0}" -f $temporalTaskQueue) -ForegroundColor Cyan
    if ($jobDetailsQueue) {
        Write-Host ("TEMPORAL_JOB_DETAILS_TASK_QUEUE: {0}" -f $jobDetailsQueue) -ForegroundColor Cyan
    }
    if ($listingQueue) {
        Write-Host ("TEMPORAL_LISTING_TASK_QUEUE: {0}" -f $listingQueue) -ForegroundColor Cyan
    }

    $workerDebugLogPath = Join-Path "logs" "worker-start.log"
    if (-not (Test-Path (Split-Path $workerDebugLogPath -Parent))) {
        New-Item -ItemType Directory -Force -Path (Split-Path $workerDebugLogPath -Parent) | Out-Null
    }
    Write-WorkerDebugLog `
        -LogPath $workerDebugLogPath `
        -EnvironmentLabel $environmentLabel `
        -UseProd:$UseProd `
        -ConvexUrl $ConvexUrl `
        -TemporalAddress $TemporalAddress `
        -TemporalNamespace $TemporalNamespace `
        -TaskQueue $temporalTaskQueue `
        -JobDetailsQueue $jobDetailsQueue `
        -ListingQueue $listingQueue
    Start-WorkerCountTicker -IntervalSeconds 30 -LogPath $workerDebugLogPath -UseProd:$UseProd -ConvexUrl $ConvexUrl

    # Ensure any old worker processes from previous runs are terminated
    Stop-ExistingWorkers

    if ($ForceScrapeAll -or $ResetWithinSchedule) {
        $resetConvexUrl = $env:CONVEX_HTTP_URL
        if ($UseProd -and -not $EnvFile -and (Test-Path $prodEnvPath)) {
            if (-not $resetConvexUrl -or $convexSourcePath -ne $prodEnvPath) {
                Load-DotEnv $prodEnvPath -Override:$true -SourceMap:$envSourceMap
                $resetConvexUrl = $env:CONVEX_HTTP_URL
                $convexSourcePath = $prodEnvPath
            }
        }

        if ($UseProd -and $resetConvexUrl) {
            if ($resetConvexUrl -match "\.convex\.cloud") {
                Write-Error "CONVEX_HTTP_URL points to .convex.cloud; prod HTTP routes require .convex.site. Skipping forced reset."
                $resetConvexUrl = $null
            } elseif ($resetConvexUrl -notmatch "\.convex\.site") {
                Write-Warning "CONVEX_HTTP_URL does not look like a .convex.site endpoint; forced reset may not hit prod."
            }
        }

        if (-not $resetConvexUrl) {
            Write-Warning "CONVEX_HTTP_URL is not set; cannot reset sites for forced scrape."
        } else {
            $respectSchedule = $ResetWithinSchedule.IsPresent
            if ($respectSchedule) {
                Write-Host "Resetting active sites (respecting schedules)..."
            } else {
                Write-Host "Resetting active sites to force a fresh scrape on first run..."
            }
            try {
                $resetPayload = @{ respectSchedule = $respectSchedule } | ConvertTo-Json -Compress
                $resetResponse = Invoke-WebRequest -Method POST -Uri "$($resetConvexUrl.TrimEnd('/'))/api/sites/reset" -ContentType "application/json" -Body $resetPayload
                if ($resetResponse -and $resetResponse.Content) {
                    Write-Host ("Site reset response: {0}" -f $resetResponse.Content)
                } else {
                    Write-Host "Site reset request sent."
                }
            } catch {
                Write-Warning "Failed to reset sites for forced scrape: $_"
            }
        }
    }

    if (-not $UseTemporal) {
        Write-Host "Starting DBOS workflow runner..." -ForegroundColor Cyan
        $runtimeConfigPath = & $resolveEnvPath "job_scrape_application/config/runtime.yaml"
        $defaultListingConcurrency = 4
        $defaultDetailConcurrency = 6
        $listingConcurrency = Get-RuntimeConfigInt -Path $runtimeConfigPath -Key "temporal_listing_worker_count" -DefaultValue $defaultListingConcurrency
        $detailConcurrency = Get-RuntimeConfigInt -Path $runtimeConfigPath -Key "temporal_job_details_worker_count" -DefaultValue $defaultDetailConcurrency
        if ($listingConcurrency -lt 1) { $listingConcurrency = $defaultListingConcurrency }
        if ($detailConcurrency -lt 1) { $detailConcurrency = $defaultDetailConcurrency }
        $script:WorkerProcesses = @()
        $script:WorkerProcesses += Start-DbosWorkerProcess -ErrorLogPath $errorLogPath -WithApi -ListingConcurrency $listingConcurrency -DetailConcurrency $detailConcurrency
        $script:WorkerProcess = $script:WorkerProcesses[0].Process
        $cancelSub = Register-EngineEvent -SourceIdentifier ConsoleCancelEvent -Action {
            Write-Host "[signal] Ctrl+C received; beginning shutdown..." -ForegroundColor Red
            $script:CancelRequested = $true
            if (-not $script:ShutdownStopwatch) {
                $script:ShutdownStopwatch = [System.Diagnostics.Stopwatch]::StartNew()
            } else {
                $script:ShutdownStopwatch.Restart()
            }
            $timer = $script:ShutdownStopwatch
            Stop-WorkerAndContainer -WorkerProcesses $script:WorkerProcesses -SkipContainer -Timer $timer -Reason "Ctrl+C"
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
                $exited = $script:WorkerProcesses | Where-Object { $_.Process -and $_.Process.HasExited }
                if ($exited) {
                    $exitCode = $exited[0].Process.ExitCode
                    break
                }
                if (-not [Console]::IsInputRedirected) {
                    try {
                        if ([Console]::KeyAvailable) {
                            $key = [Console]::ReadKey($true)
                            if (($key.Modifiers -band [ConsoleModifiers]::Control) -and $key.Key -eq "R") {
                                Write-Host "Ctrl+R detected: restarting worker..." -ForegroundColor Yellow
                                try {
                                    foreach ($worker in $script:WorkerProcesses) {
                                        if ($worker.Process -and -not $worker.Process.HasExited) {
                                            Stop-Process -Id $worker.Process.Id -Force -ErrorAction SilentlyContinue
                                            Wait-Process -Id $worker.Process.Id -ErrorAction SilentlyContinue
                                        }
                                    }
                                } catch {}
                                $script:WorkerProcesses = @()
                                $script:WorkerProcesses += Start-DbosWorkerProcess -ErrorLogPath $errorLogPath -WithApi -ListingConcurrency $listingConcurrency -DetailConcurrency $detailConcurrency
                                $script:WorkerProcess = $script:WorkerProcesses[0].Process
                                continue
                            }
                        }
                    } catch {
                        # Ignore console polling failures in non-interactive sessions.
                    }
                }
                Start-Sleep -Milliseconds 100
            }
        } finally {
            if ($cancelSub) {
                Unregister-Event -SubscriptionId $cancelSub.Id -ErrorAction SilentlyContinue
            }
            Stop-WorkerAndContainer -WorkerProcesses $script:WorkerProcesses -SkipContainer
        }

        if (-not $script:CancelRequested) {
            if ($exitCode -eq $null -and $script:WorkerProcesses.Count -gt 0) {
                $exitCode = $script:WorkerProcesses[0].Process.ExitCode
            }
            if ($exitCode -ne 0) {
                throw "Worker exited unexpectedly (exit $exitCode). See $errorLogPath for details."
            }
        }
        return
    }

    $TemporalHost = ($TemporalAddress -split ":")[0]
    $TemporalPort = 7233
    if ($TemporalAddress -match ":(\d+)$") {
        $TemporalPort = [int]$matches[1]
    }
    $TemporalUiPort = 8233
    $TemporalUiHost = if ([string]::IsNullOrWhiteSpace($TemporalHost) -or $TemporalHost -eq "0.0.0.0" -or $TemporalHost -eq "::") { "localhost" } else { $TemporalHost }
    if ($TemporalUiHost -match ":" -and -not $TemporalUiHost.StartsWith("[")) {
        $TemporalUiHost = "[${TemporalUiHost}]"
    }
    $TemporalUiUrl = "http://${TemporalUiHost}:${TemporalUiPort}"
    $TemporalContainerName = "temporalite"
    $TemporalImageName = "temporal-dev:local"
    $TemporalDockerfile = "docker/temporal/Dockerfile.temporal-dev"
    $TemporalDockerContext = "docker/temporal"
    $TemporalComposeFile = "docker/temporal/docker-compose.yml"

    Write-Host "[preflight] Running checks before starting services..." -ForegroundColor Cyan
    Run-PreflightChecks -UseProd:$UseProd

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

    $JobDetailsQueue = "spidercloud-job-details-queue"

    $ListingQueue = "spidercloud-listing-queue"
    $env:TEMPORAL_JOB_DETAILS_TASK_QUEUE = $JobDetailsQueue
    $env:TEMPORAL_LISTING_TASK_QUEUE = $ListingQueue

    Write-Host "Ensuring scrape schedule exists (every 5 minutes)..."
    $maxScheduleAttempts = 5
    for ($i = 1; $i -le $maxScheduleAttempts; $i++) {
        uv run python -m job_scrape_application.workflows.create_schedule --skip-trigger
        if ($LASTEXITCODE -eq 0) {
            break
        }
        if ($i -lt $maxScheduleAttempts) {
            Write-Warning "Create schedule failed (exit $LASTEXITCODE). Retrying in 4s... [$i/$maxScheduleAttempts]"
            Start-Sleep -Seconds 4
        }
    }
    Assert-LastExit "Create/update Temporal schedule"

    Write-Host "Triggering heuristic schedule to kick off immediately..."
    uv run python -m job_scrape_application.workflows.trigger_schedule
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Trigger schedule failed (exit $LASTEXITCODE); continuing startup."
    }

    # Clear any stale progress bars from uv before showing live worker logs
    Clear-Host
    if ($TemporalUiUrl) {
        Write-Host ("Temporal UI available at {0}" -f $TemporalUiUrl) -ForegroundColor Cyan
    }

    $runtimeConfigPath = & $resolveEnvPath "job_scrape_application/config/runtime.yaml"
    $defaultGeneralWorkerCount = 4
    $defaultJobDetailsWorkerCount = 4
    $defaultListingWorkerCount = 4
    $generalWorkerCount = Get-RuntimeConfigInt -Path $runtimeConfigPath -Key "temporal_general_worker_count" -DefaultValue $defaultGeneralWorkerCount
    $jobDetailsWorkerCount = Get-RuntimeConfigInt -Path $runtimeConfigPath -Key "temporal_job_details_worker_count" -DefaultValue $defaultJobDetailsWorkerCount
    $listingWorkerCount = Get-RuntimeConfigInt -Path $runtimeConfigPath -Key "temporal_listing_worker_count" -DefaultValue $defaultListingWorkerCount
    if ($generalWorkerCount -lt 1) {
        Write-Warning "Invalid temporal_general_worker_count=$generalWorkerCount in $runtimeConfigPath; using $defaultGeneralWorkerCount."
        $generalWorkerCount = $defaultGeneralWorkerCount
    }
    if ($jobDetailsWorkerCount -lt 1) {
        Write-Warning "Invalid temporal_job_details_worker_count=$jobDetailsWorkerCount in $runtimeConfigPath; using $defaultJobDetailsWorkerCount."
        $jobDetailsWorkerCount = $defaultJobDetailsWorkerCount
    }
    if ($listingWorkerCount -lt 1) {
        Write-Warning "Invalid temporal_listing_worker_count=$listingWorkerCount in $runtimeConfigPath; using $defaultListingWorkerCount."
        $listingWorkerCount = $defaultListingWorkerCount
    }

    Write-Host ("Starting Workers ({0} general + {1} job-details + {2} listing)..." -f $generalWorkerCount, $jobDetailsWorkerCount, $listingWorkerCount)
    if ($ConvexUrl) {
        Write-Host "Using CONVEX_HTTP_URL=$ConvexUrl" -ForegroundColor Green
    } else {
        Write-Warning "CONVEX_HTTP_URL is not set. Worker will fail to reach Convex."
    }

    $TemporalTaskQueue = if ($env:TEMPORAL_TASK_QUEUE) { $env:TEMPORAL_TASK_QUEUE } else { "scraper-task-queue" }
    # Spawn multiple worker processes for higher throughput.
    $script:WorkerProcId = $null
    $script:WorkerProcesses = @()
    for ($i = 1; $i -le $generalWorkerCount; $i++) {
        $script:WorkerProcesses += Start-WorkerProcess -ErrorLogPath $errorLogPath -TemporalAddress $TemporalAddress -TemporalNamespace $TemporalNamespace -Role "all" -TaskQueue $TemporalTaskQueue -JobDetailsQueue $JobDetailsQueue -EnableScheduleAudit:($i -eq 1)
    }
    for ($i = 1; $i -le $jobDetailsWorkerCount; $i++) {
        $script:WorkerProcesses += Start-WorkerProcess -ErrorLogPath $errorLogPath -TemporalAddress $TemporalAddress -TemporalNamespace $TemporalNamespace -Role "job-details" -TaskQueue $TemporalTaskQueue -JobDetailsQueue $JobDetailsQueue
    }
    for ($i = 1; $i -le $listingWorkerCount; $i++) {
        $script:WorkerProcesses += Start-WorkerProcess -ErrorLogPath $errorLogPath -TemporalAddress $TemporalAddress -TemporalNamespace $TemporalNamespace -Role "listing" -TaskQueue $TemporalTaskQueue -JobDetailsQueue $JobDetailsQueue
    }
    $script:WorkerProcess = $script:WorkerProcesses[0].Process
    $cancelSub = Register-EngineEvent -SourceIdentifier ConsoleCancelEvent -Action {
        Write-Host "[signal] Ctrl+C received; beginning shutdown..." -ForegroundColor Red
        $script:CancelRequested = $true
        if (-not $script:ShutdownStopwatch) {
            $script:ShutdownStopwatch = [System.Diagnostics.Stopwatch]::StartNew()
        } else {
            $script:ShutdownStopwatch.Restart()
        }
        $timer = $script:ShutdownStopwatch
        Stop-WorkerAndContainer -WorkerProcesses $script:WorkerProcesses -Timer $timer -Reason "Ctrl+C"
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
            $exited = $script:WorkerProcesses | Where-Object { $_.Process -and $_.Process.HasExited }
            if ($exited) {
                $exitCode = $exited[0].Process.ExitCode
                break
            }
            if (-not [Console]::IsInputRedirected) {
                try {
                    if ([Console]::KeyAvailable) {
                        $key = [Console]::ReadKey($true)
                        if (($key.Modifiers -band [ConsoleModifiers]::Control) -and $key.Key -eq "R") {
                            Write-Host "Ctrl+R detected: restarting worker..." -ForegroundColor Yellow
                            try {
                                foreach ($worker in $script:WorkerProcesses) {
                                    if ($worker.Process -and -not $worker.Process.HasExited) {
                                        Stop-Process -Id $worker.Process.Id -Force -ErrorAction SilentlyContinue
                                        Wait-Process -Id $worker.Process.Id -ErrorAction SilentlyContinue
                                    }
                                }
                            } catch {}
                            $script:WorkerProcesses = @()
                            for ($i = 1; $i -le $generalWorkerCount; $i++) {
                                $script:WorkerProcesses += Start-WorkerProcess -ErrorLogPath $errorLogPath -TemporalAddress $TemporalAddress -TemporalNamespace $TemporalNamespace -Role "all" -TaskQueue $TemporalTaskQueue -JobDetailsQueue $JobDetailsQueue -EnableScheduleAudit:($i -eq 1)
                            }
                            for ($i = 1; $i -le $jobDetailsWorkerCount; $i++) {
                                $script:WorkerProcesses += Start-WorkerProcess -ErrorLogPath $errorLogPath -TemporalAddress $TemporalAddress -TemporalNamespace $TemporalNamespace -Role "job-details" -TaskQueue $TemporalTaskQueue -JobDetailsQueue $JobDetailsQueue
                            }
                            for ($i = 1; $i -le $listingWorkerCount; $i++) {
                                $script:WorkerProcesses += Start-WorkerProcess -ErrorLogPath $errorLogPath -TemporalAddress $TemporalAddress -TemporalNamespace $TemporalNamespace -Role "listing" -TaskQueue $TemporalTaskQueue -JobDetailsQueue $JobDetailsQueue
                            }
                            $script:WorkerProcess = $script:WorkerProcesses[0].Process
                            continue
                        }
                    }
                } catch {
                    # Ignore console polling failures in non-interactive sessions.
                }
            }
            Start-Sleep -Milliseconds 100
        }
    } finally {
        if ($cancelSub) {
            Unregister-Event -SubscriptionId $cancelSub.Id -ErrorAction SilentlyContinue
        }
        Stop-WorkerAndContainer -WorkerProcesses $script:WorkerProcesses
    }

    if (-not $script:CancelRequested) {
        if ($exitCode -eq $null -and $script:WorkerProcesses.Count -gt 0) {
            $exitCode = $script:WorkerProcesses[0].Process.ExitCode
        }
        if ($exitCode -ne 0) {
            throw "Worker exited unexpectedly (exit $exitCode). See $errorLogPath for details."
        }
    }
}

if ($env:SKIP_START_WORKER_MAIN -ne "1") {
    $script:StartWorkerEntryPoint = $true
    Start-WorkerMain
}
