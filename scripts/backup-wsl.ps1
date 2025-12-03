#!/usr/bin/env pwsh
param(
    [string]$DestinationRoot = 'C:\Users\greninja-threadrip\Proton Drive\boarcoder\My files\wsl-backups',
    [string]$SourcePath,
    [ScriptBlock]$RobocopyCommand,
    [ScriptBlock]$WslPathCommand,
    [ScriptBlock]$TestWindowsPathRunner,
    [ScriptBlock]$EnsureWindowsDirectoryRunner,
    [ScriptBlock]$TestPathRunner,
    [bool]$IsWindowsPlatform = $IsWindows
)

function Test-WindowsPath {
    param(
        [string]$Path,
        [ScriptBlock]$Runner
    )

    $runnerToUse = $Runner
    if (-not $runnerToUse) {
        $runnerToUse = { param($p) cmd.exe /c "if exist ""$p"" (exit 0) else (exit 1)" }
    }

    $result = & $runnerToUse $Path
    if ($result -is [bool]) {
        $LASTEXITCODE = $result ? 0 : 1
        return $result
    }

    return $LASTEXITCODE -eq 0
}

function Ensure-WindowsDirectory {
    param(
        [string]$Path,
        [ScriptBlock]$Runner
    )

    $runnerToUse = $Runner
    if (-not $runnerToUse) {
        $runnerToUse = { param($p) cmd.exe /c "if not exist ""$p"" mkdir ""$p""" }
    }

    $result = & $runnerToUse $Path
    if ($result -is [bool]) {
        $LASTEXITCODE = $result ? 0 : 1
    }

    return $LASTEXITCODE -eq 0
}

$defaultWslCandidateFallback = {
    param([string]$Path)
    if (-not $Path) { return $null }
    return $Path.Replace('wsl.localhost', 'wsl$')
}
$wslCandidateFallback = $defaultWslCandidateFallback

function Resolve-SourcePath {
    param(
        [string]$SourcePath,
        [string]$RepoRoot,
        [string]$DistroName,
        [ScriptBlock]$WslPathCommand = { param($path) & wslpath -w -- $path },
        [ScriptBlock]$TestWindowsPathRunner,
        [ScriptBlock]$TestPathRunner,
        [ScriptBlock]$WslCandidateFallback = $defaultWslCandidateFallback
    )

    $fallback = $WslCandidateFallback
    if (-not $fallback) {
        $fallback = $defaultWslCandidateFallback
    }

    if ($SourcePath) {
        return $SourcePath
    }

    $candidates = @()

    if ($DistroName) {
        try {
            $converted = & $WslPathCommand $RepoRoot
            if ($LASTEXITCODE -eq 0 -and $converted) {
                $clean = $converted.Trim()
                $original = $clean
                $candidates += $original
                if ($original -like '*wsl.localhost*') {
                    $convertedCandidate = & $fallback $original
                    if ($convertedCandidate) {
                        $candidates += $convertedCandidate
                    }
                } else {
                    $candidates += $clean.Replace('wsl.localhost', 'wsl$')
                }
            }
        } catch {
            # Ignore wslpath errors; fall back to repoRoot
        }
    }

    $candidates += $RepoRoot
    $fallbackChoice = $null

    foreach ($candidate in $candidates | Where-Object { $_ }) {
        if ($candidate -like '*wsl.localhost*') {
            $convertedCandidate = & $fallback $candidate
            if ($convertedCandidate) {
                $candidate = $convertedCandidate
            }
        }

        $isWinStyle = ($candidate -like '\\\\*') -or ($candidate -match '^[A-Za-z]:\\')
        if ($isWinStyle) {
            if (Test-WindowsPath -Path $candidate -Runner $TestWindowsPathRunner) {
                return $candidate
            }
        } else {
            $pathOk = $TestPathRunner ? (& $TestPathRunner $candidate) : (Test-Path -LiteralPath $candidate)
            if ($pathOk) {
                return $candidate
            }
        }

        if (-not $fallbackChoice) {
            $fallbackChoice = $candidate
        }
    }

    if ($fallbackChoice -like '*wsl.localhost*') {
        $fallbackChoice = & $fallback $fallbackChoice
    }

    if ($fallbackChoice) {
        return $fallbackChoice
    }

    return $null
}

function Is-WindowsStylePath {
    param([string]$Path)
    return ($Path -like '\\*') -or ($Path -like '///*') -or ($Path -match '^[A-Za-z]:\\')
}

function Get-RobocopyPlan {
    param(
        [string]$Src,
        [string]$Dst,
        [string[]]$Args,
        [ScriptBlock]$RobocopyCommand
    )

    $robocopyCmd = $RobocopyCommand
    if (-not $robocopyCmd) {
        $robocopyCmd = { param([string[]]$Args) & robocopy @Args }
    }

    $planArgs = @($Src, $Dst) + $Args + @(
        '/L'      # list only
        '/NJH'    # no job header
        '/NJS'    # no job summary
        '/FP'     # full path names
        '/NDL'    # no directory list
        '/NC'     # no class
        '/NS'     # no size summary
        '/NFL'    # no file list header
        '/NP'     # no progress text from robocopy
    )

    $filesToCopy = @()
    $totalBytes = 0L
    $planOutput = & $robocopyCmd @planArgs
    $copyPattern = '^\s*(New File|Newer(?: File)?|Changed)\s+(\d+)\s+(.*)$'

    foreach ($line in $planOutput) {
        if ($line -match $copyPattern) {
            $size = [int64]$matches[2]
            $path = $matches[3]
            $filesToCopy += [pscustomobject]@{ Path = $path; Bytes = $size }
            $totalBytes += $size
        }
    }

    [pscustomobject]@{
        Files = $filesToCopy
        TotalBytes = $totalBytes
    }
}

function Invoke-Backup {
    param(
        [string]$DestinationRoot = 'C:\Users\greninja-threadrip\Proton Drive\boarcoder\My files\wsl-backups',
        [string]$SourcePath,
        [ScriptBlock]$RobocopyCommand,
        [ScriptBlock]$WslPathCommand,
        [ScriptBlock]$TestWindowsPathRunner,
        [ScriptBlock]$EnsureWindowsDirectoryRunner,
        [ScriptBlock]$TestPathRunner,
        [bool]$IsWindowsPlatform = $IsWindows
    )

    $repoRoot = (Resolve-Path (Split-Path -Parent $PSScriptRoot)).Path
    $isWinPlatform = $IsWindowsPlatform
    $testPathRunner = $TestPathRunner

    if (-not $SourcePath) {
        $wslPathCmd = $WslPathCommand
        if (-not $wslPathCmd) {
            $wslPathCmd = { param($path) & wslpath -w -- $path }
        }
        $SourcePath = Resolve-SourcePath -SourcePath $null -RepoRoot $repoRoot -DistroName $env:WSL_DISTRO_NAME -WslPathCommand $wslPathCmd -TestWindowsPathRunner $TestWindowsPathRunner -TestPathRunner $testPathRunner
    }

    if (-not $SourcePath) {
        Write-Error "Source path could not be resolved automatically. Pass -SourcePath explicitly."
        throw "Source path could not be resolved automatically."
    }

    $DestinationPath = [System.IO.Path]::Combine($DestinationRoot, 'srajob2')

    $sourceIsWindowsStyle = Is-WindowsStylePath -Path $SourcePath
    $destIsWindowsStyle = Is-WindowsStylePath -Path $DestinationPath

    if ($isWinPlatform) {
        if (-not (Test-Path -LiteralPath $SourcePath)) {
            Write-Error "Source path not found: $SourcePath"
            throw "Source path not found."
        }
        if ($destIsWindowsStyle) {
            if (-not (Ensure-WindowsDirectory -Path $DestinationPath -Runner $EnsureWindowsDirectoryRunner)) {
                Write-Error "Could not create destination directory: $DestinationPath"
                throw "Could not create destination directory."
            }
        } else {
            New-Item -ItemType Directory -Force -Path $DestinationPath | Out-Null
        }
    } else {
        $sourceOk = $false
        if ($sourceIsWindowsStyle) {
            $sourceOk = Test-WindowsPath -Path $SourcePath -Runner $TestWindowsPathRunner
        } else {
            $sourceOk = $testPathRunner ? (& $testPathRunner $SourcePath) : (Test-Path -LiteralPath $SourcePath)
        }

        if (-not $sourceOk) {
            Write-Error "Source path not found: $SourcePath"
            throw "Source path not found."
        }

        if ($destIsWindowsStyle) {
            if (-not (Ensure-WindowsDirectory -Path $DestinationPath -Runner $EnsureWindowsDirectoryRunner)) {
                Write-Error "Could not create destination directory: $DestinationPath"
                throw "Could not create destination directory."
            }
        } else {
            New-Item -ItemType Directory -Force -Path $DestinationPath | Out-Null
        }
    }

    $commonArgs = @(
        '/E'        # include subdirectories
        '/XO'       # skip older/unchanged files; only copy newer/different
        '/FFT'      # tolerate FAT/NTFS timestamp granularity differences
        '/COPY:DAT' # copy data, attributes, timestamps
        '/R:1'      # retry once on failure
        '/W:1'      # wait 1s between retries
        '/MT:64'    # use 64 threads to saturate 32 cores/64 threads
        '/BYTES'    # ensure byte counts in output for progress parsing
        '/XD'       # exclude directories like virtualenvs/bytecode caches
        '.venv'
        '__pycache__'
    )

    $robocopyCmd = $RobocopyCommand
    if (-not $robocopyCmd) {
        $robocopyCmd = { param([string[]]$Args) & robocopy @Args }
    }

    $plan = Get-RobocopyPlan -Src $SourcePath -Dst $DestinationPath -Args $commonArgs -RobocopyCommand $robocopyCmd
    $totalFiles = $plan.Files.Count
    $totalBytes = $plan.TotalBytes

    if ($totalFiles -eq 0) {
        Write-Host "No new or updated files to copy."
        return
    }

    $progressActivity = "Backing up srajob2 to $DestinationPath"
    $bytesCopied = 0L
    $filesCopied = 0
    $progressId = Get-Random

    $copyPattern = '^\s*(New File|Newer(?: File)?|Changed)\s+(\d+)\s+(.*)$'

    $copyArgs = @($SourcePath, $DestinationPath) + $commonArgs

    & $robocopyCmd @copyArgs | ForEach-Object {
        $line = $_
        if ($line -match $copyPattern) {
            $size = [int64]$matches[2]
            $bytesCopied += $size
            $filesCopied += 1
            $percent = if ($totalBytes -gt 0) { [math]::Min(100, [math]::Round(($bytesCopied / $totalBytes) * 100, 2)) } else { 100 }
            $status = "$filesCopied of $totalFiles files"
            Write-Progress -Id $progressId -Activity $progressActivity -Status $status -PercentComplete $percent
        }
        $line
    }

    Write-Progress -Id $progressId -Activity $progressActivity -Completed

    # Robocopy returns codes >= 8 on failure; bubble those up.
    if ($LASTEXITCODE -ge 8) {
        throw "robocopy exited with code $LASTEXITCODE"
    }
}

if ($MyInvocation.InvocationName -ne '.') {
    Invoke-Backup -DestinationRoot $DestinationRoot -SourcePath $SourcePath -RobocopyCommand $RobocopyCommand -WslPathCommand $WslPathCommand -TestWindowsPathRunner $TestWindowsPathRunner -EnsureWindowsDirectoryRunner $EnsureWindowsDirectoryRunner -TestPathRunner $TestPathRunner -IsWindowsPlatform:$IsWindowsPlatform
}
