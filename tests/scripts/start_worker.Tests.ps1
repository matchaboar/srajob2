<# Pester tests for start_worker.ps1
   Validates that python invocations are composed with correct parameters and that
   helper functions can be called without running the full worker stack. #>

Param()

Describe "start_worker.ps1" {
    BeforeAll {
        $scriptPath = (Resolve-Path (Join-Path (Split-Path -Parent $PSCommandPath) ".." ".." "start_worker.ps1")).ProviderPath
        # Dot source the script so functions are available without auto-running main
        $env:SKIP_START_WORKER_MAIN = "1"
        $scriptFile = Get-Item -LiteralPath $scriptPath
        . $scriptFile.FullName

        function Set-ContainerMocks {
            Mock -CommandName Get-Command -MockWith { [pscustomobject]@{ Name = "podman" } }
            Set-Item -Path Function:podman -Value { param([Parameter(ValueFromRemainingArguments = $true)] $Args) $global:LASTEXITCODE = 0 }
            Set-Item -Path Function:docker -Value { param([Parameter(ValueFromRemainingArguments = $true)] $Args) $global:LASTEXITCODE = 0 }
            Set-Item -Path Function:"docker-compose" -Value { param([Parameter(ValueFromRemainingArguments = $true)] $Args) $global:LASTEXITCODE = 0 }
            Mock -CommandName Test-TemporalPort -MockWith { $true }
            Mock -CommandName Start-Sleep -MockWith { }
            Mock -CommandName Invoke-WebRequest -MockWith { }
        }
    }

    It "loads dotenv respecting override flag" {
        $temp = Join-Path $TestDrive ".env.test"
        Set-Content $temp @"
NEWVAR=new
EXISTING=override
"@

        $env:EXISTING = "keep"
        Load-DotEnv $temp
        $env:NEWVAR | Should -Be "new"
        $env:EXISTING | Should -Be "keep"

        Load-DotEnv $temp -Override:$true
        $env:EXISTING | Should -Be "override"
    }

    It "constructs python commands with expected parameters" {
        # Record calls to uv
        $script:UvCalls = @()
        Mock -CommandName uv -MockWith {
            param([Parameter(ValueFromRemainingArguments = $true)] $rest)
            $script:UvCalls += ,$rest
            $global:LASTEXITCODE = 0
        }

        Set-ContainerMocks
        $script:ThreadJobs = @()
        Mock -CommandName Start-ThreadJob -MockWith { $script:ThreadJobs += ,$args; return 1 }

        Push-Location $TestDrive
        try {
            # Minimal env to avoid warnings
            Set-Content ".env" ""
            $env:CONVEX_HTTP_URL = "https://convex.test"
            $env:SKIP_START_WORKER_MAIN = "0"

            Start-WorkerMain
        } finally {
            Pop-Location
        }

        # Expect schedule creation and worker start python invocations
        ($UvCalls | Where-Object { ($_ -join " ") -eq "run python -m job_scrape_application.workflows.create_schedule" }) | Should -Not -BeNullOrEmpty
        ($UvCalls | Where-Object { ($_ -join " ") -eq "run python -u -m job_scrape_application.workflows.worker" }) | Should -Not -BeNullOrEmpty
        $ThreadJobs.Count | Should -Be 0
    }

    It "retries schedule creation failures then succeeds" {
        $script:UvCalls = @()
        $script:ScheduleAttempts = 0
        Mock -CommandName uv -MockWith {
            param([Parameter(ValueFromRemainingArguments = $true)] $rest)
            $cmdline = ($rest -join " ")
            $script:UvCalls += $cmdline
            if ($cmdline -like "*create_schedule*") {
                $script:ScheduleAttempts++
                $global:LASTEXITCODE = if ($script:ScheduleAttempts -lt 2) { 32 } else { 0 }
            } else {
                $global:LASTEXITCODE = 0
            }
        }

        Set-ContainerMocks
        $script:ThreadJobs = @()
        Mock -CommandName Start-ThreadJob -MockWith { $script:ThreadJobs += ,$args; return 1 }

        Push-Location $TestDrive
        try {
            Set-Content ".env" ""
            $env:CONVEX_HTTP_URL = "https://convex.test"
            $env:SKIP_START_WORKER_MAIN = "0"

            Start-WorkerMain
        } finally {
            Pop-Location
        }

        ($UvCalls | Where-Object { $_ -like "*create_schedule*" }).Count | Should -Be 2
        ($UvCalls | Where-Object { $_ -like "*worker*" }).Count | Should -Be 1
    }

    It "surfaces error after exhausting schedule retries" {
        $script:UvCalls = @()
        Mock -CommandName uv -MockWith {
            param([Parameter(ValueFromRemainingArguments = $true)] $rest)
            $cmdline = ($rest -join " ")
            $script:UvCalls += $cmdline
            if ($cmdline -like "*create_schedule*") {
                $global:LASTEXITCODE = 2
            } else {
                $global:LASTEXITCODE = 0
            }
        }

        Set-ContainerMocks
        $script:ThreadJobs = @()
        Mock -CommandName Start-ThreadJob -MockWith { $script:ThreadJobs += ,$args; return 1 }

        Push-Location $TestDrive
        try {
            Set-Content ".env" ""
            $env:CONVEX_HTTP_URL = "https://convex.test"
            $env:SKIP_START_WORKER_MAIN = "0"

            { Start-WorkerMain } | Should -Throw
        } finally {
            Pop-Location
        }

        ($UvCalls | Where-Object { $_ -like "*create_schedule*" }).Count | Should -Be 5
        ($UvCalls | Where-Object { $_ -like "*worker*" }).Count | Should -Be 0
    }

    It "launches worker inline without Start-Process and cleans up watcher jobs" {
        $script:UvCalls = @()
        Mock -CommandName uv -MockWith {
            param([Parameter(ValueFromRemainingArguments = $true)] $rest)
            $script:UvCalls += ($rest -join " ")
            $global:LASTEXITCODE = 0
        }

        # Guard against regressions to Start-Process (which caused TextWriter disposal issues)
        Mock -CommandName Start-Process -MockWith { throw "Start-Process should not be called" }

        Set-ContainerMocks
        $script:ThreadJobs = @()
        Mock -CommandName Start-ThreadJob -MockWith { $script:ThreadJobs += ,$args; return 1 }

        Push-Location $TestDrive
        try {
            Set-Content ".env" ""
            $env:CONVEX_HTTP_URL = "https://convex.test"
            $env:SKIP_START_WORKER_MAIN = "0"

            Start-WorkerMain
        } finally {
            Pop-Location
        }

        ($UvCalls | Where-Object { $_ -like "*worker*" }).Count | Should -Be 1
        $script:ErrorWatcher | Should -BeNullOrEmpty
    }

    It "fails fast when worker exits with an error code" {
        $script:UvCalls = @()
        Mock -CommandName uv -MockWith {
            param([Parameter(ValueFromRemainingArguments = $true)] $rest)
            $cmdline = ($rest -join " ")
            $script:UvCalls += $cmdline
            if ($cmdline -like "*worker*") {
                $global:LASTEXITCODE = 9
            } else {
                $global:LASTEXITCODE = 0
            }
        }

        Set-ContainerMocks

        Push-Location $TestDrive
        try {
            Set-Content ".env" ""
            $env:CONVEX_HTTP_URL = "https://convex.test"
            $env:SKIP_START_WORKER_MAIN = "0"

            { Start-WorkerMain } | Should -Throw
        } finally {
            Pop-Location
        }

        ($UvCalls | Where-Object { $_ -like "*worker*" }).Count | Should -Be 1
    }
}
