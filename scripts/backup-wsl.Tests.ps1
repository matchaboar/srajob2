Import-Module Pester -MinimumVersion 5.0

Describe "backup-wsl.ps1" {
    BeforeAll {
        $candidates = @(
            (Join-Path $PSScriptRoot 'backup-wsl.ps1'),
            (Join-Path (Get-Location).ProviderPath 'scripts/backup-wsl.ps1'),
            (Join-Path (Get-Location).ProviderPath 'backup-wsl.ps1')
        )
        $scriptPath = $candidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
        if (-not $scriptPath) { throw "backup-wsl.ps1 not found" }
        . $scriptPath
    }

    Describe "wslCandidateFallback" {
        It "returns null for empty input" {
            (& $wslCandidateFallback $null) | Should -Be $null
        }

        It "converts wsl.localhost to wsl$" {
            (& $wslCandidateFallback '\\wsl.localhost\Ubuntu\repo') | Should -Be '\\wsl$\Ubuntu\repo'
        }
    }

    Describe "Test-WindowsPath" {
        It "runs default runner" {
            Test-WindowsPath -Path '.' | Should -BeOfType System.Boolean
        }
    }

    Describe "Ensure-WindowsDirectory" {
        It "runs default runner" {
            Ensure-WindowsDirectory -Path (Join-Path $TestDrive 'ensure-default') | Should -BeOfType System.Boolean
        }
    }

    Describe "Resolve-SourcePath" {
        It "returns wsl$ output from wslpath" {
            $result = Resolve-SourcePath -SourcePath $null -RepoRoot '/repo' -DistroName 'Ubuntu-24.04' -WslPathCommand { param($p) $global:LASTEXITCODE = 0; '\\wsl.localhost\Ubuntu-24.04\repo' } -TestWindowsPathRunner { param($p) $p -like '\\wsl$\*' }
            $result | Should -Be '\\wsl$\Ubuntu-24.04\repo'
        }

        It "falls back to repo when wslpath fails and repo exists" {
            $result = Resolve-SourcePath -SourcePath $null -RepoRoot '/repo' -DistroName 'Ubuntu-24.04' -WslPathCommand { throw "fail" } -TestWindowsPathRunner { $false } -TestPathRunner { param($p) $p -eq '/repo' }
            $result | Should -Be '/repo'
        }

        It "returns first candidate when none validate" {
            $result = Resolve-SourcePath -SourcePath $null -RepoRoot '/repo' -DistroName 'Ubuntu-24.04' -WslPathCommand { param($p) $global:LASTEXITCODE = 0; '\\wsl.localhost\Ubuntu-24.04\repo' } -TestWindowsPathRunner { $false } -TestPathRunner { $false }
            $result | Should -Be '\\wsl$\Ubuntu-24.04\repo'
        }

        It "returns provided SourcePath immediately" {
            Resolve-SourcePath -SourcePath '\\custom\path' -RepoRoot '/repo' -DistroName 'Ubuntu-24.04' | Should -Be '\\custom\path'
        }

        It "uses default wslpath helper" {
            $env:WSL_DISTRO_NAME = 'Ubuntu-24.04'
            function global:wslpath { param($a,$b,$c) $global:LASTEXITCODE = 0; '\\wsl.localhost\default\repo' }
            $result = Resolve-SourcePath -SourcePath $null -RepoRoot '/repo' -DistroName $env:WSL_DISTRO_NAME
            $result | Should -Be '\\wsl$\default\repo'
            Remove-Item function:wslpath
        }

        It "adds non-wsl.localhost candidate from wslpath" {
            $result = Resolve-SourcePath -SourcePath $null -RepoRoot '/repo' -DistroName 'Ubuntu-24.04' -WslPathCommand { param($p) $global:LASTEXITCODE = 0; '\\share\repo' } -TestWindowsPathRunner { $false } -TestPathRunner { $false }
            $result | Should -Be '\\share\repo'
        }

        It "returns windows drive candidate when validated" {
            $result = Resolve-SourcePath -SourcePath $null -RepoRoot '/repo' -DistroName 'Ubuntu-24.04' -WslPathCommand { param($p) $global:LASTEXITCODE = 0; 'C:\repo' } -TestWindowsPathRunner { $true } -TestPathRunner { $false }
            $result | Should -Be 'C:\repo'
        }

        It "returns null when no candidates exist" {
            Resolve-SourcePath -SourcePath $null -RepoRoot $null -DistroName $null -TestPathRunner { $false } -TestWindowsPathRunner { $false } | Should -Be $null
        }

        It "returns candidate when windows path validated" {
            $result = Resolve-SourcePath -SourcePath $null -RepoRoot '/repo' -DistroName 'Ubuntu-24.04' -WslPathCommand { param($p) $global:LASTEXITCODE = 0; '\\wsl$\only' } -TestWindowsPathRunner { $true } -TestPathRunner { $false }
            $result | Should -Be '\\wsl$\only'
        }

        It "falls back to default fallback when null provided" {
            $result = Resolve-SourcePath -SourcePath $null -RepoRoot '/repo' -DistroName 'Ubuntu-24.04' -WslPathCommand { param($p) $global:LASTEXITCODE = 0; '\\wsl.localhost\Ubuntu-24.04\repo' } -TestWindowsPathRunner { $false } -TestPathRunner { $false } -WslCandidateFallback $null
            $result | Should -Be '\\wsl$\Ubuntu-24.04\repo'
        }

        It "converts fallback choice when no candidates accepted" {
            $result = Resolve-SourcePath -SourcePath $null -RepoRoot '/repo' -DistroName 'Ubuntu-24.04' -WslPathCommand { param($p) $global:LASTEXITCODE = 0; '\\wsl.localhost\only' } -TestWindowsPathRunner { $false } -TestPathRunner { $false }
            $result | Should -Be '\\wsl$\only'
        }

        It "applies fallback handler after loop when candidate unresolved" {
            $result = Resolve-SourcePath -SourcePath $null -RepoRoot '/repo' -DistroName 'Ubuntu-24.04' -WslPathCommand { param($p) $global:LASTEXITCODE = 0; '\\wsl.localhost\Ubuntu-24.04\repo' } -TestWindowsPathRunner { $false } -TestPathRunner { $false } -WslCandidateFallback { param($p) $p }
            $result | Should -Be '\\wsl.localhost\Ubuntu-24.04\repo'
        }
    }

    Describe "Is-WindowsStylePath" {
        It "detects UNC path" {
            Is-WindowsStylePath '\\\\server\\share\\path' | Should -BeTrue
        }
        It "detects drive letter" {
            Is-WindowsStylePath 'C:\temp' | Should -BeTrue
        }
        It "rejects POSIX" {
            Is-WindowsStylePath '/home/user' | Should -BeFalse
        }
    }

    Describe "Get-RobocopyPlan" {
        It "uses provided robocopy command" {
            $argsSeen = [System.Collections.Generic.List[object]]::new()
            $plan = Get-RobocopyPlan -Src 'src' -Dst 'dst' -Args @('/E') -RobocopyCommand {
                $argsSeen.Add($args) | Out-Null
                @(
                    "New File                0 empty.txt"
                    "Changed                 300 dir\file2.txt"
                )
            }
            $plan.TotalBytes | Should -Be 300
            $plan.Files.Count | Should -Be 2
            ($argsSeen[0] -join ' ') | Should -Match '/L'
        }

        It "hits default robocopy path" {
            $global:robocopyCalls = 0
            function global:robocopy { param([string[]]$Args) $global:robocopyCalls++; $global:LASTEXITCODE = 0; "New File 100 file.txt" }
            $null = Get-RobocopyPlan -Src 'src' -Dst 'dst' -Args @('/E')
            $global:robocopyCalls | Should -BeGreaterThan 0
            Remove-Item function:robocopy
        }
    }

    Describe "Invoke-Backup WSL paths" {
        BeforeEach { $env:WSL_DISTRO_NAME = 'Ubuntu-24.04' }

        It "returns early when no files to copy" {
            $calls = [ref]0
            $mockRobo = {
                $calls.Value++
                $global:LASTEXITCODE = 0
                if ($args -contains '/L') { @() } else { @() }
            }
            Invoke-Backup -DestinationRoot (Join-Path $TestDrive 'dst') -SourcePath (Join-Path $TestDrive 'src') -RobocopyCommand $mockRobo -IsWindowsPlatform:$false -TestPathRunner { $true }
            $calls.Value | Should -Be 1
        }

        It "copies and includes exclusions with zero-byte totals (percent 100 branch)" {
            $argsCaptured = [System.Collections.Generic.List[object]]::new()
            $mockRobo = {
                $global:LASTEXITCODE = 0
                $argsCaptured.Add($args) | Out-Null
                if ($args -contains '/L') {
                    @("New File                0 zero.txt")
                } else {
                    @("New File                0 zero.txt")
                }
            }
            Invoke-Backup -DestinationRoot (Join-Path $TestDrive 'dst2') -SourcePath (Join-Path $TestDrive 'src2') -RobocopyCommand $mockRobo -IsWindowsPlatform:$false -TestPathRunner { $true }
            $flat = $argsCaptured | ForEach-Object { $_ }
            $flat | Should -Contain '/XD'
            $flat | Should -Contain '.venv'
            $flat | Should -Contain '__pycache__'
            $flat | Should -Contain '/MT:64'
        }

        It "throws when source path cannot be resolved" {
            { Invoke-Backup -DestinationRoot (Join-Path $TestDrive 'dst3') -SourcePath $null -IsWindowsPlatform:$false -WslPathCommand { param($p) $global:LASTEXITCODE = 0; '' } -TestPathRunner { $false } } | Should -Throw
        }

        It "throws when provided source path is missing" {
            { Invoke-Backup -DestinationRoot (Join-Path $TestDrive 'dst4') -SourcePath '/missing' -IsWindowsPlatform:$false -TestPathRunner { $false } -TestWindowsPathRunner { $false } } | Should -Throw
        }

        It "handles windows-style source path via TestWindowsPathRunner" {
            Invoke-Backup -DestinationRoot (Join-Path $TestDrive 'dst5') -SourcePath '\\\\wsl$\\path' -IsWindowsPlatform:$false -TestWindowsPathRunner { $true } -EnsureWindowsDirectoryRunner { $global:LASTEXITCODE = 0; $true } -RobocopyCommand { $global:LASTEXITCODE = 0; if ($args -contains '/L') { @("New File 1 a.txt") } else { @("New File 1 a.txt") } }
        }

        It "fails when destination windows style cannot be created" {
            { Invoke-Backup -DestinationRoot 'C:\backups' -SourcePath (Join-Path $TestDrive 'src5') -IsWindowsPlatform:$false -TestPathRunner { $true } -EnsureWindowsDirectoryRunner { $global:LASTEXITCODE = 1; $false } -RobocopyCommand { $global:LASTEXITCODE = 0; @("New File 1 a.txt") } } | Should -Throw
        }
    }

    Describe "Invoke-Backup Windows branch" {
        It "throws when windows source is missing" {
            Mock -CommandName Test-Path -MockWith { $false }
            { Invoke-Backup -DestinationRoot 'C:\backup' -SourcePath 'C:\missing' -IsWindowsPlatform:$true } | Should -Throw
        }

        It "throws when destination cannot be created" {
            Mock -CommandName Test-Path -MockWith { $true }
            { Invoke-Backup -DestinationRoot 'C:\backup' -SourcePath 'C:\source' -RobocopyCommand { $global:LASTEXITCODE = 0; "New File 10 f.txt" } -EnsureWindowsDirectoryRunner { $global:LASTEXITCODE = 1; $false } -IsWindowsPlatform:$true } | Should -Throw
        }

        It "uses default robocopy and wslpath helpers and bubbles robocopy failure" {
            $env:WSL_DISTRO_NAME = 'Ubuntu-24.04'
            function global:wslpath { param($a,$b,$c) $global:LASTEXITCODE = 0; '\\wsl.localhost\Ubuntu-24.04\repo' }
            function global:robocopy { param([string[]]$Args) $global:LASTEXITCODE = 9; @("New File                10 file.txt") }
            { Invoke-Backup -DestinationRoot 'C:\backup' -SourcePath $null -IsWindowsPlatform:$false -TestPathRunner { $true } -TestWindowsPathRunner { $true } -EnsureWindowsDirectoryRunner { $global:LASTEXITCODE = 0; $true } } | Should -Throw
            Remove-Item function:wslpath
            Remove-Item function:robocopy
        }

        It "creates destination when windows platform and source exists" {
            Mock -CommandName Test-Path -MockWith { $true }
            $ensureCalled = [ref]$false
            $mockRobo = {
                $global:LASTEXITCODE = 0
                if ($args -contains '/L') { @("New File                0 zero.txt") } else { @("New File                0 zero.txt") }
            }
            Invoke-Backup -DestinationRoot 'C:\backup' -SourcePath 'C:\source' -RobocopyCommand $mockRobo -EnsureWindowsDirectoryRunner { $ensureCalled.Value = $true; $global:LASTEXITCODE = 0; $true } -IsWindowsPlatform:$true
            $ensureCalled.Value | Should -BeTrue
        }

        It "creates destination for non-windows path when windows platform" {
            Mock -CommandName Test-Path -MockWith { $true }
            Invoke-Backup -DestinationRoot (Join-Path $TestDrive 'dst-nonwin') -SourcePath (Join-Path $TestDrive 'src-nonwin') -RobocopyCommand { $global:LASTEXITCODE = 0; if ($args -contains '/L') { @("New File 0 z.txt") } else { @("New File 0 z.txt") } } -IsWindowsPlatform:$true
        }
    }

    Describe "Invoke-Backup error on unresolved source" {
        It "throws when Resolve-SourcePath returns null" {
            Mock -CommandName Resolve-SourcePath -MockWith { $null }
            { Invoke-Backup -DestinationRoot (Join-Path $TestDrive 'dst-null') -SourcePath $null -IsWindowsPlatform:$false -TestPathRunner { $false } } | Should -Throw
        }
    }

    Describe "Script entrypoint" {
        It "invokes backup with supplied mocks" {
            $invoked = [ref]$false
            $mockRobo = {
                $invoked.Value = $true
                $global:LASTEXITCODE = 0
                if ($args -contains '/L') { @("New File 10 f.txt") } else { @("New File 10 f.txt") }
            }
            & (Join-Path $PSScriptRoot 'backup-wsl.ps1') -DestinationRoot (Join-Path $TestDrive 'dst-entry') -SourcePath (Join-Path $TestDrive 'src-entry') -RobocopyCommand $mockRobo -IsWindowsPlatform:$false -TestPathRunner { $true }
            $invoked.Value | Should -BeTrue
        }
    }
}
