param(
    [Parameter(Mandatory = $true)][string]$ResultsDirectory,
    [Parameter(Mandatory = $true)][string]$TempDirectory,
    [string]$ExportDirectory = '',
    [string]$Package = '',
    [string]$TestRun = '',
    [int]$Count = 1,
    [string]$Timeout = '10m0s',
    [int]$KillAfterSeconds = 660
)

# Runs every test executable listed in packages.tsv the way `go test ./...` runs one
# package: the binary is copied below TEMP under the package's own name, started with
# the package source directory as its working directory and the flags that go test
# passes (-test.paniconexit0 -test.timeout=10m0s -test.v=true), and its combined
# stdout/stderr is saved. Failures do not stop the run. Packages run one at a time.
#
# TempDirectory is used for TEMP and TMP exactly as given, so an 8.3 short path such
# as C:\HETERO~1 stays short, as on GitHub's Windows runners. ResultsDirectory and
# ExportDirectory must not exist yet; nothing is overwritten. Package selects a
# comma-separated subset of manifest names, and TestRun/Count add -test.run/-test.count.
$ErrorActionPreference = 'Stop'
$media = $PSScriptRoot
foreach ($required in @('packages.tsv', 'source.tar', 'summarize-all-package-results.ps1')) {
    if (-not (Test-Path -LiteralPath (Join-Path $media $required) -PathType Leaf)) {
        throw ('Missing media file: ' + $required)
    }
}
if (Test-Path -LiteralPath $ResultsDirectory) { throw 'Results directory must not already exist' }
if (-not [System.IO.Path]::IsPathRooted($TempDirectory) -or -not (Test-Path -LiteralPath $TempDirectory -PathType Container)) {
    throw 'Temp directory must be an existing absolute directory'
}
if ($ExportDirectory -and (Test-Path -LiteralPath $ExportDirectory)) {
    throw 'Export directory must not already exist'
}
if ($Count -lt 1 -or $KillAfterSeconds -lt 1) { throw 'Count and KillAfterSeconds must be positive' }

$manifest = @(Import-Csv -LiteralPath (Join-Path $media 'packages.tsv') -Delimiter "`t")
$selected = $manifest
if ($Package) {
    $names = @($Package.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ })
    foreach ($name in $names) {
        if (-not @($manifest | Where-Object { $_.name -eq $name })) { throw ('Unknown package name: ' + $name) }
    }
    $selected = @($manifest | Where-Object { $names -contains $_.name })
}

$utf8 = New-Object System.Text.UTF8Encoding($false)
$results = (New-Item -ItemType Directory -Path $ResultsDirectory).FullName
$logs = (New-Item -ItemType Directory -Path (Join-Path $results 'logs')).FullName
$source = (New-Item -ItemType Directory -Path (Join-Path $results 'src')).FullName

# Diagnostics only. Native output is redirected inside cmd.exe so that Windows
# PowerShell 5.1 does not turn stderr lines into terminating errors.
$cmd = Join-Path $env:SystemRoot 'System32\cmd.exe'
$environment = New-Object 'System.Collections.Generic.List[string]'
$environment.Add('Started: ' + (Get-Date).ToString('o'))
$environment.Add('OS: ' + [System.Environment]::OSVersion.VersionString)
$environment.Add('PowerShell: ' + $PSVersionTable.PSVersion.ToString())
$environment.Add('Media: ' + $media)
$environment.Add('Results: ' + $results)
$environment.Add('TEMP/TMP: ' + $TempDirectory)
$environment.Add('Timeout: ' + $Timeout + '; kill after seconds: ' + $KillAfterSeconds)
$environment.Add('Packages: ' + $selected.Count + ' of ' + $manifest.Count)
if ($TestRun) { $environment.Add('TestRun: ' + $TestRun) }
if ($Count -ne 1) { $environment.Add('Count: ' + $Count) }
$principal = New-Object System.Security.Principal.WindowsPrincipal([System.Security.Principal.WindowsIdentity]::GetCurrent())
$environment.Add('User: ' + [System.Security.Principal.WindowsIdentity]::GetCurrent().Name)
$environment.Add('Elevated administrator: ' + $principal.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator))
foreach ($line in @(& $cmd /d /c 'whoami /groups 2>&1')) {
    if ($line -match 'S-1-16-\d+') { $environment.Add('Integrity SID: ' + $Matches[0]) }
}
try {
    Add-Type -Namespace HeterologTests -Name PathNames -MemberDefinition @'
[System.Runtime.InteropServices.DllImport("kernel32.dll", CharSet = System.Runtime.InteropServices.CharSet.Unicode, SetLastError = true)]
public static extern uint GetLongPathName(string shortPath, System.Text.StringBuilder longPath, uint length);
[System.Runtime.InteropServices.DllImport("kernel32.dll", CharSet = System.Runtime.InteropServices.CharSet.Unicode, SetLastError = true)]
public static extern uint GetShortPathName(string longPath, System.Text.StringBuilder shortPath, uint length);
'@
    $buffer = New-Object System.Text.StringBuilder 1024
    [void][HeterologTests.PathNames]::GetLongPathName($TempDirectory, $buffer, 1024)
    $environment.Add('TEMP long form: ' + $buffer.ToString())
    $buffer = New-Object System.Text.StringBuilder 1024
    [void][HeterologTests.PathNames]::GetShortPathName($TempDirectory, $buffer, 1024)
    $environment.Add('TEMP short form: ' + $buffer.ToString())
} catch {
    $environment.Add('TEMP long/short form unavailable: ' + $_.Exception.Message)
}
$tempRoot = [System.IO.Path]::GetPathRoot($TempDirectory)
$environment.Add('fsutil 8dot3name query ' + $tempRoot.TrimEnd('\') + ' (read-only):')
foreach ($line in @(& $cmd /d /c ('fsutil 8dot3name query ' + $tempRoot.TrimEnd('\') + ' 2>&1'))) {
    $environment.Add('  ' + $line)
}
[System.IO.File]::WriteAllLines((Join-Path $results 'environment.txt'), $environment, $utf8)

& (Join-Path $env:SystemRoot 'System32\tar.exe') -xf (Join-Path $media 'source.tar') -C $source
if ($LASTEXITCODE -ne 0) { throw ('Source extraction failed with exit code ' + $LASTEXITCODE) }
if (-not (Test-Path -LiteralPath (Join-Path $source 'go.mod') -PathType Leaf)) { throw 'Extracted source has no go.mod' }

# Starts one executable with both output streams in one file, in order, like the
# single pipe that go test reads. Kills the process tree after KillAfterSeconds.
function Invoke-LoggedProcess([string]$executable, [string[]]$arguments, [string]$workingDirectory, [string]$log) {
    $quotedArguments = ($arguments | ForEach-Object { '"' + $_ + '"' }) -join ' '
    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = $cmd
    $startInfo.Arguments = '/d /s /c ""' + $executable + '" ' + $quotedArguments + ' > "' + $log + '" 2>&1"'
    $startInfo.WorkingDirectory = $workingDirectory
    $startInfo.UseShellExecute = $false
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $process = [System.Diagnostics.Process]::Start($startInfo)
    $killed = $false
    try {
        if (-not $process.WaitForExit($KillAfterSeconds * 1000)) {
            $killed = $true
            & (Join-Path $env:SystemRoot 'System32\taskkill.exe') /T /F /PID $process.Id | Out-Null
            $process.WaitForExit()
        }
        $exitCode = $process.ExitCode
    } finally {
        $process.Dispose()
    }
    $stopwatch.Stop()
    return @{
        ExitCode = $exitCode
        Killed = $killed
        Seconds = $stopwatch.Elapsed.TotalSeconds.ToString('0.0', [System.Globalization.CultureInfo]::InvariantCulture)
    }
}

$env:TEMP = $TempDirectory
$env:TMP = $TempDirectory
$runs = Join-Path $results 'runs.tsv'
[System.IO.File]::WriteAllText($runs, "name`timport_path`texit_code`tseconds`tkilled`r`n", $utf8)
$anyFailed = $false
$index = 0
foreach ($entry in $selected) {
    $index++
    Write-Host ('[{0}/{1}] {2}' -f $index, $selected.Count, $entry.import_path)
    $log = Join-Path $logs ($entry.name + '.log')
    $buildDirectory = ''
    $run = @{ ExitCode = 'error'; Killed = $false; Seconds = '' }
    try {
        $packageDirectory = $source
        if ($entry.directory -ne '.') { $packageDirectory = Join-Path $source ($entry.directory.Replace('/', '\')) }
        # go test builds into a fresh work directory below TEMP and names the binary after the package.
        $buildDirectory = Join-Path $TempDirectory ('heterolog-go-build' + [System.Guid]::NewGuid().ToString('N').Substring(0, 12))
        $binaryDirectory = Join-Path $buildDirectory 'b001'
        New-Item -ItemType Directory -Path $binaryDirectory | Out-Null
        $executable = Join-Path $binaryDirectory ($entry.binary + '.test.exe')
        Copy-Item -LiteralPath (Join-Path $media ('bin\' + $entry.name + '.test.exe')) -Destination $executable
        # Files copied from the CD keep the read-only attribute; a go-built binary has none.
        (Get-Item -LiteralPath $executable).IsReadOnly = $false
        $arguments = @('-test.paniconexit0', ('-test.timeout=' + $Timeout), '-test.v=true')
        if ($Count -ne 1) { $arguments += ('-test.count=' + $Count) }
        if ($TestRun) { $arguments += ('-test.run=' + $TestRun) }
        $run = Invoke-LoggedProcess $executable $arguments $packageDirectory $log
    } catch {
        # Record a setup failure (for example a blocked copy) and continue with the next package.
        [System.IO.File]::AppendAllText($log, ('RUNNER ERROR: ' + $_.Exception.Message + "`r`n"), $utf8)
    }
    if ($run.ExitCode -ne 0 -or $run.Killed) { $anyFailed = $true }
    [System.IO.File]::WriteAllText((Join-Path $logs ($entry.name + '.exit-code.txt')), ([string]$run.ExitCode + "`r`n"), $utf8)
    $killedText = '0'
    if ($run.Killed) { $killedText = '1' }
    [System.IO.File]::AppendAllText($runs, ("{0}`t{1}`t{2}`t{3}`t{4}`r`n" -f $entry.name, $entry.import_path, $run.ExitCode, $run.Seconds, $killedText), $utf8)
    $status = 'ok'
    if ($run.ExitCode -ne 0) { $status = 'FAIL exit=' + $run.ExitCode }
    if ($run.Killed) { $status = 'KILLED after ' + $KillAfterSeconds + 's' }
    Write-Host ('    {0} ({1}s)' -f $status, $run.Seconds)
    if ($buildDirectory -and (Test-Path -LiteralPath $buildDirectory)) {
        try {
            Remove-Item -LiteralPath $buildDirectory -Recurse -Force
        } catch {
            Write-Host ('    could not remove ' + $buildDirectory + ': ' + $_.Exception.Message)
        }
    }
}
$overall = 0
if ($anyFailed) { $overall = 1 }
[System.IO.File]::WriteAllText((Join-Path $logs 'overall-exit-code.txt'), "$overall`r`n", $utf8)

# The CLI package has no tests. Check that the Windows binary starts and parses arguments:
# -h prints usage with exit code 0, and a missing --target is rejected with exit code 2.
$cli = Join-Path $media 'bin\log-recovery.exe'
if (-not $Package -and (Test-Path -LiteralPath $cli -PathType Leaf)) {
    $smoke = New-Object 'System.Collections.Generic.List[string]'
    try {
        foreach ($case in @(@('help', 0, @('-h')), @('no-arguments', 2, @()))) {
            $smokeLog = Join-Path $results ('cli-smoke-' + $case[0] + '.log')
            $smokeRun = Invoke-LoggedProcess $cli ([string[]]$case[2]) $results $smokeLog
            $smoke.Add(('log-recovery {0}: exit={1} expected={2}' -f $case[0], $smokeRun.ExitCode, $case[1]))
            foreach ($line in [System.IO.File]::ReadAllLines($smokeLog)) { $smoke.Add('  ' + $line) }
            Remove-Item -LiteralPath $smokeLog
        }
    } catch {
        $smoke.Add('RUNNER ERROR: ' + $_.Exception.Message)
    }
    [System.IO.File]::WriteAllLines((Join-Path $results 'cli-smoke.txt'), $smoke, $utf8)
}

# Entries that tests left below TEMP outside t.TempDir cleanup, by name only.
$leftovers = @(Get-ChildItem -LiteralPath $TempDirectory -Force | ForEach-Object { $_.Name })
[System.IO.File]::WriteAllLines((Join-Path $results 'temp-leftovers.txt'), [string[]]$leftovers, $utf8)

# Files that tests added to or removed from the checkout copy, compared with source.tar.
$sourceList = Join-Path $media 'source-files.txt'
if (Test-Path -LiteralPath $sourceList -PathType Leaf) {
    $changes = New-Object 'System.Collections.Generic.List[string]'
    try {
        $listed = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
        foreach ($line in [System.IO.File]::ReadAllLines($sourceList)) {
            if ($line.StartsWith('./')) { $line = $line.Substring(2) }
            if ($line -and -not $line.EndsWith('/')) { [void]$listed.Add($line) }
        }
        $present = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
        foreach ($file in @(Get-ChildItem -LiteralPath $source -Recurse -File -Force)) {
            $relative = $file.FullName.Substring($source.Length + 1).Replace('\', '/')
            [void]$present.Add($relative)
            if (-not $listed.Contains($relative)) { $changes.Add('added: ' + $relative) }
        }
        foreach ($relative in $listed) {
            if (-not $present.Contains($relative)) { $changes.Add('missing: ' + $relative) }
        }
        if ($changes.Count -eq 0) { $changes.Add('none') }
    } catch {
        $changes.Add('RUNNER ERROR: ' + $_.Exception.Message)
    }
    [System.IO.File]::WriteAllLines((Join-Path $results 'source-changes.txt'), $changes, $utf8)
}

try {
    & (Join-Path $media 'summarize-all-package-results.ps1') -ResultsDirectory $results
} catch {
    # Keep exporting the raw logs even if counting fails.
    Write-Host ('SUMMARY FAILED: ' + $_.Exception.Message)
    $overall = 1
}

if ($ExportDirectory) {
    # Small summaries stay readable; every package log goes into one zip for the 64 MiB transfer volume.
    $export = (New-Item -ItemType Directory -Path $ExportDirectory).FullName
    foreach ($name in @('environment.txt', 'runs.tsv', 'summary.tsv', 'totals.txt', 'failures.txt', 'skips.txt', 'cli-smoke.txt', 'temp-leftovers.txt', 'source-changes.txt')) {
        $path = Join-Path $results $name
        if (Test-Path -LiteralPath $path -PathType Leaf) { Copy-Item -LiteralPath $path -Destination $export }
    }
    Copy-Item -LiteralPath (Join-Path $logs 'overall-exit-code.txt') -Destination $export
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $zip = Join-Path $export 'logs.zip'
    [System.IO.Compression.ZipFile]::CreateFromDirectory($logs, $zip, [System.IO.Compression.CompressionLevel]::Optimal, $false)
    $archive = [System.IO.Compression.ZipFile]::OpenRead($zip)
    try {
        $entries = $archive.Entries.Count
    } finally {
        $archive.Dispose()
    }
    $files = @(Get-ChildItem -LiteralPath $logs -File).Count
    if ($entries -ne $files) { throw ('logs.zip has ' + $entries + ' entries for ' + $files + ' files') }
    $drive = New-Object System.IO.DriveInfo([System.IO.Path]::GetPathRoot($export))
    Write-Host ('EXPORTED {0} ({1} log files, zip {2} bytes, {3} bytes free)' -f $export, $files, (Get-Item -LiteralPath $zip).Length, $drive.AvailableFreeSpace)
}
Write-Host ('RESULTS ' + $results)
Write-Host ('OVERALL ' + $overall)
exit $overall
