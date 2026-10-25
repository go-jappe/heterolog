param([Parameter(Mandatory = $true)][string]$ResultsDirectory)

# Counts `go test -v` results per package in RESULTS\logs (<name>.log and
# <name>.exit-code.txt, as written by run-all-package-tests.ps1) and writes
# summary.tsv, failures.txt, skips.txt and totals.txt into RESULTS.
#
# pass/fail/skip count top-level tests; subtests (names with '/') are counted in the
# subtest_* columns. A result line counts only after its own `=== RUN` line, so test
# output that a test logs for a child process (indented by t.Log) is not counted again.
# Logger output may precede a marker on the same line. Raw logs stay where they are.
$ErrorActionPreference = 'Stop'
$results = (Get-Item -LiteralPath $ResultsDirectory).FullName
$logs = Join-Path $results 'logs'
if (-not (Test-Path -LiteralPath $logs -PathType Container)) { throw 'Results directory has no logs directory' }
$utf8 = New-Object System.Text.UTF8Encoding($false)
$runs = @{}
$runsPath = Join-Path $results 'runs.tsv'
if (Test-Path -LiteralPath $runsPath -PathType Leaf) {
    foreach ($row in @(Import-Csv -LiteralPath $runsPath -Delimiter "`t")) { $runs[$row.name] = $row }
}
$contextPattern = New-Object System.Text.RegularExpressions.Regex('=== (RUN|CONT|NAME)\s+(\S+)')
$resultPattern = New-Object System.Text.RegularExpressions.Regex('--- (PASS|FAIL|SKIP): (\S+) \(')
$logPattern = New-Object System.Text.RegularExpressions.Regex('^\s+\S+\.go:\d+: ')
$runningPattern = New-Object System.Text.RegularExpressions.Regex('^\s+(\S+) \(')

# Real markers start a line or follow logger output; markers indented by spaces only
# belong to text that a test logged.
function Test-LineMarker([string]$prefix) {
    return ($prefix.Length -eq 0 -or $prefix.Trim().Length -gt 0)
}

$found = @{}
foreach ($file in @(Get-ChildItem -LiteralPath $logs -File)) {
    if ($file.Name.EndsWith('.exit-code.txt')) {
        $found[$file.Name.Substring(0, $file.Name.Length - '.exit-code.txt'.Length)] = $true
    } elseif ($file.Name.EndsWith('.log')) {
        $found[$file.Name.Substring(0, $file.Name.Length - '.log'.Length)] = $true
    }
}
$names = @($found.Keys | Sort-Object)

$summary = New-Object 'System.Collections.Generic.List[string]'
$summary.Add("package`texit_code`tpass`tfail`tskip`tsubtest_pass`tsubtest_fail`tsubtest_skip`ttimed_out`tkilled`tseconds")
$failures = New-Object 'System.Collections.Generic.List[string]'
$skips = New-Object 'System.Collections.Generic.List[string]'
$table = New-Object 'System.Collections.Generic.List[string]'
$table.Add(('{0,-58} {1,4} {2,5} {3,4} {4,4} {5,6} {6,4} {7,4} {8}' -f 'PACKAGE', 'EXIT', 'PASS', 'FAIL', 'SKIP', 'SUB-OK', 'SUB-F', 'SUB-S', 'NOTE'))
$total = @{ packages = 0; failedPackages = 0; PASS = 0; FAIL = 0; SKIP = 0; subPASS = 0; subFAIL = 0; subSKIP = 0 }
foreach ($name in $names) {
    $counts = @{ PASS = 0; FAIL = 0; SKIP = 0; subPASS = 0; subFAIL = 0; subSKIP = 0 }
    # Test names are case-sensitive; PowerShell hashtables are not.
    $pending = New-Object 'System.Collections.Generic.Dictionary[string,int]'
    $lastLog = New-Object 'System.Collections.Generic.Dictionary[string,string]'
    $current = ''
    $timedOut = $false
    $inRunningList = $false
    $running = New-Object 'System.Collections.Generic.List[string]'
    $firstPanic = ''
    $logPath = Join-Path $logs ($name + '.log')
    $lines = @()
    if (Test-Path -LiteralPath $logPath -PathType Leaf) { $lines = [System.IO.File]::ReadAllLines($logPath) }
    foreach ($line in $lines) {
        if ($inRunningList) {
            $runningMatch = $runningPattern.Match($line)
            if ($runningMatch.Success) { $running.Add($runningMatch.Groups[1].Value); continue }
            $inRunningList = $false
        }
        if ($line.Contains('panic: ')) {
            if (-not $firstPanic) { $firstPanic = $line.Trim() }
            if ($line.Contains('panic: test timed out after')) { $timedOut = $true }
        }
        if ($timedOut -and $line.Trim() -eq 'running tests:') { $inRunningList = $true; continue }
        if ($line.Contains('=== ')) {
            $contextMatch = $contextPattern.Match($line)
            if ($contextMatch.Success -and (Test-LineMarker ($line.Substring(0, $contextMatch.Index)))) {
                $current = $contextMatch.Groups[2].Value
                if ($contextMatch.Groups[1].Value -eq 'RUN') {
                    if ($pending.ContainsKey($current)) { $pending[$current] += 1 } else { $pending[$current] = 1 }
                }
                continue
            }
        }
        if ($current -and $logPattern.IsMatch($line)) { $lastLog[$current] = $line.Trim() }
        if (-not $line.Contains('--- ')) { continue }
        foreach ($match in $resultPattern.Matches($line)) {
            $status = $match.Groups[1].Value
            $test = $match.Groups[2].Value
            $subtest = $test.Contains('/')
            if (-not $subtest -and -not (Test-LineMarker ($line.Substring(0, $match.Index)))) { continue }
            if (-not $pending.ContainsKey($test) -or $pending[$test] -lt 1) { continue }
            $pending[$test] -= 1
            $key = $status
            if ($subtest) { $key = 'sub' + $status }
            $counts[$key] += 1
            $detail = ''
            if ($lastLog.ContainsKey($test)) { $detail = $lastLog[$test] }
            if ($status -eq 'FAIL') { $failures.Add($name + "`t" + $test + "`t" + $detail) }
            if ($status -eq 'SKIP') { $skips.Add($name + "`t" + $test + "`t" + $detail) }
        }
    }

    $exitCode = 'missing'
    $exitPath = Join-Path $logs ($name + '.exit-code.txt')
    if (Test-Path -LiteralPath $exitPath -PathType Leaf) { $exitCode = [System.IO.File]::ReadAllText($exitPath).Trim() }
    $killed = '0'
    $seconds = ''
    if ($runs.ContainsKey($name)) {
        $killed = $runs[$name].killed
        $seconds = $runs[$name].seconds
    }
    $timedOutText = '0'
    if ($timedOut) {
        $timedOutText = '1'
        $failures.Add($name + "`t(timeout)`t" + ($running -join ' '))
    }
    if ($killed -eq '1') { $failures.Add($name + "`t(killed)`t") }
    if ($exitCode -ne '0' -and $counts.FAIL -eq 0 -and $counts.subFAIL -eq 0 -and -not $timedOut -and $killed -ne '1') {
        $failures.Add($name + "`t(exit " + $exitCode + " without a --- FAIL line)`t" + $firstPanic)
    }
    $summary.Add((@($name, $exitCode, $counts.PASS, $counts.FAIL, $counts.SKIP, $counts.subPASS, $counts.subFAIL, $counts.subSKIP, $timedOutText, $killed, $seconds) -join "`t"))
    $note = ''
    if ($timedOut) { $note = 'TIMEOUT' }
    if ($killed -eq '1') { $note = 'KILLED' }
    $table.Add(('{0,-58} {1,4} {2,5} {3,4} {4,4} {5,6} {6,4} {7,4} {8}' -f $name, $exitCode, $counts.PASS, $counts.FAIL, $counts.SKIP, $counts.subPASS, $counts.subFAIL, $counts.subSKIP, $note))
    $total.packages += 1
    if ($exitCode -ne '0' -or $killed -eq '1') { $total.failedPackages += 1 }
    foreach ($key in @('PASS', 'FAIL', 'SKIP', 'subPASS', 'subFAIL', 'subSKIP')) { $total[$key] += $counts[$key] }
}

$totals = @(
    ('packages={0}' -f $total.packages),
    ('ok_packages={0}' -f ($total.packages - $total.failedPackages)),
    ('failed_packages={0}' -f $total.failedPackages),
    ('pass={0}' -f $total.PASS),
    ('fail={0}' -f $total.FAIL),
    ('skip={0}' -f $total.SKIP),
    ('subtest_pass={0}' -f $total.subPASS),
    ('subtest_fail={0}' -f $total.subFAIL),
    ('subtest_skip={0}' -f $total.subSKIP)
)
[System.IO.File]::WriteAllLines((Join-Path $results 'summary.tsv'), $summary, $utf8)
[System.IO.File]::WriteAllLines((Join-Path $results 'failures.txt'), $failures, $utf8)
[System.IO.File]::WriteAllLines((Join-Path $results 'skips.txt'), $skips, $utf8)
[System.IO.File]::WriteAllLines((Join-Path $results 'totals.txt'), [string[]]$totals, $utf8)
$table
'TOTAL ' + ($totals -join ' ')
