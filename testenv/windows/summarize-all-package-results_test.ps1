param([Parameter(Mandatory = $true)][string]$OutputDirectory)

# Exercises summarize-all-package-results.ps1 with synthetic logs only; no product code runs.
$ErrorActionPreference = 'Stop'
if (Test-Path -LiteralPath $OutputDirectory) { throw 'Output directory must not already exist' }
$fixture = (New-Item -ItemType Directory -Path $OutputDirectory).FullName
$logs = (New-Item -ItemType Directory -Path (Join-Path $fixture 'logs')).FullName
$utf8 = New-Object System.Text.UTF8Encoding($false)
function Write-Fixture([string]$name, [string[]]$lines) {
    [System.IO.File]::WriteAllLines((Join-Path $logs $name), $lines, $utf8)
}

# Top-level and subtest results, skip/fail reasons, child-process output that a test
# logged (must not be counted again), and logger output before markers.
Write-Fixture 'pkg-a.log' @(
    '=== RUN   TestPass',
    '--- PASS: TestPass (0.01s)',
    '=== RUN   TestSkipped',
    '    a_test.go:10: requires symlink privilege',
    '--- SKIP: TestSkipped (0.00s)',
    '=== RUN   TestFailed',
    '=== RUN   TestFailed/child',
    '    a_test.go:20: got 1, want 2',
    '=== RUN   TestFailed/skipped_child',
    '    a_test.go:30: unix only',
    '--- FAIL: TestFailed (0.01s)',
    '    --- FAIL: TestFailed/child (0.00s)',
    '    --- SKIP: TestFailed/skipped_child (0.00s)',
    '=== RUN   TestChildProcess',
    '    a_test.go:40: child output:',
    '        === RUN   TestChildProcess',
    '        === RUN   TestChildProcess/zone',
    '        --- PASS: TestChildProcess (0.00s)',
    '            --- PASS: TestChildProcess/zone (0.00s)',
    '        PASS',
    'logger output without newline--- PASS: TestChildProcess (0.02s)',
    '=== RUN   TestInline',
    'logger output without newline=== RUN   TestInline/sub',
    '    --- PASS: TestInline/sub (0.00s)',
    '--- PASS: TestInline (0.00s)',
    'FAIL'
)
Write-Fixture 'pkg-a.exit-code.txt' @('1')
# A timeout lists the running tests in the panic message.
Write-Fixture 'pkg-b.log' @(
    '=== RUN   TestHang',
    'panic: test timed out after 10m0s',
    "`trunning tests:",
    "`t`tTestHang (10m0s)",
    '',
    'goroutine 1 [running]:'
)
Write-Fixture 'pkg-b.exit-code.txt' @('2')
# -test.count repeats the same name.
Write-Fixture 'pkg-c.log' @(
    '=== RUN   TestRepeat',
    '--- PASS: TestRepeat (0.00s)',
    '=== RUN   TestRepeat',
    '--- PASS: TestRepeat (0.00s)',
    'PASS'
)
Write-Fixture 'pkg-c.exit-code.txt' @('0')
# Parallel tests interleave their output: a log line belongs to the test named by the
# latest === CONT or === NAME line, not to the test that ran last.
Write-Fixture 'pkg-d.log' @(
    '=== RUN   TestParallelA',
    '=== PAUSE TestParallelA',
    '=== RUN   TestParallelB',
    '=== PAUSE TestParallelB',
    '=== CONT  TestParallelB',
    '    d_test.go:21: parallel B waits',
    '=== CONT  TestParallelA',
    '    d_test.go:11: parallel A failed',
    '=== NAME  TestParallelB',
    '    d_test.go:22: parallel B skipped',
    '--- FAIL: TestParallelA (0.00s)',
    '--- SKIP: TestParallelB (0.00s)',
    'FAIL'
)
Write-Fixture 'pkg-d.exit-code.txt' @('1')
# The runner killed a hung process tree (taskkill /F leaves exit code 1 and no result line).
Write-Fixture 'pkg-e.log' @('=== RUN   TestSlow')
Write-Fixture 'pkg-e.exit-code.txt' @('1')
# A setup failure (for example a blocked copy) is recorded without a test run.
Write-Fixture 'pkg-f.log' @('RUNNER ERROR: Access to the path is denied.')
Write-Fixture 'pkg-f.exit-code.txt' @('error')
# A panic before any test (for example in init) fails the package without a --- FAIL line.
Write-Fixture 'pkg-g.log' @(
    'panic: init failed',
    '',
    'goroutine 1 [running]:'
)
Write-Fixture 'pkg-g.exit-code.txt' @('2')
Write-Fixture 'overall-exit-code.txt' @('1')
[System.IO.File]::WriteAllLines((Join-Path $fixture 'runs.tsv'), [string[]]@(
    "name`timport_path`texit_code`tseconds`tkilled",
    "pkg-a`texample/a`t1`t1.5`t0",
    "pkg-b`texample/b`t2`t600.0`t0",
    "pkg-c`texample/c`t0`t0.1`t0",
    "pkg-d`texample/d`t1`t0.2`t0",
    "pkg-e`texample/e`t1`t900.0`t1",
    "pkg-f`texample/f`terror`t`t0",
    "pkg-g`texample/g`t2`t0.1`t0"
), $utf8)

$output = @(& (Join-Path $PSScriptRoot 'summarize-all-package-results.ps1') -ResultsDirectory $fixture)
function Assert-Lines([string]$file, [string[]]$expected) {
    $actual = [System.IO.File]::ReadAllLines((Join-Path $fixture $file))
    if (($actual -join "`n") -cne ($expected -join "`n")) {
        throw ('Unexpected ' + $file + ":`n" + ($actual -join "`n"))
    }
}
Assert-Lines 'summary.tsv' @(
    "package`texit_code`tpass`tfail`tskip`tsubtest_pass`tsubtest_fail`tsubtest_skip`ttimed_out`tkilled`tseconds",
    "pkg-a`t1`t3`t1`t1`t1`t1`t1`t0`t0`t1.5",
    "pkg-b`t2`t0`t0`t0`t0`t0`t0`t1`t0`t600.0",
    "pkg-c`t0`t2`t0`t0`t0`t0`t0`t0`t0`t0.1",
    "pkg-d`t1`t0`t1`t1`t0`t0`t0`t0`t0`t0.2",
    "pkg-e`t1`t0`t0`t0`t0`t0`t0`t0`t1`t900.0",
    "pkg-f`terror`t0`t0`t0`t0`t0`t0`t0`t0`t",
    "pkg-g`t2`t0`t0`t0`t0`t0`t0`t0`t0`t0.1"
)
Assert-Lines 'failures.txt' @(
    "pkg-a`tTestFailed`t",
    "pkg-a`tTestFailed/child`ta_test.go:20: got 1, want 2",
    "pkg-b`t(timeout)`tTestHang",
    "pkg-d`tTestParallelA`td_test.go:11: parallel A failed",
    "pkg-e`t(killed)`t",
    "pkg-f`t(exit error without a --- FAIL line)`t",
    "pkg-g`t(exit 2 without a --- FAIL line)`tpanic: init failed"
)
Assert-Lines 'skips.txt' @(
    "pkg-a`tTestSkipped`ta_test.go:10: requires symlink privilege",
    "pkg-a`tTestFailed/skipped_child`ta_test.go:30: unix only",
    "pkg-d`tTestParallelB`td_test.go:22: parallel B skipped"
)
$expectedTotal = 'TOTAL packages=7 ok_packages=1 failed_packages=6 pass=5 fail=2 skip=2 subtest_pass=1 subtest_fail=1 subtest_skip=1'
if ($output -notcontains $expectedTotal) { throw ('Missing expected total: ' + $expectedTotal + "`n" + ($output -join "`n")) }
'ALL_PACKAGE_SUMMARY_SELF_TEST PASS'
