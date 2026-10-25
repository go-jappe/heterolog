param([Parameter(Mandatory = $true)][string]$OutputDirectory)

$ErrorActionPreference = 'Stop'
if (Test-Path -LiteralPath $OutputDirectory) { throw 'Output directory must not already exist' }
$fixture = New-Item -ItemType Directory -Path $OutputDirectory
# Synthetic results exercise inline markers and subtests without running product code.
@(
    '--- PASS: TestPass (0.01s)',
    'logger output without newline--- SKIP: TestSkipped (0.01s)',
    'logger output without newline--- FAIL: TestFailed (0.01s)',
    '    --- PASS: TestPass/child (0.01s)',
    '    --- SKIP: TestPass/skipped_child (0.01s)',
    '    --- FAIL: TestFailed/child (0.01s)'
) | Set-Content -LiteralPath (Join-Path $fixture.FullName 'synthetic.log')
'1' | Set-Content -LiteralPath (Join-Path $fixture.FullName 'synthetic.exit-code.txt')
'1' | Set-Content -LiteralPath (Join-Path $fixture.FullName 'overall-exit-code.txt')
$summary = @(& (Join-Path $PSScriptRoot 'summarize-results.ps1') $fixture.FullName)
foreach ($expected in @('synthetic 1 1 1 1', 'TOTAL 1 1 1', 'OVERALL 1')) {
    if ($summary -notcontains $expected) { throw ('Missing expected summary: ' + $expected) }
}
'SUMMARY_SELF_TEST PASS'
