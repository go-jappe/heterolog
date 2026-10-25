param([Parameter(Mandatory = $true)][string]$ResultsDirectory)

$ErrorActionPreference = 'Stop'
$directory = Get-Item -LiteralPath $ResultsDirectory
if (-not $directory.PSIsContainer) { throw 'Results path must be a directory' }

# Read logs inside the guest and display aggregate counts only. This script does
# not copy raw logs to the host or include their contents in the summary.
$totalPass = 0
$totalFail = 0
$totalSkip = 0
'PACKAGE PASS FAIL SKIP EXIT'
foreach ($log in Get-ChildItem -LiteralPath $directory.FullName -Filter '*.log' -File | Sort-Object Name) {
    $lines = Get-Content -LiteralPath $log.FullName
    # Logger output can precede the test marker on the same line. Match the
    # marker anywhere, then exclude subtests by their slash-qualified name.
    $results = @($lines | ForEach-Object {
        foreach ($result in [regex]::Matches($_, '--- (PASS|FAIL|SKIP): ((?:Test|Example|Fuzz)[^\s]*) \(')) {
            if (-not $result.Groups[2].Value.Contains('/')) { $result.Groups[1].Value }
        }
    })
    $pass = @($results | Where-Object { $_ -eq 'PASS' }).Count
    $fail = @($results | Where-Object { $_ -eq 'FAIL' }).Count
    $skip = @($results | Where-Object { $_ -eq 'SKIP' }).Count
    $exitPath = Join-Path $directory.FullName ($log.BaseName + '.exit-code.txt')
    $exitCode = (Get-Content -LiteralPath $exitPath -Raw).Trim()
    '{0} {1} {2} {3} {4}' -f $log.BaseName, $pass, $fail, $skip, $exitCode
    $totalPass += $pass
    $totalFail += $fail
    $totalSkip += $skip
}
'TOTAL {0} {1} {2}' -f $totalPass, $totalFail, $totalSkip
$overall = (Get-Content -LiteralPath (Join-Path $directory.FullName 'overall-exit-code.txt') -Raw).Trim()
'OVERALL ' + $overall
