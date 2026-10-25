param(
    [Parameter(Mandatory = $true)][string]$TestMediaDirectory,
    [Parameter(Mandatory = $true)][string]$NoAclTestRoot
)

# NTFS等のTEMPで通常試験を実行し、指定されたACL非対応volumeには独立した一時領域だけを作る。
# 既存volumeのformat・既存fileの削除は行わない。TestMediaDirectoryは今回ビルドしたexeの置き場。
$ErrorActionPreference = 'Stop'
$originalTemp = $env:TEMP
$originalTmp = $env:TMP
$originalNoAclRoot = $env:FSSTORE_NO_ACL_TEST_ROOT
$testRoot = Join-Path $env:TEMP ('noreplace-check-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $testRoot | Out-Null
$summary = Join-Path $testRoot 'summary.txt'
$script:testFailures = 0

function Invoke-TestCase([string]$label, [string]$exe, [string[]]$testArgs) {
    $logPath = Join-Path $testRoot ($label + '.txt')
    $stderrPath = Join-Path $testRoot ($label + '.stderr.txt')
    # 期待した失敗の診断もstderrへ出るため、stderrの有無ではなく終了codeで成否を判断する。
    $startOptions = @{
        FilePath = Join-Path $TestMediaDirectory $exe
        ArgumentList = $testArgs
        Wait = $true
        PassThru = $true
        NoNewWindow = $true
        RedirectStandardOutput = $logPath
        RedirectStandardError = $stderrPath
    }
    $process = Start-Process @startOptions
    $passed = @(Select-String -Path $logPath -Pattern '^--- PASS:').Count
    $failed = @(Select-String -Path $logPath -Pattern '^--- FAIL:').Count
    $skipped = @(Select-String -Path $logPath -Pattern '^\s*--- SKIP:').Count
    $line = "$label exit=$($process.ExitCode) pass=$passed fail=$failed skip=$skipped"
    Add-Content -Path $summary -Value $line
    Write-Host $line
    if ($process.ExitCode -ne 0) { $script:testFailures++ }
}

try {
    $env:TEMP = $testRoot
    $env:TMP = $testRoot
    $env:FSSTORE_NO_ACL_TEST_ROOT = $NoAclTestRoot
    Invoke-TestCase 'fsstore-all' 'fsstore.test.exe' @('-test.v', '-test.timeout=5m')
    Invoke-TestCase 'archive-all' 'archive.test.exe' @('-test.v', '-test.timeout=5m')
    Invoke-TestCase 'spool-all' 'spool.test.exe' @('-test.v', '-test.timeout=5m')
    Invoke-TestCase 'native-ntfs-repeat' 'fsstore.test.exe' @('-test.v', '-test.timeout=5m', '-test.count=10', '-test.run=TestRenameFileNoReplace|TestDirectorySetup')
    $fatTemp = Join-Path $NoAclTestRoot ('noreplace-move-' + [guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $fatTemp | Out-Null
    try {
        $env:TEMP = $fatTemp
        $env:TMP = $fatTemp
        Invoke-TestCase 'native-fat-repeat' 'fsstore.test.exe' @('-test.v', '-test.timeout=5m', '-test.count=10', '-test.run=^TestRenameFileNoReplace(MovesSource|PreservesExistingDestination|MissingSource|ConcurrentPublishers|WindowsLongPath|WindowsCaseAliasCollision)$')
    } finally {
        $env:TEMP = $testRoot
        $env:TMP = $testRoot
        # 試験が片付けたあと、この実行で作った空の親だけを削除する。
        [System.IO.Directory]::Delete($fatTemp, $false)
    }
} finally {
    $env:TEMP = $originalTemp
    $env:TMP = $originalTmp
    $env:FSSTORE_NO_ACL_TEST_ROOT = $originalNoAclRoot
    Write-Host ('RESULTS ' + $testRoot)
}
Write-Host ('FAILED_CASES ' + $script:testFailures)
exit $script:testFailures
