@echo off
setlocal EnableExtensions DisableDelayedExpansion
if "%~1"=="" goto usage
rem The execution policy applies to this PowerShell process only; no setting is changed.
"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-all-package-tests.ps1" %*
exit /b %ERRORLEVEL%

:usage
echo Usage: %~nx0 -ResultsDirectory NEW_DIR -TempDirectory EXISTING_TEMP_DIR [-ExportDirectory NEW_DIR] [-Package NAME[,NAME...]] [-TestRun REGEX] [-Count N] 1>&2
exit /b 2
