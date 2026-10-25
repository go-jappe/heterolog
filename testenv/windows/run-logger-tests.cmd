@echo off
setlocal EnableExtensions DisableDelayedExpansion
if "%~1"=="" goto usage
if not "%~2"=="" goto usage
set "RESULTS_DIR=%~f1"
set "PAYLOAD_DIR=%~dp0"
if exist "%RESULTS_DIR%" (
    echo Refusing to overwrite existing results: "%RESULTS_DIR%" 1>&2
    exit /b 2
)
if not exist "%PAYLOAD_DIR%config-fixture\go.mod" (
    echo Missing synthetic configuration fixture 1>&2
    exit /b 2
)
mkdir "%RESULTS_DIR%" || exit /b 2
mkdir "%RESULTS_DIR%\temp" || exit /b 2
set "TEMP=%RESULTS_DIR%\temp"
set "TMP=%RESULTS_DIR%\temp"
pushd "%RESULTS_DIR%" || exit /b 2
set "ANY_FAILED=0"
call :run_test fsstore_config
call :run_test appendlog
call :run_test failurearchive
call :run_test file
call :run_test logfile
call :run_test lifecycle
call :run_test runner
call :run_test checkpoint
call :run_test mapped_config
call :run_test config
call :run_test fsarchivehandler
> "%RESULTS_DIR%\overall-exit-code.txt" echo %ANY_FAILED%
echo OVERALL %ANY_FAILED%
popd
exit /b %ANY_FAILED%

:run_test
echo Running %~1 tests...
rem Only config uses the synthetic read-only repository root, never the private .env.
if "%~1"=="config" pushd "%PAYLOAD_DIR%config-fixture"
"%PAYLOAD_DIR%%~1.test.exe" -test.list=. > "%RESULTS_DIR%\%~1.test-names.txt" 2>&1
if not "%ERRORLEVEL%"=="0" set "ANY_FAILED=1"
"%PAYLOAD_DIR%%~1.test.exe" -test.v -test.timeout=600s > "%RESULTS_DIR%\%~1.log" 2>&1
set "TEST_EXIT=%ERRORLEVEL%"
> "%RESULTS_DIR%\%~1.exit-code.txt" echo %TEST_EXIT%
if not "%TEST_EXIT%"=="0" set "ANY_FAILED=1"
if "%~1"=="config" popd
exit /b 0

:usage
echo Usage: %~nx0 NEW_WINDOWS_RESULTS_DIRECTORY 1>&2
exit /b 2
