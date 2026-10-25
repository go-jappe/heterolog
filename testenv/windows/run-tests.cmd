@echo off
setlocal EnableExtensions DisableDelayedExpansion
if "%~1"=="" goto usage
if not "%~2"=="" goto usage

set "RESULTS_DIR=%~f1"
set "PAYLOAD_DIR=%~dp0"
if not exist "%RESULTS_DIR%\" mkdir "%RESULTS_DIR%"
if not exist "%RESULTS_DIR%\" (
    echo Cannot create results directory: "%RESULTS_DIR%" 1>&2
    exit /b 2
)
for %%F in (fsstore.log fsstore.exit-code.txt appendlog.log appendlog.exit-code.txt failurearchive.log failurearchive.exit-code.txt logfile.log logfile.exit-code.txt lifecycle.log lifecycle.exit-code.txt runner.log runner.exit-code.txt checkpoint.log checkpoint.exit-code.txt objectstorage.log objectstorage.exit-code.txt compressedlog.log compressedlog.exit-code.txt heterolog.log heterolog.exit-code.txt recordspool.log recordspool.exit-code.txt recordspoolstore.log recordspoolstore.exit-code.txt logpattern.log logpattern.exit-code.txt memorypressure.log memorypressure.exit-code.txt pressure.log pressure.exit-code.txt path.log path.exit-code.txt overall-exit-code.txt) do (
    if exist "%RESULTS_DIR%\%%F" (
        echo Refusing to overwrite result: "%RESULTS_DIR%\%%F" 1>&2
        exit /b 2
    )
)

rem Keep the working directory writable even when this script is on the ISO.
pushd "%RESULTS_DIR%" || exit /b 2
set "ANY_FAILED=0"
call :run_test fsstore
call :run_test appendlog
call :run_test failurearchive
call :run_test logfile
call :run_test lifecycle
call :run_test runner
call :run_test checkpoint
call :run_test recordspool
call :run_test recordspoolstore
call :run_test objectstorage
call :run_test compressedlog
call :run_test heterolog
call :run_test logpattern
call :run_test memorypressure
call :run_test pressure
call :run_test path
> "%RESULTS_DIR%\overall-exit-code.txt" echo %ANY_FAILED%
if errorlevel 1 set "ANY_FAILED=1"
popd
exit /b %ANY_FAILED%

:run_test
echo Running %~1 tests...
"%PAYLOAD_DIR%%~1.test.exe" -test.v -test.timeout=180s > "%RESULTS_DIR%\%~1.log" 2>&1
set "TEST_EXIT=%ERRORLEVEL%"
> "%RESULTS_DIR%\%~1.exit-code.txt" echo %TEST_EXIT%
if errorlevel 1 set "ANY_FAILED=1"
if not "%TEST_EXIT%"=="0" set "ANY_FAILED=1"
exit /b 0

:usage
echo Usage: %~nx0 WINDOWS_RESULTS_DIRECTORY 1>&2
exit /b 2
