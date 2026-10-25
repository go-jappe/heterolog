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
mkdir "%RESULTS_DIR%" || exit /b 2
pushd "%RESULTS_DIR%" || exit /b 2
set "ANY_FAILED=0"
rem The new packages run their full suite, then Windows regressions repeat ten times.
for %%P in (logpattern compressedlogspool compressedlogfailure) do call :run_test %%P . 1 %%P-full
for %%P in (logpattern compressedlogspool compressedlogfailure file lifecycle runner fsarchivehandler appendlog checkpoint compressedlog) do call :run_test %%P "Windows|Case|CompressionSourceDeletePendingPathExistsPreservesStatError" 10 %%P-repeat
> overall-exit-code.txt echo %ANY_FAILED%
popd
exit /b %ANY_FAILED%

:run_test
echo Running %~4 tests...
"%PAYLOAD_DIR%%~1.test.exe" -test.v -test.count=%~3 -test.timeout=300s -test.run="%~2" > "%~4.log" 2>&1
set "TEST_EXIT=%ERRORLEVEL%"
> "%~4.exit-code.txt" echo %TEST_EXIT%
if not "%TEST_EXIT%"=="0" set "ANY_FAILED=1"
exit /b 0

:usage
echo Usage: %~nx0 NEW_WINDOWS_RESULTS_DIRECTORY 1>&2
exit /b 2
