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
whoami /groups > integrity.txt
set "ANY_FAILED=0"
rem Run in a normal command prompt; no administrator rights are required.
rem Only the scoped regressions are repeated; run-tests.cmd runs the full filesystem suites.
set "TEST_PATTERN=PathIdentity|Windows|Private|Permissions|(Host|Filesystem).*CaseRules|EnsureDirectory|DirectoryCreation|DirectoryGuard|DirectoryState|ArchiveCheckpointSourceID|FilesystemStoreRecordsActualSuccessesWithoutDestinationSnapshot|FilesystemCheckpointStoreSeparatesSourceDestinations|PurgeByAgeIfNeededKeepsStatFailure|PrepareArchiveCheckpointDirectory|RejectsMissingCheckpointDirectory|DoesNotRecreateRemovedDirectoryWhenSaving|FilesystemCheckpointStoreCommitReturnsPermissionErrorWithAcceptedMemoryCursor|ArchiveDurability|UnconfirmedDurability|DurabilityUnconfirmed"
for %%P in (fsstore appendlog lifecycle runner checkpoint recordspool recordspoolstore pathidentity directorylease config file fsarchivehandler objectstorage compressedlog) do call :run_test %%P
> overall-exit-code.txt echo %ANY_FAILED%
popd
exit /b %ANY_FAILED%

:run_test
echo Running scoped %~1 regressions...
set "PACKAGE_PATTERN=%TEST_PATTERN%"
if "%~1"=="pathidentity" set "PACKAGE_PATTERN=."
rem Include regular-record spool file and directory durability regressions.
if "%~1"=="recordspool" set "PACKAGE_PATTERN=."
if "%~1"=="recordspoolstore" set "PACKAGE_PATTERN=."
if "%~1"=="directorylease" set "PACKAGE_PATTERN=."
if "%~1"=="objectstorage" set "PACKAGE_PATTERN=TestStorePublishesAfterPreviousLayoutParentSyncFailure"
if "%~1"=="compressedlog" set "PACKAGE_PATTERN=TestStorePublishesAfterPreviousLayoutParentSyncFailure"
"%PAYLOAD_DIR%%~1.test.exe" -test.v -test.count=10 -test.timeout=180s -test.run="%PACKAGE_PATTERN%" > "%~1.log" 2>&1
set "TEST_EXIT=%ERRORLEVEL%"
> "%~1.exit-code.txt" echo %TEST_EXIT%
if not "%TEST_EXIT%"=="0" set "ANY_FAILED=1"
exit /b 0

:usage
echo Usage: %~nx0 NEW_WINDOWS_RESULTS_DIRECTORY 1>&2
exit /b 2
