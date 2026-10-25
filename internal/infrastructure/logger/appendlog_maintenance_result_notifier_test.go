package logger

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	loggerhandler "github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
)

func TestAppendLogMaintenanceResultNotifierNotifiesFailedRunAsErrorAndDebugOnly(t *testing.T) {
	var levels []pkglogger.LogLevel
	var sources []string
	levelHandler := func(
		_ context.Context,
		entry *logging.LogEntry,
		_ observer.Tracer,
		_ observer.Meter,
	) {
		levels = append(levels, entry.Level)
		sources = append(sources, entry.Source)
	}
	errorHandler := func(
		_ context.Context,
		_ error,
		entry *logging.LogEntry,
		_ observer.Tracer,
		_ observer.Meter,
	) {
		levels = append(levels, entry.Level)
		sources = append(sources, entry.Source)
	}
	available := handlerStruct{
		OnError: []logging.NamedLoggerErrorHandler{{Name: "stdout", Handler: errorHandler}},
		OnWarn:  []logging.NamedLoggerHandler{{Name: "stdout", Handler: levelHandler}},
		OnInfo:  []logging.NamedLoggerHandler{{Name: "stdout", Handler: levelHandler}},
		OnDebug: []logging.NamedLoggerHandler{{Name: "stdout", Handler: levelHandler}},
	}
	notifier := newAppendLogMaintenanceResultNotifier(
		pkgloggerhandler.HandlerConfig{
			OnErrorConfig: []string{"stdout"},
			OnWarnConfig:  []string{"stdout"},
			OnInfoConfig:  []string{"stdout"},
			OnDebugConfig: []string{"stdout"},
		},
		available,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	maintenanceNotifier, ok := notifier.(*appendLogMaintenanceResultNotifier)
	require.True(t, ok)
	require.NotNil(t, maintenanceNotifier.onWarn)
	report := logfilelifecycle.RunReport{
		ObjectStorageUploads: []logfilelifecycle.ObjectStorageUploadReport{{Path: "archived.log.gz"}},
		Purge: logfilelifecycle.PurgeReport{
			ByAge: logfilelifecycle.PurgeStepReport{
				RemovalResults: []infrafsstore.FileRemovalResult{{
					File: infrafsstore.FileInfo{Path: "old.log"},
					Err:  errors.New("remove failed"),
				}},
			},
		},
	}

	notifier.ReportRun(context.Background(), report, errors.New("lifecycle failure"))

	require.Equal(t, []pkglogger.LogLevel{
		pkglogger.LogLevelError,
		pkglogger.LogLevelDebug,
	}, levels)
	require.Equal(t, []string{
		appendLogMaintenanceRunnerSource,
		appendLogMaintenanceRunnerSource,
	}, sources)
}

func TestAppendLogMaintenanceResultNotifierNotifiesRecoveredPendingSourcesAsWarnAndDebug(
	t *testing.T,
) {
	var (
		levels []pkglogger.LogLevel
		fields map[string]any
	)
	record := func(
		_ context.Context,
		entry *logging.LogEntry,
		_ observer.Tracer,
		_ observer.Meter,
	) {
		levels = append(levels, entry.Level)
		if entry.Level == pkglogger.LogLevelWarn {
			fields = entry.Fields
		}
	}
	notifier := newAppendLogMaintenanceResultNotifier(
		pkgloggerhandler.HandlerConfig{
			OnWarnConfig:  []string{"stdout"},
			OnInfoConfig:  []string{"stdout"},
			OnDebugConfig: []string{"stdout"},
		},
		handlerStruct{
			OnWarn:  []logging.NamedLoggerHandler{{Name: "stdout", Handler: record}},
			OnInfo:  []logging.NamedLoggerHandler{{Name: "stdout", Handler: record}},
			OnDebug: []logging.NamedLoggerHandler{{Name: "stdout", Handler: record}},
		},
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	paths := []string{
		"/var/log/app_20260831_1.log.compression-source-delete-pending.tmp",
		"/var/log/app_20260831_2.log.compression-source-delete-pending.tmp",
	}
	notifier.ReportRun(context.Background(), logfilelifecycle.RunReport{
		RecoveredCompressionSourceDeletePendingPaths: paths,
	}, nil)

	require.Equal(t, []pkglogger.LogLevel{
		pkglogger.LogLevelWarn,
		pkglogger.LogLevelDebug,
	}, levels)
	require.Equal(t, len(paths), fields["recovered_pending_compression_sources"])
	require.Equal(t, paths, fields["recovered_pending_compression_source_paths"])
}

func TestAppendLogMaintenanceResultNotifierSeparatesCheckpointWarningsFromSaveErrors(t *testing.T) {
	warning := errors.New("conflicting saved target; retained accepted upload progress")
	saveErr := errors.New("checkpoint save retry failed")
	for _, test := range []struct {
		name         string
		warning      error
		spoolWarning error
		err          error
		levels       []pkglogger.LogLevel
	}{
		{name: "unchanged success", levels: []pkglogger.LogLevel{pkglogger.LogLevelDebug}},
		{name: "retained accepted progress", warning: warning, levels: []pkglogger.LogLevel{pkglogger.LogLevelWarn, pkglogger.LogLevelDebug}},
		{name: "preserved spool file", spoolWarning: warning, levels: []pkglogger.LogLevel{pkglogger.LogLevelWarn, pkglogger.LogLevelDebug}},
		{name: "save retry failure", err: saveErr, levels: []pkglogger.LogLevel{pkglogger.LogLevelError, pkglogger.LogLevelDebug}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var entries []*logging.LogEntry
			var reportedError error
			record := func(_ context.Context, entry *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
				entries = append(entries, entry)
			}
			notifier := newAppendLogMaintenanceResultNotifier(
				pkgloggerhandler.HandlerConfig{
					OnErrorConfig: []string{"stdout"}, OnWarnConfig: []string{"stdout"},
					OnInfoConfig: []string{"stdout"}, OnDebugConfig: []string{"stdout"},
				},
				handlerStruct{
					OnError: []logging.NamedLoggerErrorHandler{{Name: "stdout", Handler: func(
						_ context.Context, err error, entry *logging.LogEntry, _ observer.Tracer, _ observer.Meter,
					) {
						reportedError = err
						entries = append(entries, entry)
					}}},
					OnWarn:  []logging.NamedLoggerHandler{{Name: "stdout", Handler: record}},
					OnInfo:  []logging.NamedLoggerHandler{{Name: "stdout", Handler: record}},
					OnDebug: []logging.NamedLoggerHandler{{Name: "stdout", Handler: record}},
				},
				nil, observer.NopTracer{}, observer.NopMeter{},
			)
			report := logfilelifecycle.RunReport{DirectoryPath: "/var/log/app", CheckpointWarning: test.warning, SpoolWarning: test.spoolWarning}
			notifier.ReportRun(context.Background(), report, test.err)
			var levels []pkglogger.LogLevel
			for _, entry := range entries {
				levels = append(levels, entry.Level)
				require.Equal(t, appendLogMaintenanceRunnerSource, entry.Source)
				require.True(t, entry.Internal)
			}
			require.Equal(t, test.levels, levels)
			if test.err != nil {
				require.ErrorIs(t, reportedError, test.err)
				require.ErrorIs(t, entries[0].Err, test.err)
			} else {
				require.NoError(t, reportedError, "an accepted checkpoint warning must not become OnError")
			}
			if test.warning != nil {
				require.ErrorIs(t, entries[0].Err, warning)
				require.Contains(t, entries[0].Message, "upload checkpoint warning")
				// 通知のfieldは利用者が読む出力なので、警告の文言を値として記録します。
				for _, entry := range entries {
					require.Equal(t, map[string]any{
						"directory_path": "/var/log/app", "checkpoint_warning": warning.Error(),
					}, entry.Fields, "a standalone warning must not report unrelated lifecycle counts")
				}
				require.False(t, appendLogMaintenanceRunChangedFiles(report))
			}
			if test.spoolWarning != nil {
				require.ErrorIs(t, entries[0].Err, warning)
				require.Contains(t, entries[0].Message, "spool preservation warning")
				for _, entry := range entries {
					require.Equal(t, map[string]any{
						"directory_path": "/var/log/app", "spool_warning": warning.Error(),
					}, entry.Fields)
				}
				require.False(t, appendLogMaintenanceRunChangedFiles(report))
			}
		})
	}
}

func TestAppendLogMaintenanceRunClassifiesUnifiedRemovalResults(t *testing.T) {
	failure := infrafsstore.FileRemovalResult{
		File: infrafsstore.FileInfo{Path: "failed.log"},
		Err:  errors.New("remove failed"),
	}
	success := infrafsstore.FileRemovalResult{
		File: infrafsstore.FileInfo{Path: "already-absent.log"},
	}
	report := logfilelifecycle.RunReport{
		Purge: logfilelifecycle.PurgeReport{
			ByAge: logfilelifecycle.PurgeStepReport{
				RemovalResults: []infrafsstore.FileRemovalResult{failure, success},
			},
		},
	}

	require.True(t, appendLogMaintenanceRunChangedFiles(report))
	fields := appendLogMaintenanceRunReportFields(report)
	require.Equal(t, 1, fields["purged_by_age_files"])
	require.Equal(t, 1, fields["purge_by_age_failures"])

	report.Purge.ByAge.RemovalResults = []infrafsstore.FileRemovalResult{failure}
	require.False(t, appendLogMaintenanceRunChangedFiles(report))
}

func TestAppendLogMaintenanceRunReportsUploadedCompressedFileRemoval(t *testing.T) {
	success := infrafsstore.FileRemovalResult{
		File: infrafsstore.FileInfo{Path: "uploaded.log.gz"},
	}
	failure := infrafsstore.FileRemovalResult{
		File: infrafsstore.FileInfo{Path: "failed.log.gz"},
		Err:  errors.New("remove failed"),
	}
	report := logfilelifecycle.RunReport{
		UploadedFileRemovals: []infrafsstore.FileRemovalResult{success, failure},
	}

	require.True(t, appendLogMaintenanceRunChangedFiles(report))
	fields := appendLogMaintenanceRunReportFields(report)
	require.Equal(t, 1, fields["removed_uploaded_compressed_files"])
	require.Equal(t, 1, fields["uploaded_compressed_remove_failures"])

	report.UploadedFileRemovals = []infrafsstore.FileRemovalResult{failure}
	require.False(t, appendLogMaintenanceRunChangedFiles(report))
}

func TestAppendLogMaintenanceResultNotifierKeepsPostRemovalErrorSeparate(t *testing.T) {
	for _, operation := range []string{"directory sync", "directory close"} {
		t.Run(operation, func(t *testing.T) {
			postRemovalErr := errors.New(operation + " failed")
			var (
				entries []*logging.LogEntry
				gotErr  error
			)
			record := func(
				_ context.Context,
				entry *logging.LogEntry,
				_ observer.Tracer,
				_ observer.Meter,
			) {
				entries = append(entries, entry)
			}
			notifier := newAppendLogMaintenanceResultNotifier(
				pkgloggerhandler.HandlerConfig{
					OnErrorConfig: []string{"stdout"},
					OnInfoConfig:  []string{"stdout"},
					OnDebugConfig: []string{"stdout"},
				},
				handlerStruct{
					OnError: []logging.NamedLoggerErrorHandler{{
						Name: "stdout",
						Handler: func(
							_ context.Context,
							err error,
							entry *logging.LogEntry,
							_ observer.Tracer,
							_ observer.Meter,
						) {
							gotErr = err
							entries = append(entries, entry)
						},
					}},
					OnInfo:  []logging.NamedLoggerHandler{{Name: "stdout", Handler: record}},
					OnDebug: []logging.NamedLoggerHandler{{Name: "stdout", Handler: record}},
				},
				nil,
				observer.NopTracer{},
				observer.NopMeter{},
			)
			report := logfilelifecycle.RunReport{
				UploadedFileRemovals: []infrafsstore.FileRemovalResult{{
					File: infrafsstore.FileInfo{Path: "uploaded.log.gz"},
				}},
			}

			notifier.ReportRun(context.Background(), report, postRemovalErr)

			require.ErrorIs(t, gotErr, postRemovalErr)
			require.Len(t, entries, 2)
			require.Equal(t, pkglogger.LogLevelError, entries[0].Level)
			require.ErrorIs(t, entries[0].Err, postRemovalErr)
			require.Equal(t, pkglogger.LogLevelDebug, entries[1].Level)
			for _, entry := range entries {
				require.Equal(t, 1, entry.Fields["removed_uploaded_compressed_files"])
				require.Equal(t, 0, entry.Fields["uploaded_compressed_remove_failures"])
			}
		})
	}
}

func TestAppendLogMaintenanceResultNotifierWritesToInternalLogFileDestination(t *testing.T) {
	root := t.TempDir()
	internalLogFile := newActivationFileConfig(t, "lifecycle-internal-log")
	internalLogFile.FsstoreConfig.Layout.LogPath = root
	internalLogFile.FsstoreConfig.Layout.LogDir = "lifecycle_logs"
	internalLogFile.FsstoreConfig.Layout.ErrorLogDir = "lifecycle_errors"
	internalLogFile.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
	internalLogFile.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
	internalDestinations, err := newInternalLogDestinationRuntime(
		pkglogger.LoggerConfig{
			InternalLogDestinations: &pkglogger.InternalLogDestinationConfig{
				File: internalLogFile,
			},
		},
		streamrouting.None,
		nil,
		syncInternalLogFileDirectoryScopeLabel,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)

	available := internalDestinations.newHandlers()
	selected := selectConfiguredHandlers(
		available,
		loggerhandler.NewConfigAdapter(pkgloggerhandler.HandlerConfig{
			OnInfoConfig: []string{"file"},
		}),
	)
	require.Len(t, selected.OnInfo, 1)
	require.Equal(t, "file", selected.OnInfo[0].Name)

	notifier := newAppendLogMaintenanceResultNotifier(
		pkgloggerhandler.HandlerConfig{
			OnInfoConfig: []string{"file"},
		},
		available,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	notifier.ReportRun(context.Background(), logfilelifecycle.RunReport{
		ObjectStorageUploads: []logfilelifecycle.ObjectStorageUploadReport{{Path: "archived.log.gz"}},
	}, nil)
	require.NoError(t, internalDestinations.Close())

	files, err := filepath.Glob(filepath.Join(
		root,
		"lifecycle-internal-log",
		"lifecycle_logs",
		"lifecycle-internal-log_*.log",
	))
	require.NoError(t, err)
	require.Len(t, files, 1)
	body, err := os.ReadFile(files[0])
	require.NoError(t, err)
	require.Contains(t, string(body), "appendlog maintenance runner completed")
}

func TestValidateAppendLogMaintenanceResultNotifierSelectorsRejectsQualifiedBackend(t *testing.T) {
	err := validateAppendLogMaintenanceResultNotifierSelectors(
		pkgloggerhandler.HandlerConfig{
			OnErrorConfig: []string{"file:maintenance"},
		},
		handlerStruct{
			OnError: []logging.NamedLoggerErrorHandler{{
				Name: "file",
				Handler: func(
					context.Context,
					error,
					*logging.LogEntry,
					observer.Tracer,
					observer.Meter,
				) {
				},
			}},
		},
	)

	var selectorErr *AppendLogMaintenanceHandlerSelectorError
	require.ErrorAs(t, err, &selectorErr)
	require.Equal(t, "error", selectorErr.Level)
	require.Equal(t, "file:maintenance", selectorErr.Selector)
}

func TestDeliveryAppendLogMaintenanceResultNotifierWritesToInternalLogFileDestination(t *testing.T) {
	root := t.TempDir()
	internalLogFile := newCloseOwnershipDeliveryFileConfig(
		root,
		"delivery-lifecycle-internal-log",
	)
	internalDestinations, err := newDeliveryInternalLogDestinationRuntime(
		pkglogger.DeliveryLoggerConfig{
			InternalLogDestinations: &pkglogger.DeliveryInternalLogDestinationConfig{
				File: internalLogFile,
			},
		},
		streamrouting.None,
		nil,
		deliveryInternalLogFileDirectoryScopeLabel,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)

	notifier := newAppendLogMaintenanceResultNotifier(
		pkgloggerhandler.HandlerConfig{
			OnInfoConfig: []string{"file"},
		},
		internalDestinations.newHandlers(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	notifier.ReportRun(context.Background(), logfilelifecycle.RunReport{
		Compressed: []logfilelifecycle.CompressionReport{{
			SourcePath:     "rotated.log",
			CompressedPath: "rotated.log.gz",
		}},
	}, nil)
	require.NoError(t, internalDestinations.Close())

	files, err := filepath.Glob(filepath.Join(
		root,
		"delivery-lifecycle-internal-log",
		"logs",
		"delivery-lifecycle-internal-log_*.log",
	))
	require.NoError(t, err)
	require.Len(t, files, 1)
	body, err := os.ReadFile(files[0])
	require.NoError(t, err)
	require.Contains(t, string(body), "appendlog maintenance runner completed")
}
