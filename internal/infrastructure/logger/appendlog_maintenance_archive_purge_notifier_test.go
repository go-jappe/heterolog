package logger

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

func TestAppendLogMaintenanceResultNotifierNotifiesUploadFailureArchivePurge(t *testing.T) {
	purgeErr := errors.New("cannot remove retryable archive")
	disabledFields := map[string]any{
		"enabled": false, "scanned_entry_count": 0, "scanned_bytes": uint64(0),
		"deleted_file_count": 0, "deleted_bytes": uint64(0),
		"remaining_entry_count": 0, "remaining_bytes": uint64(0),
		"retained_unsafe_entry_count": 0, "capacity_limit_exceeded": false,
		"errors": []string{},
	}
	tests := []struct {
		name                         string
		report                       compressedlogarchive.PurgeReport
		err                          error
		wantLevels                   []pkglogger.LogLevel
		wantResultsByFailureCategory map[string]any
	}{
		{
			name: "success with deleted files",
			report: compressedlogarchive.PurgeReport{
				PreservedFileCount: 4,
				ByFailureCategory: map[uploader.FailureCategory]compressedlogarchive.FailureCategoryPurgeReport{
					uploader.FailureCategoryRetryable: {
						Enabled: true, ScannedEntryCount: 3, ScannedBytes: 123,
						RemainingEntryCount: 3, RemainingBytes: 123,
					},
					uploader.FailureCategoryPermanent: {
						Enabled: true, ScannedEntryCount: 9, ScannedBytes: 987,
						DeletedFileCount: 2, DeletedBytes: 321,
						RemainingEntryCount: 7, RemainingBytes: 666,
					},
				},
			},
			wantLevels: []pkglogger.LogLevel{pkglogger.LogLevelInfo, pkglogger.LogLevelDebug},
			wantResultsByFailureCategory: map[string]any{
				"retryable": map[string]any{
					"enabled": true, "scanned_entry_count": 3, "scanned_bytes": uint64(123),
					"deleted_file_count": 0, "deleted_bytes": uint64(0),
					"remaining_entry_count": 3, "remaining_bytes": uint64(123),
					"retained_unsafe_entry_count": 0, "capacity_limit_exceeded": false,
					"errors": []string{},
				},
				"permanent": map[string]any{
					"enabled": true, "scanned_entry_count": 9, "scanned_bytes": uint64(987),
					"deleted_file_count": 2, "deleted_bytes": uint64(321),
					"remaining_entry_count": 7, "remaining_bytes": uint64(666),
					"retained_unsafe_entry_count": 0, "capacity_limit_exceeded": false,
					"errors": []string{},
				},
			},
		},
		{
			name: "unchanged with capacity limit exceeded",
			report: compressedlogarchive.PurgeReport{
				ByFailureCategory: map[uploader.FailureCategory]compressedlogarchive.FailureCategoryPurgeReport{
					uploader.FailureCategoryRetryable: {
						Enabled: true, ScannedEntryCount: 4, ScannedBytes: 456,
						RemainingEntryCount: 4, RemainingBytes: 456,
						CapacityLimitExceeded: true,
					},
				},
			},
			wantLevels: []pkglogger.LogLevel{pkglogger.LogLevelDebug},
			wantResultsByFailureCategory: map[string]any{
				"retryable": map[string]any{
					"enabled": true, "scanned_entry_count": 4, "scanned_bytes": uint64(456),
					"deleted_file_count": 0, "deleted_bytes": uint64(0),
					"remaining_entry_count": 4, "remaining_bytes": uint64(456),
					"retained_unsafe_entry_count": 0, "capacity_limit_exceeded": true,
					"errors": []string{},
				},
			},
		},
		{
			name: "error preserves partial results",
			report: compressedlogarchive.PurgeReport{
				ByFailureCategory: map[uploader.FailureCategory]compressedlogarchive.FailureCategoryPurgeReport{
					uploader.FailureCategoryRetryable: {
						Enabled: true, ScannedEntryCount: 8, ScannedBytes: 765,
						DeletedFileCount: 3, DeletedBytes: 234,
						RemainingEntryCount: 5, RemainingBytes: 531,
						RetainedUnsafeEntryCount: 2, CapacityLimitExceeded: true,
						Errors: []error{purgeErr, errors.New("unsafe archive entry")},
					},
				},
			},
			err:        purgeErr,
			wantLevels: []pkglogger.LogLevel{pkglogger.LogLevelError, pkglogger.LogLevelDebug},
			wantResultsByFailureCategory: map[string]any{
				"retryable": map[string]any{
					"enabled": true, "scanned_entry_count": 8, "scanned_bytes": uint64(765),
					"deleted_file_count": 3, "deleted_bytes": uint64(234),
					"remaining_entry_count": 5, "remaining_bytes": uint64(531),
					"retained_unsafe_entry_count": 2, "capacity_limit_exceeded": true,
					"errors": []string{"cannot remove retryable archive", "unsafe archive entry"},
				},
			},
		},
		{
			name: "disabled",
			report: compressedlogarchive.PurgeReport{
				ByFailureCategory: map[uploader.FailureCategory]compressedlogarchive.FailureCategoryPurgeReport{
					uploader.FailureCategoryRetryable: {},
					uploader.FailureCategoryPermanent: {},
				},
			},
			wantLevels: []pkglogger.LogLevel{pkglogger.LogLevelDebug},
			wantResultsByFailureCategory: map[string]any{
				"retryable": disabledFields,
				"permanent": disabledFields,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var entries []*logging.LogEntry
			record := func(
				_ context.Context,
				entry *logging.LogEntry,
				_ pkgobserver.Tracer,
				_ pkgobserver.Meter,
			) {
				entries = append(entries, entry)
			}
			notifier := newAppendLogMaintenanceResultNotifier(
				pkgloggerhandler.HandlerConfig{
					OnErrorConfig: []string{"stdout"},
					OnWarnConfig:  []string{"stdout"},
					OnInfoConfig:  []string{"stdout"},
					OnDebugConfig: []string{"stdout"},
				},
				handlerStruct{
					OnError: []logging.NamedLoggerErrorHandler{{Name: "stdout", Handler: func(
						ctx context.Context,
						err error,
						entry *logging.LogEntry,
						tracer pkgobserver.Tracer,
						meter pkgobserver.Meter,
					) {
						require.ErrorIs(t, err, test.err)
						require.ErrorIs(t, entry.Err, test.err)
						record(ctx, entry, tracer, meter)
					}}},
					OnWarn:  []logging.NamedLoggerHandler{{Name: "stdout", Handler: record}},
					OnInfo:  []logging.NamedLoggerHandler{{Name: "stdout", Handler: record}},
					OnDebug: []logging.NamedLoggerHandler{{Name: "stdout", Handler: record}},
				},
				nil,
				observer.NopTracer{},
				observer.NopMeter{},
			)
			directory := t.TempDir()
			notifier.ReportRun(context.Background(), logfilelifecycle.RunReport{
				DirectoryPath:             directory,
				UploadFailureArchivePurge: &test.report,
			}, test.err)

			require.Len(t, entries, len(test.wantLevels))
			for i, entry := range entries {
				require.Equal(t, test.wantLevels[i], entry.Level)
				require.Equal(t, appendLogMaintenanceRunnerSource, entry.Source)
				require.True(t, entry.Internal)
				// archiveの保守処理は独自の結果を持ち、zero値のappendlog counterを含みません。
				require.Equal(t, map[string]any{
					"directory_path": directory,
					"upload_failure_archive_purge": map[string]any{
						"preserved_file_count": test.report.PreservedFileCount,
						"by_failure_category":  test.wantResultsByFailureCategory,
					},
				}, entry.Fields)
			}
		})
	}
}
