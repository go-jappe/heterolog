package fsarchivehandler

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

func TestFailureArchiveLifecycleLogReporterSeparatesCheckpointWarningsFromSaveErrors(t *testing.T) {
	warning := errors.New("conflicting saved target; retained accepted upload progress")
	saveErr := errors.New("checkpoint save retry failed")
	for _, tc := range []struct {
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
		t.Run(tc.name, func(t *testing.T) {
			var entries []*logging.LogEntry
			var reportedErr error
			record := func(_ context.Context, entry *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
				entries = append(entries, entry)
			}
			pipeline := newTestArchiveHandler(t, time.Now(), nil, InternalLogHandlers{
				OnError: func(_ context.Context, err error, entry *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
					reportedErr = err
					entries = append(entries, entry)
				},
				OnWarn: record, OnInfo: record, OnDebug: record,
			})
			reporter := newFailureArchiveLifecycleLogReporter(pipeline.handler, nil)
			report := logfilelifecycle.RunReport{DirectoryPath: "/failure/archive/retryable", CheckpointWarning: tc.warning, SpoolWarning: tc.spoolWarning}
			reporter.ReportRun(context.Background(), report, tc.err)

			var levels []pkglogger.LogLevel
			for _, entry := range entries {
				levels = append(levels, entry.Level)
				require.Equal(t, failureArchiveLifecycleSource, entry.Source)
				require.True(t, entry.Internal)
			}
			require.Equal(t, tc.levels, levels)
			if tc.err != nil {
				require.ErrorIs(t, reportedErr, tc.err)
				require.ErrorIs(t, entries[0].Err, tc.err)
			} else {
				require.NoError(t, reportedErr, "an accepted checkpoint warning must not become OnError")
			}
			if tc.warning != nil {
				require.ErrorIs(t, entries[0].Err, warning)
				require.Contains(t, entries[0].Message, "upload checkpoint warning")
				for _, entry := range entries {
					require.Equal(t, map[string]any{
						"directory_path": report.DirectoryPath, "checkpoint_warning": warning.Error(),
					}, entry.Fields, "standalone warning entries must not fabricate lifecycle counts")
				}
				require.False(t, failureArchiveLifecycleChangedFiles(report))
			} else if tc.spoolWarning != nil {
				require.ErrorIs(t, entries[0].Err, warning)
				require.Contains(t, entries[0].Message, "spool preservation warning")
				for _, entry := range entries {
					require.Equal(t, map[string]any{
						"directory_path": report.DirectoryPath, "spool_warning": warning.Error(),
					}, entry.Fields)
				}
				require.False(t, failureArchiveLifecycleChangedFiles(report))
			} else {
				for _, entry := range entries {
					require.NotContains(t, entry.Fields, "checkpoint_warning")
					require.Contains(t, entry.Fields, "scanned_files")
				}
			}
		})
	}
}

func TestFailureArchiveLifecycleLogReporterReportsFailedRunAsErrorAndDebugOnly(t *testing.T) {
	runErr := errors.New("runner failed")
	var (
		levels  []pkglogger.LogLevel
		sources []string
		gotErr  error
	)
	recordEntry := func(entry *logging.LogEntry) {
		levels = append(levels, entry.Level)
		sources = append(sources, entry.Source)
		if got := entry.Fields["directory_path"]; got != "/failure/archive/retryable" {
			t.Fatalf("directory_path = %v, want retryable archive directory", got)
		}
		if got := entry.Fields["uploaded_files"]; got != 1 {
			t.Fatalf("uploaded_files = %v, want 1", got)
		}
	}
	handler := newTestArchiveHandler(
		t,
		time.Now(),
		nil,
		InternalLogHandlers{
			OnError: func(
				_ context.Context,
				err error,
				entry *logging.LogEntry,
				_ pkgobserver.Tracer,
				_ pkgobserver.Meter,
			) {
				gotErr = err
				recordEntry(entry)
			},
			OnWarn: func(
				_ context.Context,
				entry *logging.LogEntry,
				_ pkgobserver.Tracer,
				_ pkgobserver.Meter,
			) {
				recordEntry(entry)
			},
			OnInfo: func(
				_ context.Context,
				entry *logging.LogEntry,
				_ pkgobserver.Tracer,
				_ pkgobserver.Meter,
			) {
				recordEntry(entry)
			},
			OnDebug: func(
				_ context.Context,
				entry *logging.LogEntry,
				_ pkgobserver.Tracer,
				_ pkgobserver.Meter,
			) {
				recordEntry(entry)
			},
		},
	)

	reporter := newFailureArchiveLifecycleLogReporter(handler.handler, nil)
	reporter.ReportRun(context.Background(), logfilelifecycle.RunReport{
		DirectoryPath:        "/failure/archive/retryable",
		ObjectStorageUploads: []logfilelifecycle.ObjectStorageUploadReport{{Path: "archive.jsonl.gz"}},
		Purge: logfilelifecycle.PurgeReport{
			ByAge: logfilelifecycle.PurgeStepReport{
				RemovalResults: []infrafsstore.FileRemovalResult{{
					File: infrafsstore.FileInfo{Path: "old.jsonl"},
					Err:  errors.New("remove failed"),
				}},
			},
		},
	}, runErr)

	if !errors.Is(gotErr, runErr) {
		t.Fatalf("delegated error = %v, want wrapping %v", gotErr, runErr)
	}
	wantLevels := []pkglogger.LogLevel{
		pkglogger.LogLevelError,
		pkglogger.LogLevelDebug,
	}
	if len(levels) != len(wantLevels) {
		t.Fatalf("delegated levels = %v, want %v", levels, wantLevels)
	}
	for index := range wantLevels {
		if levels[index] != wantLevels[index] {
			t.Fatalf("delegated levels = %v, want %v", levels, wantLevels)
		}
		if sources[index] != failureArchiveLifecycleSource {
			t.Fatalf("entry source[%d] = %q, want %q", index, sources[index], failureArchiveLifecycleSource)
		}
	}
}

func TestFailureArchiveLifecycleClassifiesUnifiedRemovalResults(t *testing.T) {
	failure := infrafsstore.FileRemovalResult{
		File: infrafsstore.FileInfo{Path: "failed.jsonl"},
		Err:  errors.New("remove failed"),
	}
	success := infrafsstore.FileRemovalResult{
		File: infrafsstore.FileInfo{Path: "already-absent.jsonl"},
	}
	report := logfilelifecycle.RunReport{
		Purge: logfilelifecycle.PurgeReport{
			ByAge: logfilelifecycle.PurgeStepReport{
				RemovalResults: []infrafsstore.FileRemovalResult{failure, success},
			},
		},
	}

	if !failureArchiveLifecycleChangedFiles(report) {
		t.Fatal("successful removal result was not reported as a file change")
	}
	fields := failureArchiveLifecycleReportFields(report)
	if fields["purged_by_age_files"] != 1 || fields["purge_by_age_failures"] != 1 {
		t.Fatalf("removal outcome fields = %+v, want one success and one failure", fields)
	}

	report.Purge.ByAge.RemovalResults = []infrafsstore.FileRemovalResult{failure}
	if failureArchiveLifecycleChangedFiles(report) {
		t.Fatal("failure-only removal results were reported as a file change")
	}
}

func TestFailureArchiveLifecycleReportsUploadedCompressedFileRemoval(t *testing.T) {
	success := infrafsstore.FileRemovalResult{
		File: infrafsstore.FileInfo{Path: "uploaded.jsonl.gz"},
	}
	failure := infrafsstore.FileRemovalResult{
		File: infrafsstore.FileInfo{Path: "failed.jsonl.gz"},
		Err:  errors.New("remove failed"),
	}
	report := logfilelifecycle.RunReport{
		UploadedFileRemovals: []infrafsstore.FileRemovalResult{success, failure},
	}

	if !failureArchiveLifecycleChangedFiles(report) {
		t.Fatal("successful uploaded compressed file removal was not reported as a change")
	}
	fields := failureArchiveLifecycleReportFields(report)
	if fields["removed_uploaded_compressed_files"] != 1 ||
		fields["uploaded_compressed_remove_failures"] != 1 {
		t.Fatalf("uploaded compressed removal fields = %+v, want one success and one failure", fields)
	}

	report.UploadedFileRemovals = []infrafsstore.FileRemovalResult{failure}
	if failureArchiveLifecycleChangedFiles(report) {
		t.Fatal("failure-only uploaded compressed removal was reported as a file change")
	}
}

func TestFailureArchiveLifecycleLogReporterKeepsPostRemovalErrorSeparate(t *testing.T) {
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
				_ pkgobserver.Tracer,
				_ pkgobserver.Meter,
			) {
				entries = append(entries, entry)
			}
			handler := newTestArchiveHandler(
				t,
				time.Now(),
				nil,
				InternalLogHandlers{
					OnError: func(
						_ context.Context,
						err error,
						entry *logging.LogEntry,
						_ pkgobserver.Tracer,
						_ pkgobserver.Meter,
					) {
						gotErr = err
						entries = append(entries, entry)
					},
					OnInfo:  record,
					OnDebug: record,
				},
			)
			reporter := newFailureArchiveLifecycleLogReporter(handler.handler, nil)
			report := logfilelifecycle.RunReport{
				UploadedFileRemovals: []infrafsstore.FileRemovalResult{{
					File: infrafsstore.FileInfo{Path: "uploaded.jsonl.gz"},
				}},
			}

			reporter.ReportRun(context.Background(), report, postRemovalErr)

			if !errors.Is(gotErr, postRemovalErr) {
				t.Fatalf("delegated error = %v, want wrapping %v", gotErr, postRemovalErr)
			}
			if len(entries) != 2 {
				t.Fatalf("delegated entries = %v, want error and debug", entries)
			}
			if entries[0].Level != pkglogger.LogLevelError || entries[1].Level != pkglogger.LogLevelDebug {
				t.Fatalf("delegated levels = %v, %v, want error and debug", entries[0].Level, entries[1].Level)
			}
			if !errors.Is(entries[0].Err, postRemovalErr) {
				t.Fatalf("entry error = %v, want wrapping %v", entries[0].Err, postRemovalErr)
			}
			for _, entry := range entries {
				if entry.Fields["removed_uploaded_compressed_files"] != 1 ||
					entry.Fields["uploaded_compressed_remove_failures"] != 0 {
					t.Fatalf("uploaded compressed removal fields = %+v, want one success and no failures", entry.Fields)
				}
			}
		})
	}
}

func TestFailureArchiveLifecycleLogReporterClassifiesSuccessfulRunByFileChange(t *testing.T) {
	var levels []pkglogger.LogLevel
	record := func(
		_ context.Context,
		entry *logging.LogEntry,
		_ pkgobserver.Tracer,
		_ pkgobserver.Meter,
	) {
		levels = append(levels, entry.Level)
	}
	handler := newTestArchiveHandler(
		t,
		time.Now(),
		nil,
		InternalLogHandlers{OnWarn: record, OnInfo: record, OnDebug: record},
	)
	reporter := newFailureArchiveLifecycleLogReporter(handler.handler, nil)

	reporter.ReportRun(context.Background(), logfilelifecycle.RunReport{
		Compressed: []logfilelifecycle.CompressionReport{{
			SourcePath:     "archive.jsonl",
			CompressedPath: "archive.jsonl.gz",
		}},
	}, nil)
	if !reflect.DeepEqual(levels, []pkglogger.LogLevel{
		pkglogger.LogLevelInfo,
		pkglogger.LogLevelDebug,
	}) {
		t.Fatalf("changed run levels = %v, want info and debug", levels)
	}

	levels = nil
	reporter.ReportRun(context.Background(), logfilelifecycle.RunReport{}, nil)
	if !reflect.DeepEqual(levels, []pkglogger.LogLevel{pkglogger.LogLevelDebug}) {
		t.Fatalf("unchanged run levels = %v, want debug only", levels)
	}

	levels = nil
	paths := []string{
		"/failure/archive/retryable_20260831_1.jsonl.compression-source-delete-pending.tmp",
	}
	recoveryReport := logfilelifecycle.RunReport{
		RecoveredCompressionSourceDeletePendingPaths: paths,
	}
	reporter.ReportRun(context.Background(), recoveryReport, nil)
	if !reflect.DeepEqual(levels, []pkglogger.LogLevel{
		pkglogger.LogLevelWarn,
		pkglogger.LogLevelDebug,
	}) {
		t.Fatalf("recovered pending source levels = %v, want warn and debug", levels)
	}
	fields := failureArchiveLifecycleReportFields(recoveryReport)
	if fields["recovered_pending_compression_sources"] != len(paths) {
		t.Fatalf("recovered pending source count fields = %+v, want %d", fields, len(paths))
	}
	if !reflect.DeepEqual(fields["recovered_pending_compression_source_paths"], paths) {
		t.Fatalf("recovered pending source paths fields = %+v, want %v", fields, paths)
	}
}

// lifecycle結果は、failure archiveの構築時に渡したinternal log routingへ送ります。
// 設定は構築後に変更しないため、handlerも構築時に渡します。
func TestFailureArchiveLifecycleLogReporterUsesConfiguredHandlerRouting(t *testing.T) {
	called := false
	pipeline := newTestArchiveHandler(t, time.Now(), nil, InternalLogHandlers{OnInfo: func(
		_ context.Context,
		_ *logging.LogEntry,
		_ pkgobserver.Tracer,
		_ pkgobserver.Meter,
	) {
		called = true
	}})
	reporter := newFailureArchiveLifecycleLogReporter(pipeline.handler, nil)

	reporter.ReportRun(context.Background(), logfilelifecycle.RunReport{
		ObjectStorageUploads: []logfilelifecycle.ObjectStorageUploadReport{{Path: "archive.jsonl.gz"}},
	}, nil)

	if !called {
		t.Fatal("Reporter did not use the FsArchiveHandler routing configured at construction")
	}
}
