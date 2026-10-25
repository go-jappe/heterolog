package fsarchivehandler

import (
	"context"
	"fmt"

	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/notification"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

const failureArchiveLifecycleSource = "failure_archive_lifecycle"

// failureArchiveLifecycleLogReporterは、failure archiveのinline処理とRunnerによる
// 起動時checkpoint整合、周期保守、復旧の結果を、通常のapplication logとは独立した
// internal logへ変換します。fileの圧縮、object storage upload、purgeを実行したり、
// runnerを継続・停止したりはしません。
//
// 汎用Runnerはloggerのroutingを知らないため、このfailure archive固有の記録方針を
// fsarchivehandler packageに置き、RunReporter契約を介してinline処理とRunnerで共有します。
type failureArchiveLifecycleLogReporter struct {
	// handlerはfailure recordを保存するためではなく、同じarchive handlerが使用する
	// internal log routingとtelemetryを参照するために保持します。これにより、failure
	// 保存時とlifecycle結果の出力先を一致させます。
	handler *FsArchiveHandler
	// notificationsはrunner用にPipelineが所有する非同期通知先です。
	// inline処理や単体利用でnilを渡した場合は、呼び出し元のgoroutineで即時通知します。
	// どちらの場合も通知先のpanicは隔離します。
	notifications *notification.Dispatcher
}

var _ logfilelifecycle.RunReporter = (*failureArchiveLifecycleLogReporter)(nil)

// newFailureArchiveLifecycleLogReporterは、failure archive writerが使用するinternal log設定を
// 参照する、failure archive lifecycle結果用Reporterを作ります。lifecycle処理の実行は
// inline処理またはRunner、結果の記録方針はReporter、failure recordの保存は
// FsArchiveHandlerがそれぞれ担当します。
func newFailureArchiveLifecycleLogReporter(
	archiveHandler *FsArchiveHandler,
	notifications *notification.Dispatcher,
) logfilelifecycle.RunReporter {
	if archiveHandler == nil {
		return nil
	}
	return &failureArchiveLifecycleLogReporter{
		handler:       archiveHandler,
		notifications: notifications,
	}
}

// ReportRunはfailure archiveのinline処理またはRunnerによる一回分のlifecycle結果を
// 記録します。失敗はError、以前の実行から残ったsource削除待ちpathの
// 回復・受理済みupload途中経過を保持したcheckpoint警告・spoolの保全確認はWarn、file状態が変化した
// 成功はInfoへ送り、すべての結果をDebugへ送ります。
// 回復失敗は処理未完了を表すため、WarnではなくErrorとして扱います。
func (r *failureArchiveLifecycleLogReporter) ReportRun(
	ctx context.Context,
	report logfilelifecycle.RunReport,
	err error,
) {
	if r == nil || r.handler == nil {
		return
	}
	ctx = contextOrBackground(ctx)
	owner := r.handler
	hasError := err != nil && owner.OnError != nil
	hasWarning := owner.OnWarn != nil && (report.CheckpointWarning != nil || report.SpoolWarning != nil || len(report.RecoveredCompressionSourceDeletePendingPaths) > 0)
	hasInfo := err == nil && owner.OnInfo != nil && failureArchiveLifecycleChangedFiles(report)
	if !hasError && !hasWarning && !hasInfo && owner.OnDebug == nil {
		return
	}
	fields := failureArchiveLifecycleReportFields(report)
	occurredAt := logging.ResolveOccurredAt(owner.Clock)
	entryFor := func(level pkglogger.LogLevel, message string) *logging.LogEntry {
		return &logging.LogEntry{
			OccurredAt: occurredAt,
			Level:      level,
			Message:    message,
			Internal:   true,
			Source:     failureArchiveLifecycleSource,
			Fields:     fields,
		}
	}
	// 一回分の通知を一つのcallbackとして登録し、別のReportRunの通知が途中に入り込むのを防ぎます。
	r.notifications.Enqueue(func() {
		// 周期の終了・通常Shutdownで受理済み通知を取り落とさず、明示的な強制終了だけを伝えます。
		ctx, cancel := owner.forceWork.context(context.WithoutCancel(ctx))
		defer cancel()
		if hasError {
			entry := entryFor(pkglogger.LogLevelError, "failure archive lifecycle run failed")
			entry.Err = err
			runFailureArchiveLifecycleNotification(func() {
				owner.delegateError(ctx, fmt.Errorf("failure archive lifecycle run failed: %w", err), entry)
			})
		}
		if report.CheckpointWarning != nil && owner.OnWarn != nil {
			entry := entryFor(pkglogger.LogLevelWarn, "failure archive lifecycle upload checkpoint warning")
			entry.Err = report.CheckpointWarning
			runFailureArchiveLifecycleNotification(func() { owner.delegateWarn(ctx, entry) })
		}
		if report.SpoolWarning != nil && owner.OnWarn != nil {
			entry := entryFor(pkglogger.LogLevelWarn, "failure archive lifecycle spool preservation warning")
			entry.Err = report.SpoolWarning
			runFailureArchiveLifecycleNotification(func() { owner.delegateWarn(ctx, entry) })
		}
		if len(report.RecoveredCompressionSourceDeletePendingPaths) > 0 && owner.OnWarn != nil {
			entry := entryFor(pkglogger.LogLevelWarn, "failure archive lifecycle recovered pending compression sources")
			runFailureArchiveLifecycleNotification(func() { owner.delegateWarn(ctx, entry) })
		}
		if hasInfo {
			entry := entryFor(pkglogger.LogLevelInfo, "failure archive lifecycle run completed")
			runFailureArchiveLifecycleNotification(func() {
				handler.CallInfoHandlerIfPresent(owner.OnInfo, ctx, entry, owner.Tracer, owner.Meter)
			})
		}
		if owner.OnDebug != nil {
			entry := entryFor(pkglogger.LogLevelDebug, "failure archive lifecycle run result")
			runFailureArchiveLifecycleNotification(func() {
				handler.CallDebugHandlerIfPresent(owner.OnDebug, ctx, entry, owner.Tracer, owner.Meter, true)
			})
		}
	})
}

// runFailureArchiveLifecycleNotificationは一つの通知先のpanicを隔離し、
// 同じlifecycle結果に含まれる残りの通知を続けられるようにします。
func runFailureArchiveLifecycleNotification(notify func()) {
	defer func() { _ = recover() }()
	notify()
}

func failureArchiveLifecycleChangedFiles(report logfilelifecycle.RunReport) bool {
	if report.CheckpointWarning != nil || report.SpoolWarning != nil {
		return false
	}
	if report.UploadFailureArchivePurge != nil {
		for _, result := range report.UploadFailureArchivePurge.ByFailureCategory {
			if result.DeletedFileCount > 0 {
				return true
			}
		}
		return false
	}
	uploadedCompressedRemoved, _ := failureArchiveUploadedFileRemovalOutcomeCounts(report)
	ageRemoved, _ := report.Purge.ByAge.RemovalOutcomeCounts()
	generationRemoved, _ := report.Purge.ByGeneration.RemovalOutcomeCounts()
	directorySizeRemoved, _ := report.Purge.ByDirSize.RemovalOutcomeCounts()
	return len(report.Compressed) > 0 ||
		len(report.ObjectStorageUploads) > 0 ||
		uploadedCompressedRemoved > 0 ||
		ageRemoved > 0 ||
		generationRemoved > 0 ||
		directorySizeRemoved > 0
}

func failureArchiveLifecycleReportFields(report logfilelifecycle.RunReport) map[string]any {
	if report.SpoolWarning != nil {
		return map[string]any{
			"directory_path": report.DirectoryPath,
			"spool_warning":  report.SpoolWarning.Error(),
		}
	}
	if report.CheckpointWarning != nil {
		return map[string]any{
			"directory_path":     report.DirectoryPath,
			"checkpoint_warning": report.CheckpointWarning.Error(),
		}
	}
	if report.UploadFailureArchivePurge != nil {
		return map[string]any{
			"directory_path":               report.DirectoryPath,
			"upload_failure_archive_purge": failureArchiveUploadFailurePurgeFields(*report.UploadFailureArchivePurge),
		}
	}
	uploadedCompressedRemoved, uploadedCompressedRemovalFailed :=
		failureArchiveUploadedFileRemovalOutcomeCounts(report)
	ageRemoved, ageFailed := report.Purge.ByAge.RemovalOutcomeCounts()
	generationRemoved, generationFailed := report.Purge.ByGeneration.RemovalOutcomeCounts()
	directorySizeRemoved, directorySizeFailed := report.Purge.ByDirSize.RemovalOutcomeCounts()
	return map[string]any{
		"directory_path": report.DirectoryPath,
		"scanned_files":  report.ScannedFiles,
		"recovered_pending_compression_sources": len(
			report.RecoveredCompressionSourceDeletePendingPaths,
		),
		"recovered_pending_compression_source_paths": report.RecoveredCompressionSourceDeletePendingPaths,
		"compressed_files":                           len(report.Compressed),
		"uploaded_files":                             len(report.ObjectStorageUploads),
		"removed_uploaded_compressed_files":          uploadedCompressedRemoved,
		"uploaded_compressed_remove_failures":        uploadedCompressedRemovalFailed,
		"purged_by_age_files":                        ageRemoved,
		"purged_by_generation_files":                 generationRemoved,
		"purged_by_directory_size_files":             directorySizeRemoved,
		"purge_by_age_failures":                      ageFailed,
		"purge_by_generation_failures":               generationFailed,
		"purge_by_directory_size_failures":           directorySizeFailed,
	}
}

func failureArchiveUploadFailurePurgeFields(report compressedlogarchive.PurgeReport) map[string]any {
	results := make(map[string]any, len(report.ByFailureCategory))
	for category, result := range report.ByFailureCategory {
		var errorMessages []string
		for _, err := range result.Errors {
			if err != nil {
				errorMessages = append(errorMessages, err.Error())
			}
		}
		results[string(category)] = map[string]any{
			"enabled":                     result.Enabled,
			"scanned_entry_count":         result.ScannedEntryCount,
			"scanned_bytes":               result.ScannedBytes,
			"deleted_file_count":          result.DeletedFileCount,
			"deleted_bytes":               result.DeletedBytes,
			"remaining_entry_count":       result.RemainingEntryCount,
			"remaining_bytes":             result.RemainingBytes,
			"retained_unsafe_entry_count": result.RetainedUnsafeEntryCount,
			"capacity_limit_exceeded":     result.CapacityLimitExceeded,
			"errors":                      errorMessages,
		}
	}
	return map[string]any{"preserved_file_count": report.PreservedFileCount, "by_failure_category": results}
}

func failureArchiveUploadedFileRemovalOutcomeCounts(
	report logfilelifecycle.RunReport,
) (removed int, failed int) {
	for _, result := range report.UploadedFileRemovals {
		if result.Err != nil {
			failed++
			continue
		}
		removed++
	}
	return removed, failed
}
