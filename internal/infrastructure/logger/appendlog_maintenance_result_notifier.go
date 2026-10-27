package logger

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-jappe/heterolog/internal/application/observer"
	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	loggerhandler "github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/onerrorhandler"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

const appendLogMaintenanceRunnerSource = "appendlog_maintenance_runner"

// AppendLogMaintenanceHandlerSelectorErrorは、appendlogの周期保守処理のinternal log通知先
// として設定されたselectorが、同じLoggerContractKindのhandler候補に存在しないことを表します。
// Levelは通知のlevel、Selectorは設定された値（前後の空白を除いたもの）です。
type AppendLogMaintenanceHandlerSelectorError struct {
	Level    string
	Selector string
}

func (e *AppendLogMaintenanceHandlerSelectorError) Error() string {
	return fmt.Sprintf(
		"appendlog maintenance runner %s handler selector %q is not configured",
		e.Level,
		e.Selector,
	)
}

// validateAppendLogMaintenanceResultNotifierSelectorsは、appendlogの周期保守処理が生成する
// level別のinternal logについて、設定されたselectorが同じLoggerContractKindのhandler候補に
// 実在することを確認します。設定ミスを「保守処理は動いているが、その実行結果だけが
// 記録されない」状態にしないための構築時検証です。
func validateAppendLogMaintenanceResultNotifierSelectors(
	routing pkgloggerhandler.HandlerConfig,
	available handlerStruct,
) error {
	checks := []struct {
		level     string
		selectors []string
		available map[string]struct{}
	}{
		{
			level:     "error",
			selectors: routing.OnErrorConfig,
			available: availableErrorHandlerSelectors(available.OnError),
		},
		{
			level:     "warn",
			selectors: routing.OnWarnConfig,
			available: availableHandlerSelectors(available.OnWarn),
		},
		{
			level:     "info",
			selectors: routing.OnInfoConfig,
			available: availableHandlerSelectors(available.OnInfo),
		},
		{
			level:     "debug",
			selectors: routing.OnDebugConfig,
			available: availableHandlerSelectors(available.OnDebug),
		},
	}
	for _, check := range checks {
		for _, configured := range check.selectors {
			selector := normalizeHandlerName(configured)
			if selector == "" {
				continue
			}
			if _, exists := check.available[selector]; !exists {
				return &AppendLogMaintenanceHandlerSelectorError{
					Level:    check.level,
					Selector: strings.TrimSpace(configured),
				}
			}
		}
	}
	return nil
}

func availableErrorHandlerSelectors(handlers []logging.NamedLoggerErrorHandler) map[string]struct{} {
	selectors := make(map[string]struct{}, len(handlers))
	for _, candidate := range handlers {
		if candidate.Handler != nil {
			selectors[normalizeHandlerName(candidate.Name)] = struct{}{}
		}
	}
	return selectors
}

func availableHandlerSelectors(handlers []logging.NamedLoggerHandler) map[string]struct{} {
	selectors := make(map[string]struct{}, len(handlers))
	for _, candidate := range handlers {
		if candidate.Handler != nil {
			selectors[normalizeHandlerName(candidate.Name)] = struct{}{}
		}
	}
	return selectors
}

// appendLogMaintenanceResultNotifierは、保守処理のRunReportとエラーを内部ログへ変換し、
// 設定されたOnError・OnWarn・OnInfo・OnDebugの通知先へ渡します。
// 起動時の宛先整合、周期の圧縮・送信・削除・purgeに加え、未完了ファイル処理や
// checkpoint保存の再試行、独立した圧縮ログ送信失敗archiveのpurge結果も受け取ります。
// 保存先のpath・stream・indexは選択されたfallback loggerが管理します。
// 通常のログ追記・rotationとinline lifecycleの結果は、この通知処理の対象外です。
type appendLogMaintenanceResultNotifier struct {
	clock   clock.Clock
	tracer  pkgobserver.Tracer
	meter   pkgobserver.Meter
	onError logging.LoggerErrorHandler
	onWarn  logging.LoggerHandlerFunc
	onInfo  logging.LoggerHandlerFunc
	onDebug logging.LoggerHandlerFunc
}

var _ logfilelifecycle.RunReporter = (*appendLogMaintenanceResultNotifier)(nil)

// newAppendLogMaintenanceResultNotifierは、保守結果を通知する内部ログのroutingを解決します。
// availableには同じLoggerContractKindで使う通知先をbackend名ごとに渡します。
// 選択された通知先がなければnilを返します。返り値は共通処理のRunReporter契約です。
func newAppendLogMaintenanceResultNotifier(
	routing pkgloggerhandler.HandlerConfig,
	available handlerStruct,
	clk clock.Clock,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) logfilelifecycle.RunReporter {
	selected := selectConfiguredHandlers(available, loggerhandler.NewConfigAdapter(routing))
	if len(selected.OnError) == 0 && len(selected.OnWarn) == 0 &&
		len(selected.OnInfo) == 0 && len(selected.OnDebug) == 0 {
		return nil
	}
	if clk == nil {
		clk = clock.NewUTCClock()
	}
	return &appendLogMaintenanceResultNotifier{
		clock:   clk,
		tracer:  observer.EnsureTracer(tracer),
		meter:   observer.EnsureMeter(meter),
		onError: onerrorhandler.CombineErrorHandlers(selected.OnError...),
		onWarn:  onerrorhandler.CombineWarnHandlers(selected.OnWarn...),
		onInfo:  onerrorhandler.CombineInfoHandlers(selected.OnInfo...),
		onDebug: onerrorhandler.CombineDebugHandlers(selected.OnDebug...),
	}
}

// ReportRunはRunReporter契約の入口として保守結果とエラーを受け取り、内部ログの通知先へ渡します。
// エラーはOnError、checkpointやspoolの保全警告と圧縮元の削除待ち解消はOnWarnへ通知します。
// 正常にファイル処理が進んだ結果はOnInfo、各実行の詳細はOnDebugへ渡します。
// 保守処理の実行とRunnerの継続・停止は呼び出し元が管理します。
func (h *appendLogMaintenanceResultNotifier) ReportRun(
	ctx context.Context,
	report logfilelifecycle.RunReport,
	err error,
) {
	if h == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	fields := appendLogMaintenanceRunReportFields(report)

	if err != nil && h.onError != nil {
		entry := h.entry(pkglogger.LogLevelError, "appendlog maintenance runner failed", fields)
		entry.Err = err
		h.onError(
			ctx,
			fmt.Errorf("appendlog maintenance runner failed: %w", err),
			entry,
			h.tracer,
			h.meter,
		)
	}
	if report.CheckpointWarning != nil && h.onWarn != nil {
		entry := h.entry(pkglogger.LogLevelWarn, "appendlog maintenance runner upload checkpoint warning", fields)
		entry.Err = report.CheckpointWarning
		h.onWarn(ctx, entry, h.tracer, h.meter)
	}
	if report.SpoolWarning != nil && h.onWarn != nil {
		entry := h.entry(pkglogger.LogLevelWarn, "appendlog maintenance runner spool preservation warning", fields)
		entry.Err = report.SpoolWarning
		h.onWarn(ctx, entry, h.tracer, h.meter)
	}

	if len(report.RecoveredCompressionSourceDeletePendingPaths) > 0 && h.onWarn != nil {
		h.onWarn(
			ctx,
			h.entry(
				pkglogger.LogLevelWarn,
				"appendlog maintenance runner recovered pending compression sources",
				fields,
			),
			h.tracer,
			h.meter,
		)
	}

	if err == nil && appendLogMaintenanceRunChangedFiles(report) && h.onInfo != nil {
		h.onInfo(
			ctx,
			h.entry(pkglogger.LogLevelInfo, "appendlog maintenance runner completed", fields),
			h.tracer,
			h.meter,
		)
	}

	if h.onDebug != nil {
		h.onDebug(
			ctx,
			h.entry(pkglogger.LogLevelDebug, "appendlog maintenance runner result", fields),
			h.tracer,
			h.meter,
		)
	}
}

func (h *appendLogMaintenanceResultNotifier) entry(
	level pkglogger.LogLevel,
	message string,
	fields map[string]any,
) *logging.LogEntry {
	return &logging.LogEntry{
		OccurredAt: logging.ResolveOccurredAt(h.clock),
		Level:      level,
		Message:    message,
		Internal:   true,
		Source:     appendLogMaintenanceRunnerSource,
		Fields:     fields,
	}
}

func appendLogMaintenanceRunChangedFiles(report logfilelifecycle.RunReport) bool {
	if report.CheckpointWarning != nil || report.SpoolWarning != nil {
		return false
	}
	if report.UploadFailureArchivePurge != nil {
		for _, failureCategoryReport := range report.UploadFailureArchivePurge.ByFailureCategory {
			if failureCategoryReport.DeletedFileCount > 0 {
				return true
			}
		}
		return false
	}
	uploadedCompressedRemoved, _ := appendLogUploadedFileRemovalOutcomeCounts(report)
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

func appendLogMaintenanceRunReportFields(report logfilelifecycle.RunReport) map[string]any {
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
			"directory_path": report.DirectoryPath,
			"upload_failure_archive_purge": appendLogUploadFailureArchivePurgeFields(
				*report.UploadFailureArchivePurge,
			),
		}
	}
	uploadedCompressedRemoved, uploadedCompressedRemovalFailed :=
		appendLogUploadedFileRemovalOutcomeCounts(report)
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

func appendLogUploadFailureArchivePurgeFields(report compressedlogarchive.PurgeReport) map[string]any {
	resultsByFailureCategory := make(map[string]any, len(report.ByFailureCategory))
	for failureCategory, failureCategoryReport := range report.ByFailureCategory {
		errorMessages := make([]string, 0, len(failureCategoryReport.Errors))
		for _, err := range failureCategoryReport.Errors {
			if err != nil {
				errorMessages = append(errorMessages, err.Error())
			}
		}
		resultsByFailureCategory[string(failureCategory)] = map[string]any{
			"enabled":                     failureCategoryReport.Enabled,
			"scanned_entry_count":         failureCategoryReport.ScannedEntryCount,
			"scanned_bytes":               failureCategoryReport.ScannedBytes,
			"deleted_file_count":          failureCategoryReport.DeletedFileCount,
			"deleted_bytes":               failureCategoryReport.DeletedBytes,
			"remaining_entry_count":       failureCategoryReport.RemainingEntryCount,
			"remaining_bytes":             failureCategoryReport.RemainingBytes,
			"retained_unsafe_entry_count": failureCategoryReport.RetainedUnsafeEntryCount,
			"capacity_limit_exceeded":     failureCategoryReport.CapacityLimitExceeded,
			"errors":                      errorMessages,
		}
	}
	return map[string]any{
		"preserved_file_count": report.PreservedFileCount,
		"by_failure_category":  resultsByFailureCategory,
	}
}

func appendLogUploadedFileRemovalOutcomeCounts(
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
