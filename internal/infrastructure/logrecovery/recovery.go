// Package logrecoveryは、停止中のアプリケーションのログファイルを手動復旧します。
// 通常loggerを生成せず、writer・周期runner・spool再送・purgeを起動しません。
package logrecovery

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/go-jappe/heterolog/internal/domain/target"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

var (
	// ErrInvalidRecoveryTargetは、復旧対象のtargetが保存先の識別子として使えないことを表します。
	ErrInvalidRecoveryTarget = errors.New("invalid recovery target")
	// ErrNoConfiguredLogFilesは、選択したlogger設定とscopeに復旧対象のログ系列がないことを表します。
	// fileやfailure archiveを出力先・退避先に選んでいない設定で返します。
	ErrNoConfiguredLogFiles = errors.New("no configured log files")
)

// Reportは今回実行した処理と通知です。送信済みファイルの削除は含みますがpurgeは含みません。
type Report struct {
	Directories []DirectoryReport `json:"directories"`
	Errors      []string          `json:"errors,omitempty"`
	Warnings    []string          `json:"warnings,omitempty"`
}

// DirectoryReportは一つの保存先の実行結果です。同じファイルの複数宛先送信は一件にまとめます。
type DirectoryReport struct {
	// Scopeは処理元の分類です。例: file-logger、file-logger/internal、log-backend-failure-archive/opensearch。
	Scope           string   `json:"scope"`
	DirectoryPath   string   `json:"directory"`
	ScannedFiles    int      `json:"scanned_files"`
	CompressedFiles []string `json:"compressed_files,omitempty"`
	// AcceptedFilesは今回、少なくとも一宛先へのuploadまたは全指定退避先への保存が
	// 完了したファイルです。全宛先での完了や、remote uploadだけの成功は意味しません。
	AcceptedFiles               []string            `json:"accepted_files,omitempty"`
	RemovedFiles                []string            `json:"removed_files,omitempty"`
	RecoveredDeletePendingFiles []string            `json:"recovered_delete_pending_files,omitempty"`
	FailedFiles                 []FileFailureReport `json:"failed_files,omitempty"`
	Error                       string              `json:"error,omitempty"`
}

// FileFailureReportは処理が完了しなかったファイルと、その理由です。
// 削除後の同期失敗も含むため、Pathのファイルが残っているとは限りません。
type FileFailureReport struct {
	Path  string `json:"path"`
	Error string `json:"error"`
}

type notificationCollector struct {
	mu       sync.Mutex
	errors   []string
	warnings []string
}

func appendUnique(values []string, value string) []string {
	if value != "" && !slices.Contains(values, value) {
		values = append(values, value)
	}
	return values
}

func (n *notificationCollector) ReportRun(_ context.Context, report logfilelifecycle.RunReport, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err != nil {
		n.errors = appendUnique(n.errors, err.Error())
	}
	for _, warning := range []error{report.CheckpointWarning, report.SpoolWarning} {
		if warning != nil {
			n.warnings = appendUnique(n.warnings, warning.Error())
		}
	}
}

// Recoverは全対象を一巡します。Retry指定時は、同じStoreを保持して失敗した対象だけを
// 既存の再試行処理へ登録します。終了要求後も受理済みの復旧と保存の完了を待ちます。
// 呼び出し元は同じ保存先を使うアプリケーションと他の復旧CLIを停止してください。
// この前提はプロセス内のDirectoryGuardでは確認・強制できません。
// configsには、アプリと同じ設定変換で作った設定を渡してください。checkpoint IDと宛先登録規則も
// アプリと同じものを使い、cursorをリセットしません。
func Recover(ctx context.Context, configs Configs, opts Options) (report Report, resultErr error) {
	report = Report{Directories: []DirectoryReport{}}
	fail := func(err error) (Report, error) {
		report.Errors = appendUnique(report.Errors, err.Error())
		return report, err
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if !target.IsValidTarget(configs.Target) {
		return fail(fmt.Errorf("%w %q", ErrInvalidRecoveryTarget, configs.Target))
	}
	if opts.LoggerContract == "" {
		opts.LoggerContract = pkglogger.LoggerContractSync
	}
	if opts.Scope == "" {
		opts.Scope = RecoveryScopeAll
	}
	if !opts.Scope.IsValid() {
		return fail(fmt.Errorf("invalid recovery scope %q", opts.Scope))
	}
	specs, err := recoverySpecs(configs, opts)
	if err != nil {
		return fail(err)
	}
	if len(specs) == 0 {
		return fail(fmt.Errorf("%w for logger %q and scope %q", ErrNoConfiguredLogFiles, opts.LoggerContract, opts.Scope))
	}
	// 通常起動と同じく、構築側がspool rootの所有権を保持します。
	// 一巡・再試行・強制停止のいずれも、各系列のrouteを閉じてからrootを解放します。
	spoolOwners := compressedlog.NewRootRuntimeBuilder()
	defer func() {
		if err := spoolOwners.Close(); err != nil {
			resultErr = errors.Join(resultErr, err)
			report.Errors = appendUnique(report.Errors, err.Error())
		}
	}()
	if opts.Retry {
		return recoverWithRetry(ctx, specs, spoolOwners, opts)
	}
	notifications := &notificationCollector{}
	var errs []error
	for _, spec := range specs {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if spec.checkpointDirectory != "" {
			if err := checkpoint.PrepareDirectory(spec.checkpointDirectory); err != nil {
				errs = append(errs, fmt.Errorf("prepare %s checkpoint: %w", spec.name, err))
				continue
			}
		}
		lifecycles, err := spec.build(spoolOwners, notifications)
		if err != nil {
			errs = append(errs, fmt.Errorf("build %s recovery: %w", spec.name, err))
			continue
		}
		for _, lifecycle := range lifecycles {
			result, runErr := lifecycle.RecoverOnce(ctx)
			closeErr := lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()
			<-lifecycle.NotificationsDone()
			combined := errors.Join(runErr, closeErr)
			item := directoryReport(spec.name, result, combined)
			report.Directories = append(report.Directories, item)
			if combined != nil {
				errs = append(errs, fmt.Errorf("recover %s directory %q: %w", spec.name, result.DirectoryPath, combined))
			}
		}
	}
	notifications.mu.Lock()
	report.Errors = append(report.Errors, notifications.errors...)
	report.Warnings = append(report.Warnings, notifications.warnings...)
	notifications.mu.Unlock()
	for _, err := range errs {
		report.Errors = appendUnique(report.Errors, err.Error())
	}
	if err := errors.Join(errs...); err != nil {
		return report, err
	}
	if len(report.Errors) > 0 {
		return report, errors.New("recovery reported errors; inspect the recovery report")
	}
	return report, nil
}

func directoryReport(scope string, result logfilelifecycle.RunReport, err error) DirectoryReport {
	item := DirectoryReport{Scope: scope, DirectoryPath: result.DirectoryPath, ScannedFiles: result.ScannedFiles}
	for _, compressed := range result.Compressed {
		item.CompressedFiles = appendUnique(item.CompressedFiles, compressed.CompressedPath)
	}
	for _, uploaded := range result.ObjectStorageUploads {
		item.AcceptedFiles = appendUnique(item.AcceptedFiles, uploaded.Path)
	}
	for _, removed := range result.UploadedFileRemovals {
		if removed.Err == nil {
			item.RemovedFiles = appendUnique(item.RemovedFiles, removed.File.Path)
		}
	}
	item.RecoveredDeletePendingFiles = append([]string(nil), result.RecoveredCompressionSourceDeletePendingPaths...)
	for _, failure := range result.RecoveryErrors {
		if failure.Err != nil {
			item.FailedFiles = append(item.FailedFiles, FileFailureReport{Path: failure.Path, Error: failure.Err.Error()})
		}
	}
	if err != nil {
		item.Error = err.Error()
	}
	return item
}
