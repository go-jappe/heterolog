// Package fsarchivehandler はbackend-neutralなfailure archive保存と保守Pipelineを提供します。
package fsarchivehandler

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/application/requestctx"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	logfilerunner "github.com/go-jappe/heterolog/internal/infrastructure/logfile/runner"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/field"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/overload"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/overload/provider"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/notification"
	failurearchiveuploader "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/failurearchive"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// InternalLogHandlersは、failure archive自身のerror、warn、info、debugを委譲する
// handler群です。呼び出し側が設定名から解決済みの関数だけを設定します。
// 書き込み・圧縮・送信・削除の通知は、必要なlockを解放した時点で呼びます。
// callbackから同じPipelineのShutdownの完了を同期的に待たないでください。
// Shutdownは通知を含む処理の終了を待つため、終了の判断は呼び出し側で行います。
type InternalLogHandlers struct {
	OnError logging.LoggerErrorHandler
	OnWarn  logging.LoggerHandlerFunc
	OnInfo  logging.LoggerHandlerFunc
	OnDebug logging.LoggerHandlerFunc
}

// NewFailureArchivePipelineはretryable/permanentの共通archive rootを排他的に取得してから、
// FsArchiveHandler、設定modeに応じたlifecycle処理、Leaseを一つのPipelineとして構築します。
// 構築途中で失敗した場合は内部resourceをShutdownしてLeaseを解放します。cleanup自体が
// 完了しなかった場合だけ、呼び出し側がShutdownを再試行できるよう、部分構築済みPipelineと
// errorを同時に返します。
// checkpointを使用する場合は、呼び出し前に保存先を
// checkpoint.PrepareDirectoryで準備してください。
// 圧縮ファイルの送信失敗をspoolへ保存する場合は、対象backendすべての共有runtimeを
// WithCompressedLogSpoolRuntimesで渡してください。Pipelineは不足分を構築しません。
func NewFailureArchivePipeline(
	ctx context.Context,
	timeSource clock.Clock,
	cfg loggerdurable.FileArchiveConfigProvider,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	internalLogHandlers InternalLogHandlers,
	options ...FailureArchivePipelineOption,
) (pipeline *FailureArchivePipeline, err error) {
	if cfg == nil {
		return nil, fmt.Errorf(ErrorMsgInvalidConfigStatus, ErrNilFileArchiveConfig)
	}
	if timeSource == nil {
		return nil, fmt.Errorf(ErrorMsgInvalidConfigStatus, errors.New("file archive clock is nil"))
	}
	tracer = observer.EnsureTracer(tracer)
	meter = observer.EnsureMeter(meter)
	archiveHandler := &FsArchiveHandler{
		Clock:   timeSource,
		Config:  cfg,
		Tracer:  tracer,
		Meter:   meter,
		OnError: internalLogHandlers.OnError,
		OnWarn:  internalLogHandlers.OnWarn,
		OnInfo:  internalLogHandlers.OnInfo,
		OnDebug: internalLogHandlers.OnDebug,
		// 構築途中の失敗でも同じ終了処理を通せるよう、最初に既定間隔の再開処理を用意します。
		logFileProcessingRecovery: &logfile.LogFileProcessingRecovery[archiveLogFileProcessingRecoveryKey]{},
	}
	archivePipeline := &FailureArchivePipeline{handler: archiveHandler}
	var pipelineOptions failureArchivePipelineOptions
	for _, option := range options {
		if option != nil {
			option(&pipelineOptions)
		}
	}

	ctx, span := archiveHandler.Tracer.StartSpan(ctx, SpanFailureArchivePipelineCreate)
	defer span.End()
	// file生成より前に、保存設定とlogger側の過負荷設定をまとめて検証する。
	err = errors.Join(
		failurearchive.ValidateFileArchiveConfigProvider(cfg),
		validateFileArchiveOverloadPolicyConfigProvider(cfg.OverloadPolicyConfig()),
	)
	if err != nil {
		return nil, fmt.Errorf(ErrorMsgInvalidConfigStatus, err)
	}
	spoolRuntimes := pipelineOptions.spoolRuntimes
	overloadHandler, err := newOverloadHandler(cfg, meter, pipelineOptions.spillStdout)
	if err != nil {
		return nil, fmt.Errorf("failed to create file archive overload handler: %w", err)
	}

	// rootを変更しない依存関係の検証・構築が完了してからLeaseを取得し、checkpoint更新、
	// directory作成、active file open、runner起動までを一つの所有期間に含めます。
	archiveRoot := failurearchive.ResolveDirectoryPath(
		cfg.ArchivePath(),
		cfg.Target(),
		cfg.Dir(),
		"",
	)
	archiveRoot, err = pathidentity.ResolveDirectory(archiveRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve failure archive root directory: %w", err)
	}
	archiveHandler.archiveRoot = archiveRoot
	scopeLabel := failureArchiveDirectoryScopeLabel()
	archivePipeline.archiveRootLease, err = directorylease.Acquire(scopeLabel, archiveRoot)
	if err != nil {
		return nil, fmt.Errorf(
			"acquire failure archive root lease for %q: %w",
			archiveRoot,
			err,
		)
	}

	constructionComplete := false
	defer func() {
		if constructionComplete {
			return
		}
		pipeline, err = archivePipeline.cleanupFailedConstruction(err)
	}()

	archiveHandler.Fsstore = &failurearchive.FileStore{
		Config: cfg,
	}
	archiveHandler.OverloadHandler = overloadHandler
	// retryable/permanentの初期active fileはClockが返す同じ構築時刻から
	// 解決し、異なる日付系列へ分かれないようにします。
	initialFileAt := archiveHandler.Clock.Now()
	fileDate, err := logpattern.ResolveDateStringForFileNameAt(
		archiveHandler.Config,
		initialFileAt,
	)
	if err != nil {
		return nil, fmt.Errorf("resolve failure archive file date: %w", err)
	}
	// 永久失敗記録パスの取得
	logDirPermanent, err := archiveHandler.fileSystemStoreDirectoryPath(cfg.DirCasePermanent())
	if err != nil {
		return nil, fmt.Errorf(MsgInvalidDirectoryPath, logDirPermanent, err)
	}
	// ログファイルの番号の取得
	archiveHandler.Fsstore.FileNamePermanent = logpattern.FileNamePattern{
		Dir:       logDirPermanent,
		Target:    archiveHandler.Config.Target(),
		Date:      fileDate,
		Extension: failurearchive.FileExtension,
	}

	// 再送可能失敗データ永続化パスの取得
	logDirRetry, err := archiveHandler.fileSystemStoreDirectoryPath(cfg.DirCaseRetryable())
	if err != nil {
		return nil, fmt.Errorf(MsgInvalidDirectoryPath, logDirRetry, err)
	}
	// ログファイルの番号の取得
	archiveHandler.Fsstore.FileNameRetryable = logpattern.FileNamePattern{
		Dir:       logDirRetry,
		Target:    archiveHandler.Config.Target(),
		Date:      fileDate,
		Extension: failurearchive.FileExtension,
	}
	if archiveHandler.inlineLifecycleEnabled() {
		uploadCfg := cfg.ObjectStorageUpload()
		archiveHandler.retryableArchiveUploader, err = failurearchiveuploader.New(
			uploadCfg,
			failurearchive.FailureCategoryRetryable,
		)
		if err != nil {
			return nil, fmt.Errorf(
				ErrorMsgCreateArchiveUploader,
				failurearchive.FailureCategoryRetryable,
				err,
			)
		}
		archiveHandler.permanentArchiveUploader, err = failurearchiveuploader.New(
			uploadCfg,
			failurearchive.FailureCategoryPermanent,
		)
		if err != nil {
			return nil, fmt.Errorf(
				ErrorMsgCreateArchiveUploader,
				failurearchive.FailureCategoryPermanent,
				err,
			)
		}
	}
	if err := archiveHandler.initArchiveDirectoryRuntimes(); err != nil {
		return nil, err
	}
	if archiveHandler.inlineLifecycleEnabled() {
		if err := archivePipeline.manageInlineArchiveUploaders(spoolRuntimes); err != nil {
			return nil, err
		}
	}

	if err = logpattern.ResolveMaxIndex(&archiveHandler.Fsstore.FileNamePermanent); err != nil {
		return nil, fmt.Errorf(MsgInvalidFileIndex, archiveHandler.Fsstore.FileNamePermanent.Dir, err)
	}

	if err = logpattern.ResolveMaxIndex(&archiveHandler.Fsstore.FileNameRetryable); err != nil {
		return nil, fmt.Errorf(MsgInvalidFileIndex, archiveHandler.Fsstore.FileNameRetryable.Dir, err)
	}
	// directoryのfileだけでなくcheckpointに記録済みの世代も使用済みとし、その後から書き込みます。
	if err = archiveHandler.advancePastRecordedArchiveGeneration(failurearchive.FailureCategoryPermanent, &archiveHandler.Fsstore.FileNamePermanent); err != nil {
		return nil, err
	}
	if err = archiveHandler.advancePastRecordedArchiveGeneration(failurearchive.FailureCategoryRetryable, &archiveHandler.Fsstore.FileNameRetryable); err != nil {
		return nil, err
	}

	// ファイルを開いてからFileLoggerを格納
	if err := archiveHandler.openRetryableFile(ctx, initialFileAt); err != nil {
		return nil, fmt.Errorf(ErrorMsgOpenFile, err)
	}

	if err := archiveHandler.openPermanentFile(ctx, initialFileAt); err != nil {
		return nil, fmt.Errorf(ErrorMsgOpenFile, err)
	}

	if archiveHandler.runnerLifecycleEnabled() {
		if archivePipeline.runnerNotifications == nil {
			archivePipeline.runnerNotifications =
				notification.NewDispatcher()
		}
		lifecycleReporter := newFailureArchiveLifecycleLogReporter(
			archiveHandler,
			archivePipeline.runnerNotifications,
		)
		runners, err := logfilerunner.NewFailureArchiveRunners(
			logfilelifecycle.FailureArchiveOptions{
				Config:        cfg,
				Clock:         timeSource,
				Tracer:        tracer,
				Meter:         meter,
				Reporter:      lifecycleReporter,
				SpoolRuntimes: spoolRuntimes,
			},
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create failure archive lifecycle runners: %w", err)
		}
		// 初期確認で失敗しても、構築済みrunnerをPipelineの後片付け対象に含めます。
		archivePipeline.runnerRuntime = newFailureArchiveRunnerRuntime(runners)
		if err := archivePipeline.runnerRuntime.reconcileObjectStorageUploadDestinations(ctx); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrFailureArchiveUploadDestinationReconciliation, err)
		}
		archivePipeline.runnerRuntime.start(ctx)
	}

	constructionComplete = true
	return archivePipeline, nil
}

// advancePastRecordedArchiveGenerationは、起動時に解決したfailure archiveのfile名patternを、
// checkpointへ送信・退避の完了位置や途中経過として記録済みの世代より後へ進めます。
// 手動復旧で全fileを送信・削除した後などに記録済みの世代番号へ新しいrecordを書くと、
// 送信済みと判定されて送信されないまま削除され、object storageの同じobjectも上書きするためです。
// system clockが記録済みの期間より前へ戻った場合は、その期間を続けます。
// uploadが有効ならcheckpointを読めない場合にエラーを返し、無効なら読めない記録は使いません。
func (f *FsArchiveHandler) advancePastRecordedArchiveGeneration(
	category failurearchive.FailureCategory,
	fileName *logpattern.FileNamePattern,
) error {
	recorded, err := checkpointstore.LatestRecordedGeneration(f.Config.ArchiveCheckpoint(), string(category), *fileName)
	if err != nil {
		uploadRequired := f.Config.ObjectStorageUpload().Enabled &&
			(f.inlineLifecycleEnabled() || f.runnerLifecycleEnabled())
		if uploadRequired {
			return fmt.Errorf("read recorded %s failure archive generations: %w", category, err)
		}
		return nil
	}
	if err := logpattern.AdvancePastRecordedGeneration(fileName, recorded.Period, recorded.Index); err != nil {
		return fmt.Errorf("advance %s failure archive past recorded generation: %w", category, err)
	}
	return nil
}

// failureArchiveDirectoryScopeLabelは、failure archiveを使用する構成種別を
// directory Leaseの診断用ScopeLabelとして返します。
func failureArchiveDirectoryScopeLabel() directorylease.ScopeLabel {
	return directorylease.ScopeLabel("failure_archive")
}

// cleanupFailedConstructionは構築途中のresourceを終了します。初期化に失敗したPipelineでは
// 最終走査による圧縮・upload・purgeを行いません。後片付けが完了すればLeaseも解放して
// nil Pipelineを返します。終了が不完全ならLeaseを保持した部分Pipelineを
// 返し、呼び出し側がShutdownまたはComposite.Drainを再試行できるようにします。
func (f *FailureArchivePipeline) cleanupFailedConstruction(initErr error) (*FailureArchivePipeline, error) {
	cleanupErr := f.shutdown(context.Background())
	if cleanupErr == nil || f.shutdownState == shutdownStateCompleted {
		return nil, errors.Join(initErr, cleanupErr)
	}
	return f, errors.Join(
		initErr,
		fmt.Errorf("cleanup incomplete failure archive handler construction: %w", cleanupErr),
	)
}

func NewOverloadHandlerProvider(
	cfg loggerdurable.FileArchiveConfigProvider,
	meter pkgobserver.Meter,
) (overload.Handler, error) {
	return newOverloadHandler(cfg, meter, nil)
}

// newOverloadHandlerは過負荷時のpolicyに応じたhandlerを構築します。
// spillStdoutはdegrade policyのstdout退避でos.Stdoutの代わりに使うwriterで、nilならos.Stdoutです。
func newOverloadHandler(
	cfg loggerdurable.FileArchiveConfigProvider,
	meter pkgobserver.Meter,
	spillStdout io.Writer,
) (overload.Handler, error) {
	if cfg == nil {
		return nil, ErrNilFileArchiveConfig
	}
	overloadCfg := cfg.OverloadPolicyConfig()
	if overloadCfg == nil {
		return nil, errors.New("file archive overload policy config is nil")
	}

	switch overloadCfg.OverloadPolicy() {
	case "":
		// env loaderはdropを明示する。直接providerを組み立てる既存利用では
		// zero valueを従来どおりdropとして扱う。
		return provider.NewOverloadDropHandler(meter), nil
	case durable.OverloadPolicyDrop:
		return provider.NewOverloadDropHandler(meter), nil
	case durable.OverloadPolicyDegrade:
		return provider.NewDegradeHandlerWithStdout(overloadCfg, spillStdout)
	default:
		return nil, fmt.Errorf(
			"%w: %q",
			ErrUnsupportedFileArchiveOverloadPolicy,
			overloadCfg.OverloadPolicy(),
		)
	}
}

// スタックトレース付きログ
func (f *FsArchiveHandler) internalCallerErrorEntry(msg string, err error, fields ...pkglogger.Field) *logging.LogEntry {
	return f.newCallerLogEntry(pkglogger.LogLevelError, msg, err, true, fields...)
}

// internalCallerWarnEntryはfailure archive内部の警告entryを作成します。
func (f *FsArchiveHandler) internalCallerWarnEntry(msg string, fields ...pkglogger.Field) *logging.LogEntry {
	return f.newCallerLogEntry(pkglogger.LogLevelWarn, msg, nil, true, fields...)
}

// スタックトレース付きログ
func (f *FsArchiveHandler) internalCallerDebugEntry(msg string, fields ...pkglogger.Field) *logging.LogEntry {
	return f.newCallerLogEntry(pkglogger.LogLevelDebug, msg, nil, true, fields...)
}

func (f *FsArchiveHandler) newCallerLogEntry(level pkglogger.LogLevel, msg string, err error, internal bool, fields ...pkglogger.Field) *logging.LogEntry {
	ids, _ := field.ExtractIDsAndExtraFields(fields)
	funcNames, sourceFiles, lines := logging.CollectCallerDetails(logging.GetCallerDepth(NewStackTraceConfigAdapter(f.Config)))
	entry := &logging.LogEntry{
		OccurredAt: logging.ResolveOccurredAt(f.Clock),
		Level:      level,
		Message:    msg,
		File:       sourceFiles,
		Line:       lines,
		FuncName:   funcNames,
		Internal:   internal,
	}

	if ids.RequestID != "" {
		entry.RequestID = ids.RequestID
	}
	if ids.SpanID != "" {
		entry.SpanID = ids.SpanID
	}
	if ids.TraceID != "" {
		entry.TraceID = ids.TraceID
	}

	return entry
}

func (l *FsArchiveHandler) internalErrorEntryWithContext(ctx context.Context, msg string, err error) *logging.LogEntry {

	// コンテキストからtrace_id, span_id, request_idを取得
	ids := requestctx.IDsFromContext(ctx)

	entry := &logging.LogEntry{
		TraceID:    ids.TraceID,
		SpanID:     ids.SpanID,
		RequestID:  ids.RequestID,
		OccurredAt: logging.ResolveOccurredAt(l.Clock),
		Level:      pkglogger.LogLevelError,
		Message:    msg,
		Internal:   true,
		Err:        err,
	}

	return entry
}

func (l *FsArchiveHandler) fileSystemStoreDirectoryPath(terminalDir string) (string, error) {
	return failurearchive.GetFileArchiveDirectoryPath(l.Config, terminalDir)
}
