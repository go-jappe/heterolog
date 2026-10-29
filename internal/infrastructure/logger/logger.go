package logger

import (
	"context"
	"errors"
	"fmt"
	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	infraclickhouse "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse"
	infracloudwatchlogs "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs"
	infraloki "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki"
	infraopensearch "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch"
	clickhouselogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/clickhouse"
	cloudwatchlogslogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/cloudwatchlogs"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/file"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	lokilogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/loki"
	multi "github.com/go-jappe/heterolog/internal/infrastructure/logger/multi"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/onerrorhandler"
	opensearchlogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/opensearch"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/slack"
	stdout "github.com/go-jappe/heterolog/internal/infrastructure/logger/stdout"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	"github.com/go-jappe/heterolog/pkg/otel"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
	"io"
	"sync"
)

// rollbackContextCloserは、top-level loggerのconstructorが途中で失敗した場合に、
// それ以前に作成済みのClose(context.Context)を持つresourceを終了するため、
// rollbackLoggerConstructionが扱うio.Closerへ適合させます。構築時ctxのcancel状態に
// cleanupを左右させないため、Closeにはcontext.Background()を渡します。
type rollbackContextCloser struct {
	close func(context.Context) error
}

// failureHandlerDrainerは、構築済みfailure handlerをbackend loggerへ組み込む前に
// constructorが失敗した場合、そのhandlerを終了するために必要な最小契約です。
// buildLoggersはこの契約を使い、backend loggerへまだ所有権を渡していないfailure handlerと
// そのchild resourceをconstruction rollbackで終了します。
//
// ここでいうDrainはspool backlogの再送やlog dataの削除ではありません。新しいfailure処理の
// 受付を止め、すでに受理した処理とそこから開始した非同期処理の完了を待ち、最後にchildが
// 所有するresourceを終了する操作です。渡されたcontextのcancel・deadlineまでに終了を確認
// できなければerrorを返し、受付停止状態を維持したまま再試行できなければなりません。
type failureHandlerDrainer interface {
	Drain(context.Context) error
}

// topLevelLoggerDelegateは、同期top-level runtimeがapplication向けlog処理を委譲する
// fan-out loggerの内部契約です。StopAcceptingはresourceを閉じずに新規受付だけを止め、
// Closeは受理済み処理とchild loggerを終了します。
type topLevelLoggerDelegate interface {
	applogging.CloseableLogger
	StopAccepting()
}

// topLevelAsyncLoggerDelegateは、非同期top-level runtime向けの同じ二段階終了契約です。
type topLevelAsyncLoggerDelegate interface {
	applogging.AsyncCloseableLogger
	StopAccepting()
}

// topLevelDeliveryLoggerDelegateは、Delivery top-level runtime向けの同じ二段階終了契約です。
type topLevelDeliveryLoggerDelegate interface {
	applogging.DeliveryClosableLogger
	StopAccepting()
}

// errLoggerOwnedResourceCleanupIncompleteは、constructorが作成済みbackendまたは
// failure handlerの終了を確認できなかったことを表します。このerrorを含む間は、
// それらがfailure spoolへアクセスし続ける可能性があるためroot Leaseを解放しません。
var errLoggerOwnedResourceCleanupIncomplete = errors.New(
	"logger owned resource cleanup is incomplete",
)

func markLoggerOwnedResourceCleanupIncomplete(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", errLoggerOwnedResourceCleanupIncomplete, err)
}

// topLevelLoggerCloseStageは、top-level Logger runtimeが次に終了するresource群を
// 表します。zero値からCloseを開始でき、各段階が完了した時点で次へ進みます。
// Deliveryはfailure spoolを所有しないため、backend終了後にfallback handlerへ進みます。
type topLevelLoggerCloseStage uint8

const (
	topLevelLoggerCloseStageAppendLogMaintenance topLevelLoggerCloseStage = iota
	topLevelLoggerCloseStageBackendLogger
	topLevelLoggerCloseStageFailureSpoolPipelines
	topLevelLoggerCloseStageFallbackHandlers
	topLevelLoggerCloseStageInternalLogDestinations
	topLevelLoggerCloseStageFileLoggerSet
	topLevelLoggerCloseStageCompressedLogSpool
	topLevelLoggerCloseStageCompleted
)

// topLevelLoggerRuntimeは、applicationへ公開する同期top-level Loggerと、その一構成が
// 使用する全resourceを所有します。
// Closeはappendlog保守、backend、failure spool writer、spool runner・Lease、fallback、
// internal-log保存先、base FileLoggerSet、compressed-log spool rootの順です。backendの最終flushとspool終了処理が
// 完了するまで、内部エラーの記録先とfile handleを開いたまま保ちます。
type topLevelLoggerRuntime struct {
	topLevelLoggerDelegate
	fallbackCloserOwner         *fallbackHandlerCloserOwner
	fileLoggerSet               *file.LoggerSet
	internalLogDestinations     *internalLogDestinationRuntime
	appendLogMaintenanceRuntime *appendLogMaintenanceRuntime
	failureSpoolPipelines       *failureSpoolPipelineSet
	compressedLogSpoolRuntimes  *compressedLogSpoolRuntimes
	closeMu                     sync.Mutex
	closeController             shutdown.Controller
	closeStage                  topLevelLoggerCloseStage
	appendLogMaintenanceErr     error
	failureSpoolPipelinesErr    error
	completedCloseErrors        []error
}

func (l *topLevelLoggerRuntime) closeAttempt() error {
	if l == nil {
		return nil
	}
	l.closeMu.Lock()
	defer l.closeMu.Unlock()
	l.topLevelLoggerDelegate.StopAccepting()

	if l.closeStage == topLevelLoggerCloseStageAppendLogMaintenance {
		l.appendLogMaintenanceErr = l.appendLogMaintenanceRuntime.Close()
		l.closeStage = topLevelLoggerCloseStageBackendLogger
	}
	if l.closeStage == topLevelLoggerCloseStageBackendLogger {
		if err := retainCompletedCloseError(l.topLevelLoggerDelegate, l.topLevelLoggerDelegate.Close(), &l.completedCloseErrors); err != nil {
			// backendが最終flushを再試行できるよう、file fallback LoggerSetは
			// backend全体のClose成功まで開いたままにします。
			return errors.Join(l.appendLogMaintenanceErr, err)
		}
		l.closeStage = topLevelLoggerCloseStageFailureSpoolPipelines
	}
	if l.closeStage == topLevelLoggerCloseStageFailureSpoolPipelines {
		complete, err := l.failureSpoolPipelines.close()
		l.failureSpoolPipelinesErr = err
		if !complete {
			return errors.Join(l.appendLogMaintenanceErr, err)
		}
		l.closeStage = topLevelLoggerCloseStageFallbackHandlers
	}
	if l.closeStage == topLevelLoggerCloseStageFallbackHandlers {
		if err := retainCompletedCloseError(l.fallbackCloserOwner, l.fallbackCloserOwner.Close(), &l.completedCloseErrors); err != nil {
			return errors.Join(l.appendLogMaintenanceErr, l.failureSpoolPipelinesErr, err)
		}
		l.closeStage = topLevelLoggerCloseStageInternalLogDestinations
	}
	if l.closeStage == topLevelLoggerCloseStageInternalLogDestinations {
		if err := retainCompletedCloseError(l.internalLogDestinations, l.internalLogDestinations.Close(), &l.completedCloseErrors); err != nil {
			return errors.Join(l.appendLogMaintenanceErr, l.failureSpoolPipelinesErr, err)
		}
		l.closeStage = topLevelLoggerCloseStageFileLoggerSet
	}
	if l.closeStage == topLevelLoggerCloseStageFileLoggerSet {
		if l.fileLoggerSet != nil {
			if err := retainCompletedCloseError(l.fileLoggerSet, l.fileLoggerSet.Close(), &l.completedCloseErrors); err != nil {
				return errors.Join(l.appendLogMaintenanceErr, l.failureSpoolPipelinesErr, err)
			}
		}
		l.closeStage = topLevelLoggerCloseStageCompressedLogSpool
	}
	if l.closeStage == topLevelLoggerCloseStageCompressedLogSpool {
		if err := l.compressedLogSpoolRuntimes.Close(); err != nil {
			l.completedCloseErrors = append(l.completedCloseErrors, err)
		}
		l.closeStage = topLevelLoggerCloseStageCompleted
	}
	return errors.Join(l.appendLogMaintenanceErr, l.failureSpoolPipelinesErr, errors.Join(l.completedCloseErrors...))
}

// topLevelAsyncLoggerRuntimeは、applicationへ公開するbest-effort非同期top-level Loggerと
// その一構成が使用する全resourceを所有し、同期版と同じ所有順序で閉じます。
type topLevelAsyncLoggerRuntime struct {
	topLevelAsyncLoggerDelegate
	fallbackCloserOwner         *fallbackHandlerCloserOwner
	fileLoggerSet               *file.LoggerSet
	internalLogDestinations     *internalLogDestinationRuntime
	appendLogMaintenanceRuntime *appendLogMaintenanceRuntime
	failureSpoolPipelines       *failureSpoolPipelineSet
	compressedLogSpoolRuntimes  *compressedLogSpoolRuntimes
	closeMu                     sync.Mutex
	closeController             shutdown.Controller
	closeStage                  topLevelLoggerCloseStage
	appendLogMaintenanceErr     error
	failureSpoolPipelinesErr    error
	completedCloseErrors        []error
}

// topLevelDeliveryLoggerRuntimeは、applicationへ公開するDelivery top-level Loggerと、
// そのbackend、internal log用handler、専用internal-log/baseのFileLogger setを所有します。
type topLevelDeliveryLoggerRuntime struct {
	topLevelDeliveryLoggerDelegate
	fallbackCloserOwner         *fallbackHandlerCloserOwner
	fileLoggerSet               *file.DeliveryLoggerSet
	internalLogDestinations     *deliveryInternalLogDestinationRuntime
	appendLogMaintenanceRuntime *appendLogMaintenanceRuntime
	compressedLogSpoolRuntimes  *compressedLogSpoolRuntimes
	closeMu                     sync.Mutex
	closeController             shutdown.Controller
	closeStage                  topLevelLoggerCloseStage
	appendLogMaintenanceErr     error
	completedCloseErrors        []error
}

func (l *topLevelDeliveryLoggerRuntime) closeAttempt() error {
	if l == nil {
		return nil
	}
	l.closeMu.Lock()
	defer l.closeMu.Unlock()
	l.topLevelDeliveryLoggerDelegate.StopAccepting()
	if l.closeStage == topLevelLoggerCloseStageAppendLogMaintenance {
		l.appendLogMaintenanceErr = l.appendLogMaintenanceRuntime.Close()
		l.closeStage = topLevelLoggerCloseStageBackendLogger
	}
	if l.closeStage == topLevelLoggerCloseStageBackendLogger {
		if err := retainCompletedCloseError(l.topLevelDeliveryLoggerDelegate, l.topLevelDeliveryLoggerDelegate.Close(), &l.completedCloseErrors); err != nil {
			return errors.Join(l.appendLogMaintenanceErr, err)
		}
		l.closeStage = topLevelLoggerCloseStageFallbackHandlers
	}
	if l.closeStage == topLevelLoggerCloseStageFallbackHandlers {
		if err := retainCompletedCloseError(l.fallbackCloserOwner, l.fallbackCloserOwner.Close(), &l.completedCloseErrors); err != nil {
			return errors.Join(l.appendLogMaintenanceErr, err)
		}
		l.closeStage = topLevelLoggerCloseStageInternalLogDestinations
	}
	if l.closeStage == topLevelLoggerCloseStageInternalLogDestinations {
		if err := retainCompletedCloseError(l.internalLogDestinations, l.internalLogDestinations.Close(), &l.completedCloseErrors); err != nil {
			return errors.Join(l.appendLogMaintenanceErr, err)
		}
		l.closeStage = topLevelLoggerCloseStageFileLoggerSet
	}
	if l.closeStage == topLevelLoggerCloseStageFileLoggerSet {
		if l.fileLoggerSet != nil {
			if err := retainCompletedCloseError(l.fileLoggerSet, l.fileLoggerSet.Close(), &l.completedCloseErrors); err != nil {
				return errors.Join(l.appendLogMaintenanceErr, err)
			}
		}
		l.closeStage = topLevelLoggerCloseStageCompressedLogSpool
	}
	if l.closeStage == topLevelLoggerCloseStageCompressedLogSpool {
		if err := l.compressedLogSpoolRuntimes.Close(); err != nil {
			l.completedCloseErrors = append(l.completedCloseErrors, err)
		}
		l.closeStage = topLevelLoggerCloseStageCompleted
	}
	return errors.Join(l.appendLogMaintenanceErr, errors.Join(l.completedCloseErrors...))
}

func (l *topLevelAsyncLoggerRuntime) closeAttempt() error {
	if l == nil {
		return nil
	}
	l.closeMu.Lock()
	defer l.closeMu.Unlock()
	l.topLevelAsyncLoggerDelegate.StopAccepting()

	if l.closeStage == topLevelLoggerCloseStageAppendLogMaintenance {
		l.appendLogMaintenanceErr = l.appendLogMaintenanceRuntime.Close()
		l.closeStage = topLevelLoggerCloseStageBackendLogger
	}
	if l.closeStage == topLevelLoggerCloseStageBackendLogger {
		if err := retainCompletedCloseError(l.topLevelAsyncLoggerDelegate, l.topLevelAsyncLoggerDelegate.Close(), &l.completedCloseErrors); err != nil {
			return errors.Join(l.appendLogMaintenanceErr, err)
		}
		l.closeStage = topLevelLoggerCloseStageFailureSpoolPipelines
	}
	if l.closeStage == topLevelLoggerCloseStageFailureSpoolPipelines {
		complete, err := l.failureSpoolPipelines.close()
		l.failureSpoolPipelinesErr = err
		if !complete {
			return errors.Join(l.appendLogMaintenanceErr, err)
		}
		l.closeStage = topLevelLoggerCloseStageFallbackHandlers
	}
	if l.closeStage == topLevelLoggerCloseStageFallbackHandlers {
		if err := retainCompletedCloseError(l.fallbackCloserOwner, l.fallbackCloserOwner.Close(), &l.completedCloseErrors); err != nil {
			return errors.Join(l.appendLogMaintenanceErr, l.failureSpoolPipelinesErr, err)
		}
		l.closeStage = topLevelLoggerCloseStageInternalLogDestinations
	}
	if l.closeStage == topLevelLoggerCloseStageInternalLogDestinations {
		if err := retainCompletedCloseError(l.internalLogDestinations, l.internalLogDestinations.Close(), &l.completedCloseErrors); err != nil {
			return errors.Join(l.appendLogMaintenanceErr, l.failureSpoolPipelinesErr, err)
		}
		l.closeStage = topLevelLoggerCloseStageFileLoggerSet
	}
	if l.closeStage == topLevelLoggerCloseStageFileLoggerSet {
		if l.fileLoggerSet != nil {
			if err := retainCompletedCloseError(l.fileLoggerSet, l.fileLoggerSet.Close(), &l.completedCloseErrors); err != nil {
				return errors.Join(l.appendLogMaintenanceErr, l.failureSpoolPipelinesErr, err)
			}
		}
		l.closeStage = topLevelLoggerCloseStageCompressedLogSpool
	}
	if l.closeStage == topLevelLoggerCloseStageCompressedLogSpool {
		if err := l.compressedLogSpoolRuntimes.Close(); err != nil {
			l.completedCloseErrors = append(l.completedCloseErrors, err)
		}
		l.closeStage = topLevelLoggerCloseStageCompleted
	}
	return errors.Join(l.appendLogMaintenanceErr, l.failureSpoolPipelinesErr, errors.Join(l.completedCloseErrors...))
}

func (c rollbackContextCloser) Close() error {
	return c.close(context.Background())
}

// rollbackLoggerConstructionは、top-level loggerのconstructorが完成する前に失敗した場合、
// それ以前に所有権を取得したchildを構築順の逆順で閉じ、元の構築errorとClose errorを
// まとめて返します。ここでいうrollbackはlog dataや設定を以前の状態へ戻す処理ではなく、
// 完成しなかったlogger構成にgoroutine、timer、client、file handleなどを残さないための
// resource cleanupです。
func rollbackLoggerConstruction(
	initErr error,
	loggers []io.Closer,
) error {
	errs := []error{initErr}
	for i := len(loggers) - 1; i >= 0; i-- {
		if err := loggers[i].Close(); err != nil {
			errs = append(errs, markLoggerOwnedResourceCleanupIncomplete(
				fmt.Errorf("rollback close %T: %w", loggers[i], err),
			))
		}
	}
	return errors.Join(errs...)
}

// rollbackFailureHandlerConstructionは、failure handlerをbackend loggerへ組み込み、
// FlushManager.CloseからDrainできる状態になる前に後続の構築が失敗した場合、
// 部分構築済みのdurable handlerを終了します。
// factoryは利用可能なhandlerと構築errorを同時に返すため、factory自身のerror時にも必要です。
// requestの終了状態には従わず、Composite自身のTimeoutを使うためBackgroundを渡します。
func rollbackFailureHandlerConstruction(
	initErr error,
	handler failureHandlerDrainer,
) error {
	if handler == nil {
		return initErr
	}
	if drainErr := handler.Drain(context.Background()); drainErr != nil {
		return errors.Join(initErr, markLoggerOwnedResourceCleanupIncomplete(
			fmt.Errorf("rollback failure handler: %w", drainErr),
		))
	}
	return initErr
}

func newFileLoggerSet(
	cfg *logger.FileLoggerConfig,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) (*file.LoggerSet, error) {
	return newFileLoggerSetWithDirectoryScopeLabel(
		syncFileDirectoryScopeLabel,
		cfg,
		tracer,
		meter,
	)
}

func newFileLoggerSetWithDirectoryScopeLabel(
	directoryScopeLabel directorylease.ScopeLabel,
	cfg *logger.FileLoggerConfig,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	spoolOwners ...*compressedlog.RootRuntimeBuilder,
) (*file.LoggerSet, error) {
	if cfg == nil {
		return nil, nil
	}
	var spoolRuntimes []compressedlog.RootRuntimes
	if len(spoolOwners) > 0 {
		var runtimes compressedlog.RootRuntimes
		if cfg.LifecycleMode != pkgfsstore.FileLifecycleModeDisable {
			upload := activeCompressedLogSpoolUpload(cfg.ObjectStorageUploadConfig, cfg.FsstoreConfig.Layout)
			var err error
			runtimes, err = buildCompressedLogSpoolRuntimes(upload, spoolOwners...)
			if err != nil {
				return nil, err
			}
		}
		spoolRuntimes = append(spoolRuntimes, runtimes)
	}
	return file.NewLoggerSetWithDirectoryScopeLabel(
		directoryScopeLabel,
		file.NewConfigAdapter(*cfg),
		tracer,
		meter,
		spoolRuntimes...,
	)
}

func newDeliveryFileLoggerSet(
	cfg *logger.DeliveryFileLoggerConfig,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) (*file.DeliveryLoggerSet, error) {
	return newDeliveryFileLoggerSetWithDirectoryScopeLabel(
		deliveryFileDirectoryScopeLabel,
		cfg,
		tracer,
		meter,
	)
}

func newDeliveryFileLoggerSetWithDirectoryScopeLabel(
	directoryScopeLabel directorylease.ScopeLabel,
	cfg *logger.DeliveryFileLoggerConfig,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) (*file.DeliveryLoggerSet, error) {
	if cfg == nil {
		return nil, nil
	}
	return file.NewDeliveryLoggerSetWithDirectoryScopeLabel(
		directoryScopeLabel,
		file.NewDeliveryFileLoggerConfigAdapter(*cfg),
		tracer,
		meter,
	)
}

func closeFileLoggerSetWithError(initErr error, loggerSet *file.LoggerSet) error {
	if loggerSet == nil {
		return initErr
	}
	if closeErr := loggerSet.Close(); closeErr != nil {
		return errors.Join(initErr, fmt.Errorf("close file logger set: %w", closeErr))
	}
	return initErr
}

func closeOwnedLoggerResourcesWithError(
	initErr error,
	failureSpoolPipelines *failureSpoolPipelineSet,
	fallbackCloserOwner *fallbackHandlerCloserOwner,
	internalDestinations *internalLogDestinationRuntime,
	loggerSet *file.LoggerSet,
) error {
	var errs []error
	if initErr != nil {
		errs = append(errs, initErr)
	}
	// backendまたはCompositeの終了が未完了なら、借用中であり得るspool writerを
	// Shutdownせず、runner・Lease・fallback resourceも維持します。到達不能resourceを
	// process内に残す方が、同じrootを新しいinstanceへ誤って再取得させるより安全です。
	if errors.Is(initErr, errLoggerOwnedResourceCleanupIncomplete) {
		return errors.Join(errs...)
	}
	complete, pipelineErr := failureSpoolPipelines.close()
	if pipelineErr != nil {
		if !complete {
			pipelineErr = markLoggerOwnedResourceCleanupIncomplete(pipelineErr)
		}
		errs = append(errs, pipelineErr)
	}
	if !complete {
		return errors.Join(errs...)
	}
	if err := fallbackCloserOwner.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := internalDestinations.Close(); err != nil {
		errs = append(errs, err)
	}
	if loggerSet != nil {
		if err := loggerSet.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close file logger set: %w", err))
		}
	}
	return errors.Join(errs...)
}

// fileOutputEnabledはFile configの存在と通常出力としてのactivationを区別します。
// destinationsが空の直接構築configは、従来どおりFile pointerを通常出力指定として扱います。
func fileOutputEnabled(cfg logger.LoggerConfig) bool {
	if cfg.File == nil {
		return false
	}
	return len(cfg.LogOutputDestinations) == 0 ||
		cfg.LogOutputDestinations.Has(logger.LogOutputFile)
}

func logOutputEnabled(
	destinations logger.LogOutputDestinations,
	target logger.LogOutputDestination,
) bool {
	return len(destinations) == 0 || destinations.Has(target)
}

func deliveryFileOutputEnabled(cfg logger.DeliveryLoggerConfig) bool {
	return cfg.File != nil &&
		logOutputEnabled(cfg.LogOutputDestinations, logger.LogOutputFile)
}

// baseFileRuntimeEnabledは通常出力、または通常File設定をそのまま使うinternal logが
// 存在する場合だけbase FileLoggerSetを起動します。internal logが専用File設定を持ち、
// Fileが通常出力ではない場合、base設定は専用設定のtemplateにすぎないため起動しません。
func baseFileRuntimeEnabled(cfg logger.LoggerConfig) bool {
	if cfg.File == nil {
		return false
	}
	if fileOutputEnabled(cfg) {
		return true
	}
	if cfg.InternalLogDestinations != nil &&
		cfg.InternalLogDestinations.File != nil {
		return false
	}
	return logger.LoggerConfigRequiresBaseBackend(cfg, logger.LogOutputFile)
}

func baseDeliveryFileRuntimeEnabled(cfg logger.DeliveryLoggerConfig) bool {
	if cfg.File == nil {
		return false
	}
	if deliveryFileOutputEnabled(cfg) {
		return true
	}
	if cfg.InternalLogDestinations != nil &&
		cfg.InternalLogDestinations.File != nil {
		return false
	}
	return logger.DeliveryLoggerConfigRequiresBaseBackend(cfg, logger.LogOutputFile)
}

func validateLoggerConstructionConfig(
	sourceLabel string,
	cfg logger.LoggerConfig,
	cloudWatchLogsStreamRoutingGranularity pkgcloudwatchlogs.StreamRouting,
) error {
	if err := logger.ValidateLoggerBackendConfigs(sourceLabel, cfg); err != nil {
		return err
	}
	if err := validateLoggerFilePurgeConfigs(sourceLabel, cfg); err != nil {
		return err
	}
	if err := validateLoggerCloudWatchLogsStreamRouting(
		sourceLabel,
		cfg,
		cloudWatchLogsStreamRoutingGranularity,
	); err != nil {
		return err
	}
	if err := logger.ValidateLoggerHandlerRouting(sourceLabel, cfg); err != nil {
		return err
	}
	if err := logger.ValidateLoggerRetryClientConfigs(sourceLabel, cfg); err != nil {
		return err
	}
	// 内部ログの通知先に選ばれたbackendは、通知の初回に通知先を作れず通知を黙って捨てることが
	// ないよう、resourceを作る前に通知先を作れることを確かめます。
	return validateLoggerNotificationTargets(sourceLabel, cfg, cloudWatchLogsStreamRoutingGranularity)
}

// NewLoggerは同期loggerを構築します。選択済みbackendのfailure storeで
// DurableTypeSpoolが有効な場合、spoolConfigをrunner tuningとして必須とし、writer、
// runner、root Leaseを一つのpipelineとして起動します。CloseはbackendをDrainした後、
// writerをShutdownしてからrunnerを停止し、最後にLeaseを解放します。
// 構築後にctxが終了してもrunnerは停止せず、返されたloggerのCloseまで維持します。
// failure spoolが一つも有効でない場合だけspoolConfigにnilを指定できます。
// uploadを使用する場合だけ、共通のcheckpoint保存先をworker開始前に準備します。
// 同じ保存先を共有するloggerの初回構築は、呼び出し元で順番に行ってください。
// cloudWatchLogsStreamRoutingGranularityはpkg/loggerのbackend DTOとは分離された
// application側のstream分割方針で、通常出力とinternal log出力の両方へ適用します。
// TimeoutとMaxLogEntryBytesは、log rootやspoolなどのresourceを作る前に検証します。
func NewLogger(
	ctx context.Context,
	cfg logger.LoggerConfig,
	cloudWatchLogsStreamRoutingGranularity pkgcloudwatchlogs.StreamRouting,
	spoolConfig *pkgspool.SpoolConfig,
	memoryMonitor memorypressure.Monitor,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) (applogging.CloseableLogger, error) {
	// fan-out構築時と同じ規則を、filesystem上のresourceを作る前に適用します。
	if err := multi.ValidateMultiLoggerConfig(NewMultiLoggerAdapter(cfg)); err != nil {
		return nil, err
	}
	if err := validateLoggerConstructionConfig(
		logger.LoggerContractSync.DisplayName(),
		cfg,
		cloudWatchLogsStreamRoutingGranularity,
	); err != nil {
		return nil, err
	}
	plan, err := buildFailureSpoolPipelinePlan([]failureSpoolPipelineSource{{
		contractKind: logger.LoggerContractSync,
		config:       cfg,
	}})
	if err != nil {
		return nil, err
	}
	if err := prepareLoggerArchiveCheckpointDirectory(cfg); err != nil {
		return nil, err
	}
	failureSpoolPipelines, err := newFailureSpoolPipelineSet(
		ctx,
		plan,
		spoolConfig,
		tracer,
		meter,
	)
	if err != nil {
		return nil, err
	}

	owned, err := newLoggerCore(
		ctx,
		cfg,
		cloudWatchLogsStreamRoutingGranularity,
		memoryMonitor,
		tracer,
		meter,
		failureSpoolPipelines,
	)
	if err != nil {
		return nil, err
	}
	return owned, nil
}

// newLoggerCoreはsync logger本体を構築し、failure spoolが有効な場合は各backendの
// writerを事前構築済みpipelineへ登録します。返すtop-level ownerがbackendとpipelineの
// Close順を一つの場所で管理します。
func newLoggerCore(
	ctx context.Context,
	cfg logger.LoggerConfig,
	cloudWatchLogsStreamRoutingGranularity pkgcloudwatchlogs.StreamRouting,
	memoryMonitor memorypressure.Monitor,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	failureSpoolPipelines *failureSpoolPipelineSet,
) (_ *topLevelLoggerRuntime, resultErr error) {
	spoolOwners, err := newLoggerCompressedLogSpoolRuntimeBuilder(cfg)
	if err != nil {
		return nil, closeFailureSpoolPipelinesAfterConstructionError(err, failureSpoolPipelines)
	}
	defer func() {
		if resultErr != nil && !errors.Is(resultErr, errLoggerOwnedResourceCleanupIncomplete) {
			resultErr = errors.Join(resultErr, spoolOwners.Close())
		}
	}()

	var fileLoggerSet *file.LoggerSet
	if baseFileRuntimeEnabled(cfg) {
		var err error
		fileLoggerSet, err = newFileLoggerSetWithDirectoryScopeLabel(
			syncFileDirectoryScopeLabel,
			cfg.File,
			tracer,
			meter,
			spoolOwners,
		)
		if err != nil {
			return nil, closeFailureSpoolPipelinesAfterConstructionError(
				err,
				failureSpoolPipelines,
			)
		}
	}
	fallbackCloserOwner := newFallbackHandlerCloserOwner()
	internalDestinations, err := newInternalLogDestinationRuntime(
		cfg,
		cloudWatchLogsStreamRoutingGranularity,
		fileLoggerSet,
		syncInternalLogFileDirectoryScopeLabel,
		tracer,
		meter,
		spoolOwners,
	)
	if err != nil {
		err = closeFailureSpoolPipelinesAfterConstructionError(err, failureSpoolPipelines)
		return nil, closeFileLoggerSetWithError(err, fileLoggerSet)
	}

	loggers, ownedClosers, err := buildLoggers(
		ctx,
		cfg,
		cloudWatchLogsStreamRoutingGranularity,
		memoryMonitor,
		tracer,
		meter,
		fileLoggerSet,
		internalDestinations,
		fallbackCloserOwner,
		logger.LoggerContractSync,
		failureSpoolPipelines,
		spoolOwners,
	)
	if err != nil {
		return nil, closeOwnedLoggerResourcesWithError(
			err,
			failureSpoolPipelines,
			fallbackCloserOwner,
			internalDestinations,
			fileLoggerSet,
		)
	}
	if len(loggers) == 0 {
		err := rollbackLoggerConstruction(fmt.Errorf("no loggers available"), ownedClosers)
		return nil, closeOwnedLoggerResourcesWithError(
			err,
			failureSpoolPipelines,
			fallbackCloserOwner,
			internalDestinations,
			fileLoggerSet,
		)
	}
	if err := failureSpoolPipelines.validateComplete(); err != nil {
		err = rollbackLoggerConstruction(err, ownedClosers)
		return nil, closeOwnedLoggerResourcesWithError(
			err,
			failureSpoolPipelines,
			fallbackCloserOwner,
			internalDestinations,
			fileLoggerSet,
		)
	}
	var baseLifecycleFile *logger.FileLoggerConfig
	if fileLoggerSet != nil {
		baseLifecycleFile = cfg.File
	}
	appendLogMaintenanceRuntime, appendLogMaintenanceErr := buildLogFileMaintenanceRuntime(
		ctx,
		baseLifecycleFile,
		internalDestinations.dedicatedFileConfig(),
		internalDestinations.newHandlers,
		fallbackCloserOwner,
		tracer,
		meter,
		spoolOwners,
	)
	if appendLogMaintenanceErr != nil {
		err = rollbackLoggerConstruction(appendLogMaintenanceErr, ownedClosers)
		return nil, closeOwnedLoggerResourcesWithError(
			err,
			failureSpoolPipelines,
			fallbackCloserOwner,
			internalDestinations,
			fileLoggerSet,
		)
	}
	multiCfg := NewMultiLoggerAdapter(cfg)
	fanout, err := multi.NewMultiLogger(multiCfg, loggers...)
	if err != nil {
		err = errors.Join(err, appendLogMaintenanceRuntime.Close())
		err = rollbackLoggerConstruction(err, ownedClosers)
		return nil, closeOwnedLoggerResourcesWithError(
			err,
			failureSpoolPipelines,
			fallbackCloserOwner,
			internalDestinations,
			fileLoggerSet,
		)
	}
	result := &topLevelLoggerRuntime{
		topLevelLoggerDelegate:      fanout,
		fallbackCloserOwner:         fallbackCloserOwner,
		fileLoggerSet:               fileLoggerSet,
		internalLogDestinations:     internalDestinations,
		appendLogMaintenanceRuntime: appendLogMaintenanceRuntime,
		failureSpoolPipelines:       failureSpoolPipelines,
		compressedLogSpoolRuntimes:  &compressedLogSpoolRuntimes{runtimes: spoolOwners.OwnedRuntimes()},
	}
	if err := appendLogMaintenanceRuntime.Start(ctx); err != nil {
		closeErr := result.Close()
		if closeErr != nil && result.closeStage == topLevelLoggerCloseStageBackendLogger {
			closeErr = markLoggerOwnedResourceCleanupIncomplete(closeErr)
		}
		err = errors.Join(err, closeErr)
		return nil, err
	}
	if err := failureSpoolPipelines.Start(ctx); err != nil {
		closeErr := result.Close()
		if closeErr != nil && result.closeStage == topLevelLoggerCloseStageBackendLogger {
			closeErr = markLoggerOwnedResourceCleanupIncomplete(closeErr)
		}
		return nil, errors.Join(err, closeErr)
	}
	return result, nil
}

// NewAsyncLoggerは非同期loggerを構築します。failure spoolの有効判定、spoolConfigの
// 必須性、writer・runnerの所有順序はNewLoggerと同じです。Closeは受理済みasync処理を
// 完了してからrunnerを停止します。構築後のctx終了だけではrunnerを停止しません。
// checkpoint保存先の準備と、共有するloggerの構築順序はNewLoggerと同じです。
// CloudWatch Logsのstream分割方針もNewLoggerと同じ独立引数で受け取ります。
// Timeout、MaxParallel、MaxLogEntryBytesは、resourceを作る前に検証します。
func NewAsyncLogger(
	ctx context.Context,
	cfg logger.AsyncBestEffortMultiLoggerConfig,
	cloudWatchLogsStreamRoutingGranularity pkgcloudwatchlogs.StreamRouting,
	spoolConfig *pkgspool.SpoolConfig,
	memoryMonitor memorypressure.Monitor,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) (applogging.AsyncCloseableLogger, error) {
	// fan-out構築時と同じ規則を、filesystem上のresourceを作る前に適用します。
	if err := multi.ValidateAsyncBestEffortMultiLoggerConfig(NewAsyncBestEffortMultiLoggerAdapter(cfg)); err != nil {
		return nil, err
	}

	// 子ロガー構築は従来どおり LoggerConfig を使う
	lc := cfg.ToLoggerConfig()
	if err := validateLoggerConstructionConfig(
		logger.LoggerContractAsyncBestEffort.DisplayName(),
		lc,
		cloudWatchLogsStreamRoutingGranularity,
	); err != nil {
		return nil, err
	}
	plan, err := buildFailureSpoolPipelinePlan([]failureSpoolPipelineSource{{
		contractKind: logger.LoggerContractAsyncBestEffort,
		config:       lc,
	}})
	if err != nil {
		return nil, err
	}
	if err := prepareLoggerArchiveCheckpointDirectory(lc); err != nil {
		return nil, err
	}
	failureSpoolPipelines, err := newFailureSpoolPipelineSet(
		ctx,
		plan,
		spoolConfig,
		tracer,
		meter,
	)
	if err != nil {
		return nil, err
	}

	owned, err := newAsyncLoggerCore(
		ctx,
		cfg,
		cloudWatchLogsStreamRoutingGranularity,
		memoryMonitor,
		tracer,
		meter,
		failureSpoolPipelines,
	)
	if err != nil {
		return nil, err
	}
	return owned, nil
}

// newAsyncLoggerCoreはasync logger本体を構築し、sync版と同じtop-level ownerへ
// failure spool pipelineを組み込みます。
func newAsyncLoggerCore(
	ctx context.Context,
	cfg logger.AsyncBestEffortMultiLoggerConfig,
	cloudWatchLogsStreamRoutingGranularity pkgcloudwatchlogs.StreamRouting,
	memoryMonitor memorypressure.Monitor,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	failureSpoolPipelines *failureSpoolPipelineSet,
) (_ *topLevelAsyncLoggerRuntime, resultErr error) {
	lc := cfg.ToLoggerConfig()
	spoolOwners, err := newLoggerCompressedLogSpoolRuntimeBuilder(lc)
	if err != nil {
		return nil, closeFailureSpoolPipelinesAfterConstructionError(err, failureSpoolPipelines)
	}
	defer func() {
		if resultErr != nil && !errors.Is(resultErr, errLoggerOwnedResourceCleanupIncomplete) {
			resultErr = errors.Join(resultErr, spoolOwners.Close())
		}
	}()

	var fileLoggerSet *file.LoggerSet
	if baseFileRuntimeEnabled(lc) {
		var err error
		fileLoggerSet, err = newFileLoggerSetWithDirectoryScopeLabel(
			asyncFileDirectoryScopeLabel,
			lc.File,
			tracer,
			meter,
			spoolOwners,
		)
		if err != nil {
			return nil, closeFailureSpoolPipelinesAfterConstructionError(
				err,
				failureSpoolPipelines,
			)
		}
	}
	fallbackCloserOwner := newFallbackHandlerCloserOwner()
	internalDestinations, err := newInternalLogDestinationRuntime(
		lc,
		cloudWatchLogsStreamRoutingGranularity,
		fileLoggerSet,
		asyncInternalLogFileDirectoryScopeLabel,
		tracer,
		meter,
		spoolOwners,
	)
	if err != nil {
		err = closeFailureSpoolPipelinesAfterConstructionError(err, failureSpoolPipelines)
		return nil, closeFileLoggerSetWithError(err, fileLoggerSet)
	}

	loggers, ownedClosers, err := buildLoggers(
		ctx,
		lc,
		cloudWatchLogsStreamRoutingGranularity,
		memoryMonitor,
		tracer,
		meter,
		fileLoggerSet,
		internalDestinations,
		fallbackCloserOwner,
		logger.LoggerContractAsyncBestEffort,
		failureSpoolPipelines,
		spoolOwners,
	)
	if err != nil {
		return nil, closeOwnedLoggerResourcesWithError(
			err,
			failureSpoolPipelines,
			fallbackCloserOwner,
			internalDestinations,
			fileLoggerSet,
		)
	}
	if len(loggers) == 0 {
		err := rollbackLoggerConstruction(fmt.Errorf("no loggers available"), ownedClosers)
		return nil, closeOwnedLoggerResourcesWithError(
			err,
			failureSpoolPipelines,
			fallbackCloserOwner,
			internalDestinations,
			fileLoggerSet,
		)
	}
	if err := failureSpoolPipelines.validateComplete(); err != nil {
		err = rollbackLoggerConstruction(err, ownedClosers)
		return nil, closeOwnedLoggerResourcesWithError(
			err,
			failureSpoolPipelines,
			fallbackCloserOwner,
			internalDestinations,
			fileLoggerSet,
		)
	}
	var baseLifecycleFile *logger.FileLoggerConfig
	if fileLoggerSet != nil {
		baseLifecycleFile = lc.File
	}
	appendLogMaintenanceRuntime, appendLogMaintenanceErr := buildLogFileMaintenanceRuntime(
		ctx,
		baseLifecycleFile,
		internalDestinations.dedicatedFileConfig(),
		internalDestinations.newHandlers,
		fallbackCloserOwner,
		tracer,
		meter,
		spoolOwners,
	)
	if appendLogMaintenanceErr != nil {
		err = rollbackLoggerConstruction(appendLogMaintenanceErr, ownedClosers)
		return nil, closeOwnedLoggerResourcesWithError(
			err,
			failureSpoolPipelines,
			fallbackCloserOwner,
			internalDestinations,
			fileLoggerSet,
		)
	}

	// timeout/maxParallel は multi 側が cfg provider から読む。
	multiCfg := NewAsyncBestEffortMultiLoggerAdapter(cfg)
	asyncFanout, err := multi.NewAsyncBestEffortMultiLoggerWithMeter(multiCfg, meter, loggers...)
	if err != nil {
		err = errors.Join(err, appendLogMaintenanceRuntime.Close())
		err = rollbackLoggerConstruction(err, ownedClosers)
		return nil, closeOwnedLoggerResourcesWithError(
			err,
			failureSpoolPipelines,
			fallbackCloserOwner,
			internalDestinations,
			fileLoggerSet,
		)
	}
	result := &topLevelAsyncLoggerRuntime{
		topLevelAsyncLoggerDelegate: asyncFanout,
		fallbackCloserOwner:         fallbackCloserOwner,
		fileLoggerSet:               fileLoggerSet,
		internalLogDestinations:     internalDestinations,
		appendLogMaintenanceRuntime: appendLogMaintenanceRuntime,
		failureSpoolPipelines:       failureSpoolPipelines,
		compressedLogSpoolRuntimes:  &compressedLogSpoolRuntimes{runtimes: spoolOwners.OwnedRuntimes()},
	}
	if err := appendLogMaintenanceRuntime.Start(ctx); err != nil {
		closeErr := result.Close()
		if closeErr != nil && result.closeStage == topLevelLoggerCloseStageBackendLogger {
			closeErr = markLoggerOwnedResourceCleanupIncomplete(closeErr)
		}
		err = errors.Join(err, closeErr)
		return nil, err
	}
	if err := failureSpoolPipelines.Start(ctx); err != nil {
		closeErr := result.Close()
		if closeErr != nil && result.closeStage == topLevelLoggerCloseStageBackendLogger {
			closeErr = markLoggerOwnedResourceCleanupIncomplete(closeErr)
		}
		return nil, errors.Join(err, closeErr)
	}
	return result, nil
}

// NewDeliveryLoggerは送達確認loggerを構築します。CloudWatch Logsのstream分割方針は
// backend DTOに含めず、同じDelivery契約の通常出力とinternal log出力へ適用します。
// checkpoint保存先の準備と、共有するloggerの構築順序はNewLoggerと同じです。
// TimeoutとMaxLogEntryBytesは、resourceを作る前に検証します。
func NewDeliveryLogger(
	ctx context.Context,
	cfg logger.DeliveryLoggerConfig,
	cloudWatchLogsStreamRoutingGranularity pkgcloudwatchlogs.StreamRouting,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) (_ applogging.DeliveryClosableLogger, resultErr error) {
	// fan-out構築時と同じ規則を、filesystem上のresourceを作る前に適用します。
	if err := multi.ValidateDeliveryMultiLoggerConfig(NewDeliveryMultiLoggerAdapter(cfg)); err != nil {
		return nil, err
	}
	if err := logger.ValidateDeliveryLoggerBackendConfigs(logger.LoggerContractDelivery.DisplayName(), cfg); err != nil {
		return nil, err
	}
	if err := validateDeliveryLoggerFilePurgeConfigs(logger.LoggerContractDelivery.DisplayName(), cfg); err != nil {
		return nil, err
	}
	if err := validateDeliveryLoggerCloudWatchLogsStreamRouting(
		logger.LoggerContractDelivery.DisplayName(),
		cfg,
		cloudWatchLogsStreamRoutingGranularity,
	); err != nil {
		return nil, err
	}
	if err := logger.ValidateDeliveryLoggerHandlerRouting(logger.LoggerContractDelivery.DisplayName(), cfg); err != nil {
		return nil, err
	}
	if err := logger.ValidateDeliveryLoggerRetryClientConfigs(logger.LoggerContractDelivery.DisplayName(), cfg); err != nil {
		return nil, err
	}
	if err := validateDeliveryLoggerNotificationTargets(
		logger.LoggerContractDelivery.DisplayName(),
		cfg,
		cloudWatchLogsStreamRoutingGranularity,
	); err != nil {
		return nil, err
	}
	if err := prepareDeliveryArchiveCheckpointDirectory(cfg); err != nil {
		return nil, err
	}
	spoolOwners, err := newDeliveryCompressedLogSpoolRuntimeBuilder(cfg)
	if err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil && !errors.Is(resultErr, errLoggerOwnedResourceCleanupIncomplete) {
			resultErr = errors.Join(resultErr, spoolOwners.Close())
		}
	}()

	var fileLoggerSet *file.DeliveryLoggerSet
	if baseDeliveryFileRuntimeEnabled(cfg) {
		var err error
		fileLoggerSet, err = newDeliveryFileLoggerSetWithDirectoryScopeLabel(
			deliveryFileDirectoryScopeLabel,
			cfg.File,
			tracer,
			meter,
		)
		if err != nil {
			return nil, err
		}
	}
	fallbackCloserOwner := newFallbackHandlerCloserOwner()
	internalDestinations, err := newDeliveryInternalLogDestinationRuntime(
		cfg,
		cloudWatchLogsStreamRoutingGranularity,
		fileLoggerSet,
		deliveryInternalLogFileDirectoryScopeLabel,
		tracer,
		meter,
	)
	if err != nil {
		return nil, closeDeliveryFileLoggerSetWithError(err, fileLoggerSet)
	}
	targets, ownedClosers, err := buildDeliveryLoggers(
		ctx,
		cfg,
		cloudWatchLogsStreamRoutingGranularity,
		tracer,
		meter,
		fileLoggerSet,
		internalDestinations,
		fallbackCloserOwner,
		spoolOwners,
	)
	if err != nil {
		return nil, closeOwnedDeliveryLoggerResourcesWithError(
			err,
			fallbackCloserOwner,
			internalDestinations,
			fileLoggerSet,
		)
	}
	if len(targets) == 0 {
		err := rollbackLoggerConstruction(fmt.Errorf("no loggers available"), ownedClosers)
		return nil, closeOwnedDeliveryLoggerResourcesWithError(
			err,
			fallbackCloserOwner,
			internalDestinations,
			fileLoggerSet,
		)
	}
	var baseLifecycleFile *logger.DeliveryFileLoggerConfig
	if fileLoggerSet != nil {
		baseLifecycleFile = cfg.File
	}
	appendLogMaintenanceRuntime, appendLogMaintenanceErr := buildDeliveryFileMaintenanceRuntime(
		ctx,
		baseLifecycleFile,
		internalDestinations.dedicatedFileConfig(),
		internalDestinations.newHandlers,
		fallbackCloserOwner,
		tracer,
		meter,
		spoolOwners,
	)
	if appendLogMaintenanceErr != nil {
		err = rollbackLoggerConstruction(appendLogMaintenanceErr, ownedClosers)
		return nil, closeOwnedDeliveryLoggerResourcesWithError(
			err,
			fallbackCloserOwner,
			internalDestinations,
			fileLoggerSet,
		)
	}

	multiCfg := NewDeliveryMultiLoggerAdapter(cfg)
	deliveryFanout, err := multi.NewDeliveryMultiLoggerWithTargets(multiCfg, targets...)
	if err != nil {
		err = errors.Join(err, appendLogMaintenanceRuntime.Close())
		err = rollbackLoggerConstruction(err, ownedClosers)
		return nil, closeOwnedDeliveryLoggerResourcesWithError(
			err,
			fallbackCloserOwner,
			internalDestinations,
			fileLoggerSet,
		)
	}
	var result applogging.DeliveryClosableLogger = &topLevelDeliveryLoggerRuntime{
		topLevelDeliveryLoggerDelegate: deliveryFanout,
		fallbackCloserOwner:            fallbackCloserOwner,
		fileLoggerSet:                  fileLoggerSet,
		internalLogDestinations:        internalDestinations,
		appendLogMaintenanceRuntime:    appendLogMaintenanceRuntime,
		compressedLogSpoolRuntimes:     &compressedLogSpoolRuntimes{runtimes: spoolOwners.OwnedRuntimes()},
	}
	if err := appendLogMaintenanceRuntime.Start(ctx); err != nil {
		err = errors.Join(err, result.Close())
		return nil, err
	}
	return result, nil
}

func closeDeliveryFileLoggerSetWithError(
	initErr error,
	loggerSet *file.DeliveryLoggerSet,
) error {
	if loggerSet == nil {
		return initErr
	}
	if closeErr := loggerSet.Close(); closeErr != nil {
		return errors.Join(initErr, fmt.Errorf("close delivery file logger set: %w", closeErr))
	}
	return initErr
}

func closeOwnedDeliveryLoggerResourcesWithError(
	initErr error,
	fallbackCloserOwner *fallbackHandlerCloserOwner,
	internalDestinations *deliveryInternalLogDestinationRuntime,
	loggerSet *file.DeliveryLoggerSet,
) error {
	var errs []error
	if initErr != nil {
		errs = append(errs, initErr)
	}
	if err := fallbackCloserOwner.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := internalDestinations.Close(); err != nil {
		errs = append(errs, err)
	}
	if loggerSet != nil {
		if err := loggerSet.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close delivery file logger set: %w", err))
		}
	}
	return errors.Join(errs...)
}

// 子ロガー群を生成（子は error を返す Logger のまま保持）
func buildLoggers(
	ctx context.Context,
	cfg logger.LoggerConfig,
	cloudWatchLogsStreamRoutingGranularity pkgcloudwatchlogs.StreamRouting,
	memoryMonitor memorypressure.Monitor,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	fileLoggerSet *file.LoggerSet,
	internalDestinations *internalLogDestinationRuntime,
	fallbackCloserOwner *fallbackHandlerCloserOwner,
	contractKind logger.LoggerContractKind,
	failureSpoolPipelines *failureSpoolPipelineSet,
	spoolOwners ...*compressedlog.RootRuntimeBuilder,
) (
	loggers []infralogging.BackendLogEntryHandler,
	resultClosers []io.Closer,
	err error,
) {
	var ownedClosers []io.Closer
	var pendingFailureHandler failureHandlerDrainer

	// 通常FileLoggerとそのfile resourceは、返却するtopLevelLoggerRuntimeが所有します。
	// ここではbackend childだけをrollback対象として保持し、二重所有を避けます。
	defer func() {
		if err == nil {
			return
		}
		err = rollbackFailureHandlerConstruction(err, pendingFailureHandler)
		err = rollbackLoggerConstruction(err, ownedClosers)
		loggers = nil
		resultClosers = nil
	}()

	// fallback loggerはsource loggerごとに独立した状態を持たせます。
	// File handlerも呼び出すたびに新しくなりますが、書き込み先のrole別FileLoggerは
	// internal-log保存先runtimeが解決したLoggerSetから取得します。
	newAvailableHandlers := func(source string) handlerStruct {
		available := internalDestinations.newHandlers()
		fallbackCloserOwner.Register(source, available)
		return available
	}
	if fileLoggerSet != nil && cfg.File != nil {
		adapter := file.NewConfigAdapter(*cfg.File)
		if err := fileLoggerSet.ConfigureInternalHandlers(func(role file.InternalHandlerRole) file.InternalHandlers {
			internalHandler := selectConfiguredHandlers(
				newAvailableHandlers(common.SourceNameFileLogger+"."+string(role)),
				adapter.Handler(),
			)
			return file.InternalHandlers{
				OnError: onerrorhandler.CombineErrorHandlers(internalHandler.OnError...),
				OnWarn:  onerrorhandler.CombineWarnHandlers(internalHandler.OnWarn...),
				OnInfo:  onerrorhandler.CombineInfoHandlers(internalHandler.OnInfo...),
				OnDebug: onerrorhandler.CombineDebugHandlers(internalHandler.OnDebug...),
			}
		}); err != nil {
			return nil, nil, err
		}
	}

	if fileOutputEnabled(cfg) {
		mainLogger, err := fileLoggerSet.NewFileLogger(ctx)
		if err != nil {
			return nil, nil, err
		}

		loggers = append(loggers, mainLogger)

		if cfg.File.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled {
			errorLogger, err := fileLoggerSet.NewErrorFileLogger(ctx)
			if err != nil {
				return nil, nil, err
			}

			loggers = append(loggers, errorLogger)
		}

		if cfg.File.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled {
			debugLogger, err := fileLoggerSet.NewDebugFileLogger(ctx)
			if err != nil {
				return nil, nil, err
			}

			loggers = append(loggers, debugLogger)
		}
	}

	if cfg.OpenSearch != nil &&
		logOutputEnabled(cfg.LogOutputDestinations, logger.LogOutputOpenSearch) {
		if cfg.OpenSearchClient == nil {
			return nil, nil, fmt.Errorf("opensearch client config is nil")
		}

		osLoggerCfg := opensearchlogger.NewOpenSearchLoggerConfigAdapter(
			cfg.OpenSearch.OpenSearchLoggerBaseConfig,
		)
		timestampFormatter, formatterErr := opensearchlogger.NewConfiguredTimestampFormatter(osLoggerCfg)
		if formatterErr != nil {
			return nil, nil, formatterErr
		}
		internalHandler := selectConfiguredHandlers(
			newAvailableHandlers(common.SourceNameOpenSearchLogger),
			osLoggerCfg.Handler(),
		)

		loggerBase := &opensearchlogger.LoggerBase{
			Clock:              clock.NewUTCClock(),
			TimestampFormatter: timestampFormatter,
			OnError:            onerrorhandler.CombineErrorHandlers(internalHandler.OnError...),
			OnWarn:             onerrorhandler.CombineWarnHandlers(internalHandler.OnWarn...),
			OnInfo:             onerrorhandler.CombineInfoHandlers(internalHandler.OnInfo...),
			OnDebug:            onerrorhandler.CombineDebugHandlers(internalHandler.OnDebug...),
		}

		fmCfg := opensearchlogger.NewFlushManagerConfigAdapter(
			cfg.OpenSearch.OpenSearchFlushManagerConfig,
		)

		spoolRuntimes, err := buildCompressedLogSpoolRuntimesForFailureStore(fmCfg.FailureStoreConfig(), spoolOwners...)
		if err != nil {
			return nil, nil, err
		}
		failureHandlers, err := opensearchlogger.BuildFailureHandler(
			ctx,
			loggerBase.Clock,
			fmCfg,
			memoryMonitor,
			tracer,
			meter,
			failureStoreInternalLogHandlerFactory(common.SourceNameOpenSearchLogger, newAvailableHandlers),
			failureSpoolPipelines.registrarFor(contractKind, logger.LogOutputOpenSearch),
			spoolRuntimes,
		)
		if failureHandlers != nil {
			pendingFailureHandler = failureHandlers
		}
		if err != nil {
			return nil, nil, err
		}
		clientCfg := infraopensearch.NewOpenSearchClientConfigAdapter(cfg.OpenSearchClient)
		retryCfg := opensearchlogger.NewRetryConfigProvider(
			cfg.OpenSearch.OpenSearchRetryableClientCaseTransportErrorConfig,
		)
		osClient, err := infraopensearch.NewClient(clientCfg, retryCfg, tracer, meter)
		if err != nil {
			return nil, nil, err
		}

		lg, err := opensearchlogger.NewOpenSearchLogger(
			ctx,
			osLoggerCfg,
			osClient,
			fmCfg,
			resource.NewResourceAdapter(cfg.OpenSearch.Resource),
			failureHandlers,
			tracer,
			meter,
			loggerBase,
		)
		if err != nil {
			return nil, nil, err
		}

		loggers = append(loggers, lg)
		ownedClosers = append(ownedClosers, rollbackContextCloser{close: lg.Close})
		pendingFailureHandler = nil
	}

	if cfg.Loki != nil &&
		logOutputEnabled(cfg.LogOutputDestinations, logger.LogOutputLoki) {
		if cfg.LokiClient == nil {
			return nil, nil, fmt.Errorf("loki client config is nil")
		}

		lokiCfg := *cfg.Loki

		lokiLoggerCfg := lokilogger.NewLokiLoggerConfigAdapter(
			lokiCfg.LokiLoggerBaseConfig,
		)
		timestampFormatter, formatterErr := lokilogger.NewConfiguredTimestampFormatter(lokiLoggerCfg)
		if formatterErr != nil {
			return nil, nil, formatterErr
		}
		internalHandler := selectConfiguredHandlers(
			newAvailableHandlers(common.SourceNameLokiLogger),
			lokiLoggerCfg.Handler(),
		)

		loggerBase := &lokilogger.LoggerBase{
			Clock:        clock.NewUTCClock(),
			LogFormatter: formatter.NewPlainTextFormatter(timestampFormatter),
			OnError:      onerrorhandler.CombineErrorHandlers(internalHandler.OnError...),
			OnWarn:       onerrorhandler.CombineWarnHandlers(internalHandler.OnWarn...),
			OnInfo:       onerrorhandler.CombineInfoHandlers(internalHandler.OnInfo...),
			OnDebug:      onerrorhandler.CombineDebugHandlers(internalHandler.OnDebug...),
		}

		fmCfg := lokilogger.NewFlushManagerConfigAdapter(
			lokiCfg.LokiFlushManagerConfig,
		)

		spoolRuntimes, err := buildCompressedLogSpoolRuntimesForFailureStore(fmCfg.FailureStoreConfig(), spoolOwners...)
		if err != nil {
			return nil, nil, err
		}
		failureHandlers, err := lokilogger.BuildFailureHandler(
			ctx,
			loggerBase.Clock,
			fmCfg,
			memoryMonitor,
			tracer,
			meter,
			failureStoreInternalLogHandlerFactory(common.SourceNameLokiLogger, newAvailableHandlers),
			failureSpoolPipelines.registrarFor(contractKind, logger.LogOutputLoki),
			spoolRuntimes,
		)
		if failureHandlers != nil {
			pendingFailureHandler = failureHandlers
		}
		if err != nil {
			return nil, nil, err
		}
		lokiClient, err := infraloki.NewClient(
			*cfg.LokiClient,
			lokilogger.NewRetryConfigProvider(lokiCfg.LokiRetryableClientCaseTransportErrorConfig),
			tracer,
			meter,
		)
		if err != nil {
			return nil, nil, err
		}

		lg, err := lokilogger.NewLokiLogger(
			ctx,
			lokiLoggerCfg,
			lokiClient,
			fmCfg,
			resource.NewResourceAdapter(lokiCfg.LokiFlushManagerConfig.Resource),
			failureHandlers,
			tracer,
			meter,
			loggerBase,
		)
		if err != nil {
			return nil, nil, err
		}

		loggers = append(loggers, lg)
		ownedClosers = append(ownedClosers, rollbackContextCloser{close: lg.Close})
		pendingFailureHandler = nil
	}

	if cfg.CloudWatchLogs != nil &&
		logOutputEnabled(cfg.LogOutputDestinations, logger.LogOutputCloudWatchLogs) {
		if cfg.CloudWatchLogsClient == nil {
			return nil, nil, fmt.Errorf("cloudwatch logs client config is nil")
		}

		cloudWatchCfg := *cfg.CloudWatchLogs

		cloudWatchLoggerCfg := cloudwatchlogslogger.NewCloudWatchLogsLoggerConfigAdapter(
			cloudWatchCfg.CloudWatchLogsLoggerBaseConfig,
		)
		cloudWatchTimestampFormatter, err := cloudwatchlogslogger.NewConfiguredTimestampFormatter(cloudWatchLoggerCfg)
		if err != nil {
			return nil, nil, err
		}
		internalHandler := selectConfiguredHandlers(
			newAvailableHandlers(common.SourceNameCloudWatchLogsLogger),
			cloudWatchLoggerCfg.Handler(),
		)

		loggerBase := &cloudwatchlogslogger.LoggerBase{
			Clock:        clock.NewUTCClock(),
			LogFormatter: formatter.NewPlainTextFormatter(cloudWatchTimestampFormatter),
			OnError:      onerrorhandler.CombineErrorHandlers(internalHandler.OnError...),
			OnWarn:       onerrorhandler.CombineWarnHandlers(internalHandler.OnWarn...),
			OnInfo:       onerrorhandler.CombineInfoHandlers(internalHandler.OnInfo...),
			OnDebug:      onerrorhandler.CombineDebugHandlers(internalHandler.OnDebug...),
		}

		fmCfg := cloudwatchlogslogger.NewFlushManagerConfigAdapter(
			cloudWatchCfg.CloudWatchLogsFlushManagerConfig,
			cloudWatchLogsStreamRoutingGranularity,
		)

		spoolRuntimes, err := buildCompressedLogSpoolRuntimesForFailureStore(fmCfg.FailureStoreConfig(), spoolOwners...)
		if err != nil {
			return nil, nil, err
		}
		failureHandlers, err := cloudwatchlogslogger.BuildFailureHandler(
			ctx,
			loggerBase.Clock,
			fmCfg,
			memoryMonitor,
			tracer,
			meter,
			failureStoreInternalLogHandlerFactory(common.SourceNameCloudWatchLogsLogger, newAvailableHandlers),
			failureSpoolPipelines.registrarFor(contractKind, logger.LogOutputCloudWatchLogs),
			spoolRuntimes,
		)
		if failureHandlers != nil {
			pendingFailureHandler = failureHandlers
		}
		if err != nil {
			return nil, nil, err
		}
		cloudWatchClient, err := infracloudwatchlogs.NewClient(
			*cfg.CloudWatchLogsClient,
			cloudwatchlogslogger.NewRetryConfigProvider(cloudWatchCfg.CloudWatchLogsRetryableClientCaseTransportErrorConfig),
			tracer,
			meter,
		)
		if err != nil {
			return nil, nil, err
		}
		lg, err := cloudwatchlogslogger.NewCloudWatchLogsLogger(
			ctx,
			cloudWatchLoggerCfg,
			cloudWatchClient,
			fmCfg,
			resource.NewResourceAdapter(cloudWatchCfg.CloudWatchLogsFlushManagerConfig.Resource),
			failureHandlers,
			tracer,
			meter,
			loggerBase,
		)
		if err != nil {
			return nil, nil, err
		}

		loggers = append(loggers, lg)
		ownedClosers = append(ownedClosers, rollbackContextCloser{close: lg.Close})
		pendingFailureHandler = nil
	}

	if cfg.ClickHouse != nil &&
		logOutputEnabled(cfg.LogOutputDestinations, logger.LogOutputClickHouse) {
		if cfg.ClickHouseClient == nil {
			return nil, nil, fmt.Errorf("clickhouse client config is nil")
		}

		clickHouseCfg := *cfg.ClickHouse

		clickHouseLoggerCfg := clickhouselogger.NewClickHouseLoggerConfigAdapter(
			clickHouseCfg.ClickHouseLoggerBaseConfig,
		)
		timestampFormatter, formatterErr := clickhouselogger.NewConfiguredTimestampFormatter(clickHouseLoggerCfg)
		if formatterErr != nil {
			return nil, nil, formatterErr
		}
		internalHandler := selectConfiguredHandlers(
			newAvailableHandlers(common.SourceNameClickHouseLogger),
			clickHouseLoggerCfg.Handler(),
		)

		loggerBase := &clickhouselogger.LoggerBase{
			Clock:        clock.NewUTCClock(),
			LogFormatter: formatter.NewPlainTextFormatter(timestampFormatter),
			OnError:      onerrorhandler.CombineErrorHandlers(internalHandler.OnError...),
			OnWarn:       onerrorhandler.CombineWarnHandlers(internalHandler.OnWarn...),
			OnInfo:       onerrorhandler.CombineInfoHandlers(internalHandler.OnInfo...),
			OnDebug:      onerrorhandler.CombineDebugHandlers(internalHandler.OnDebug...),
		}

		fmCfg := clickhouselogger.NewFlushManagerConfigAdapter(
			clickHouseCfg.ClickHouseFlushManagerConfig,
		)

		spoolRuntimes, err := buildCompressedLogSpoolRuntimesForFailureStore(fmCfg.FailureStoreConfig(), spoolOwners...)
		if err != nil {
			return nil, nil, err
		}
		failureHandlers, err := clickhouselogger.BuildFailureHandler(
			ctx,
			loggerBase.Clock,
			fmCfg,
			memoryMonitor,
			tracer,
			meter,
			failureStoreInternalLogHandlerFactory(common.SourceNameClickHouseLogger, newAvailableHandlers),
			failureSpoolPipelines.registrarFor(contractKind, logger.LogOutputClickHouse),
			spoolRuntimes,
		)
		if failureHandlers != nil {
			pendingFailureHandler = failureHandlers
		}
		if err != nil {
			return nil, nil, err
		}
		clickHouseClient, err := infraclickhouse.NewClient(
			*cfg.ClickHouseClient,
			clickhouselogger.NewRetryConfigProvider(clickHouseCfg.ClickHouseRetryableClientCaseTransportErrorConfig),
			tracer,
			meter,
		)
		if err != nil {
			return nil, nil, err
		}

		lg, err := clickhouselogger.NewClickHouseLogger(
			ctx,
			clickHouseLoggerCfg,
			clickHouseClient,
			fmCfg,
			resource.NewResourceAdapter(clickHouseCfg.ClickHouseFlushManagerConfig.Resource),
			failureHandlers,
			tracer,
			meter,
			loggerBase,
		)
		if err != nil {
			return nil, nil, err
		}

		loggers = append(loggers, lg)
		ownedClosers = append(ownedClosers, rollbackContextCloser{close: lg.Close})
		pendingFailureHandler = nil
	}

	if cfg.Slack != nil &&
		logOutputEnabled(cfg.LogOutputDestinations, logger.LogOutputSlack) {
		slackAdapter := slack.NewConfigAdapter(*cfg.Slack)

		internalHandler := selectConfiguredHandlers(
			newAvailableHandlers(common.SourceNameSlackLogger),
			slackAdapter.Handler(),
		)
		lg, err := slack.NewSlackLogger(ctx, slackAdapter, tracer, meter,
			slack.WithInternalLogHandlers(
				onerrorhandler.CombineErrorHandlers(internalHandler.OnError...),
				onerrorhandler.CombineWarnHandlers(internalHandler.OnWarn...),
				onerrorhandler.CombineInfoHandlers(internalHandler.OnInfo...),
				onerrorhandler.CombineDebugHandlers(internalHandler.OnDebug...),
			),
		)
		if err != nil {
			return nil, nil, err
		}

		loggers = append(loggers, lg)
		ownedClosers = append(ownedClosers, lg)
	}

	if cfg.Stdout != nil &&
		logOutputEnabled(cfg.LogOutputDestinations, logger.LogOutputStdout) {
		stdoutAdapter := stdout.NewConfigAdapter(*cfg.Stdout)
		internalHandler := selectConfiguredHandlers(
			newAvailableHandlers(common.SourceNameStdoutLogger),
			stdoutAdapter.Handler(),
		)
		lg, err := stdout.NewStdoutLogger(
			ctx,
			stdoutAdapter,
			resource.NewOtelResourceAdapter(otelResourceOrEmpty(cfg.Resource)),
			tracer,
			meter,
			stdout.WithInternalLogHandlers(
				onerrorhandler.CombineErrorHandlers(internalHandler.OnError...),
				onerrorhandler.CombineWarnHandlers(internalHandler.OnWarn...),
				onerrorhandler.CombineInfoHandlers(internalHandler.OnInfo...),
				onerrorhandler.CombineDebugHandlers(internalHandler.OnDebug...),
			),
		)
		if err != nil {
			return nil, nil, err
		}

		loggers = append(loggers, lg)
	}

	return loggers, ownedClosers, nil
}

func buildDeliveryLoggers(
	ctx context.Context,
	cfg logger.DeliveryLoggerConfig,
	cloudWatchLogsStreamRoutingGranularity pkgcloudwatchlogs.StreamRouting,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	fileLoggerSet *file.DeliveryLoggerSet,
	internalDestinations *deliveryInternalLogDestinationRuntime,
	fallbackCloserOwner *fallbackHandlerCloserOwner,
	spoolOwners ...*compressedlog.RootRuntimeBuilder,
) (
	targets []multi.DeliveryTarget,
	resultClosers []io.Closer,
	err error,
) {
	var ownedClosers []io.Closer
	var pendingFailureHandler failureHandlerDrainer

	defer func() {
		if err == nil {
			return
		}
		err = rollbackFailureHandlerConstruction(err, pendingFailureHandler)
		err = rollbackLoggerConstruction(err, ownedClosers)
		targets = nil
		resultClosers = nil
	}()
	// 各childは論理出力名とともに登録し、multi loggerが作る送達結果（panic・timeout・
	// Close開始後・entry検証での拒否）もchild自身の結果と同じBackend名で報告させます。
	addTarget := func(output logger.LogOutputDestination, child infralogging.DeliveryBackendLogEntryHandler) {
		targets = append(targets, multi.DeliveryTarget{Backend: output.String(), Logger: child})
	}

	// Delivery側もsource loggerごとにfallback loggerを新しく構築します。
	// Fileだけは解決済みDeliveryLoggerSetからrole別DeliveryFileLoggerを共有します。
	newAvailableHandlers := func(source string) handlerStruct {
		available := internalDestinations.newHandlers()
		fallbackCloserOwner.Register(source, available)
		return available
	}
	if fileLoggerSet != nil && cfg.File != nil {
		adapter := file.NewDeliveryFileLoggerConfigAdapter(*cfg.File)
		if err := fileLoggerSet.ConfigureInternalHandlers(func(role file.InternalHandlerRole) file.InternalHandlers {
			internalHandler := selectConfiguredHandlers(
				newAvailableHandlers("delivery."+common.SourceNameFileLogger+"."+string(role)),
				adapter.Handler(),
			)
			return file.InternalHandlers{
				OnError: onerrorhandler.CombineErrorHandlers(internalHandler.OnError...),
				OnWarn:  onerrorhandler.CombineWarnHandlers(internalHandler.OnWarn...),
				OnInfo:  onerrorhandler.CombineInfoHandlers(internalHandler.OnInfo...),
				OnDebug: onerrorhandler.CombineDebugHandlers(internalHandler.OnDebug...),
			}
		}); err != nil {
			return nil, nil, err
		}
	}

	if deliveryFileOutputEnabled(cfg) {
		_, err := fileLoggerSet.NewFileLogger(ctx)
		if err != nil {
			return nil, nil, err
		}

		if cfg.File.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled {
			_, err := fileLoggerSet.NewErrorFileLogger(ctx)
			if err != nil {
				return nil, nil, err
			}
		}

		if cfg.File.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled {
			_, err := fileLoggerSet.NewDebugFileLogger(ctx)
			if err != nil {
				return nil, nil, err
			}
		}

		// default / error / debugはfile backend内の物理roleであり、applicationが
		// 指定した送信先は一つです。multiへは論理file targetだけを登録します。
		addTarget(logger.LogOutputFile, fileLoggerSet.LogicalLogger())
	}

	if cfg.OpenSearch != nil &&
		logOutputEnabled(cfg.LogOutputDestinations, logger.LogOutputOpenSearch) {
		if cfg.OpenSearchClient == nil {
			return nil, nil, fmt.Errorf("delivery opensearch client config is nil")
		}

		openSearchCfg := *cfg.OpenSearch

		openSearchLoggerCfg := opensearchlogger.NewDeliveryOpenSearchLoggerConfigAdapter(
			openSearchCfg.DeliveryOpenSearchLoggerBaseConfig,
		)
		timestampFormatter, formatterErr := opensearchlogger.NewConfiguredTimestampFormatter(openSearchLoggerCfg)
		if formatterErr != nil {
			return nil, nil, formatterErr
		}
		internalHandler := selectConfiguredHandlers(
			newAvailableHandlers("delivery."+common.SourceNameOpenSearchLogger),
			openSearchLoggerCfg.Handler(),
		)

		loggerBase := &opensearchlogger.LoggerBase{
			Clock:              clock.NewUTCClock(),
			TimestampFormatter: timestampFormatter,
			OnError:            onerrorhandler.CombineErrorHandlers(internalHandler.OnError...),
			OnWarn:             onerrorhandler.CombineWarnHandlers(internalHandler.OnWarn...),
			OnInfo:             onerrorhandler.CombineInfoHandlers(internalHandler.OnInfo...),
			OnDebug:            onerrorhandler.CombineDebugHandlers(internalHandler.OnDebug...),
		}

		fmCfg := opensearchlogger.NewDeliveryFlushManagerConfigAdapter(
			openSearchCfg.DeliveryOpenSearchFlushManagerConfig,
		)

		spoolRuntimes, err := buildCompressedLogSpoolRuntimesForDeliveryFailureStore(fmCfg.DeliveryFailureStoreConfig(), spoolOwners...)
		if err != nil {
			return nil, nil, err
		}
		failureHandlers, err := opensearchlogger.BuildDeliveryFailureHandler(
			ctx,
			loggerBase.Clock,
			fmCfg,
			tracer,
			meter,
			failureStoreInternalLogHandlerFactory(
				"delivery."+common.SourceNameOpenSearchLogger,
				newAvailableHandlers,
			),
			spoolRuntimes,
		)
		if failureHandlers != nil {
			pendingFailureHandler = failureHandlers
		}
		if err != nil {
			return nil, nil, err
		}
		clientCfg := infraopensearch.NewOpenSearchClientConfigAdapter(cfg.OpenSearchClient)
		retryCfg := opensearchlogger.NewRetryConfigProvider(
			openSearchCfg.OpenSearchRetryableClientCaseTransportErrorConfig,
		)
		osClient, err := infraopensearch.NewClient(clientCfg, retryCfg, tracer, meter)
		if err != nil {
			return nil, nil, err
		}

		lg := opensearchlogger.NewDeliveryOpenSearchLogger(
			ctx,
			openSearchLoggerCfg,
			osClient,
			fmCfg,
			resource.NewResourceAdapter(openSearchCfg.DeliveryOpenSearchFlushManagerConfig.Resource),
			failureHandlers,
			tracer,
			meter,
			loggerBase,
		)

		addTarget(logger.LogOutputOpenSearch, lg)
		ownedClosers = append(ownedClosers, lg)
		pendingFailureHandler = nil
	}

	if cfg.Loki != nil &&
		logOutputEnabled(cfg.LogOutputDestinations, logger.LogOutputLoki) {
		if cfg.LokiClient == nil {
			return nil, nil, fmt.Errorf("delivery loki client config is nil")
		}

		lokiCfg := *cfg.Loki

		lokiLoggerCfg := lokilogger.NewDeliveryLokiLoggerConfigAdapter(
			lokiCfg.DeliveryLokiLoggerBaseConfig,
		)
		timestampFormatter, formatterErr := lokilogger.NewConfiguredTimestampFormatter(lokiLoggerCfg)
		if formatterErr != nil {
			return nil, nil, formatterErr
		}
		internalHandler := selectConfiguredHandlers(
			newAvailableHandlers("delivery."+common.SourceNameLokiLogger),
			lokiLoggerCfg.Handler(),
		)

		loggerBase := &lokilogger.LoggerBase{
			Clock:        clock.NewUTCClock(),
			LogFormatter: formatter.NewPlainTextFormatter(timestampFormatter),
			OnError:      onerrorhandler.CombineErrorHandlers(internalHandler.OnError...),
			OnWarn:       onerrorhandler.CombineWarnHandlers(internalHandler.OnWarn...),
			OnInfo:       onerrorhandler.CombineInfoHandlers(internalHandler.OnInfo...),
			OnDebug:      onerrorhandler.CombineDebugHandlers(internalHandler.OnDebug...),
		}

		fmCfg := lokilogger.NewDeliveryFlushManagerConfigAdapter(
			lokiCfg.DeliveryLokiFlushManagerConfig,
		)

		spoolRuntimes, err := buildCompressedLogSpoolRuntimesForDeliveryFailureStore(fmCfg.DeliveryFailureStoreConfig(), spoolOwners...)
		if err != nil {
			return nil, nil, err
		}
		failureHandlers, err := lokilogger.BuildDeliveryFailureHandler(
			ctx,
			loggerBase.Clock,
			fmCfg,
			tracer,
			meter,
			failureStoreInternalLogHandlerFactory(
				"delivery."+common.SourceNameLokiLogger,
				newAvailableHandlers,
			),
			spoolRuntimes,
		)
		if failureHandlers != nil {
			pendingFailureHandler = failureHandlers
		}
		if err != nil {
			return nil, nil, err
		}
		lokiClient, err := infraloki.NewClient(
			*cfg.LokiClient,
			lokilogger.NewRetryConfigProvider(lokiCfg.LokiRetryableClientCaseTransportErrorConfig),
			tracer,
			meter,
		)
		if err != nil {
			return nil, nil, err
		}

		lg, err := lokilogger.NewDeliveryLokiLogger(
			ctx,
			lokiLoggerCfg,
			lokiClient,
			fmCfg,
			resource.NewResourceAdapter(lokiCfg.DeliveryLokiFlushManagerConfig.Resource),
			failureHandlers,
			tracer,
			meter,
			loggerBase,
		)
		if err != nil {
			return nil, nil, err
		}

		addTarget(logger.LogOutputLoki, lg)
		ownedClosers = append(ownedClosers, lg)
		pendingFailureHandler = nil
	}

	if cfg.CloudWatchLogs != nil &&
		logOutputEnabled(cfg.LogOutputDestinations, logger.LogOutputCloudWatchLogs) {
		if cfg.CloudWatchLogsClient == nil {
			return nil, nil, fmt.Errorf("delivery cloudwatch logs client config is nil")
		}

		cloudWatchCfg := *cfg.CloudWatchLogs

		cloudWatchLoggerCfg := cloudwatchlogslogger.NewDeliveryCloudWatchLogsLoggerConfigAdapter(
			cloudWatchCfg.DeliveryCloudWatchLogsLoggerBaseConfig,
		)
		cloudWatchTimestampFormatter, err := cloudwatchlogslogger.NewConfiguredTimestampFormatter(cloudWatchLoggerCfg)
		if err != nil {
			return nil, nil, err
		}
		internalHandler := selectConfiguredHandlers(
			newAvailableHandlers("delivery."+common.SourceNameCloudWatchLogsLogger),
			cloudWatchLoggerCfg.Handler(),
		)

		loggerBase := &cloudwatchlogslogger.LoggerBase{
			Clock:        clock.NewUTCClock(),
			LogFormatter: formatter.NewPlainTextFormatter(cloudWatchTimestampFormatter),
			OnError:      onerrorhandler.CombineErrorHandlers(internalHandler.OnError...),
			OnWarn:       onerrorhandler.CombineWarnHandlers(internalHandler.OnWarn...),
			OnInfo:       onerrorhandler.CombineInfoHandlers(internalHandler.OnInfo...),
			OnDebug:      onerrorhandler.CombineDebugHandlers(internalHandler.OnDebug...),
		}

		fmCfg := cloudwatchlogslogger.NewDeliveryFlushManagerConfigAdapter(
			cloudWatchCfg.DeliveryCloudWatchLogsFlushManagerConfig,
			cloudWatchLogsStreamRoutingGranularity,
		)

		spoolRuntimes, err := buildCompressedLogSpoolRuntimesForDeliveryFailureStore(fmCfg.DeliveryFailureStoreConfig(), spoolOwners...)
		if err != nil {
			return nil, nil, err
		}
		failureHandlers, err := cloudwatchlogslogger.BuildDeliveryFailureHandler(
			ctx,
			loggerBase.Clock,
			fmCfg,
			tracer,
			meter,
			failureStoreInternalLogHandlerFactory(
				"delivery."+common.SourceNameCloudWatchLogsLogger,
				newAvailableHandlers,
			),
			spoolRuntimes,
		)
		if failureHandlers != nil {
			pendingFailureHandler = failureHandlers
		}
		if err != nil {
			return nil, nil, err
		}
		cloudWatchClient, err := infracloudwatchlogs.NewClient(
			*cfg.CloudWatchLogsClient,
			cloudwatchlogslogger.NewRetryConfigProvider(cloudWatchCfg.CloudWatchLogsRetryableClientCaseTransportErrorConfig),
			tracer,
			meter,
		)
		if err != nil {
			return nil, nil, err
		}
		lg, err := cloudwatchlogslogger.NewDeliveryCloudWatchLogsLogger(
			ctx,
			cloudWatchLoggerCfg,
			cloudWatchClient,
			fmCfg,
			resource.NewResourceAdapter(cloudWatchCfg.DeliveryCloudWatchLogsFlushManagerConfig.Resource),
			failureHandlers,
			tracer,
			meter,
			loggerBase,
		)
		if err != nil {
			return nil, nil, err
		}

		addTarget(logger.LogOutputCloudWatchLogs, lg)
		ownedClosers = append(ownedClosers, lg)
		pendingFailureHandler = nil
	}

	// ClickHouse構築後に追加する従来のlogger順序を維持するため、
	// Slack/Stdoutはここで一度だけ構築します。
	var deliverySlackLogger *slack.DeliverySlackLogger
	if cfg.Slack != nil &&
		logOutputEnabled(cfg.LogOutputDestinations, logger.LogOutputSlack) {
		slackAdapter := slack.NewDeliveryConfigAdapter(*cfg.Slack)

		internalHandler := selectConfiguredHandlers(
			newAvailableHandlers("delivery."+common.SourceNameSlackLogger),
			slackAdapter.Handler(),
		)
		var err error
		deliverySlackLogger, err = slack.NewDeliverySlackLogger(ctx, slackAdapter, tracer, meter,
			slack.WithInternalLogHandlers(
				onerrorhandler.CombineErrorHandlers(internalHandler.OnError...),
				onerrorhandler.CombineWarnHandlers(internalHandler.OnWarn...),
				onerrorhandler.CombineInfoHandlers(internalHandler.OnInfo...),
				onerrorhandler.CombineDebugHandlers(internalHandler.OnDebug...),
			),
		)
		if err != nil {
			return nil, nil, err
		}
	}

	var deliveryStdoutLogger *stdout.DeliveryStdoutLogger
	if cfg.Stdout != nil &&
		logOutputEnabled(cfg.LogOutputDestinations, logger.LogOutputStdout) {
		stdoutAdapter := stdout.NewDeliveryConfigAdapter(*cfg.Stdout)
		internalHandler := selectConfiguredHandlers(
			newAvailableHandlers("delivery."+common.SourceNameStdoutLogger),
			stdoutAdapter.Handler(),
		)
		var err error
		deliveryStdoutLogger, err = stdout.NewDeliveryStdoutLogger(
			ctx,
			stdoutAdapter,
			resource.NewOtelResourceAdapter(otelResourceOrEmpty(cfg.Resource)),
			tracer,
			meter,
			stdout.WithInternalLogHandlers(
				onerrorhandler.CombineErrorHandlers(internalHandler.OnError...),
				onerrorhandler.CombineWarnHandlers(internalHandler.OnWarn...),
				onerrorhandler.CombineInfoHandlers(internalHandler.OnInfo...),
				onerrorhandler.CombineDebugHandlers(internalHandler.OnDebug...),
			),
		)
		if err != nil {
			return nil, nil, err
		}
	}

	if cfg.ClickHouse != nil &&
		logOutputEnabled(cfg.LogOutputDestinations, logger.LogOutputClickHouse) {
		if cfg.ClickHouseClient == nil {
			return nil, nil, fmt.Errorf("delivery clickhouse client config is nil")
		}

		clickHouseCfg := *cfg.ClickHouse

		clickHouseLoggerCfg := clickhouselogger.NewDeliveryClickHouseLoggerConfigAdapter(
			clickHouseCfg.DeliveryClickHouseLoggerBaseConfig,
		)
		timestampFormatter, formatterErr := clickhouselogger.NewConfiguredTimestampFormatter(clickHouseLoggerCfg)
		if formatterErr != nil {
			return nil, nil, formatterErr
		}
		internalHandler := selectConfiguredHandlers(
			newAvailableHandlers("delivery."+common.SourceNameClickHouseLogger),
			clickHouseLoggerCfg.Handler(),
		)

		loggerBase := &clickhouselogger.LoggerBase{
			Clock:        clock.NewUTCClock(),
			LogFormatter: formatter.NewPlainTextFormatter(timestampFormatter),
			OnError:      onerrorhandler.CombineErrorHandlers(internalHandler.OnError...),
			OnWarn:       onerrorhandler.CombineWarnHandlers(internalHandler.OnWarn...),
			OnInfo:       onerrorhandler.CombineInfoHandlers(internalHandler.OnInfo...),
			OnDebug:      onerrorhandler.CombineDebugHandlers(internalHandler.OnDebug...),
		}

		fmCfg := clickhouselogger.NewDeliveryFlushManagerConfigAdapter(
			clickHouseCfg.DeliveryClickHouseFlushManagerConfig,
		)

		spoolRuntimes, err := buildCompressedLogSpoolRuntimesForDeliveryFailureStore(fmCfg.DeliveryFailureStoreConfig(), spoolOwners...)
		if err != nil {
			return nil, nil, err
		}
		failureHandlers, err := clickhouselogger.BuildDeliveryFailureHandler(
			ctx,
			loggerBase.Clock,
			fmCfg,
			tracer,
			meter,
			failureStoreInternalLogHandlerFactory(
				"delivery."+common.SourceNameClickHouseLogger,
				newAvailableHandlers,
			),
			spoolRuntimes,
		)
		if failureHandlers != nil {
			pendingFailureHandler = failureHandlers
		}
		if err != nil {
			return nil, nil, err
		}
		clickHouseClient, err := infraclickhouse.NewClient(
			*cfg.ClickHouseClient,
			clickhouselogger.NewRetryConfigProvider(clickHouseCfg.ClickHouseRetryableClientCaseTransportErrorConfig),
			tracer,
			meter,
		)
		if err != nil {
			return nil, nil, err
		}

		lg, err := clickhouselogger.NewDeliveryClickHouseLogger(
			ctx,
			clickHouseLoggerCfg,
			clickHouseClient,
			fmCfg,
			resource.NewResourceAdapter(clickHouseCfg.DeliveryClickHouseFlushManagerConfig.Resource),
			failureHandlers,
			tracer,
			meter,
			loggerBase,
		)
		if err != nil {
			return nil, nil, err
		}

		addTarget(logger.LogOutputClickHouse, lg)
		ownedClosers = append(ownedClosers, lg)
		pendingFailureHandler = nil
	}

	if deliverySlackLogger != nil {
		addTarget(logger.LogOutputSlack, deliverySlackLogger)
	}

	if deliveryStdoutLogger != nil {
		addTarget(logger.LogOutputStdout, deliveryStdoutLogger)
	}

	return targets, ownedClosers, nil
}

func otelResourceOrEmpty(r *otel.Resource) *otel.Resource {
	if r != nil {
		return r
	}
	return &otel.Resource{}
}
