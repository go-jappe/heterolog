package file

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

var (
	// ErrLoggerSetClosedは既存のfile package利用者向けのerror名を維持しつつ、
	// application共通のClose後拒否原因としてerrors.Isで判定できるようにします。
	ErrLoggerSetClosed         = fmt.Errorf("file logger set is closed: %w", applogging.ErrLoggerClosed)
	ErrLoggerSetAlreadyStarted = errors.New("file logger set already created a logger")
)

// RoleCloseErrorは、LoggerSetまたはDeliveryLoggerSetのCloseで、一つのroleのfileを
// 閉じられなかったことを表します。Roleはそのrole、PathはそのroleのFileLoggerが
// 書き込んでいたactive file、DeliveryはDeliveryLoggerSetのroleであるかです。
// 原因はErrで保持し、errors.Is/Asで判定できます。複数roleの失敗はerrors.Joinでまとめます。
type RoleCloseError struct {
	Role     string
	Path     string
	Delivery bool
	Err      error
}

func (e *RoleCloseError) Error() string {
	subject := "file logger"
	if e.Delivery {
		subject = "delivery file logger"
	}
	return fmt.Sprintf("close %s role=%q path=%q: %v", subject, e.Role, e.Path, e.Err)
}

func (e *RoleCloseError) Unwrap() error { return e.Err }

// LoggerSetは、一つのtop-level loggerが使用するdefault/error/debug roleの
// FileLogger instanceを所有します。通常出力とinternal log用fallback handlerは同じ
// LoggerSetからroleに対応するFileLoggerを取得することで、FileLogger、FileStore、
// *os.File、writer lockを共有します。
//
// ErrorLoggerとDebugLoggerは対応する分離設定が有効な場合だけ独立したinstanceになります。
// 分離が無効な場合はDefaultLoggerと同じinstanceを返します。fallback handlerからしか
// fileを使用しない構成では、最初のeventを受け取るまでFileLoggerを生成しません。
type LoggerSet struct {
	config FileLoggerConfigProvider
	tracer observer.Tracer
	meter  observer.Meter

	// muは生成済みloggerの取得・公開と設定状態だけを保護します。
	// 初回生成やCloseの完了待ちでは保持せず、別roleの既存logger取得を妨げません。
	mu      sync.Mutex
	loggers map[loggerType]*FileLogger
	// closeStateはmuの保護下でlogger生成可否とresource Closeの状態を管理します。
	closeState      common.LoggerCloseState
	closeController shutdown.Controller

	// newInternalHandlersは、新しいroleのFileLoggerを生成するときに専用handler群を作ります。
	newInternalHandlers InternalHandlerFactory

	// creationCloseMuは初回生成・handler設定変更・Closeとその再試行を直列化します。
	// 生成済みloggerの取得では使用せず、muを保持したままこのlockを待ちません。
	// 初回生成を待つ呼び出しをtesting/synctestで観測できるよう、channelで待つlockを使います。
	// channelは構築時に作り、構築した側（synctestのbubbleの内外）に属させます。
	creationCloseMu common.WaitableMutex
	// closedLoggersはresourceの解放が完了したFileLoggerです。終了時のエラー報告が
	// あっても解放済みなら登録し、別Loggerの終了を再試行するときに二重closeしません。
	closedLoggers map[*FileLogger]struct{}
	// directoryLeaseは、このLoggerSetが使用し得るroleの保存先を、別のtop-level
	// loggerから保護します。全resourceの解放が完了してから解除します。
	directoryLease            *directorylease.Lease
	directoryPaths            loggerDirectoryPaths
	spoolRuntimes             compressedlog.RootRuntimes
	ownsSpoolRuntimes         bool
	archiveDestinationCursors archiveDestinationCursors
}

// NewLoggerSetはrole別FileLoggerの所有境界を作ります。この時点で保存先directoryの
// process-local ownershipは予約しますが、directory作成やfile openは行いません。
// checkpointを使用する場合は、呼び出し前に保存先を
// checkpoint.PrepareDirectoryで準備してください。
// inline uploadが有効なroleは、この時点で宛先の対象範囲をcheckpointへ登録します。
// 各New*FileLoggerまたはfallback handlerの初回利用時に必要なroleだけを作成します。
func NewLoggerSet(
	cfg FileLoggerConfigProvider,
	tracer observer.Tracer,
	meter observer.Meter,
) (*LoggerSet, error) {
	return NewLoggerSetWithDirectoryScopeLabel(
		defaultLoggerSetDirectoryScopeLabel,
		cfg,
		tracer,
		meter,
	)
}

// NewLoggerSetWithDirectoryScopeLabelは、診断用のLogger構成種別を付けて
// role別FileLoggerの所有境界を作ります。directory leaseはconstructorで取得するため、fileがlazyに
// openされるfallback-only構成でも、別LoggerSetが同じ保存先を先に使うことはありません。
// checkpoint保存先はNewLoggerSetと同じく、呼び出し前に準備してください。
func NewLoggerSetWithDirectoryScopeLabel(
	directoryScopeLabel directorylease.ScopeLabel,
	cfg FileLoggerConfigProvider,
	tracer observer.Tracer,
	meter observer.Meter,
	providedSpoolRuntimes ...compressedlog.RootRuntimes,
) (*LoggerSet, error) {
	if cfg == nil {
		return nil, fmt.Errorf(ErrorMsgInvalidConfigStatus, errors.New("file logger config is nil"))
	}
	if err := validateFileLoggerConfigProvider(cfg); err != nil {
		return nil, fmt.Errorf(ErrorMsgInvalidConfigStatus, err)
	}
	directories, err := resolveLoggerDirectoryPaths(cfg.Fsstore().Layout())
	if err != nil {
		return nil, err
	}
	directoryLease, err := acquireLoggerSetDirectoryLease(directoryScopeLabel, cfg.Fsstore().Layout(), directories)
	if err != nil {
		return nil, err
	}
	cursors, err := registerLoggerSetArchiveDestinations(cfg, directories)
	if err != nil {
		directoryLease.Release()
		return nil, err
	}
	var spoolRuntimes compressedlog.RootRuntimes
	ownsSpoolRuntimes := false
	if len(providedSpoolRuntimes) > 0 {
		spoolRuntimes = providedSpoolRuntimes[0]
	} else if usesInlineLifecycle(cfg.LifecycleMode()) && cfg.ObjectStorageUpload().UsesCompressedLogFileUploadSpool(
		cfg.Fsstore().Layout().IsSeparateErrorLogsEnabled(), cfg.Fsstore().Layout().IsSeparateDebugLogsEnabled(),
	) {
		spoolConfig := cfg.ObjectStorageUpload().CompressedLogFileUploadSpool()
		if spoolConfig == nil {
			directoryLease.Release()
			return nil, fmt.Errorf("%w: spool config provider is nil", compressedlog.ErrInvalidCompressedLogFileUploadSpoolConfig)
		}
		services := cfg.ObjectStorageUpload().CompressedLogFileUploadSpoolServices(
			cfg.Fsstore().Layout().IsSeparateErrorLogsEnabled(), cfg.Fsstore().Layout().IsSeparateDebugLogsEnabled(),
		)
		configs := compressedlog.NewConfigAdapters(spoolConfig, services)
		spoolRuntimes, err = compressedlog.NewRootRuntimes(configs...)
		if err != nil {
			directoryLease.Release()
			return nil, err
		}
		ownsSpoolRuntimes = true
	}
	set := &LoggerSet{
		config:                    cfg,
		tracer:                    observer.EnsureTracer(tracer),
		meter:                     observer.EnsureMeter(meter),
		loggers:                   make(map[loggerType]*FileLogger),
		closedLoggers:             make(map[*FileLogger]struct{}),
		directoryLease:            directoryLease,
		directoryPaths:            directories,
		spoolRuntimes:             spoolRuntimes,
		ownsSpoolRuntimes:         ownsSpoolRuntimes,
		archiveDestinationCursors: cursors,
	}
	// lockのchannelは最初の使用時に作られるため、構築時に一度取得して作っておきます。
	set.creationCloseMu.Lock()
	set.creationCloseMu.Unlock()
	return set, nil
}

// ConfigureInternalHandlersは、この集合から後で生成されるFileLoggerごとに専用の
// internal log handler群を作るfactoryを設定します。fallback-onlyでlazy生成される
// Loggerにも適用するため、最初のLoggerを取得する前に一度だけ呼びます。
//
// factoryが返すfallback handler/logger状態はrole間で共有しません。ただしfactoryへ
// 同じLoggerSetを閉じ込めることで、同じroleのFileLoggerとfile resourceは共有できます。
func (s *LoggerSet) ConfigureInternalHandlers(
	factory InternalHandlerFactory,
) error {
	s.creationCloseMu.Lock()
	defer s.creationCloseMu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closeState != common.LoggerCloseStateOpen {
		return ErrLoggerSetClosed
	}
	if len(s.loggers) != 0 {
		return ErrLoggerSetAlreadyStarted
	}
	s.newInternalHandlers = factory
	return nil
}

// NewFileLoggerは通常ログ用のDefaultLogger instanceを取得します。
func (s *LoggerSet) NewFileLogger(ctx context.Context) (*FileLogger, error) {
	return s.fileLogger(ctx, DefaultLogger)
}

// NewErrorFileLogger はerror eventの物理保存先を取得します。
// error分離が無効なら、以後のerror event用にDefaultLoggerと同じinstanceを返します。
// 以前の分離error fileをdefault fileへ移行する処理は行いません。
func (s *LoggerSet) NewErrorFileLogger(ctx context.Context) (*FileLogger, error) {
	return s.fileLogger(ctx, s.resolveRole(ErrorLogger))
}

// NewDebugFileLogger はdebug eventの物理保存先を取得します。
// debug分離が無効なら、以後のdebug event用にDefaultLoggerと同じinstanceを返します。
// 以前の分離debug fileをdefault fileへ移行する処理は行いません。
func (s *LoggerSet) NewDebugFileLogger(ctx context.Context) (*FileLogger, error) {
	return s.fileLogger(ctx, s.resolveRole(DebugLogger))
}

func (s *LoggerSet) resolveRole(requested loggerType) loggerType {
	switch requested {
	case ErrorLogger:
		if !s.config.Fsstore().Layout().IsSeparateErrorLogsEnabled() {
			return DefaultLogger
		}
	case DebugLogger:
		if !s.config.Fsstore().Layout().IsSeparateDebugLogsEnabled() {
			return DefaultLogger
		}
	}
	return requested
}

func (s *LoggerSet) fileLogger(ctx context.Context, role loggerType) (*FileLogger, error) {
	if s.closeController.IsForced() {
		return nil, ErrLoggerSetClosed
	}
	s.mu.Lock()
	if s.closeState != common.LoggerCloseStateOpen {
		s.mu.Unlock()
		return nil, ErrLoggerSetClosed
	}
	if logger := s.loggers[role]; logger != nil {
		s.mu.Unlock()
		return logger, nil
	}
	s.mu.Unlock()

	// 未生成のroleだけ構築・Close用のlockを待ちます。取得後に再確認し、
	// 同じroleの二重生成や、Close済みの集合への新規生成を防ぎます。
	s.creationCloseMu.Lock()
	defer s.creationCloseMu.Unlock()
	s.mu.Lock()
	if s.closeState != common.LoggerCloseStateOpen {
		s.mu.Unlock()
		return nil, ErrLoggerSetClosed
	}
	if logger := s.loggers[role]; logger != nil {
		s.mu.Unlock()
		return logger, nil
	}
	newInternalHandlers := s.newInternalHandlers
	s.mu.Unlock()

	var handlers InternalHandlers
	if newInternalHandlers != nil {
		handlers = newInternalHandlers(InternalHandlerRole(role))
	}
	// constructor内でupload失敗spoolのmaintenanceが起動し得るため、通知先と
	// LoggerSet所有のClose契約はgoroutineへ公開する前に確定させます。
	logger, err := newFileLoggerBase(ctx, s.config, role, handlers, true, s.tracer, s.meter, s.directoryPaths, s.spoolRuntimes)
	if err != nil {
		return nil, err
	}
	logger.ownerCloseController.Store(&s.closeController)
	if len(s.archiveDestinationCursors) > 0 {
		logger.archiveGenerationPending = s.archiveGenerationPending
	}
	// 構築から公開までcreationCloseMuを保持し、Closeが未公開のresourceを
	// 取りこぼさないようにします。muは完成したinstanceの公開時だけ再取得します。
	s.mu.Lock()
	s.loggers[role] = logger
	s.mu.Unlock()
	return logger, nil
}

// newErrorFallbackHandlerはerror levelのinternal logを分離設定に対応するFileLoggerへ記録します。
func (s *LoggerSet) newErrorFallbackHandler() logging.LoggerErrorHandler {
	handler := s.fallbackHandler(pkglogger.LogLevelError)
	return func(
		ctx context.Context,
		_ error,
		entry *logging.LogEntry,
		_ observer.Tracer,
		_ observer.Meter,
	) {
		handler(ctx, entry, nil, nil)
	}
}

// newWarnFallbackHandlerはwarn levelのinternal logをDefaultLoggerへ記録します。
func (s *LoggerSet) newWarnFallbackHandler() logging.LoggerHandlerFunc {
	return s.fallbackHandler(pkglogger.LogLevelWarn)
}

// newInfoFallbackHandlerはinfo levelのinternal logをDefaultLoggerへ記録します。
func (s *LoggerSet) newInfoFallbackHandler() logging.LoggerHandlerFunc {
	return s.fallbackHandler(pkglogger.LogLevelInfo)
}

// newDebugFallbackHandlerはdebug levelのinternal logを分離設定に対応するFileLoggerへ記録します。
func (s *LoggerSet) newDebugFallbackHandler() logging.LoggerHandlerFunc {
	return s.fallbackHandler(pkglogger.LogLevelDebug)
}

// NewNamedErrorFallbackHandler はhandler aggregation用の名前付きerror handlerを返します。
func (s *LoggerSet) NewNamedErrorFallbackHandler() logging.NamedLoggerErrorHandler {
	return logging.NamedLoggerErrorHandler{
		Name:    common.HandlerNameFileLogger,
		Handler: s.newErrorFallbackHandler(),
	}
}

// NewNamedWarnFallbackHandler はhandler aggregation用の名前付きwarn handlerを返します。
func (s *LoggerSet) NewNamedWarnFallbackHandler() logging.NamedLoggerHandler {
	return logging.NamedLoggerHandler{
		Name:    common.HandlerNameFileLogger,
		Handler: s.newWarnFallbackHandler(),
		Level:   pkglogger.LogLevelWarn,
	}
}

// NewNamedInfoFallbackHandler はhandler aggregation用の名前付きinfo handlerを返します。
func (s *LoggerSet) NewNamedInfoFallbackHandler() logging.NamedLoggerHandler {
	return logging.NamedLoggerHandler{
		Name:    common.HandlerNameFileLogger,
		Handler: s.newInfoFallbackHandler(),
		Level:   pkglogger.LogLevelInfo,
	}
}

// NewNamedDebugFallbackHandler はhandler aggregation用の名前付きdebug handlerを返します。
func (s *LoggerSet) NewNamedDebugFallbackHandler() logging.NamedLoggerHandler {
	return logging.NamedLoggerHandler{
		Name:    common.HandlerNameFileLogger,
		Handler: s.newDebugFallbackHandler(),
		Level:   pkglogger.LogLevelDebug,
	}
}

func (s *LoggerSet) fallbackHandler(level pkglogger.LogLevel) logging.LoggerHandlerFunc {
	return func(
		ctx context.Context,
		entry *logging.LogEntry,
		_ observer.Tracer,
		_ observer.Meter,
	) {
		if entry == nil {
			return
		}

		role := resolveInternalLogFileRole(s.config.Fsstore().Layout(), level)
		logger, err := s.fileLogger(ctx, role)
		if err != nil {
			if errors.Is(err, applogging.ErrLoggerClosed) {
				return
			}
			log.Printf(MsgFailedCreateFallbackFileLogger, err)
			return
		}
		if !logger.shouldLog(entry.Level) {
			return
		}

		// raw handlerはtestなどから直接呼ばれる場合もあるため、発生時刻や
		// Sampledを確定する前にcopyし、callerが所有するentryを変更しません。
		localEntry := *entry
		logging.EnsureEntryOccurredAt(&localEntry, logger.Clock)
		if !sampling.ApplySamplingLogic(&localEntry, logger.config.Sampling()) {
			return
		}

		ctx, span := logger.Tracer.StartSpan(ctx, SpanFallbackFileWrite)
		defer span.End()

		if err := logger.writeFallbackEntry(ctx, &localEntry); err != nil {
			if errors.Is(err, applogging.ErrLoggerClosed) {
				return
			}
			// overload policyによるdropは設定した処置で、metricとspan eventに記録済みです。
			// 内部logの記録失敗として標準loggerへは出力しません。
			if errors.Is(err, pkglogger.ErrLogDropped) {
				return
			}
			log.Printf(ErrorMsgWriteLog, err)
		}
	}
}

// Closeは、この集合が作成したFileLoggerのfile resourceを閉じます。fallback-onlyで
// 一度も使われなかったroleにはFileLogger自体がないため、openもcloseも行いません。
// 進行中の初回生成を待ってから、生成されたfile resourceも含めて閉じます。
// 終了時のエラーは返しますが、解放済みresourceは保持しません。終了が未完了のLoggerが
// 残る場合だけ、同じClose内でそのLoggerの後片付けを再試行します。
func (s *LoggerSet) Close() error {
	if s == nil {
		return nil
	}
	if !s.closeController.IsForced() && s.closeController.IsComplete() && s.ShutdownComplete() {
		return nil
	}
	if !s.closeController.IsForced() {
		s.mu.Lock()
		if s.closeState == common.LoggerCloseStateOpen {
			s.closeState = common.LoggerCloseStateStarted
		}
		s.mu.Unlock()
	}
	return s.closeController.Close("file logger set", func() (bool, error) {
		err := s.closeOwnedResources()
		s.mu.Lock()
		complete := s.closeState == common.LoggerCloseStateCompleted
		s.mu.Unlock()
		return complete, err
	})
}

// RequestCloseは新規受付を止め、通常終了を別goroutineで開始します。
// この集合の通知内から終了を要求する場合に使います。完了はCloseで確認できます。
func (s *LoggerSet) RequestClose() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.closeState == common.LoggerCloseStateOpen {
		s.closeState = common.LoggerCloseStateStarted
	}
	s.mu.Unlock()
	go s.Close()
}

// ForceCloseは生成済みroleだけへ中止を通知し、未生成のfileは開きません。
func (s *LoggerSet) ForceClose() error {
	if s == nil {
		return nil
	}
	err := s.RequestForceClose()
	go s.Close()
	return err
}

// RequestForceCloseは中止通知だけを行い、親の終了順序を追い越しません。
func (s *LoggerSet) RequestForceClose() error {
	if s == nil {
		return nil
	}
	return s.closeController.ForceClose(func() {
		s.mu.Lock()
		if s.closeState == common.LoggerCloseStateOpen {
			s.closeState = common.LoggerCloseStateStarted
		}
		loggers := make([]*FileLogger, 0, len(s.loggers))
		for _, logger := range s.loggers {
			loggers = append(loggers, logger)
		}
		s.mu.Unlock()
		for _, logger := range loggers {
			logger.forceOwnedFileResource()
		}
	})
}

func (s *LoggerSet) closeOwnedResources() error {
	s.creationCloseMu.Lock()
	defer s.creationCloseMu.Unlock()

	s.mu.Lock()
	if s.closeState == common.LoggerCloseStateCompleted {
		s.mu.Unlock()
		return nil
	}
	s.closeState = common.LoggerCloseStateStarted
	if s.closedLoggers == nil {
		s.closedLoggers = make(map[*FileLogger]struct{})
	}
	type closeCandidate struct {
		role   loggerType
		logger *FileLogger
	}
	loggers := make([]closeCandidate, 0, len(s.loggers))
	for role, logger := range s.loggers {
		if _, closed := s.closedLoggers[logger]; closed {
			continue
		}
		loggers = append(loggers, closeCandidate{
			role:   role,
			logger: logger,
		})
	}
	s.mu.Unlock()

	var errs []error
	allLoggersClosed := true
	for _, candidate := range loggers {
		if s.closeController.IsForced() {
			candidate.logger.forceOwnedFileResource()
		}
		if err := candidate.logger.closeOwnedFileResource(); err != nil {
			errs = append(errs, &RoleCloseError{
				Role: string(candidate.role),
				Path: candidate.logger.Fsstore.ActiveFilePath,
				Err:  err,
			})
		}
		// Closeのエラー報告とresourceの解放完了は別です。fileやcheckpointの
		// エラーだけを理由に、使用終了した保存先を予約し続けないようにします。
		candidate.logger.backgroundMu.Lock()
		completed := candidate.logger.closeState == common.LoggerCloseStateCompleted
		candidate.logger.backgroundMu.Unlock()
		if !completed {
			allLoggersClosed = false
			continue
		}
		s.mu.Lock()
		s.closedLoggers[candidate.logger] = struct{}{}
		s.mu.Unlock()
	}

	if allLoggersClosed {
		if s.ownsSpoolRuntimes {
			// RootRuntimeは終了処理を終えてから過去の実行エラーを返す場合があります。
			// そのエラーも通知しますが、LoggerSetの保存先は解放できます。
			errs = append(errs, s.spoolRuntimes.Close())
		}
		s.mu.Lock()
		s.closeState = common.LoggerCloseStateCompleted
		s.mu.Unlock()
		s.directoryLease.Release()
	}
	return errors.Join(errs...)
}
