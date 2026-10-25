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
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// DeliveryLoggerSetは、一つのtop-level Delivery loggerが使うdefault/error/debug roleの
// DeliveryFileLogger instanceを所有します。通常Delivery出力とFile fallbackは、同じ
// roleのDeliveryFileLoggerとfile resourceを共有します。他destination向けfallback
// handlerの状態はroleごとに分離します。fallback-only構成では最初のinternal logまで
// DeliveryFileLoggerを生成しません。
type DeliveryLoggerSet struct {
	config DeliveryFileLoggerConfigProvider
	tracer observer.Tracer
	meter  observer.Meter

	// muは生成済みloggerの取得・公開と設定状態だけを保護します。
	// 初回生成やCloseの完了待ちでは保持せず、別roleの既存logger取得を妨げません。
	mu      sync.Mutex
	loggers map[loggerType]*DeliveryFileLogger
	// closeStateはmuの保護下でlogger生成可否とresource Closeの状態を管理します。
	closeState      common.LoggerCloseState
	closeController shutdown.Controller

	// newInternalHandlersは、新しいroleのDeliveryFileLoggerを生成するときに専用handler群を作ります。
	newInternalHandlers InternalHandlerFactory

	// creationCloseMuは初回生成・handler設定変更・Closeとその再試行を直列化します。
	// 生成済みloggerの取得では使用せず、muを保持したままこのlockを待ちません。
	// 初回生成を待つ呼び出しをtesting/synctestで観測できるよう、channelで待つlockを使います。
	// channelは構築時に作り、構築した側（synctestのbubbleの内外）に属させます。
	creationCloseMu common.WaitableMutex
	closedLoggers   map[*DeliveryFileLogger]struct{}
	// directoryLeaseは、このDeliveryLoggerSetが使用し得るroleの保存先を予約し、
	// 全resourceの解放が完了してから解除します。
	directoryLease *directorylease.Lease
	directoryPaths loggerDirectoryPaths
}

// deliveryRoleLoggerは、levelごとに分かれたDeliveryFileLoggerを、一つの論理的な
// file delivery targetとしてMultiLoggerへ公開します。各FileLoggerの所有権は持たず、
// DeliveryLoggerSetが一括して閉じます。
type deliveryRoleLogger struct {
	loggerSet *DeliveryLoggerSet
}

var _ logging.DeliveryBackendLogEntryHandler = (*deliveryRoleLogger)(nil)

// NewDeliveryLoggerSetはrole別DeliveryFileLoggerの所有境界を作ります。この時点で
// 保存先directoryのprocess-local ownershipは予約しますが、directory作成やfile openは
// 行いません。各accessorまたはfallback handlerの初回利用時に必要なroleだけを作成します。
func NewDeliveryLoggerSet(
	cfg DeliveryFileLoggerConfigProvider,
	tracer observer.Tracer,
	meter observer.Meter,
) (*DeliveryLoggerSet, error) {
	return NewDeliveryLoggerSetWithDirectoryScopeLabel(
		defaultDeliveryLoggerSetDirectoryScopeLabel,
		cfg,
		tracer,
		meter,
	)
}

// NewDeliveryLoggerSetWithDirectoryScopeLabelは、診断用のLogger構成種別を付けて
// Delivery用のrole別FileLogger所有境界を作ります。保存先はfileのlazy openより前に予約します。
func NewDeliveryLoggerSetWithDirectoryScopeLabel(
	directoryScopeLabel directorylease.ScopeLabel,
	cfg DeliveryFileLoggerConfigProvider,
	tracer observer.Tracer,
	meter observer.Meter,
) (*DeliveryLoggerSet, error) {
	if cfg == nil {
		return nil, fmt.Errorf(ErrorMsgInvalidConfigStatus, errors.New("delivery file logger config is nil"))
	}
	if err := validateDeliveryFileLoggerConfigProvider(cfg, DefaultLogger); err != nil {
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
	set := &DeliveryLoggerSet{
		config:         cfg,
		tracer:         observer.EnsureTracer(tracer),
		meter:          observer.EnsureMeter(meter),
		loggers:        make(map[loggerType]*DeliveryFileLogger),
		closedLoggers:  make(map[*DeliveryFileLogger]struct{}),
		directoryLease: directoryLease,
		directoryPaths: directories,
	}
	// lockのchannelは最初の使用時に作られるため、構築時に一度取得して作っておきます。
	set.creationCloseMu.Lock()
	set.creationCloseMu.Unlock()
	return set, nil
}

// ConfigureInternalHandlersは、この集合から後で生成されるDeliveryFileLoggerごとに
// 専用のinternal log handler群を作るfactoryを設定します。最初のLogger生成前に一度だけ
// 呼びます。factoryが作るhandler状態は共有せず、同じroleのDeliveryFileLoggerと
// file resourceだけを同じDeliveryLoggerSet経由で共有します。
func (s *DeliveryLoggerSet) ConfigureInternalHandlers(
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
func (s *DeliveryLoggerSet) NewFileLogger(ctx context.Context) (*DeliveryFileLogger, error) {
	return s.getOrCreateFileLogger(ctx, DefaultLogger)
}

// NewErrorFileLoggerはerror eventの保存先を取得します。
// error分離が無効なら、以後のerror event用にDefaultLoggerと同じinstanceを返します。
// 以前の分離error fileをdefault fileへ移行する処理は行いません。
func (s *DeliveryLoggerSet) NewErrorFileLogger(ctx context.Context) (*DeliveryFileLogger, error) {
	return s.getOrCreateFileLogger(ctx, s.resolveRole(ErrorLogger))
}

// NewDebugFileLoggerはdebug eventの保存先を取得します。
// debug分離が無効なら、以後のdebug event用にDefaultLoggerと同じinstanceを返します。
// 以前の分離debug fileをdefault fileへ移行する処理は行いません。
func (s *DeliveryLoggerSet) NewDebugFileLogger(ctx context.Context) (*DeliveryFileLogger, error) {
	return s.getOrCreateFileLogger(ctx, s.resolveRole(DebugLogger))
}

// LogicalLoggerは、default / error / debugの内部roleを送信先として露出せず、
// fileという一つのbackend targetへlevelに応じて配送するloggerを返します。
func (s *DeliveryLoggerSet) LogicalLogger() logging.DeliveryBackendLogEntryHandler {
	return &deliveryRoleLogger{loggerSet: s}
}

func (l *deliveryRoleLogger) loggerForLevel(
	ctx context.Context,
	level pkglogger.LogLevel,
) (*DeliveryFileLogger, error) {
	switch level {
	case pkglogger.LogLevelError, pkglogger.LogLevelFatal:
		return l.loggerSet.NewErrorFileLogger(ctx)
	case pkglogger.LogLevelDebug:
		return l.loggerSet.NewDebugFileLogger(ctx)
	default:
		return l.loggerSet.NewFileLogger(ctx)
	}
}

func (s *DeliveryLoggerSet) rejectIfClosed() error {
	if s.closeController.IsForced() {
		return ErrLoggerSetClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closeState != common.LoggerCloseStateOpen {
		return ErrLoggerSetClosed
	}
	return nil
}

// HandleLogEntryは生成済みentryのlevelから保存roleを選び、そのroleの物理file loggerへ
// entryを渡します。ID、Fields、OccurredAtはrole選択時に作り直しません。
func (l *deliveryRoleLogger) HandleLogEntry(
	ctx context.Context,
	entry *logging.LogEntry,
) (pkglogger.DeliveryReport, error) {
	if entry == nil {
		return deliveryRoleSelectionFailure(fmt.Errorf("delivery file logger log entry is nil"))
	}
	logger, err := l.loggerForLevel(ctx, entry.Level)
	if err != nil {
		return deliveryRoleSelectionFailure(err)
	}
	return logger.HandleLogEntry(ctx, entry)
}

func deliveryRoleSelectionFailure(err error) (pkglogger.DeliveryReport, error) {
	status := pkglogger.DeliveryFailed
	if errors.Is(err, applogging.ErrLoggerClosed) {
		status = pkglogger.DeliveryRejected
	}
	return pkglogger.NewTargetDeliveryReport(pkglogger.DeliveryResult{
		Backend: BackendServices,
		Status:  status,
		Err:     err,
	}), err
}

func (s *DeliveryLoggerSet) resolveRole(requested loggerType) loggerType {
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

// getOrCreateFileLoggerは、roleで指定した通常・error・debug用のDeliveryFileLoggerを取得します。
// 生成済みなら同じインスタンスを返します。未生成なら、集合の構築時に確定した保存先で
// ログファイルを開き、内部ログ通知先を設定してからloggersへ登録します。
// 同じroleへの並行呼び出しによる二重生成を防ぎ、生成に失敗した場合は登録せずエラーを返します。
// 受付時点で終了処理中または強制終了済みならErrLoggerSetClosedを返します。
// 返したLoggerとファイルの所有者はこの集合であり、DeliveryLoggerSet.Closeでまとめて閉じます。
func (s *DeliveryLoggerSet) getOrCreateFileLogger(
	ctx context.Context,
	role loggerType,
) (*DeliveryFileLogger, error) {
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

	logger, err := newDeliveryFileLoggerBase(ctx, s.config, role, s.tracer, s.meter, s.directoryPaths[role])
	if err != nil {
		return nil, err
	}
	logger.managedByDeliveryLoggerSet = true
	logger.ownerCloseController.Store(&s.closeController)
	if newInternalHandlers != nil {
		handlers := newInternalHandlers(InternalHandlerRole(role))
		logger.OnError = handlers.OnError
		logger.OnWarn = handlers.OnWarn
		logger.OnInfo = handlers.OnInfo
		logger.OnDebug = handlers.OnDebug
	}
	// 構築からhandler設定・公開までcreationCloseMuを保持し、Closeが未公開の
	// resourceを取りこぼさないようにします。muは完成したinstanceの公開時だけ再取得します。
	s.mu.Lock()
	s.loggers[role] = logger
	s.mu.Unlock()
	return logger, nil
}

func (s *DeliveryLoggerSet) fallbackHandler(level pkglogger.LogLevel) logging.LoggerHandlerFunc {
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
		logger, err := s.getOrCreateFileLogger(ctx, role)
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
		localEntry := *entry
		logging.EnsureEntryOccurredAt(&localEntry, logger.Clock)
		if err := logger.writeFallbackEntry(ctx, &localEntry); err != nil {
			if errors.Is(err, applogging.ErrLoggerClosed) {
				return
			}
			log.Printf(ErrorMsgWriteLog, err)
		}
	}
}

func (s *DeliveryLoggerSet) NewNamedErrorFallbackHandler() logging.NamedLoggerErrorHandler {
	handler := s.fallbackHandler(pkglogger.LogLevelError)
	return logging.NamedLoggerErrorHandler{
		Name: common.HandlerNameFileLogger,
		Handler: func(
			ctx context.Context,
			_ error,
			entry *logging.LogEntry,
			tracer observer.Tracer,
			meter observer.Meter,
		) {
			handler(ctx, entry, tracer, meter)
		},
	}
}

func (s *DeliveryLoggerSet) namedFallbackHandler(level pkglogger.LogLevel) logging.NamedLoggerHandler {
	return logging.NamedLoggerHandler{
		Name:    common.HandlerNameFileLogger,
		Handler: s.fallbackHandler(level),
		Level:   level,
	}
}

func (s *DeliveryLoggerSet) NewNamedWarnFallbackHandler() logging.NamedLoggerHandler {
	return s.namedFallbackHandler(pkglogger.LogLevelWarn)
}

func (s *DeliveryLoggerSet) NewNamedInfoFallbackHandler() logging.NamedLoggerHandler {
	return s.namedFallbackHandler(pkglogger.LogLevelInfo)
}

func (s *DeliveryLoggerSet) NewNamedDebugFallbackHandler() logging.NamedLoggerHandler {
	return s.namedFallbackHandler(pkglogger.LogLevelDebug)
}

// Closeは進行中の初回生成を待ち、生成されたfile resourceも含めて閉じます。
// fileのCloseがエラーでも各Loggerの後片付けは完了させ、保存先を解放してエラーを返します。
func (s *DeliveryLoggerSet) Close() error {
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
	return s.closeController.Close("delivery file logger set", func() (bool, error) {
		err := s.closeOwnedResources()
		s.mu.Lock()
		complete := s.closeState == common.LoggerCloseStateCompleted
		s.mu.Unlock()
		return complete, err
	})
}

// ForceCloseは呼出し元の待機を解除し、file使用終了後の後片付けは継続します。
func (s *DeliveryLoggerSet) ForceClose() error {
	if s == nil {
		return nil
	}
	err := s.RequestForceClose()
	go s.Close()
	return err
}

// RequestForceCloseは中止だけを通知し、fileの解放は親のClose順序に任せます。
func (s *DeliveryLoggerSet) RequestForceClose() error {
	if s == nil {
		return nil
	}
	return s.closeController.ForceClose(nil)
}

func (s *DeliveryLoggerSet) closeOwnedResources() error {
	s.creationCloseMu.Lock()
	defer s.creationCloseMu.Unlock()

	s.mu.Lock()
	if s.closeState == common.LoggerCloseStateCompleted {
		s.mu.Unlock()
		return nil
	}
	s.closeState = common.LoggerCloseStateStarted
	type closeCandidate struct {
		role   loggerType
		logger *DeliveryFileLogger
	}
	loggers := make([]closeCandidate, 0, len(s.loggers))
	for role, logger := range s.loggers {
		if _, closed := s.closedLoggers[logger]; !closed {
			loggers = append(loggers, closeCandidate{
				role:   role,
				logger: logger,
			})
		}
	}
	s.mu.Unlock()

	var errs []error
	for _, candidate := range loggers {
		if err := candidate.logger.closeOwnedFileResource(); err != nil {
			errs = append(errs, &RoleCloseError{
				Role:     string(candidate.role),
				Path:     candidate.logger.Fsstore.ActiveFilePath,
				Delivery: true,
				Err:      err,
			})
		}
		s.mu.Lock()
		s.closedLoggers[candidate.logger] = struct{}{}
		s.mu.Unlock()
	}
	s.mu.Lock()
	s.closeState = common.LoggerCloseStateCompleted
	s.mu.Unlock()
	s.directoryLease.Release()
	return errors.Join(errs...)
}
