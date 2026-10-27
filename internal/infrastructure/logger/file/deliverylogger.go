package file

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	fileerrors "github.com/go-jappe/heterolog/internal/infrastructure/logger/file/errors"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	loggerhandler "github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	loggertime "github.com/go-jappe/heterolog/internal/infrastructure/logger/time"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

const BackendServices string = "file"

var _ logging.DeliveryBackendLogEntryHandler = (*DeliveryFileLogger)(nil)

type DeliveryFileLoggerConfigProvider interface {
	Time() loggertime.TimeConfigProvider
	Runtime() DeliveryRuntimeProvider
	LogBody() LogBodyConfigProvider
	Fsstore() appendlog.FileWriteStoreConfigProvider
	LifecycleMode() pkgfsstore.FileLifecycleMode
	Handler() loggerhandler.ConfigProvider
	// ArchiveCheckpointとObjectStorageUploadは、runner modeのuploadが使うcheckpointと送信設定です。
	// writerは起動時に、このcheckpointへ記録済みの世代より後から書き込みます。
	ArchiveCheckpoint() pkgfsstore.ArchiveCheckpointConfig
	ObjectStorageUpload() objectstorage.LogFileUploadConfigProvider
}

type DeliveryRuntimeProvider interface {
	IsDebug() bool
	Target() string
}

// ロガー構造体
type DeliveryFileLogger struct {
	// Clockの時刻とタイムゾーンを、ファイル名・rotationの日付基準に使います。
	// 構築時にUTCClockを設定します。差し替える場合もnilにはせず、処理中に別の時計へ補完しません。
	Clock clock.Clock
	// LogFormatterは本文に記録する発生時刻のtimezoneとlayoutを保持します。
	LogFormatter  formatter.PlainTextFormatter
	logger        loggerType
	config        DeliveryFileLoggerConfigProvider
	directoryPath string

	Fsstore appendlog.FileStore // ファイル永続化操作
	// lifecycleGuardはmanager purgeへ書き込み中pathを公開する。
	lifecycleGuard *appendlog.DirectoryGuard
	// lifecycleTokenのownerは、このphysical DeliveryFileLogger instanceです。
	// active pathを、このloggerと同じdirectoryのlifecycle処理へ公開します。
	lifecycleToken *appendlog.DirectoryGuardToken

	// managedByDeliveryLoggerSetの場合、logical loggerのCloseでは共有fileを閉じません。
	managedByDeliveryLoggerSet bool
	ownerCloseController       atomic.Pointer[shutdown.Controller]
	// directoryLeaseはLoggerSet外で単独構築されたphysical handlerだけが所有します。
	// DeliveryLoggerSet配下ではSetがrole群を一括して予約するためnilです。
	directoryLease *directorylease.Lease
	// lifecycleMuとcloseStateは、Close開始後のwrite受付をfile I/Oより前に拒否し、
	// 受付済みwriteとCloseの順序を確定するための境界です。
	lifecycleMu     sync.Mutex
	closeState      loggercommon.LoggerCloseState
	closeController shutdown.Controller

	OnError logging.LoggerErrorHandler
	OnWarn  logging.LoggerHandlerFunc
	OnInfo  logging.LoggerHandlerFunc
	OnDebug logging.LoggerHandlerFunc
	Tracer  pkgobserver.Tracer // observebility計装用
	Meter   pkgobserver.Meter  // observebility計装用

	// internalLogReentryは、OnErrorへ渡すcontextへこのDeliveryFileLoggerの印を付け、通知を
	// 実行するgoroutineを記録します。通知の中から同じloggerへ書き込んで失敗した場合、
	// そのerrorはOnErrorを再び呼ばず、同じfileへ直接記録します（dispatchWriteError）。
	internalLogReentry loggerhandler.ReentryGuard
}

// NewDeliveryFileLoggerはdefault roleだけを所有するphysical handlerを作ります。
// 同じdirectoryでerror/debug roleも使う場合は、一括所有するDeliveryLoggerSetを使用します。
func NewDeliveryFileLogger(ctx context.Context, cfg DeliveryFileLoggerConfigProvider, tracer pkgobserver.Tracer, meter pkgobserver.Meter) (*DeliveryFileLogger, error) {
	return newStandaloneDeliveryFileLogger(ctx, cfg, DefaultLogger, tracer, meter)
}

// NewDeliveryErrorFileLoggerはerror roleだけを所有するphysical handlerを作ります。
// 同じdirectoryの別roleと共有する場合はDeliveryLoggerSetを使用します。
func NewDeliveryErrorFileLogger(ctx context.Context, cfg DeliveryFileLoggerConfigProvider, tracer pkgobserver.Tracer, meter pkgobserver.Meter) (*DeliveryFileLogger, error) {
	return newStandaloneDeliveryFileLogger(ctx, cfg, ErrorLogger, tracer, meter)
}

// NewDeliveryDebugFileLoggerはdebug roleだけを所有するphysical handlerを作ります。
// 同じdirectoryの別roleと共有する場合はDeliveryLoggerSetを使用します。
func NewDeliveryDebugFileLogger(ctx context.Context, cfg DeliveryFileLoggerConfigProvider, tracer pkgobserver.Tracer, meter pkgobserver.Meter) (*DeliveryFileLogger, error) {
	return newStandaloneDeliveryFileLogger(ctx, cfg, DebugLogger, tracer, meter)
}

func newStandaloneDeliveryFileLogger(
	ctx context.Context,
	cfg DeliveryFileLoggerConfigProvider,
	role loggerType,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) (*DeliveryFileLogger, error) {
	if err := validateDeliveryFileLoggerConfigProvider(cfg, role); err != nil {
		return nil, fmt.Errorf(ErrorMsgInvalidConfigStatus, err)
	}
	directory, err := pathidentity.ResolveDirectory(configuredLoggerDirectory(cfg.Fsstore().Layout(), role))
	if err != nil {
		return nil, fmt.Errorf("resolve delivery file logger directory: %w", err)
	}
	lease, err := directorylease.Acquire(
		deliveryFileLoggerDirectoryScopeLabel(role),
		directory,
	)
	if err != nil {
		return nil, fmt.Errorf("acquire delivery file logger directory lease: %w", err)
	}

	logger, err := newDeliveryFileLoggerBase(ctx, cfg, role, tracer, meter, directory)
	if err != nil {
		lease.Release()
		return nil, err
	}
	logger.directoryLease = lease
	return logger, nil
}

// HandleLogEntryは生成済みentryをfile delivery backendへ記録し、file target一件分の
// DeliveryReportを返します。application由来の値は再解決せず、file固有のLogFormatter、
// role filter、stack trace、write処理だけを適用します。
func (l *DeliveryFileLogger) HandleLogEntry(
	ctx context.Context,
	entry *logging.LogEntry,
) (logger.DeliveryReport, error) {
	if err := l.rejectIfClosed(); err != nil {
		return logger.NewTargetDeliveryReport(l.deliveryRejectedResult(err)), err
	}
	if entry == nil {
		err := fmt.Errorf("delivery file logger log entry is nil")
		return logger.NewTargetDeliveryReport(l.deliveryRejectedResult(err)), err
	}

	logging.EnsureEntryOccurredAt(entry, l.Clock)
	if entry.StackTraceRequested {
		// ErrorWithStackにerrorが渡されなかった場合も拒否せず、要求されたstackを持つ
		// ERRORとして記録します。Formatterはerrorの欄を出力しません。
		if len(entry.FuncName) == 0 && len(entry.File) == 0 && len(entry.Line) == 0 {
			depth := logging.GetCallerDepth(NewStackTraceConfigAdapter(l.config.Runtime().IsDebug(), l.config.LogBody()))
			logging.EnsureEntryStackTrace(entry, depth)
		}
		result, err := l.logWithMeta(ctx, entry)
		return logger.NewTargetDeliveryReport(result), err
	}
	if !l.shouldLog(entry.Level) {
		return logger.NewTargetDeliveryReport(l.deliverySkippedResult()), nil
	}

	if ctx == nil {
		ctx = context.Background()
	}

	// 実際にログ処理という大きめな粒度に対して span を切る
	ctx, span := l.Tracer.StartSpan(ctx, SpanFileWrite)
	defer span.End()

	result, err := l.logWithMeta(ctx, entry)
	return logger.NewTargetDeliveryReport(result), err
}

func (l *DeliveryFileLogger) shouldLog(level logger.LogLevel) bool {
	if l.logger == DefaultLogger {
		// error/debug の分離が有効な場合、default logger は通常ログだけを担当する。
		if l.config.Fsstore().Layout().IsSeparateErrorLogsEnabled() && level >= logger.LogLevelError {
			return false
		}
		if l.config.Fsstore().Layout().IsSeparateDebugLogsEnabled() && level == logger.LogLevelDebug {
			return false
		}
	}

	if l.logger == ErrorLogger {
		return level >= logger.LogLevelError
	}

	if l.logger == DebugLogger {
		return level == logger.LogLevelDebug
	}

	return true
}

func (l *DeliveryFileLogger) logWithMeta(ctx context.Context, entry *logging.LogEntry) (logger.DeliveryResult, error) {
	if !l.shouldLog(entry.Level) {
		return l.deliverySkippedResult(), nil
	}
	return l.writeEntry(ctx, entry, true)
}

// writeEntryはentryを同じ物理streamへ書き込みます。roleによる振り分けは呼び出し元
// （logWithMeta、DeliveryLoggerSetのfallback handler）が判定済みです。
func (l *DeliveryFileLogger) writeEntry(
	ctx context.Context,
	entry *logging.LogEntry,
	dispatchInternalLogs bool,
) (logger.DeliveryResult, error) {
	// 全てのオプションが on の場合、高並列性能より単純さ・整合性優先
	// 重い環境では各オプションを　off にして調整してください
	if err := l.lockForWrite(); err != nil {
		return l.deliveryRejectedResult(err), err
	}
	writeErr := l.ensureFileWritable()
	if writeErr == nil {
		writeErr = l.rotateIfEnable(ctx)
	}
	if writeErr == nil {
		writeErr = l.writeLog(ctx, entry)
	}
	l.Fsstore.Mu.Unlock()

	if writeErr != nil {
		if dispatchInternalLogs {
			l.dispatchWriteError(ctx, writeErr, entry)
		}
		return l.deliveryFailedResult(writeErr), writeErr
	}
	return l.deliverySucceededResult(), nil
}

// writeFallbackEntryはDeliveryLoggerの送達結果契約を通らず、共有物理streamへ
// internal logをbest-effortで書き込みます。この経路ではOn*を再実行しません。
func (l *DeliveryFileLogger) writeFallbackEntry(ctx context.Context, entry *logging.LogEntry) error {
	_, err := l.writeEntry(ctx, entry, false)
	return err
}

// dispatchWriteErrorは書き込みのerrorをOnErrorへ通知します。このloggerのOnErrorの中から
// 同じloggerへの書き込みが失敗した場合（再入）は、OnErrorを再び呼ばず、
// writeFallbackEntryで同じfileへ直接記録します。internal logはこのstreamの状態を表すため、
// roleで振り分けずにこのloggerのfileへ書き込みます。呼び出し元はwriter lockを解放してから呼びます。
func (l *DeliveryFileLogger) dispatchWriteError(
	ctx context.Context,
	err error,
	entry *logging.LogEntry,
) {
	if entry == nil {
		return
	}
	internalEntry := *entry
	internalEntry.Level = logger.LogLevelError
	internalEntry.Internal = true
	internalEntry.Source = fileLoggerInternalSource(l.logger)
	internalEntry.Err = err
	internalEntry.OccurredAt = logging.ResolveOccurredAt(l.Clock)
	l.internalLogReentry.CallErrorHandler(
		l.OnError,
		ctx,
		l.wrapErrorByLogger(err, MsgDispatchError),
		&internalEntry,
		l.Tracer,
		l.Meter,
		l.writeReentrantInternalLog,
	)
}

// writeReentrantInternalLogは、再入した書き込みのinternal logをwriteFallbackEntryで同じfileへ
// 記録します。この書き込み自身のinternal logは通知しないため、再入はここで終わります。Close開始後の
// 拒否以外の記録失敗は、DeliveryLoggerSetのfallback書き込みと同じく標準loggerへ残します。
func (l *DeliveryFileLogger) writeReentrantInternalLog(ctx context.Context, entry *logging.LogEntry) {
	if err := l.writeFallbackEntry(ctx, entry); err != nil && !errors.Is(err, logger.ErrLoggerClosed) {
		log.Printf(ErrorMsgWriteLog, err)
	}
}

func (l *DeliveryFileLogger) wrapErrorByLogger(err error, op string) error {
	switch l.logger {
	case DefaultLogger:
		return &fileerrors.DefaultLoggerInternalError{Op: op, Err: err}
	case DebugLogger:
		return &fileerrors.DebugLoggerInternalError{Op: op, Err: err}
	case ErrorLogger:
		return &fileerrors.ErrorLoggerInternalError{Op: op, Err: err}
	default:
		return &fileerrors.DefaultLoggerInternalError{Op: op, Err: err}
	}
}

// rotationの新open待ちは後段のrotation処理で再開します。それ以外のnilはエラーです。
func (l *DeliveryFileLogger) ensureFileWritable() error {
	if l.Fsstore.File == nil && !l.Fsstore.RotationPending() {
		return &FileLoggerError{
			Op:  "failed to write log",
			Err: errors.New("file is already closed or not opened"),
		}
	}
	return nil
}

// 日付の変更またはファイルサイズの上限に達しているかを確認し、
// 必要であれば新しいファイルを作成（ローテーション）する。
func (l *DeliveryFileLogger) rotateIfEnable(ctx context.Context) error {
	if l.Fsstore.RotationPending() {
		return l.rotate(ctx, l.Clock.Now())
	}
	if l.config.Fsstore().Rotation().EnableRotate() {
		rotationTime := l.Clock.Now()
		return logpattern.CheckAndRotateAt(
			ctx,
			l.Fsstore.File,
			appendlog.NewNameLogPatternConfigAdapter(l.config.Fsstore()),
			&l.Fsstore.FileName,
			rotationTime,
			func(ctx context.Context) error {
				return l.rotate(ctx, rotationTime)
			},
		)
	}
	return nil
}

// ログ書き込み
func (l *DeliveryFileLogger) writeLog(ctx context.Context, meta *logging.LogEntry) error {
	if ctx == nil {
		ctx = context.Background()
	}

	_, err := l.Fsstore.WriteStringWithRetry(
		ctx,
		SpanFileWriteIO,
		l.LogFormatter.Format(meta),
		meta.RequestID,
		nil,
		nil,
		ErrorMesFailedWriteFile,
		nil,
		l.config.Fsstore().Retry(),
	)
	if err != nil {
		return &FileLoggerError{Op: "failed to write log", Err: err}
	}
	return nil
}

// openLogFileはfileOperationTimeが属するrotation期間のactive fileを開きます。
// rotation判定と呼び出す場合は、判定時に確定した同じ時刻を渡します。
func (l *DeliveryFileLogger) openLogFile(ctx context.Context, fileOperationTime time.Time) error {
	if l.Fsstore.File != nil {
		return &FileLoggerError{Op: "failed to open log file", Err: ErrFileAlreadyOpen}
	}
	previousPath := l.Fsstore.ActiveFilePath
	previousFileName := l.Fsstore.FileName
	transition := func() (string, error) {
		if err := updateActiveFilePath(
			&l.Fsstore,
			appendlog.NewNameLogPatternConfigAdapter(l.config.Fsstore()),
			fileOperationTime,
		); err != nil {
			return "", err
		}
		newPath := l.Fsstore.ActiveFilePath
		file, err := appendlog.OpenFile(newPath)
		if err != nil {
			l.Fsstore.ActiveFilePath = previousPath
			l.Fsstore.FileName = previousFileName
			return "", err
		}
		l.Fsstore.File = file
		return newPath, nil
	}

	return l.lifecycleGuard.TransitionActive(l.lifecycleToken, transition)
}

func (l *DeliveryFileLogger) deliveryFailedResult(err error) logger.DeliveryResult {
	return logger.DeliveryResult{
		Backend: BackendServices,
		Status:  logger.DeliveryFailed,
		Err:     err,
	}
}

func (l *DeliveryFileLogger) deliverySucceededResult() logger.DeliveryResult {
	return logger.DeliveryResult{
		Backend: BackendServices,
		Status:  logger.DeliverySucceeded,
	}
}

func (l *DeliveryFileLogger) deliveryRejectedResult(err error) logger.DeliveryResult {
	return logger.DeliveryResult{
		Backend: BackendServices,
		Status:  logger.DeliveryRejected,
		Err:     err,
	}
}

func (l *DeliveryFileLogger) deliverySkippedResult() logger.DeliveryResult {
	return logger.DeliveryResult{
		Backend: BackendServices,
		Status:  logger.DeliverySkipped,
	}
}

func (l *DeliveryFileLogger) Close() error {
	if l == nil || l.managedByDeliveryLoggerSet {
		return nil
	}
	if !l.closeController.IsForced() && l.closeController.IsComplete() && l.ShutdownComplete() {
		return nil
	}
	if !l.closeController.IsForced() {
		l.lifecycleMu.Lock()
		if l.closeState == loggercommon.LoggerCloseStateOpen {
			l.closeState = loggercommon.LoggerCloseStateStarted
		}
		l.lifecycleMu.Unlock()
	}
	return l.closeController.Close("delivery file logger", func() (bool, error) {
		return true, l.closeOwnedFileResource()
	})
}

// ForceCloseは実行中writeの終了を待つ呼出し元だけを解除します。
// fileとdirectory leaseはwriteが戻るまで終了workerが保持します。
func (l *DeliveryFileLogger) ForceClose() error {
	if l == nil || l.managedByDeliveryLoggerSet {
		return nil
	}
	err := l.RequestForceClose()
	go l.Close()
	return err
}

// RequestForceCloseは親の終了順序を維持したまま待機の中止を通知します。
func (l *DeliveryFileLogger) RequestForceClose() error {
	if l == nil || l.managedByDeliveryLoggerSet {
		return nil
	}
	return l.closeController.ForceClose(nil)
}

// closeOwnedFileResourceは、このDeliveryFileLoggerが所有するopen fileと
// directory guard tokenを閉じます。
func (l *DeliveryFileLogger) closeOwnedFileResource() error {
	l.lifecycleMu.Lock()
	if l.closeState == loggercommon.LoggerCloseStateCompleted {
		l.lifecycleMu.Unlock()
		return nil
	}
	l.closeState = loggercommon.LoggerCloseStateStarted
	l.lifecycleMu.Unlock()

	l.Fsstore.Mu.Lock()
	closeErr := l.Fsstore.Close()
	// 受理済みwriteはこのlockの取得前に終了しています。os.FileはCloseがエラーでも
	// 再利用できないため、参照とactive保護は解除し、エラーだけを呼び出し元へ返します。
	l.Fsstore.File = nil
	l.lifecycleGuard.RemoveToken(l.lifecycleToken)
	l.Fsstore.Mu.Unlock()

	l.lifecycleMu.Lock()
	l.closeState = loggercommon.LoggerCloseStateCompleted
	l.lifecycleMu.Unlock()
	l.directoryLease.Release()
	return closeErr
}

func (l *DeliveryFileLogger) rejectIfClosed() error {
	if l == nil || l.forceRequested() {
		return logger.ErrLoggerClosed
	}
	l.lifecycleMu.Lock()
	defer l.lifecycleMu.Unlock()
	if l.closeState != loggercommon.LoggerCloseStateOpen {
		return logger.ErrLoggerClosed
	}
	return nil
}

// lockForWriteはClose開始前に受け付けたwriteを先にfile lockへ進め、Close開始後の
// writeはbackend attempt前の拒否としてErrLoggerClosedを返します。
func (l *DeliveryFileLogger) lockForWrite() error {
	if l == nil || l.forceRequested() {
		return logger.ErrLoggerClosed
	}
	l.lifecycleMu.Lock()
	if l.closeState != loggercommon.LoggerCloseStateOpen {
		l.lifecycleMu.Unlock()
		return logger.ErrLoggerClosed
	}
	l.Fsstore.Mu.Lock()
	l.lifecycleMu.Unlock()
	return nil
}

// newDeliveryFileLoggerBaseの設定とdirectoryは、standalone constructorまたは
// DeliveryLoggerSetの予約時に検証・解決済みです。lazy生成時には再解決しません。
func newDeliveryFileLoggerBase(
	ctx context.Context,
	cfg DeliveryFileLoggerConfigProvider,
	loggerType loggerType,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	directory string,
) (*DeliveryFileLogger, error) {
	tracer = observer.EnsureTracer(tracer)
	meter = observer.EnsureMeter(meter)

	ctx, span := tracer.StartSpan(ctx, SpanFileLoggerCreate)
	defer span.End()

	timestampFormatter, err := logging.NewTimestampFormatter(
		cfg.Time().EnableLocalTime(),
		cfg.Time().TimeZone(),
		cfg.LogBody().LogLineDateFormat(),
	)
	if err != nil {
		return nil, fmt.Errorf(ErrorMsgCreateLogger, err)
	}

	fl := &DeliveryFileLogger{
		Clock:         clock.NewUTCClock(),
		LogFormatter:  formatter.NewPlainTextFormatter(timestampFormatter),
		logger:        loggerType,
		config:        cfg,
		directoryPath: directory,
		Fsstore: appendlog.FileStore{
			Tracer: tracer,
			Meter:  meter,
		},
		Tracer: tracer,
		Meter:  meter,
	}

	startupTime := fl.Clock.Now()
	if err := fl.initFileName(startupTime); err != nil {
		return nil, err
	}
	fl.initLifecycleGuard()

	if err := fl.openLogFile(ctx, startupTime); err != nil {
		return nil, fmt.Errorf(ErrorMsgOpenFile, err)
	}

	return fl, nil
}

func (l *DeliveryFileLogger) initFileName(fileOperationTime time.Time) error {
	logDir, err := l.GetFileSystemStoreDirectoryPath()
	if err != nil {
		return fmt.Errorf(MsgInvalidDirectoryPath, err)
	}
	logDir, err = fsstore.GetFileSystemStoreDirectoryPath(logDir, "", "")
	if err != nil {
		return fmt.Errorf(MsgInvalidDirectoryPath, err)
	}
	dateString, err := logpattern.ResolveDateStringForFileNameAt(
		appendlog.NewNameLogPatternConfigAdapter(l.config.Fsstore()),
		fileOperationTime,
	)
	if err != nil {
		return err
	}

	l.Fsstore.FileName = logpattern.FileNamePattern{
		Dir:       logDir,
		Prefix:    l.getPrefixIfEnabled(),
		Target:    l.config.Fsstore().Layout().Target(),
		Date:      dateString,
		Extension: LogFileExtension,
	}

	if err := logpattern.ResolveMaxIndex(&l.Fsstore.FileName); err != nil {
		return fmt.Errorf(MsgInvalidFileIndex, err)
	}
	// Deliveryのuploadはrunner modeだけです。他のmodeでも残存checkpointの記録は再利用しません。
	uploadRequired := l.config.LifecycleMode() == pkgfsstore.FileLifecycleModeRunner &&
		roleUploadEnabled(l.config.ObjectStorageUpload(), l.logger)
	if err := advancePastRecordedGeneration(
		&l.Fsstore.FileName, l.config.ArchiveCheckpoint(), l.logger, uploadRequired,
	); err != nil {
		return err
	}

	return nil
}

// rotateはrotationTimeでactive fileを切り替えます。判定と再openの
// 日付境界を揃えるため、呼び出し元が確定した同じ時刻を渡します。
func (l *DeliveryFileLogger) rotate(ctx context.Context, rotationTime time.Time) error {
	_, err := l.Fsstore.RotateWithRetry(
		ctx,
		nil,
		nil,
		func(ctx context.Context) error {
			return l.openLogFile(ctx, rotationTime)
		},
		l.config.Fsstore().Retry(),
	)
	return err
}

func (f *DeliveryFileLogger) GetFileSystemStoreDirectoryPath() (string, error) {
	if f.directoryPath != "" {
		return f.directoryPath, nil
	}

	return fsstore.GetFileSystemStoreDirectoryPath(
		f.config.Fsstore().Layout().LogPath(),
		f.config.Fsstore().Layout().Target(),
		f.getLogDir())
}

// ロガータイプに応じてディレクトリを分けて返却
func (f *DeliveryFileLogger) getLogDir() string {
	switch f.logger {
	case ErrorLogger:
		return f.config.Fsstore().Layout().ErrorLogDir()
	case DefaultLogger:
		return f.config.Fsstore().Layout().LogDir()
	case DebugLogger:
		return f.config.Fsstore().Layout().DebugLogDir()
	default:
		return f.config.Fsstore().Layout().LogDir()
	}
}

func (f *DeliveryFileLogger) getPrefixIfEnabled() string {
	if f.logger == ErrorLogger && f.config.Fsstore().Layout().IsSeparateErrorLogsEnabled() {
		return f.getPrefix()
	}

	if f.logger == DebugLogger && f.config.Fsstore().Layout().IsSeparateDebugLogsEnabled() {
		return f.getPrefix()
	}
	return ""
}

// 必要に応じてログファイル名にerror_ debug_などにつける接頭辞を返却する
func (f *DeliveryFileLogger) getPrefix() string {
	// エラーログ、デバッグログは接頭辞を付与
	prefix := ""
	if f.logger == ErrorLogger {
		prefix = f.config.Fsstore().Layout().SeparateErrorLogsPrefix()
	} else if f.logger == DebugLogger {
		prefix = f.config.Fsstore().Layout().SeparateDebugLogsPrefix()
	}
	return prefix
}
