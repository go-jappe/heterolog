package multi

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// MultiLogger は複数の Logger を束ねる Logger。
//
// この Logger は「並列実行する同期 Logger」です。
// つまり、各 logger へのログ出力は goroutine で並列に実行しますが、
// 呼び出し元には全 logger の処理完了、または timeout 到達まで戻りません。
//
// 渡すchild handler実装は、複数goroutineから同時に呼ばれても安全である必要があります。
// 例えば内部でbytes.Buffer、map、slice、bufio.Writerなどのmutableな状態を持つ場合は、
// 各backend側でmutexなどを使って保護してください。
type MultiLogger struct {
	loggers []infralogging.BackendLogEntryHandler

	// timeout は MultiLogger 全体の待ち時間上限。
	// 注意:
	// timeout は「MultiLogger が待つ時間」を制限するものです。
	// logger goroutine 自体を強制停止するものではありません。
	// そのため、各 Logger 実装は渡された ctx の Done を適切に見る必要があります。
	timeout time.Duration
	// maxLogEntryBytesは、application入力をbackendへfan-outする前に適用する一件の上限です。
	maxLogEntryBytes int
	// nowは、一つのapplication入力から全backendで共有する発生時刻を確定します。
	now     func() time.Time
	stateMu sync.Mutex
	// inFlightはClose開始前に受理したchild logger呼び出しを追跡します。
	// runAllがtimeoutで先に戻った後も、child goroutine自体が終わるまでDoneしません。
	inFlight sync.WaitGroup
	// closeStateはstateMuの保護下で新規受付とCloseの状態遷移を管理します。
	closeState loggercommon.LoggerCloseState

	closeMu        sync.Mutex
	closedChildren []bool
	// terminalCloseErrは資源解放済みchildの確定した終了エラーを保持します。
	terminalCloseErr error
}

var _ applogging.CloseableLogger = (*MultiLogger)(nil)

func newMultiLogger(
	timeout time.Duration,
	maxLogEntryBytes int,
	loggers ...infralogging.BackendLogEntryHandler,
) *MultiLogger {
	return &MultiLogger{
		loggers:          append([]infralogging.BackendLogEntryHandler(nil), loggers...),
		timeout:          timeout,
		maxLogEntryBytes: maxLogEntryBytes,
		now:              time.Now,
		closedChildren:   make([]bool, len(loggers)),
	}
}

// NewMultiLoggerはapplication設定を検証し、複数のphysical handlerを束ねる同期
// top-level loggerを作成します。timeoutと一件のlog entry上限に暗黙の既定値は適用しません。
func NewMultiLogger(
	cfg MultiLoggerConfigProvider,
	loggers ...infralogging.BackendLogEntryHandler,
) (*MultiLogger, error) {
	timeout, maxLogEntryBytes, err := validatedMultiLoggerConfig(cfg)
	if err != nil {
		return nil, err
	}
	return newMultiLogger(timeout, maxLogEntryBytes, loggers...), nil
}

// ValidateMultiLoggerConfigはNewMultiLoggerと同じ規則でtimeoutと一件のlog entry上限を
// 検証します。top-level constructorはfileやspoolなどのresourceを作る前に呼び出します。
func ValidateMultiLoggerConfig(cfg MultiLoggerConfigProvider) error {
	_, _, err := validatedMultiLoggerConfig(cfg)
	return err
}

func validatedMultiLoggerConfig(cfg MultiLoggerConfigProvider) (time.Duration, int, error) {
	if cfg == nil {
		return 0, 0, fmt.Errorf("multi logger %w", ErrConfigRequired)
	}
	timeout := cfg.Timeout()
	if timeout <= 0 {
		return 0, 0, fmt.Errorf("multi logger %w: %s", ErrInvalidTimeout, timeout)
	}
	maxLogEntryBytes := cfg.MaxLogEntryBytes()
	if err := pkglogger.ValidateMaxLogEntryBytes(maxLogEntryBytes); err != nil {
		return 0, 0, err
	}
	return timeout, maxLogEntryBytes, nil
}

func wrapHandlerErr(handler any, op string, err error) error {
	// どのphysical handlerのどの操作で失敗したか分かるように型情報を付与する。
	return &HandlerError{Op: op, Handler: handler, Err: err}
}

func loggerPanicError(recovered any) error {
	return &PanicError{Logger: "logger", Value: recovered}
}

// closeableLogEntryHandlerはapplication契約と分離したchildの終了能力です。
type closeableLogEntryHandler interface {
	Close() error
}

// contextCloseableLogEntryHandlerはbackend内部のflushへcontextを渡すchildの
// 終了能力です。
type contextCloseableLogEntryHandler interface {
	Close(context.Context) error
}

func closeLogger(l any) (err error) {
	defer func() {
		// Keep a child panic inside the normal close-error path so siblings
		// still close and only unfinished children are retried by the owner.
		if recovered := recover(); recovered != nil {
			err = loggerPanicError(recovered)
		}
		loggercommon.WaitForForcedShutdown(l, err)
	}()
	if closeable, ok := l.(closeableLogEntryHandler); ok {
		return closeable.Close()
	}
	if closeable, ok := l.(contextCloseableLogEntryHandler); ok {
		return closeable.Close(context.Background())
	}
	return nil
}

// runAllは生成済みの一つのentryを全childへ渡し、全childの完了またはtimeoutまで待ちます。
// OccurredAtはchildを開始する前に一度だけ確定し、各childには独立したcloneを渡します。
func (m *MultiLogger) runAll(
	ctx context.Context,
	entry *infralogging.LogEntry,
) error {
	if entry == nil {
		return fmt.Errorf("multi logger entry is nil")
	}
	op := multiLoggerOperation(entry.Level, entry.StackTraceRequested)
	var now func() time.Time
	if m != nil {
		now = m.now
	}
	entry = canonicalLogEntryForFanOut(entry, now)

	if m == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	m.stateMu.Lock()
	if m.closeState != loggercommon.LoggerCloseStateOpen {
		m.stateMu.Unlock()
		return multiLoggerClosingError(op)
	}
	if len(m.loggers) == 0 {
		m.stateMu.Unlock()
		return nil
	}
	m.inFlight.Add(len(m.loggers))
	m.stateMu.Unlock()

	// 呼び出し元 ctx の cancel / deadline はログ処理へ伝播させない。
	// ただし Value は引き継がれるため、request id や trace id などは参照できる。
	//
	// これにより、リクエスト側の ctx が cancel されても、
	// MultiLogger は自身の timeout まではログ出力を試みる。
	runCtx := context.WithoutCancel(ctx)

	if m.timeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(runCtx, m.timeout)
		defer cancel()
	}

	type loggerResult struct {
		index   int
		failure *applogging.LoggerFailure
	}

	// timeout で runAll が先に return した後に logger goroutine が完了しても、
	// resultCh への送信で goroutine が詰まらないよう、logger 数分の buffer を持たせる。
	//
	// 各 goroutine は最大1回しか送信しないため、cap=len(m.loggers) にしておけば
	// runAll 側が途中で受信をやめても send がブロックしにくい。
	resultCh := make(chan loggerResult, len(m.loggers))

	for i, l := range m.loggers {
		i := i
		l := l

		go func() {
			defer m.inFlight.Done()
			result := loggerResult{index: i}
			defer func() {
				if recovered := recover(); recovered != nil {
					result.failure = &applogging.LoggerFailure{
						HandlerType: fmt.Sprintf("%T", l),
						Op:          op,
						Err:         loggerPanicError(recovered),
					}
				}
				resultCh <- result
			}()

			if err := l.HandleLogEntry(runCtx, infralogging.CloneLogEntry(entry)); err != nil {
				result.failure = &applogging.LoggerFailure{
					HandlerType: fmt.Sprintf("%T", l),
					Op:          op,
					Err:         err,
				}
			}
		}()
	}

	failures := make([]applogging.LoggerFailure, 0)
	completed := make([]bool, len(m.loggers))
	completedCount := 0

	// 同じ index の結果を二重に処理しないための記録処理。
	// 通常は二重到達しないが、防御的に completed を見ている。
	recordResult := func(result loggerResult) {
		if completed[result.index] {
			return
		}

		completed[result.index] = true
		completedCount++

		if result.failure != nil {
			failures = append(failures, *result.failure)
		}
	}

	// MultiLoggerError として返すことで、呼び出し側が errors.As で型として拾える。
	toMultiLoggerError := func() error {
		if len(failures) == 0 {
			return nil
		}

		return &applogging.MultiLoggerError{
			Op:       op,
			Failures: failures,
		}
	}

	for completedCount < len(m.loggers) {
		select {
		case result := <-resultCh:
			recordResult(result)

		case <-runCtx.Done():
			// timeout と同時に resultCh に結果が到着している可能性がある。
			// そのため、すでに完了済みの結果は timeout 扱いにせず、先に可能な分だけ回収する。
		drainResults:
			for {
				select {
				case result := <-resultCh:
					recordResult(result)
				default:
					break drainResults
				}
			}

			// drain の結果、全 logger が完了していた場合は timeout failure を追加しない。
			// これは「timeout と最後の成功結果が同時に到着した」ケースで、
			// Failures 空の MultiLoggerError を返してしまうのを防ぐため。
			if completedCount == len(m.loggers) {
				return toMultiLoggerError()
			}

			timeoutErr := fmt.Errorf("multi logger timed out: %w", runCtx.Err())

			for i, done := range completed {
				if done {
					continue
				}

				failures = append(failures, applogging.LoggerFailure{
					HandlerType: fmt.Sprintf("%T", m.loggers[i]),
					Op:          op,
					Err:         timeoutErr,
				})
			}

			return &applogging.MultiLoggerError{
				Op:       op,
				Failures: failures,
			}
		}
	}

	return toMultiLoggerError()
}

func (m *MultiLogger) rejectIfClosing(op string) error {
	if m != nil {
		m.stateMu.Lock()
		closing := m.closeState != loggercommon.LoggerCloseStateOpen
		m.stateMu.Unlock()
		if closing {
			return multiLoggerClosingError(op)
		}
	}
	return nil
}

// multiLoggerClosingErrorは、Close開始後に到着した要求をopの拒否として返します。
func multiLoggerClosingError(op string) error {
	return &OperationError{Op: op, Err: fmt.Errorf("multi logger is closing: %w", applogging.ErrLoggerClosed)}
}

// logはapplication入力を一つのLogEntryへ変換してから各backendへfan-outします。
// Close開始済みの理由を入力validationより先に返し、確認後にCloseが始まる競合は
// runAll側の再確認で拒否します。
func (m *MultiLogger) log(
	ctx context.Context,
	level pkglogger.LogLevel,
	message string,
	err error,
	stackTraceRequested bool,
	fields []applogging.Field,
) error {
	op := multiLoggerOperation(level, stackTraceRequested)
	if m == nil {
		return &OperationError{Op: op, Err: ErrNoMultiLogger}
	}
	if closeErr := m.rejectIfClosing(op); closeErr != nil {
		return closeErr
	}

	entry, validationErr := buildApplicationLogEntry(
		ctx,
		m.maxLogEntryBytes,
		m.now,
		level,
		message,
		err,
		stackTraceRequested,
		fields,
	)
	if validationErr != nil {
		// validation中にCloseが始まった場合も、利用者には終了開始を優先して伝えます。
		if closeErr := m.rejectIfClosing(op); closeErr != nil {
			return closeErr
		}
		return &OperationError{Op: op, Err: validationErr}
	}
	return m.runAll(ctx, entry)
}

func (m *MultiLogger) Debug(
	ctx context.Context,
	message string,
	fields ...applogging.Field,
) error {
	return m.log(ctx, pkglogger.LogLevelDebug, message, nil, false, fields)
}

func (m *MultiLogger) Info(
	ctx context.Context,
	message string,
	fields ...applogging.Field,
) error {
	return m.log(ctx, pkglogger.LogLevelInfo, message, nil, false, fields)
}

func (m *MultiLogger) Warn(
	ctx context.Context,
	message string,
	fields ...applogging.Field,
) error {
	return m.log(ctx, pkglogger.LogLevelWarn, message, nil, false, fields)
}

func (m *MultiLogger) Error(
	ctx context.Context,
	message string,
	fields ...applogging.Field,
) error {
	return m.log(ctx, pkglogger.LogLevelError, message, nil, false, fields)
}

func (m *MultiLogger) ErrorWithStack(
	ctx context.Context,
	message string,
	err error,
	fields ...applogging.Field,
) error {
	return m.log(ctx, pkglogger.LogLevelError, message, err, true, fields)
}

func multiLoggerOperation(
	level pkglogger.LogLevel,
	stackTraceRequested bool,
) string {
	if stackTraceRequested {
		return "multi.ErrorWithStack"
	}
	switch level {
	case pkglogger.LogLevelDebug:
		return "multi.Debug"
	case pkglogger.LogLevelInfo:
		return "multi.Info"
	case pkglogger.LogLevelWarn:
		return "multi.Warn"
	default:
		return "multi.Error"
	}
}

// StopAcceptingはresourceを閉じずに新しいlog要求の受付だけを停止します。
// top-level runtimeはappendlog maintenanceなどを停止する前にこのmethodを呼び、
// 受理済み処理の完了待ちとchild loggerの終了は後続のCloseで行います。
// 複数回呼んでも状態は変わらず、Close済みの状態を戻しません。
func (m *MultiLogger) StopAccepting() {
	if m == nil {
		return
	}
	m.stateMu.Lock()
	if m.closeState == loggercommon.LoggerCloseStateOpen {
		m.closeState = loggercommon.LoggerCloseStateStarted
	}
	m.stateMu.Unlock()
}

// Close は Close() error、またはClose(context.Context) errorを実装するchild loggerを閉じる。
//
// Close はログ出力とは異なり context を受け取らないため、この MultiLogger の timeout 対象外。
// 複数loggerのClose errorはerrors.Joinでまとめ、次のCloseでは失敗したchildだけを再試行する。
func (m *MultiLogger) Close() error {
	if m == nil {
		return nil
	}

	m.closeMu.Lock()
	defer m.closeMu.Unlock()

	m.StopAccepting()
	m.stateMu.Lock()
	if m.closeState == loggercommon.LoggerCloseStateCompleted {
		m.stateMu.Unlock()
		return m.terminalCloseErr
	}
	m.stateMu.Unlock()

	// runAllがtimeoutでreturn済みでも、その呼び出しが開始したchild処理が
	// resourceを使用し終えるまではchild loggerを閉じません。
	m.inFlight.Wait()

	var errs []error

	for i, l := range m.loggers {
		if m.closedChildren[i] {
			continue
		}
		if err := closeLogger(l); err != nil {
			wrapped := wrapHandlerErr(l, "Close", err)
			if childShutdownComplete(l) {
				m.closedChildren[i] = true
				m.terminalCloseErr = errors.Join(m.terminalCloseErr, wrapped)
			} else {
				errs = append(errs, wrapped)
			}
			continue
		}
		m.closedChildren[i] = true
	}

	closeErr := errors.Join(m.terminalCloseErr, errors.Join(errs...))
	if allChildrenClosed(m.closedChildren) {
		m.stateMu.Lock()
		m.closeState = loggercommon.LoggerCloseStateCompleted
		m.stateMu.Unlock()
	}
	return closeErr
}
