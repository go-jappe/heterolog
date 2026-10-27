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
	"github.com/go-jappe/heterolog/pkg/logger"
)

type DeliveryMultiLogger struct {
	loggers []infralogging.DeliveryBackendLogEntryHandler
	// backendsは、loggersと同じ順序の論理出力名です。multi loggerが作る送達結果
	// （childのpanic・timeout、Close開始後の拒否、entry検証での拒否）のBackendに使います。
	backends         []string
	timeout          time.Duration
	maxLogEntryBytes int
	now              func() time.Time

	stateMu sync.Mutex
	// inFlightはClose開始前に受理したchild logger呼び出しを追跡します。
	// runAllがtimeoutで先に戻っても、child goroutineが完了するまでCloseを待たせます。
	inFlight sync.WaitGroup
	// closeStateはstateMuの保護下で新規受付とCloseの状態遷移を管理します。
	closeState loggercommon.LoggerCloseState

	closeMu          sync.Mutex
	closedChildren   []bool
	terminalCloseErr error
}

var _ applogging.DeliveryClosableLogger = (*DeliveryMultiLogger)(nil)

// DeliveryTargetは、DeliveryMultiLoggerへ登録する一つのchildと、その論理出力名です。
// Backendには、childが自身の送達結果に記録するBackendと同じ名前（pkg/loggerの
// LogOutputDestinationの名前）を指定します。childが結果を返さなかった場合（panic・timeout）と、
// childへ渡さずに拒否した場合（Close開始後・entry検証）にmulti loggerが作る送達結果も、
// この名前で送信先を示します。
type DeliveryTarget struct {
	Backend string
	Logger  infralogging.DeliveryBackendLogEntryHandler
}

func newDeliveryMultiLogger(
	timeout time.Duration,
	maxLogEntryBytes int,
	targets ...DeliveryTarget,
) *DeliveryMultiLogger {
	loggers := make([]infralogging.DeliveryBackendLogEntryHandler, 0, len(targets))
	backends := make([]string, 0, len(targets))
	for _, target := range targets {
		loggers = append(loggers, target.Logger)
		backends = append(backends, target.Backend)
	}
	return &DeliveryMultiLogger{
		loggers:          loggers,
		backends:         backends,
		timeout:          timeout,
		maxLogEntryBytes: maxLogEntryBytes,
		now:              time.Now,
		closedChildren:   make([]bool, len(targets)),
	}
}

// NewDeliveryMultiLoggerWithTargetsはapplication設定を検証し、論理出力名を持つ複数の
// Delivery childを束ねるtop-level loggerを作成します。timeoutと一件のlog entry上限に
// 暗黙の既定値は適用しません。各childの論理出力名は必須で、multi loggerが作る送達結果も
// その名前で送信先を示します。
func NewDeliveryMultiLoggerWithTargets(
	cfg DeliveryMultiLoggerConfigProvider,
	targets ...DeliveryTarget,
) (*DeliveryMultiLogger, error) {
	timeout, maxLogEntryBytes, err := validatedDeliveryMultiLoggerConfig(cfg)
	if err != nil {
		return nil, err
	}
	return newDeliveryMultiLogger(timeout, maxLogEntryBytes, targets...), nil
}

// ValidateDeliveryMultiLoggerConfigはNewDeliveryMultiLoggerWithTargetsと同じ規則でtimeoutと一件の
// log entry上限を検証します。top-level constructorはresourceを作る前に呼び出します。
func ValidateDeliveryMultiLoggerConfig(cfg DeliveryMultiLoggerConfigProvider) error {
	_, _, err := validatedDeliveryMultiLoggerConfig(cfg)
	return err
}

func validatedDeliveryMultiLoggerConfig(cfg DeliveryMultiLoggerConfigProvider) (time.Duration, int, error) {
	if cfg == nil {
		return 0, 0, fmt.Errorf("delivery multi logger %w", ErrConfigRequired)
	}
	timeout := cfg.Timeout()
	if timeout <= 0 {
		return 0, 0, fmt.Errorf("delivery multi logger %w: %s", ErrInvalidTimeout, timeout)
	}
	maxLogEntryBytes := cfg.MaxLogEntryBytes()
	if err := logger.ValidateMaxLogEntryBytes(maxLogEntryBytes); err != nil {
		return 0, 0, err
	}
	return timeout, maxLogEntryBytes, nil
}

func (m *DeliveryMultiLogger) log(
	ctx context.Context,
	level logger.LogLevel,
	message string,
	err error,
	stackTraceRequested bool,
	fields []logger.Field,
) (logger.DeliveryReport, error) {
	op := deliveryLoggerOperation(level, stackTraceRequested)
	if report, closeErr, rejected := m.rejectIfClosing(op); rejected {
		return report, closeErr
	}

	maxLogEntryBytes := logger.DefaultMaxLogEntryBytes
	var now func() time.Time = time.Now
	if m != nil {
		maxLogEntryBytes = m.maxLogEntryBytes
		now = m.now
	}
	entry, validationErr := buildApplicationLogEntry(
		ctx,
		maxLogEntryBytes,
		now,
		level,
		message,
		err,
		stackTraceRequested,
		fields,
	)
	if validationErr != nil {
		return m.rejectBeforeDispatch(op, validationErr)
	}
	return m.runAll(ctx, entry)
}

func (m *DeliveryMultiLogger) Debug(
	ctx context.Context,
	message string,
	fields ...logger.Field,
) (logger.DeliveryReport, error) {
	return m.log(ctx, logger.LogLevelDebug, message, nil, false, fields)
}

func (m *DeliveryMultiLogger) Info(
	ctx context.Context,
	message string,
	fields ...logger.Field,
) (logger.DeliveryReport, error) {
	return m.log(ctx, logger.LogLevelInfo, message, nil, false, fields)
}

func (m *DeliveryMultiLogger) Warn(
	ctx context.Context,
	message string,
	fields ...logger.Field,
) (logger.DeliveryReport, error) {
	return m.log(ctx, logger.LogLevelWarn, message, nil, false, fields)
}

func (m *DeliveryMultiLogger) Error(
	ctx context.Context,
	message string,
	fields ...logger.Field,
) (logger.DeliveryReport, error) {
	return m.log(ctx, logger.LogLevelError, message, nil, false, fields)
}

func (m *DeliveryMultiLogger) ErrorWithStack(
	ctx context.Context,
	message string,
	err error,
	fields ...logger.Field,
) (logger.DeliveryReport, error) {
	return m.log(ctx, logger.LogLevelError, message, err, true, fields)
}

func deliveryLoggerOperation(level logger.LogLevel, stackTraceRequested bool) string {
	if stackTraceRequested {
		return "delivery.ErrorWithStack"
	}
	switch level {
	case logger.LogLevelDebug:
		return "delivery.Debug"
	case logger.LogLevelInfo:
		return "delivery.Info"
	case logger.LogLevelWarn:
		return "delivery.Warn"
	default:
		return "delivery.Error"
	}
}

type deliveryLoggerReport struct {
	index  int
	report logger.DeliveryReport
	err    error
}

// runAllは生成済みの一つのentryを全Delivery childへ渡し、各送達結果を集約します。
// OccurredAtはchildを開始する前に一度だけ確定し、各childには独立したcloneを渡します。
func (m *DeliveryMultiLogger) runAll(
	ctx context.Context,
	entry *infralogging.LogEntry,
) (logger.DeliveryReport, error) {
	if entry == nil {
		err := fmt.Errorf("delivery multi logger entry is nil")
		return m.rejectBeforeDispatch("delivery.Log", err)
	}
	op := deliveryLoggerOperation(entry.Level, entry.StackTraceRequested)
	var now func() time.Time
	if m != nil {
		now = m.now
	}
	entry = canonicalLogEntryForFanOut(entry, now)

	if m == nil {
		return logger.DeliveryReport{}, noDeliveryLoggersError(op)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	m.stateMu.Lock()
	if m.closeState != loggercommon.LoggerCloseStateOpen {
		m.stateMu.Unlock()
		return deliveryClosingReport(op, m.backends)
	}
	if len(m.loggers) == 0 {
		m.stateMu.Unlock()
		return logger.DeliveryReport{}, noDeliveryLoggersError(op)
	}
	m.inFlight.Add(len(m.loggers))
	m.stateMu.Unlock()

	runCtx := context.WithoutCancel(ctx)
	if m.timeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(runCtx, m.timeout)
		defer cancel()
	}

	resultCh := make(chan deliveryLoggerReport, len(m.loggers))
	for i, l := range m.loggers {
		i := i
		l := l
		backend := m.backends[i]
		go func() {
			defer m.inFlight.Done()
			result := deliveryLoggerReport{index: i}
			defer func() {
				if recovered := recover(); recovered != nil {
					panicErr := deliveryLoggerPanicError(recovered)
					// handlerは、送信した後や確認応答を受け取った後にpanicすることがあります。
					// 完了したreportがなければ、配送結果は不明です。
					result.report = logger.DeliveryReport{
						Targets: []logger.DeliveryResult{
							{
								Backend: backend,
								Status:  logger.DeliveryUnconfirmed,
								Err:     panicErr,
							},
						},
					}
					result.err = panicErr
				}
				resultCh <- result
			}()

			result.report, result.err = l.HandleLogEntry(
				runCtx,
				infralogging.CloneLogEntry(entry),
			)
		}()
	}

	results := make([]deliveryLoggerReport, len(m.loggers))
	completed := make([]bool, len(m.loggers))
	completedCount := 0

	recordResult := func(result deliveryLoggerReport) {
		if completed[result.index] {
			return
		}
		results[result.index] = result
		completed[result.index] = true
		completedCount++
	}

	for completedCount < len(m.loggers) {
		select {
		case result := <-resultCh:
			recordResult(result)

		case <-runCtx.Done():
		drainResults:
			for {
				select {
				case result := <-resultCh:
					recordResult(result)
				default:
					break drainResults
				}
			}

			if completedCount < len(m.loggers) {
				timeoutErr := fmt.Errorf("delivery multi logger timed out: %w", runCtx.Err())
				for i, done := range completed {
					if done {
						continue
					}

					childErr := wrapHandlerErr(m.loggers[i], op, timeoutErr)
					results[i] = deliveryLoggerReport{
						index: i,
						report: logger.DeliveryReport{
							Targets: []logger.DeliveryResult{
								{
									Backend: m.backends[i],
									Status:  logger.DeliveryUnconfirmed,
									Err:     childErr,
								},
							},
						},
						err: childErr,
					}
				}
			}

			return mergeDeliveryLoggerReports(op, m.loggers, results)
		}
	}

	return mergeDeliveryLoggerReports(op, m.loggers, results)
}

func (m *DeliveryMultiLogger) rejectIfClosing(
	op string,
) (logger.DeliveryReport, error, bool) {
	if m != nil {
		m.stateMu.Lock()
		closing := m.closeState != loggercommon.LoggerCloseStateOpen
		m.stateMu.Unlock()
		if closing {
			report, err := deliveryClosingReport(op, m.backends)
			return report, err, true
		}
	}
	return logger.DeliveryReport{}, nil, false
}

// deliveryClosingReportはClose開始後に到着した新規delivery要求をbackendへ渡さず、
// 各targetが送信開始前に拒否されたことをDeliveryRejectedで明示します。
// backend呼び出しを開始した後に結果が分からないtimeoutとは異なり、
// この分岐では未送信が確定しているためDeliveryUnconfirmedにはしません。
// backendsは各childの論理出力名です。
func deliveryClosingReport(
	op string,
	backends []string,
) (logger.DeliveryReport, error) {
	err := &OperationError{
		Op:  op,
		Err: fmt.Errorf("delivery multi logger is closing: %w", logger.ErrLoggerClosed),
	}
	return deliveryRejectedReport(backends, err), err
}

// deliveryEntryValidationReportはapplication共通validationでfan-out前に拒否した
// entryについて、どのchild backendも送信を開始していないことを報告します。
// backendsは各childの論理出力名です。
func deliveryEntryValidationReport(
	op string,
	backends []string,
	validationErr error,
) (logger.DeliveryReport, error) {
	err := &OperationError{Op: op, Err: validationErr}
	return deliveryRejectedReport(backends, err), err
}

// deliveryRejectedReportは、送信を開始せずに拒否した全childをDeliveryRejectedとして報告します。
func deliveryRejectedReport(backends []string, err error) logger.DeliveryReport {
	report := logger.DeliveryReport{
		Targets: make([]logger.DeliveryResult, 0, len(backends)),
	}
	for _, backend := range backends {
		report.Targets = append(report.Targets, logger.DeliveryResult{
			Backend: backend,
			Status:  logger.DeliveryRejected,
			Err:     err,
		})
	}
	return report
}

func deliveryLoggerPanicError(recovered any) error {
	return &PanicError{Logger: "delivery logger", Value: recovered}
}

// noDeliveryLoggersErrorは、送達先を一つも持たないDelivery loggerへの要求を拒否します。
func noDeliveryLoggersError(op string) error {
	return &OperationError{
		Op:  op,
		Err: fmt.Errorf("no delivery loggers available: %w", logger.ErrNoDeliveryTargets),
	}
}

func mergeDeliveryLoggerReports(
	op string,
	loggers []infralogging.DeliveryBackendLogEntryHandler,
	results []deliveryLoggerReport,
) (logger.DeliveryReport, error) {
	report := logger.DeliveryReport{}
	errs := make([]error, 0, len(results))

	for i, result := range results {
		report.Targets = append(report.Targets, result.report.Targets...)
		report.Durables = append(report.Durables, result.report.Durables...)
		report.Evacuations = append(report.Evacuations, result.report.Evacuations...)

		if result.err != nil {
			errs = append(errs, wrapHandlerErr(loggers[i], op, result.err))
		}

		if len(result.report.Targets) == 0 {
			errs = append(errs, wrapHandlerErr(loggers[i], op, logger.ErrNoDeliveryTargets))
			continue
		}

		for _, target := range result.report.Targets {
			if err := deliveryTargetStatusError(op, loggers[i], target); err != nil {
				errs = append(errs, err)
			}
		}
	}

	return report, errors.Join(errs...)
}

func deliveryTargetStatusError(
	op string,
	deliveryLogger infralogging.DeliveryBackendLogEntryHandler,
	target logger.DeliveryResult,
) error {
	if registeredDeliveryStatus(target.Status) && target.Err == nil {
		return nil
	}
	return wrapHandlerErr(deliveryLogger, op, &TargetDeliveryError{
		Backend: target.Backend,
		Status:  target.Status,
		Err:     target.Err,
	})
}

// rejectBeforeDispatchはapplication入力またはentryの検証失敗を、全backendが未送信である
// DeliveryRejected reportへ変換します。Close開始済みならClose理由を優先します。
func (m *DeliveryMultiLogger) rejectBeforeDispatch(
	op string,
	cause error,
) (logger.DeliveryReport, error) {
	if report, err, rejected := m.rejectIfClosing(op); rejected {
		return report, err
	}
	if m == nil {
		return logger.DeliveryReport{}, &OperationError{Op: op, Err: cause}
	}
	return deliveryEntryValidationReport(op, m.backends, cause)
}

// StopAcceptingはresourceを閉じずに新しいlog要求の受付だけを停止します。
// 受付停止後は既存の拒否経路が全targetをDeliveryRejectedとして返し、受理済み処理の
// 完了待ちとchild loggerの終了は後続のCloseで行います。複数回呼んでも状態は変わりません。
func (m *DeliveryMultiLogger) StopAccepting() {
	if m == nil {
		return
	}
	m.stateMu.Lock()
	if m.closeState == loggercommon.LoggerCloseStateOpen {
		m.closeState = loggercommon.LoggerCloseStateStarted
	}
	m.stateMu.Unlock()
}

func (m *DeliveryMultiLogger) Close() error {
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
