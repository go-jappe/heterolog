package multi

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// AsyncBestEffortMultiLoggerは、applicationの呼出しとは別のgoroutineで複数の
// child handlerを実行します。maxParallelは同時実行数だけを制限し、上限到達時は空きができる
// まで受付側へbackpressureをかけます。上限到達を理由にchildを破棄しません。ただし、内部ログの
// 通知の中から行われた書き込みは、上限到達時も空きを待たず呼び出し元goroutineで実行します。
// best-effortは、childが返したerrorをapplicationの呼出元へ返さない契約を表します。
type AsyncBestEffortMultiLogger struct {
	loggers []infralogging.BackendLogEntryHandler

	timeout time.Duration
	// maxLogEntryBytesは、application入力をbackendへfan-outする前に適用する一件の上限です。
	maxLogEntryBytes int
	// nowは、一つのapplication入力から全backendで共有する発生時刻を確定します。
	now func() time.Time

	// semは実行中のchild処理数をmaxParallel以内に制限します。空きがない場合、runAllは
	// 新しいgoroutineを作らず、実行中のchildが完了して空きを返すまで待ちます。
	sem chan struct{}
	// muは新規受付の判定と全child分のwg登録を、StopAcceptingの状態遷移と同じ境界で
	// 直列化します。
	mu sync.Mutex
	// wgは受付済みentryの全child処理を追跡します。semaphoreの空きを待っていて、まだ
	// goroutineを開始していないchildも受付時点から含みます。
	wg sync.WaitGroup

	// closeStateはmuの保護下で新規受付とCloseの状態遷移を管理します。
	closeState loggercommon.LoggerCloseState

	closeMu          sync.Mutex
	closedChildren   []bool
	terminalCloseErr error
}

var _ applogging.AsyncCloseableLogger = (*AsyncBestEffortMultiLogger)(nil)

// NewAsyncBestEffortMultiLogger は設定から timeout/maxParallel を読み取って MultiLogger を作成
func NewAsyncBestEffortMultiLogger(
	cfg AsyncBestEffortMultiLoggerConfigProvider,
	loggers ...infralogging.BackendLogEntryHandler,
) (*AsyncBestEffortMultiLogger, error) {
	timeout, maxParallel, maxLogEntryBytes, err := validatedAsyncBestEffortMultiLoggerConfig(cfg)
	if err != nil {
		return nil, err
	}

	return &AsyncBestEffortMultiLogger{
		loggers:          append([]infralogging.BackendLogEntryHandler(nil), loggers...),
		timeout:          timeout,
		maxLogEntryBytes: maxLogEntryBytes,
		now:              time.Now,
		sem:              make(chan struct{}, maxParallel),
		closedChildren:   make([]bool, len(loggers)),
	}, nil
}

// ValidateAsyncBestEffortMultiLoggerConfigはNewAsyncBestEffortMultiLoggerと同じ規則で
// timeout、同時実行数、一件のlog entry上限を検証します。top-level constructorは
// fileやspoolなどのresourceを作る前に呼び出します。
func ValidateAsyncBestEffortMultiLoggerConfig(cfg AsyncBestEffortMultiLoggerConfigProvider) error {
	_, _, _, err := validatedAsyncBestEffortMultiLoggerConfig(cfg)
	return err
}

func validatedAsyncBestEffortMultiLoggerConfig(
	cfg AsyncBestEffortMultiLoggerConfigProvider,
) (time.Duration, int, int, error) {
	if cfg == nil {
		return 0, 0, 0, fmt.Errorf("async best effort multi logger %w", ErrConfigRequired)
	}

	timeout := cfg.Timeout()
	if timeout <= 0 {
		return 0, 0, 0, fmt.Errorf("async best effort multi logger %w: %s", ErrInvalidTimeout, timeout)
	}

	maxParallel := cfg.MaxParallel()
	if maxParallel <= 0 {
		return 0, 0, 0, fmt.Errorf("async best effort multi logger %w: %d", ErrInvalidMaxParallel, maxParallel)
	}
	maxLogEntryBytes := cfg.MaxLogEntryBytes()
	if err := pkglogger.ValidateMaxLogEntryBytes(maxLogEntryBytes); err != nil {
		return 0, 0, 0, err
	}
	return timeout, maxParallel, maxLogEntryBytes, nil
}

// logはapplication入力からbackend非依存のlogical LogEntryを一度だけ生成し、
// そのentryを非同期fan-outへ
// 渡します。入力検証に失敗した要求とClose開始後の要求は受理しません。
func (m *AsyncBestEffortMultiLogger) log(
	ctx context.Context,
	level pkglogger.LogLevel,
	message string,
	err error,
	stackTraceRequested bool,
	fields []applogging.Field,
) {
	if m == nil {
		return
	}

	m.mu.Lock()
	accepting := m.closeState == loggercommon.LoggerCloseStateOpen
	m.mu.Unlock()
	if !accepting {
		return
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
		return
	}
	m.runAll(ctx, entry)
}

func (m *AsyncBestEffortMultiLogger) Debug(
	ctx context.Context,
	message string,
	fields ...applogging.Field,
) {
	m.log(ctx, pkglogger.LogLevelDebug, message, nil, false, fields)
}

func (m *AsyncBestEffortMultiLogger) Info(
	ctx context.Context,
	message string,
	fields ...applogging.Field,
) {
	m.log(ctx, pkglogger.LogLevelInfo, message, nil, false, fields)
}

func (m *AsyncBestEffortMultiLogger) Warn(
	ctx context.Context,
	message string,
	fields ...applogging.Field,
) {
	m.log(ctx, pkglogger.LogLevelWarn, message, nil, false, fields)
}

func (m *AsyncBestEffortMultiLogger) Error(
	ctx context.Context,
	message string,
	fields ...applogging.Field,
) {
	m.log(ctx, pkglogger.LogLevelError, message, nil, false, fields)
}

func (m *AsyncBestEffortMultiLogger) ErrorWithStack(
	ctx context.Context,
	message string,
	err error,
	fields ...applogging.Field,
) {
	m.log(ctx, pkglogger.LogLevelError, message, err, true, fields)
}

// runAllは一つのentryを全childで実行します。すべてのchildが実行枠を得てgoroutineを
// 開始するまでは待ちますが、開始済みchildの処理完了は待ちません。
func (m *AsyncBestEffortMultiLogger) runAll(
	ctx context.Context,
	entry *infralogging.LogEntry,
) {
	if m == nil || entry == nil || len(m.loggers) == 0 {
		return
	}
	entry = canonicalLogEntryForFanOut(entry, m.now)
	if ctx == nil {
		ctx = context.Background()
	}

	// cancel伝播を切る（Valueは保持）
	baseCtx := context.WithoutCancel(ctx)

	m.mu.Lock()
	if m.closeState != loggercommon.LoggerCloseStateOpen {
		m.mu.Unlock()
		return
	}
	// 全child分を先にAddするため、Closeがこの後に受付を停止してWaitを開始しても、
	// semaphoreの空きを待ってまだgoroutineを開始していないchildまで完了対象になります。
	m.wg.Add(len(m.loggers))
	sem := m.sem
	m.mu.Unlock()

	for _, l := range m.loggers {
		l := l
		localEntry := infralogging.CloneLogEntry(entry)
		// goroutine生成前に空きを待つため、並行数上限に達しても待機goroutineを
		// 無制限に増やさず、childを破棄しません。
		select {
		case sem <- struct{}{}:
		default:
			// 空きがなく、この書き込みが内部ログの通知の中から行われた（いずれかのloggerの
			// 通知へ渡したcontext、または通知を実行中のgoroutine）場合は、空きを待たずに
			// 現在のgoroutineでchildを実行します。通知元のchildがこのloggerの実行枠を保持した
			// まま通知の完了を待っているため、空きを待つと、その枠が戻らず永久に停止し得ます。
			// 実行枠を新たに使わず、timeoutとpanicの扱いはgoroutineで実行する場合と同じです。
			// 判定は空きがない場合だけ行うため、通常の書き込みには追加の処理はありません。
			if handler.InInternalLogDispatch(ctx) {
				func() {
					defer m.wg.Done()
					m.runChild(baseCtx, l, localEntry)
				}()
				continue
			}
			sem <- struct{}{}
		}
		go func() {
			defer m.wg.Done()
			defer func() { <-sem }()
			m.runChild(baseCtx, l, localEntry)
		}()
	}
}

// runChildは一つのchildへentryを渡します。childのpanicは回収して標準loggerへ記録します。
func (m *AsyncBestEffortMultiLogger) runChild(
	baseCtx context.Context,
	l infralogging.BackendLogEntryHandler,
	entry *infralogging.LogEntry,
) {
	defer func() {
		if recovered := recover(); recovered != nil {
			// application loggerへ戻すと同じfailure経路を再帰し得るため、
			// process標準loggerへ直接記録してbest-effort処理を継続します。
			log.Printf(
				"async handler=%T: %v",
				l,
				loggerPanicError(recovered),
			)
		}
	}()

	logCtx, cancel := context.WithTimeout(baseCtx, m.timeout)
	defer cancel()

	_ = l.HandleLogEntry(logCtx, entry)
}

// StopAcceptingはresourceを閉じずに新しいlog要求の受付だけを停止します。
// 受付停止後の要求は受理せず、受理済み処理の完了待ちとchild loggerの終了は後続の
// Closeで行います。複数回呼んでも状態は変わりません。
func (m *AsyncBestEffortMultiLogger) StopAccepting() {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.closeState == loggercommon.LoggerCloseStateOpen {
		m.closeState = loggercommon.LoggerCloseStateStarted
	}
	m.mu.Unlock()
}

// Closeは受理済みの非同期処理を待ってから、Close能力を持つchild handlerを閉じる。
// Close開始後の新しいlog呼び出しは受理しない。
func (m *AsyncBestEffortMultiLogger) Close() error {
	if m == nil {
		return nil
	}

	m.closeMu.Lock()
	defer m.closeMu.Unlock()

	m.StopAccepting()
	m.mu.Lock()
	if m.closeState == loggercommon.LoggerCloseStateCompleted {
		m.mu.Unlock()
		return m.terminalCloseErr
	}
	m.mu.Unlock()

	// runAllは受付時に全child分をwgへ登録するため、semaphoreの空きを待っているchildも
	// 含め、各child loggerと共有resourceを閉じる前に全処理の完了を待ちます。
	// 最初のCloseがchildのerrorで失敗してもStartedを維持し、新しいlogは受理しません。
	m.wg.Wait()

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
		m.mu.Lock()
		m.closeState = loggercommon.LoggerCloseStateCompleted
		m.mu.Unlock()
	}
	return closeErr
}
