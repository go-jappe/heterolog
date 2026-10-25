package flushfailure

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

var (
	ErrNoFailureHandler           = errors.New("no failure handler configured")
	ErrFailureHandlerNotAccepting = errors.New("failure handler is not accepting new work")
	// ErrFailureHandlingDrainIncompleteは、Drain開始前に受理した処理の終了を待ち切れなかったことを表します。
	// 待機を止めたcontextのerrorも同じerrorから判定できます。
	ErrFailureHandlingDrainIncomplete = errors.New("failure handling drain incomplete")
	// ErrNilFailureStoreConfigは、失敗ログの保存先を構築する設定がnilであることを表します。
	ErrNilFailureStoreConfig = errors.New("failure store config is nil")
)

const DefaultCompositeFailureHandlerTimeout = 10 * time.Second

// FailureHandler は、backend送信失敗後のdurable処理を同期的に実行します。
// 戻り値はbackendへの送信失敗ではなく、durable処理自体の失敗を表します。
type FailureHandler interface {
	HandleFailure(ctx context.Context, result loggerfailure.SendResult) error
}

// FailureReportHandler は、durable処理を同期的に実行して結果を返します。
// 本来の送信先に対する結果は呼び出し側が所有するため、実装はTargetsを設定しません。
type FailureReportHandler interface {
	HandleFailureWithReport(
		ctx context.Context,
		result loggerfailure.SendResult,
	) (pkglogger.DeliveryReport, error)
}

// FailureHandlerWithReport は、failure処理に加えて、その確定結果をDeliveryReportとして
// 返せるchild handlerの契約です。Delivery用Compositeはこの型だけを受け取るため、
// report非対応handlerの混入をcompile時に防ぎます。
type FailureHandlerWithReport interface {
	FailureHandler
	FailureReportHandler
}

// HandlerShutdownは、Compositeが受理済みのfailure処理をすべて待った後、child handlerへ
// 最終cleanupを要求する任意契約です。childがHandleFailureの返却後も内部処理を継続する場合、
// Shutdownはその完了を待ってから、childが所有するresourceを閉じます。
// Drainと同じcontextを受け取り、失敗後に再実行できるよう冪等かつ再試行可能でなければなりません。
type HandlerShutdown interface {
	Shutdown(ctx context.Context) error
}

// CompositeFailureHandler はfailure handler群を並列実行します。
// semは、複数のComposite呼び出しをまたいで同時に実行するchild handler数を制限します。
type CompositeFailureHandler struct {
	Handlers []FailureHandler
	Timeout  time.Duration
	sem      chan struct{}

	// lifecycleMuは新規failure処理の受付、実行中処理数、およびDrainの状態遷移を
	// 一つの順序で管理します。Drain開始後に新しい処理がinFlightへ入ることを防ぎます。
	lifecycleMu sync.Mutex
	state       compositeLifecycleState
	inFlight    int
	// inFlightDoneはinFlightが0から増えるたびに作り直し、その世代の全処理が
	// 完了した時点でcloseします。Drainはlockを保持せずにこのchannelを待ちます。
	inFlightDone chan struct{}
}

type compositeLifecycleState uint8

const (
	compositeLifecycleStateAccepting compositeLifecycleState = iota
	compositeLifecycleStateDraining
	compositeLifecycleStateDrained
)

var _ FailureHandler = (*CompositeFailureHandler)(nil)

// NewCompositeFailureHandlerは、正の同時実行上限でfailure handler群を構築します。
// 0以下を暗黙のdefaultへ補正せず、設定errorとして返します。
func NewCompositeFailureHandler(
	handlers []FailureHandler,
	maxConcurrent int,
) (*CompositeFailureHandler, error) {
	if err := pkgdurable.ValidateFailureHandlerMaxConcurrent(maxConcurrent); err != nil {
		return nil, err
	}
	return newCompositeFailureHandler(handlers, maxConcurrent), nil
}

func newCompositeFailureHandler(
	handlers []FailureHandler,
	maxConcurrent int,
) *CompositeFailureHandler {
	return &CompositeFailureHandler{
		Handlers: handlers,
		Timeout:  DefaultCompositeFailureHandlerTimeout,
		sem:      make(chan struct{}, maxConcurrent),
		state:    compositeLifecycleStateAccepting,
	}
}

// HandleFailure はchild handlerを並列実行し、すべての完了またはtimeoutを待ちます。
// childのerrorは包んだまま集約するため、過負荷によるdropを表すpkglogger.ErrLogDropped
// などの原因を、呼び出し元がerrors.Isで判定できます。
func (c *CompositeFailureHandler) HandleFailure(
	ctx context.Context,
	result loggerfailure.SendResult,
) error {
	if c == nil {
		return ErrNoFailureHandler
	}
	finishHandling, err := c.beginHandling()
	if err != nil {
		return err
	}
	finishTransferred := false
	defer func() {
		if !finishTransferred {
			finishHandling()
		}
	}()

	handleCtx, cancel := c.handleContext(ctx)
	defer cancel()

	errCh := make(chan error, len(c.Handlers))
	var wg sync.WaitGroup
	started := 0

	for index, handler := range c.Handlers {
		if handler == nil {
			continue
		}

		started++
		wg.Add(1)
		go func(index int, handler FailureHandler) {
			defer wg.Done()

			if err := c.acquire(handleCtx); err != nil {
				errCh <- &FailureHandlerError{
					Index: index, Handler: handler, Operation: FailureHandlerOperationAcquire, Err: err,
				}
				return
			}
			defer c.release()

			defer func() {
				if recovered := recover(); recovered != nil {
					errCh <- &FailureHandlerError{
						Index: index, Handler: handler, Operation: FailureHandlerOperationPanic, PanicValue: recovered,
					}
				}
			}()

			if err := handler.HandleFailure(handleCtx, result); err != nil {
				errCh <- &FailureHandlerError{
					Index: index, Handler: handler, Operation: FailureHandlerOperationHandle, Err: err,
				}
			}
		}(index, handler)
	}

	if started == 0 {
		return ErrNoFailureHandler
	}

	done := waitAndFinish(&wg, finishHandling)
	finishTransferred = true
	select {
	case <-done:
		close(errCh)
		return errors.Join(collectErrors(errCh)...)
	case <-handleCtx.Done():
		errs := collectAvailableErrors(errCh)
		errs = append(errs, fmt.Errorf(
			"failure handling timed out: %w",
			handleCtx.Err(),
		))
		return errors.Join(errs...)
	}
}

// DrainはCompositeを受付中から終了へ移す操作です。新しいHandleFailureと
// HandleFailureWithReportを不可逆に拒否し、Drain開始前に受理した呼び出しと、それらが開始した
// child処理がすべて終わるまで待ちます。その後、HandlerShutdownを実装するchildを順に終了します。
// spoolに残ったrecordの再送、backend bufferのFlush、log dataの削除はDrainの責務ではありません。
//
// 内部では終了期限を設けません。callerが明示したcancel/deadlineで待機を完了できなかった
// 場合も受付は再開せず、呼び出し元は未確定batchを保持したままCloseを再試行できます。
// 同一instanceへの並行呼び出しは契約に含めません。通常は所有LoggerのCloseが、
// Loggerへ所有権を渡す前はconstructor rollbackが、Drainを逐次呼び出します。
func (c *CompositeFailureHandler) Drain(ctx context.Context) error {
	if c == nil {
		return nil
	}

	ctx = contextOrBackground(ctx)

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("failure handling drain not started: %w", err)
	}

	done, alreadyDrained := c.beginDrain()
	if alreadyDrained {
		return nil
	}

	if done != nil {
		if err := shutdown.WaitContext(ctx, "accepted failure handlers", done); err != nil {
			return fmt.Errorf("%w: %w", ErrFailureHandlingDrainIncomplete, err)
		}
	}

	complete, err := c.shutdownHandlers(ctx)
	if complete {
		c.markDrained()
	}
	return err
}

// shutdownHandlersはDrainの最終段階として、Compositeのin-flight処理がなくなった後に
// HandlerShutdown対応childへcallerのContextを渡して順に終了します。途中のerrorは集約し、
// 未完了childがあれば次回Drainで再試行します。強制終了errorでも実際の終了は待ちます。
func (c *CompositeFailureHandler) shutdownHandlers(ctx context.Context) (bool, error) {
	var errs []error
	complete := true
	for index, handler := range c.Handlers {
		handlerShutdown, ok := handler.(HandlerShutdown)
		if !ok || handlerShutdown == nil {
			continue
		}
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			complete = false
			break
		}
		if err := shutdownHandler(ctx, handlerShutdown); err != nil {
			loggercommon.WaitForForcedShutdown(handler, err)
			if !loggercommon.ShutdownComplete(handler) {
				complete = false
			}
			errs = append(errs, &FailureHandlerError{
				Index: index, Handler: handler, Operation: FailureHandlerOperationShutdown, Err: err,
			})
		}
	}
	return complete, errors.Join(errs...)
}

// ShutdownCompleteはchildの実処理と資源解放がすべて終了したことを表します。
func (c *CompositeFailureHandler) ShutdownComplete() bool {
	if c == nil {
		return true
	}
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	return c.state == compositeLifecycleStateDrained
}

func shutdownHandler(ctx context.Context, handlerShutdown HandlerShutdown) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic: %v", recovered)
		}
	}()
	return handlerShutdown.Shutdown(ctx)
}

// beginHandlingはDrainとの同じlock境界で新規failure処理を受理し、その処理とchildの
// 完了時に呼ぶ関数を返します。Drain開始後の要求は受理しません。
func (c *CompositeFailureHandler) beginHandling() (func(), error) {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	if c.state != compositeLifecycleStateAccepting {
		return nil, ErrFailureHandlerNotAccepting
	}
	if c.inFlight == 0 {
		c.inFlightDone = make(chan struct{})
	}
	c.inFlight++

	return func() {
		c.lifecycleMu.Lock()
		c.inFlight--
		if c.inFlight == 0 {
			close(c.inFlightDone)
		}
		c.lifecycleMu.Unlock()
	}, nil
}

// beginDrainはDrainの最初の段階として新規failure処理の受付を不可逆に停止し、その時点で
// beginHandlingが受理済みとして登録している全処理を待つchannelを返します。
// すでにDrain完了済みならalreadyDrainedを返します。
func (c *CompositeFailureHandler) beginDrain() (done <-chan struct{}, alreadyDrained bool) {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	if c.state == compositeLifecycleStateDrained {
		return nil, true
	}
	c.state = compositeLifecycleStateDraining
	if c.inFlight > 0 {
		done = c.inFlightDone
	}
	return done, false
}

func (c *CompositeFailureHandler) markDrained() {
	c.lifecycleMu.Lock()
	c.state = compositeLifecycleStateDrained
	c.lifecycleMu.Unlock()
}

func (c *CompositeFailureHandler) handleContext(ctx context.Context) (context.Context, context.CancelFunc) {
	// backend送信完了後の退避はrequestの終了後も試みる必要があるため、
	// caller cancellationから切り離し、Composite自身のtimeoutだけを適用する。
	ctx = context.WithoutCancel(contextOrBackground(ctx))
	if c.Timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.Timeout)
}

func (c *CompositeFailureHandler) acquire(ctx context.Context) error {
	select {
	case c.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *CompositeFailureHandler) release() {
	<-c.sem
}

// waitAndFinishはchild処理がすべて終了した時点でCompositeのin-flight登録を解除します。
// 呼び出し元がtimeoutで先に戻っても、Drainはこの完了まで待つことができます。
func waitAndFinish(wg *sync.WaitGroup, finish func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		finish()
		close(done)
	}()
	return done
}

func collectErrors(errCh <-chan error) []error {
	errs := make([]error, 0, len(errCh))
	for err := range errCh {
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

// collectAvailableErrorsは、timeout時点ですでに完了しているchildのerrorだけを
// blockせず回収します。実行中childはCompositeのin-flight管理に残り、Drainが待ちます。
func collectAvailableErrors(errCh <-chan error) []error {
	errs := make([]error, 0, len(errCh))
	for {
		select {
		case err := <-errCh:
			if err != nil {
				errs = append(errs, err)
			}
		default:
			return errs
		}
	}
}

func contextOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
