package opensearch

import (
	"context"
	"io"
	"log"
	"sync"
	"sync/atomic"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infraopensearch "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch"
	opensearchwrapper "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch/wrapper"
	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
)

// fallbackHandlerControllerはhandlerと遅延生成されるFallbackLoggerを
// 同じ所有単位にまとめます。Close開始後は新しいhandler実行を受け入れず、
// 実行中handlerのenqueueが終えてからloggerのworkerをdrainして閉じます。
type fallbackHandlerController struct {
	adapter          OpenSearchLoggerConfigProvider
	clientCfg        infraopensearch.OpenSearchClientConfigProvider
	retryCfg         opensearchwrapper.RetryConfigProvider
	flushManagerCfg  OpenSearchFlushManagerRuntimeConfigProvider
	resourceProvider resource.ResourceProvider
	opts             []OpenSearchLoggerOption

	mu      sync.Mutex
	closeMu sync.Mutex
	active  sync.WaitGroup
	closing bool
	closed  bool
	// forceRequestedは遅延初期化のI/Oがmuを保持していても中止を受け付けます。
	forceRequested atomic.Bool
	// initializationAttemptedはFallbackLoggerの遅延初期化を試行済みかを表します。
	// 初期化に成功したかはloggerの有無で判断し、失敗後の再試行は行いません。
	initializationAttempted bool
	// initializationErrは遅延初期化に失敗した理由です。loggerを作れなかった場合は、
	// 通知を黙って捨てないよう、通知ごとにこの理由を標準loggerへ一行残します。
	initializationErr error
	logger            *FallbackLogger
}

var _ io.Closer = (*fallbackHandlerController)(nil)

func newFallbackHandlerController(
	adapter OpenSearchLoggerConfigProvider,
	clientCfg infraopensearch.OpenSearchClientConfigProvider,
	retryCfg opensearchwrapper.RetryConfigProvider,
	fmCfg OpenSearchFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	opts ...OpenSearchLoggerOption,
) *fallbackHandlerController {
	return &fallbackHandlerController{
		adapter:          adapter,
		clientCfg:        clientCfg,
		retryCfg:         retryCfg,
		flushManagerCfg:  fmCfg,
		resourceProvider: resourceProvider,
		opts:             append([]OpenSearchLoggerOption(nil), opts...),
	}
}

func (h *fallbackHandlerController) acquire(
	tracer observer.Tracer,
	meter observer.Meter,
) (*FallbackLogger, bool) {
	if h.forceRequested.Load() {
		return nil, false
	}
	h.mu.Lock()
	if h.closing || h.forceRequested.Load() {
		h.mu.Unlock()
		return nil, false
	}

	// acquireはclosingの確認とactive.Add(1)を同じmuの下で行います。
	// Closeも同じmuの下でclosingをtrueにしてからactive.Wait()へ進むため、
	// Wait開始後に新しいhandlerが実行中として登録されることはありません。
	h.active.Add(1)
	if !h.initializationAttempted {
		h.initializationAttempted = true
		fallbackLogger, err := newFallbackLogger(
			h.adapter,
			h.clientCfg,
			h.retryCfg,
			h.flushManagerCfg,
			h.resourceProvider,
			tracer,
			meter,
			h.opts...,
		)
		if err != nil {
			h.initializationErr = err
		} else {
			h.logger = fallbackLogger
		}
	}
	fallbackLogger, initializationErr := h.logger, h.initializationErr
	h.mu.Unlock()

	if fallbackLogger == nil {
		h.active.Done()
		if initializationErr != nil {
			// fallback logger自身の初期化失敗へ別のfallbackを重ねると再帰し得るため、
			// Go標準logger（通常はstderr）だけへ記録します。初期化は再試行しませんが、通知を
			// 黙って捨てないよう、作れなかった通知先への通知ごとに一行残します。
			log.Printf(MsgFailedCreateFallbackLogger, initializationErr)
		}
		return nil, false
	}
	return fallbackLogger, true
}

func (h *fallbackHandlerController) release() {
	h.active.Done()
}

func (h *fallbackHandlerController) Handle(
	ctx context.Context,
	entry *logging.LogEntry,
	tracer observer.Tracer,
	meter observer.Meter,
) {
	fallbackLogger, ok := h.acquire(tracer, meter)
	if !ok {
		return
	}
	defer h.release()

	if !fallbackLogger.shouldLog(entry.Level) {
		return
	}
	if !sampling.ApplySamplingLogic(entry, fallbackLogger.config.Sampling()) {
		return
	}

	logging.EnsureEntryOccurredAt(entry, fallbackLogger.Clock)
	fallbackLogger.enqueueProcess(entry)
}

func (h *fallbackHandlerController) HandleError(
	ctx context.Context,
	_ error,
	entry *logging.LogEntry,
	tracer observer.Tracer,
	meter observer.Meter,
) {
	h.Handle(ctx, entry, tracer, meter)
}

func (h *fallbackHandlerController) Close() error {
	if h == nil {
		return nil
	}

	h.closeMu.Lock()
	defer h.closeMu.Unlock()

	h.mu.Lock()
	h.closing = true
	fallbackLogger := h.logger
	h.mu.Unlock()

	h.active.Wait()
	var err error
	if fallbackLogger != nil {
		err = fallbackLogger.Close(context.Background())
	}
	if err == nil || loggercommon.ShutdownComplete(fallbackLogger) {
		h.mu.Lock()
		h.closed = true
		h.mu.Unlock()
	}
	return err
}
