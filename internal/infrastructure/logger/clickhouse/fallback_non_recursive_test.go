package clickhouse

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestClickHouseFallbackBackendFailureDoesNotInvokeInternalOrFailureHandlers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := newTestClickHouseLoggerBaseConfig()
		cfg.FlushInterval = time.Hour
		cfg.MaxEntries = 1
		// 試験用helperではなく、internal log handlerが使うproduction factoryの配線を検証します。
		fallback, err := newFallbackLogger(
			context.Background(),
			NewClickHouseLoggerConfigAdapter(cfg),
			&pkgclickhouse.Config{
				Endpoint: "http://127.0.0.1:8123",
				Table:    "logs",
			},
			newTestClickHouseFlushManagerConfig(),
			newTestClickHouseResourceProvider(),
			nil,
			observer.NopTracer{},
			observer.NopMeter{},
		)
		if err != nil {
			t.Fatalf("create ClickHouse fallback logger: %v", err)
		}
		// 途中で失敗した場合もbubble内のworkerを停止します。Closeは完了後の再呼び出しで何もしません。
		t.Cleanup(func() {
			if err := fallback.Close(context.Background()); err != nil {
				t.Errorf("close ClickHouse fallback logger: %v", err)
			}
		})

		flushManager, ok := fallback.flushManager.(*ClickHouseFlushManager)
		if !ok {
			t.Fatalf("fallback flush manager type = %T, want *ClickHouseFlushManager", fallback.flushManager)
		}
		// fallbackは専用のFlushWithNoFallbackを使い、通常Flushのfailure処理へ戻しません。
		// factoryもFailureHandlerを配線せず、誤った通常経路への接続を検出しやすくします。
		if flushManager.FailureHandler != nil {
			t.Fatal("ClickHouse fallback flush manager has a failure handler")
		}

		// fallbackの送信経路が通常Flushのfailure処理へ戻った場合に検出できるよう、factoryが
		// 作ったmanagerへprobeを接続し、HTTP clientだけを失敗するbackendへ差し替えます。
		failureProbe := &clickHouseFallbackFailureHandlerProbe{}
		failureHandler, err := flushfailure.NewCompositeFailureHandler(
			[]flushfailure.FailureHandler{failureProbe},
			1,
		)
		if err != nil {
			t.Fatalf("create ClickHouse failure handler probe: %v", err)
		}
		flushManager.FailureHandler = failureHandler
		client := &recordingClickHouseClient{
			err: &pkgclickhouse.HTTPStatusError{
				Code:   http.StatusServiceUnavailable,
				Status: http.StatusText(http.StatusServiceUnavailable),
			},
		}
		flushManager.client = client

		var internalHandlerCalls atomic.Int32
		fallback.OnError = func(
			context.Context,
			error,
			*logging.LogEntry,
			observer.Tracer,
			observer.Meter,
		) {
			internalHandlerCalls.Add(1)
		}
		levelHandler := func(
			context.Context,
			*logging.LogEntry,
			observer.Tracer,
			observer.Meter,
		) {
			internalHandlerCalls.Add(1)
		}
		fallback.OnWarn = levelHandler
		fallback.OnInfo = levelHandler
		fallback.OnDebug = levelHandler

		fallback.enqueueProcess(
			context.Background(),
			newTestLogEntry(pkglogger.LogLevelError, "fallback recursion guard test", nil),
		)
		// MaxEntries到達のwakeで、fallback専用workerが一度だけ送信を終えるまで待ちます。
		synctest.Wait()
		if got := client.batchCount(); got != 1 {
			t.Fatalf("ClickHouse backend call count = %d, want 1", got)
		}
		if got := failureProbe.calls.Load(); got != 0 {
			t.Fatalf("failure handler call count = %d, want 0", got)
		}
		if got := internalHandlerCalls.Load(); got != 0 {
			t.Fatalf("On* handler call count = %d, want 0", got)
		}

		if err := fallback.Close(context.Background()); err != nil {
			t.Fatalf("close ClickHouse fallback logger: %v", err)
		}
		if got := client.batchCount(); got != 1 {
			t.Fatalf("ClickHouse backend call count after close = %d, want 1", got)
		}
		if got := failureProbe.calls.Load(); got != 0 {
			t.Fatalf("failure handler call count after close = %d, want 0", got)
		}
		if got := internalHandlerCalls.Load(); got != 0 {
			t.Fatalf("On* handler call count after close = %d, want 0", got)
		}
	})
}

type clickHouseFallbackFailureHandlerProbe struct {
	calls atomic.Int32
}

func (h *clickHouseFallbackFailureHandlerProbe) HandleFailure(
	context.Context,
	loggerfailure.SendResult,
) error {
	h.calls.Add(1)
	return nil
}

var _ flushfailure.FailureHandler = (*clickHouseFallbackFailureHandlerProbe)(nil)
