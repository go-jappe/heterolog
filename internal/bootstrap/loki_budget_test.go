package bootstrap_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/bootstrap"
	appconfig "github.com/go-jappe/heterolog/internal/config"
	"github.com/go-jappe/heterolog/internal/presentation/envconfig"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// 外部のserviceなしで、環境設定からruntimeまでの経路を試します。通常の再試行も
// Retry-Afterを使います。must_fitは、以前の線形の予算を拒否し、すべての要求と最大の
// 再試行の待ち時間に足りる時間を確保する必要があります。
func TestLokiMustFitEnvConfigCompletesRetryAfter(t *testing.T) {
	for _, contract := range []string{"sync", "async", "delivery"} {
		for _, mode := range []string{"retry", "retry_with_breaker"} {
			for _, parentSeconds := range []int{4, 13, 14} {
				t.Run(fmt.Sprintf("%s/%s/parent_%ds", contract, mode, parentSeconds), func(t *testing.T) {
					isolateEnvConfigForTest(t)
					prefix := map[string]string{"sync": "", "async": "ASYNC_", "delivery": "DELIVERY_"}[contract]
					path := writeEnvFileForTest(t,
						fmt.Sprintf("HETEROLOG_ENABLE_LOGGING=%t", contract == "sync"),
						fmt.Sprintf("HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=%t", contract == "async"),
						fmt.Sprintf("HETEROLOG_ENABLE_DELIVERY_LOGGING=%t", contract == "delivery"),
						"HETEROLOG_"+prefix+"LOG_TYPE=loki",
						"HETEROLOG_"+prefix+"LOG_TIMEOUT_BUDGET_POLICY=must_fit",
						fmt.Sprintf("HETEROLOG_%sLOG_TIMEOUT_SECOND=%d", prefix, parentSeconds),
						"HETEROLOG_"+prefix+"LOKI_ENDPOINT=https://logging.example.test",
						"HETEROLOG_"+prefix+"LOKI_TIMEOUT_SECOND=1",
						"HETEROLOG_"+prefix+"LOKI_RETRY_CLIENT_MODE="+mode,
						"HETEROLOG_MAX_"+prefix+"LOKI_RETRYABLE_CLIENT_RETRIES=2", // 初回と合わせて3回送ります。
						"HETEROLOG_"+prefix+"LOKI_RETRYABLE_CLIENT_MILLISECOND=200",
						"HETEROLOG_MAX_"+prefix+"LOKI_RETRYABLE_CLIENT_DELAY_MILLISECOND=5000",
						"HETEROLOG_"+prefix+"LOKI_DURABLE_TYPES=none",
						"HETEROLOG_ENABLE_LOG_LOKI_SAMPLING=false", "HETEROLOG_ENABLE_LOGGING_LOKI_BY_REQUEST_ID=false",
						"HETEROLOG_ENABLE_ASYNC_LOG_LOKI_SAMPLING=false", "HETEROLOG_ENABLE_ASYNC_LOGGING_LOKI_BY_REQUEST_ID=false",
						"HETEROLOG_ENABLE_LOKI_NO_BULK_FLUSH_MODE=true", "HETEROLOG_MAX_ASYNC_LOKI_PUSH_ENTRIES=1",
						"HETEROLOG_LOKI_FLUSH_INTERVAL_SECOND=3600", "HETEROLOG_ASYNC_LOKI_FLUSH_INTERVAL_SECOND=3600",
						"HETEROLOG_ENABLE_LOKI_COMPRESS_REQUEST=false", "HETEROLOG_ENABLE_ASYNC_LOKI_COMPRESS_REQUEST=false", "HETEROLOG_ENABLE_DELIVERY_LOKI_COMPRESS_REQUEST=false",
					)
					loaded, err := envconfig.Load(path)
					if parentSeconds <= 13 {
						var hierarchy *appconfig.TimeoutHierarchyError
						if !errors.As(err, &hierarchy) || hierarchy.Backend != pkglogger.LogOutputLoki ||
							hierarchy.ClientRetryBudget != 13*time.Second {
							t.Fatalf("Load() = %v, want Loki retry budget 13s rejection", err)
						}
						return
					}
					if err != nil {
						t.Fatalf("Load() = %v", err)
					}
					synctestWithHangWatchdog(t, func(t *testing.T) {
						testLokiRetryAfterRuntime(t, loaded, contract)
					})
				})
			}
		}
	}
}

// synctestHangLimitは、synctestの試験が偽の時刻で終わるはずの処理を、実時間で待つ上限です。
// 判定には使わず、bubble内のgoroutineが止まった場合の停止検出だけに使います。
const synctestHangLimit = 2 * time.Minute

// synctestWithHangWatchdogは、synctest.Testの完了を実時間で監視します。bubble内の
// goroutineがsync.Mutexなど偽の時刻を進められない待ちで止まると、試験は既定のtimeoutまで
// 停止するため、上限を過ぎた時点で全goroutineのstackを出力して失敗させます。
func synctestWithHangWatchdog(t *testing.T, f func(*testing.T)) {
	t.Helper()
	name := t.Name()
	watchdog := time.AfterFunc(synctestHangLimit, func() {
		debug.SetTraceback("all")
		panic(fmt.Sprintf(
			"%s: synctest bubble did not finish within %s of real time; a goroutine is blocked on an operation that does not advance fake time (for example sync.Mutex)",
			name,
			synctestHangLimit,
		))
	})
	defer watchdog.Stop()
	synctest.Test(t, f)
}

type lokiBudgetRoundTripper func(*http.Request) (*http.Response, error)

func (f lokiBudgetRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func testLokiRetryAfterRuntime(t *testing.T, loaded *envconfig.EnvConfig, contract string) {
	t.Helper()
	var attempts []time.Time
	var bodies []string
	client := &http.Client{Timeout: time.Second, Transport: lokiBudgetRoundTripper(func(req *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		if err := req.Body.Close(); err != nil {
			return nil, err
		}
		attempts = append(attempts, time.Now())
		bodies = append(bodies, string(body))
		time.Sleep(900 * time.Millisecond)
		status := http.StatusServiceUnavailable
		if len(attempts) == 3 {
			status = http.StatusNoContent
		}
		return &http.Response{
			StatusCode: status, Header: http.Header{"Retry-After": {"5"}},
			Body: io.NopCloser(strings.NewReader("")), Request: req,
		}, nil
	})}
	ctx := context.Background()
	const message = "must fit preserves this entry through retry-after"
	started := time.Now()
	switch contract {
	case "sync":
		cfg, opts, err := loaded.Config()
		if err != nil {
			t.Fatal(err)
		}
		cfg.LokiClient.HTTPClient = client
		runtime, err := bootstrap.New(ctx, cfg, opts...)
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.Info(ctx, message); err != nil {
			t.Errorf("Info() = %v", err)
		}
		if err := runtime.Close(); err != nil {
			t.Fatal(err)
		}
	case "async":
		cfg, opts, err := loaded.AsyncConfig()
		if err != nil {
			t.Fatal(err)
		}
		cfg.LokiClient.HTTPClient = client
		runtime, err := bootstrap.NewAsync(ctx, cfg, opts...)
		if err != nil {
			t.Fatal(err)
		}
		runtime.Info(ctx, message)
		if err := runtime.Close(); err != nil {
			t.Fatal(err)
		}
	case "delivery":
		cfg, opts, err := loaded.DeliveryConfig()
		if err != nil {
			t.Fatal(err)
		}
		cfg.LokiClient.HTTPClient = client
		runtime, err := bootstrap.NewDelivery(ctx, cfg, opts...)
		if err != nil {
			t.Fatal(err)
		}
		report, err := runtime.Info(ctx, message)
		if err != nil || len(report.Targets) != 1 || report.Targets[0].Status != pkglogger.DeliverySucceeded {
			t.Errorf("Info() = %+v, %v, want confirmed Loki delivery", report, err)
		}
		if err := runtime.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if len(attempts) != 3 {
		t.Fatalf("requests = %d, want three attempts within the accepted budget", len(attempts))
	}
	for i, at := range attempts {
		if got, want := at.Sub(started), time.Duration(i)*5900*time.Millisecond; got != want {
			t.Errorf("attempt %d starts at %s, want %s", i+1, got, want)
		}
		if bodies[i] != bodies[0] || !strings.Contains(bodies[i], message) {
			t.Errorf("attempt %d did not preserve the original entry: %s", i+1, bodies[i])
		}
	}
	if got, want := time.Since(started), 12700*time.Millisecond; got != want {
		t.Errorf("elapsed = %s, want %s", got, want)
	}
}
