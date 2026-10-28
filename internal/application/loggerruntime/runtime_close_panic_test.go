package loggerruntime_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/loki"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

func TestCloseFinishesSiblingsAfterTransientObserverPanic(t *testing.T) {
	for _, panicOnClose := range []bool{false, true} {
		name := "normal_control"
		if panicOnClose {
			name = "observer_panics_once_after_acknowledgement"
		}
		t.Run(name, func(t *testing.T) {
			// 終わらなかった後片付けは、実時間で待ってから再試行します。bubbleの偽の時計は、
			// すべてのgoroutineが止まったときだけ進みます。
			synctest.Test(t, func(t *testing.T) {
				lokiTransport := &closePanicTransport{host: "loki.close-panic.invalid", status: http.StatusNoContent}
				cloudWatchTransport := &closePanicTransport{host: "cloudwatch.close-panic.invalid", status: http.StatusOK, response: `{}`}
				tracer := &closePanicTracer{cause: errors.New("transient observer panic during final flush")}
				tracer.panicOnce.Store(panicOnClose)
				logger, err := loggerruntime.New(context.Background(), closePanicConfig(lokiTransport, cloudWatchTransport),
					loggerruntime.WithObservers(tracer, nil))
				if err != nil {
					t.Fatal(err)
				}
				defer logger.Close()
				if err := logger.Info(context.Background(), "accepted before shutdown"); err != nil {
					t.Fatal(err)
				}
				if lokiTransport.calls.Load() != 0 || cloudWatchTransport.calls.Load() != 0 {
					t.Fatal("fixture must keep both deliveries buffered until shutdown")
				}
				// 正常なCloseは、終わらなかった後片付けを内部で再試行します。最初の子loggerの
				// observerは、batchが受け付けられた後に失敗します。それでも、隣の子loggerの最後の
				// 書き出しを妨げたり、そのbatchを再送したりしてはいけません。
				closed := make(chan error, 1)
				go func() { closed <- logger.Close() }()
				// Waitは、最初の試行が終わり、後片付けが再試行の前で止まった（または既に
				// 終わった）ときに返ります。
				synctest.Wait()
				if tracer.panicOnce.Load() {
					t.Fatal("fixture did not reach the final-flush observer")
				}
				if lokiTransport.calls.Load() != 1 || cloudWatchTransport.calls.Load() != 1 {
					t.Fatalf("requests after the first close attempt Loki=%d CloudWatch=%d, want the sibling flushed in the same attempt",
						lokiTransport.calls.Load(), cloudWatchTransport.calls.Load())
				}
				if err := <-closed; err != nil {
					t.Fatalf("Close did not finish recovery from a one-time callback failure: %v", err)
				}
				if err := logger.Close(); err != nil {
					t.Fatalf("repeated Close: %v", err)
				}
				if err := logger.ForceClose(); err != nil {
					t.Fatalf("ForceClose after completed cleanup: %v", err)
				}
				if lokiTransport.calls.Load() != 1 || cloudWatchTransport.calls.Load() != 1 {
					t.Fatalf("requests Loki=%d CloudWatch=%d, want each accepted batch sent exactly once",
						lokiTransport.calls.Load(), cloudWatchTransport.calls.Load())
				}
				if err := logger.Info(context.Background(), "after shutdown"); !errors.Is(err, pkglogger.ErrLoggerClosed) {
					t.Fatalf("logger accepted an entry after cleanup: %v", err)
				}
			})
		})
	}
}

func closePanicConfig(lokiTransport, cloudWatchTransport *closePanicTransport) pkglogger.LoggerConfig {
	cfg := loggerruntime.DefaultConfig()
	cfg.Stdout = nil
	cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputLoki, pkglogger.LogOutputCloudWatchLogs}
	runtime := pkglogger.RuntimeConfig{AppName: "close-panic-test", LogLevel: pkglogger.LogLevelInfo}
	body := pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano}
	store := durable.FailureStoreConfig{DurableType: []durable.DurableType{durable.DurableTypeNone}}
	cfg.LokiClient = &loki.Config{Endpoint: "http://" + lokiTransport.host, HTTPClient: &http.Client{Transport: lokiTransport}}
	cfg.Loki = &pkglogger.LokiConfig{
		LokiLoggerBaseConfig: pkglogger.LokiLoggerBaseConfig{
			RuntimeConfig: runtime, LogBodyConfig: body, FlushInterval: time.Hour,
			MaxEntries: 100, BufferFlushBytes: 1 << 20, FallbackQueueMaxEntries: 100,
			MaxLineBytes: 1 << 16, MaxPushRequestBytes: 1 << 20,
		},
		LokiFlushManagerConfig: pkglogger.LokiFlushManagerConfig{
			FailureStoreConfig: store, Resource: pkglogger.Resource{ServiceName: "close-panic-test"},
		},
	}
	cfg.CloudWatchLogsClient = &cloudwatchlogs.Config{
		Region: "ap-northeast-1", Endpoint: "http://" + cloudWatchTransport.host,
		LogGroupName: "logs", AccessKeyID: "test", SecretAccessKey: "test",
		HTTPClient: &http.Client{Transport: cloudWatchTransport},
	}
	cfg.CloudWatchLogs = &pkglogger.CloudWatchLogsConfig{
		CloudWatchLogsLoggerBaseConfig: pkglogger.CloudWatchLogsLoggerBaseConfig{
			RuntimeConfig: runtime, LogBodyConfig: body, FlushInterval: time.Hour,
			BufferFlushMaxEvents: 100, BufferFlushBytes: 1 << 20, FallbackQueueMaxEvents: 100,
		},
		CloudWatchLogsFlushManagerConfig: pkglogger.CloudWatchLogsFlushManagerConfig{
			FailureStoreConfig: store, LogGroupName: "logs", LogStreamName: "events",
		},
	}
	return cfg
}

type closePanicTransport struct {
	calls    atomic.Int32
	host     string
	status   int
	response string
}

func (tr *closePanicTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Host != tr.host || request.Method != http.MethodPost {
		return nil, fmt.Errorf("unexpected request: %s %s", request.Method, request.URL)
	}
	defer request.Body.Close()
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	if !strings.Contains(string(body), "accepted before shutdown") {
		return nil, fmt.Errorf("accepted log missing from request: %s", body)
	}
	tr.calls.Add(1)
	return &http.Response{
		StatusCode: tr.status, Header: make(http.Header),
		Body: io.NopCloser(strings.NewReader(tr.response)), Request: request,
	}, nil
}

type closePanicTracer struct {
	panicOnce atomic.Bool
	cause     error
}

func (tr *closePanicTracer) StartSpan(ctx context.Context, name string, _ ...pkgobserver.Attr) (context.Context, pkgobserver.Span) {
	return ctx, closePanicSpan{tracer: tr, finalFlush: name == "heterolog.loki.flush"}
}

type closePanicSpan struct {
	tracer     *closePanicTracer
	finalFlush bool
}

func (span closePanicSpan) End() {
	if span.finalFlush && span.tracer.panicOnce.CompareAndSwap(true, false) {
		panic(span.tracer.cause)
	}
}
func (closePanicSpan) TraceID() pkgobserver.TraceID           { return "" }
func (closePanicSpan) AddEvent(string, ...pkgobserver.Attr)   {}
func (closePanicSpan) SetAttributes(...pkgobserver.Attr)      {}
func (closePanicSpan) RecordError(error, ...pkgobserver.Attr) {}
