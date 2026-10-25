package heterolog_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/loki"
)

type runtimeRecordingTransport struct {
	mu     sync.Mutex
	bodies []string
}

func (r *runtimeRecordingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.bodies = append(r.bodies, string(body))
	r.mu.Unlock()
	return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
}

func TestPublicShutdownFlushesBufferedLoki(t *testing.T) {
	transport := &runtimeRecordingTransport{}
	cfg := heterolog.DefaultConfig()
	cfg.Stdout = nil
	cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputLoki}
	cfg.LokiClient = &loki.Config{
		Endpoint:   "http://loki.example.invalid",
		HTTPClient: &http.Client{Transport: transport, Timeout: time.Second},
	}
	cfg.Loki = &pkglogger.LokiConfig{
		LokiLoggerBaseConfig: pkglogger.LokiLoggerBaseConfig{
			RuntimeConfig:           pkglogger.RuntimeConfig{AppName: "library-test", LogLevel: heterolog.LevelInfo},
			LogBodyConfig:           pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano},
			FlushInterval:           time.Hour,
			MaxEntries:              100,
			BufferFlushBytes:        1 << 20,
			FallbackQueueMaxEntries: 100,
			MaxLineBytes:            1 << 16,
			MaxPushRequestBytes:     1 << 20,
		},
		LokiFlushManagerConfig: pkglogger.LokiFlushManagerConfig{
			FailureStoreConfig: durable.FailureStoreConfig{DurableType: []durable.DurableType{durable.DurableTypeNone}},
			Resource:           pkglogger.Resource{ServiceName: "library-test"},
		},
	}
	logger, err := heterolog.New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logger.Close() })
	if err := logger.Info(context.Background(), "buffered before shutdown"); err != nil {
		t.Fatal(err)
	}
	transport.mu.Lock()
	beforeClose := len(transport.bodies)
	transport.mu.Unlock()
	if beforeClose != 0 {
		t.Fatalf("sent %d batches before shutdown", beforeClose)
	}
	if err := logger.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if len(transport.bodies) != 1 || !strings.Contains(transport.bodies[0], "buffered before shutdown") {
		t.Fatalf("flushed batches = %v", transport.bodies)
	}
}
