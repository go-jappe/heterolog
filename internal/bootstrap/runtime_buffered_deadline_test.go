package bootstrap_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/pkg/clickhouse"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

type bufferedDeadlineTransport struct {
	t       testing.TB
	root    string
	mu      sync.Mutex
	bodies  []string
	replays int
}

func (tr *bufferedDeadlineTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	// spoolの再送処理は、最初の巡回で、Closeより前に保存済みのbatchを再送することが
	// あります。その再送にはすぐ一時的な失敗を返して記録を有効なまま残し、loggerの
	// 送信には数えません。
	if spoolHoldsRecordForTest(tr.t, tr.root) {
		tr.mu.Lock()
		tr.replays++
		tr.mu.Unlock()
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Header:     make(http.Header),
			Body:       http.NoBody,
			Request:    req,
		}, nil
	}
	tr.mu.Lock()
	tr.bodies = append(tr.bodies, string(body))
	tr.mu.Unlock()
	<-req.Context().Done()
	return nil, req.Context().Err()
}

func TestBufferedDeadlinePreservesAcceptedLogs(t *testing.T) {
	for _, backend := range []string{"loki", "clickhouse"} {
		for _, loggerDeadline := range []bool{false, true} {
			label := "HTTP_client_timeout_control"
			if loggerDeadline {
				label = "logger_timeout"
			}
			t.Run(backend+"/"+label, func(t *testing.T) {
				root := t.TempDir()
				transport := &bufferedDeadlineTransport{t: t, root: root}
				cfg := runtimeMemoryPressureConfig(root, transport)
				cfg.Loki.MaxEntries = 2
				cfg.Loki.FailureStoreConfig.DurableStoreConfig.SpoolConfig.OverloadPolicyConfig.UseMemoryPressureForOverload = false
				cfg.Timeout = 2 * time.Second
				cfg.LokiClient.HTTPClient.Timeout = 5 * time.Millisecond
				if loggerDeadline {
					cfg.Timeout = 100 * time.Millisecond
					cfg.LokiClient.HTTPClient.Timeout = 2 * time.Second
				}
				if backend == "clickhouse" {
					base := cfg.Loki.LokiLoggerBaseConfig
					store := cfg.Loki.FailureStoreConfig
					store.DurableStoreConfig.SpoolConfig.Dir = "clickhouse"
					cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputClickHouse}
					cfg.ClickHouseClient = &clickhouse.Config{Endpoint: "http://clickhouse.invalid", Table: "logs", HTTPClient: cfg.LokiClient.HTTPClient}
					cfg.ClickHouse = &pkglogger.ClickHouseConfig{
						ClickHouseLoggerBaseConfig:   pkglogger.ClickHouseLoggerBaseConfig{RuntimeConfig: base.RuntimeConfig, LogBodyConfig: base.LogBodyConfig, FlushInterval: time.Hour, MaxEntries: 2, BufferFlushBytes: 1 << 20, FallbackQueueMaxEntries: 100, MaxInsertRequestBytes: 1 << 20},
						ClickHouseFlushManagerConfig: pkglogger.ClickHouseFlushManagerConfig{FailureStoreConfig: store, Resource: cfg.Loki.Resource},
					}
					cfg.Loki = nil
					cfg.LokiClient = nil
				}
				spool := bootstrap.DefaultSpoolConfig()
				spool.RunnerConfig.LokiConfig.Runner.Interval = time.Hour
				spool.RunnerConfig.ClickHouseConfig.Runner.Interval = time.Hour
				logger, err := bootstrap.New(context.Background(), cfg, loggerruntime.WithSpool(spool))
				if err != nil {
					t.Fatal(err)
				}
				defer logger.Close()
				if err := logger.Info(context.Background(), "previously accepted log"); err != nil {
					t.Fatal(err)
				}
				flushErr := logger.Info(context.Background(), "log that triggers flushing")
				closeErr := logger.Close()
				records := readRuntimeMemorySpoolRecords(t, root)
				transport.mu.Lock()
				bodies := append([]string(nil), transport.bodies...)
				replays := transport.replays
				transport.mu.Unlock()
				t.Logf("trigger err=%v close err=%v sends=%d replays=%d durable records=%d", flushErr, closeErr, len(bodies), replays, len(records))
				if len(bodies) != 1 || !strings.Contains(bodies[0], "previously accepted log") || !strings.Contains(bodies[0], "log that triggers flushing") {
					t.Fatalf("actual batch = %v", bodies)
				}
				if loggerDeadline && !errors.Is(flushErr, context.DeadlineExceeded) {
					t.Fatalf("flush error=%v", flushErr)
				}
				if !loggerDeadline && flushErr != nil {
					t.Fatalf("HTTP timeout should be durably handled: %v", flushErr)
				}
				if closeErr != nil {
					t.Fatalf("Close: %v", closeErr)
				}
				if len(records) != 2 {
					t.Fatalf("durable records=%d, want both accepted messages preserved", len(records))
				}
				wantState := pkgspool.StateActive
				if loggerDeadline {
					wantState = pkgspool.StateQuarantine
				}
				stateRoot := filepath.Join(root, "memory-test", "spool", backend, string(wantState))
				stateRecords := readRuntimeMemorySpoolRecords(t, stateRoot)
				if len(stateRecords) != 2 {
					t.Fatalf("records in %s=%d, want 2", wantState, len(stateRecords))
				}
				messages := map[string]int{}
				for _, record := range stateRecords {
					var payload struct {
						Message string `json:"message"`
					}
					if err := json.Unmarshal(record.Payload, &payload); err != nil {
						t.Fatal(err)
					}
					messages[payload.Message]++
					if loggerDeadline && record.InitialFailureCode != "outcome_unknown" {
						t.Errorf("quarantine reason=%q", record.InitialFailureCode)
					}
				}
				if messages["previously accepted log"] != 1 || messages["log that triggers flushing"] != 1 {
					t.Fatalf("saved messages=%v", messages)
				}
			})
		}
	}
}
