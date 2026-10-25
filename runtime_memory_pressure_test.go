package heterolog_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jappe/heterolog"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/loki"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

type runtimeMemoryMonitor struct {
	critical bool
	checks   atomic.Int32
	starts   atomic.Int32
	resets   atomic.Int32
}

func (*runtimeMemoryMonitor) Current(context.Context) (memorypressure.MemoryPressure, error) {
	return memorypressure.MemoryPressure{}, nil
}

func (m *runtimeMemoryMonitor) IsCritical(context.Context) (bool, error) {
	m.checks.Add(1)
	return m.critical, nil
}

func (*runtimeMemoryMonitor) Last() memorypressure.MonitorSnapshot {
	return memorypressure.MonitorSnapshot{}
}

func (m *runtimeMemoryMonitor) Reset() { m.resets.Add(1) }

func (m *runtimeMemoryMonitor) Start(context.Context) error {
	m.starts.Add(1)
	return nil
}

type runtimeMemoryFailureTransport struct{ calls atomic.Int32 }

func (tr *runtimeMemoryFailureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	tr.calls.Add(1)
	return &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("temporary failure")),
		Request:    req,
	}, nil
}

func TestPublicMemoryPressureMonitorControlsFailureOverload(t *testing.T) {
	// Sync and async construct their backend dependencies separately. Delivery
	// logging does not own the failure spool or its overload policy.
	for _, kind := range []string{"sync", "async"} {
		for _, pressure := range []struct {
			name     string
			critical bool
		}{
			{name: "healthy"},
			{name: "critical", critical: true},
		} {
			t.Run(kind+"/"+pressure.name, func(t *testing.T) {
				ctx := context.Background()
				root := t.TempDir()
				transport := &runtimeMemoryFailureTransport{}
				monitor := &runtimeMemoryMonitor{critical: pressure.critical}
				meter := &runtimeObserverMeter{}
				cfg := runtimeMemoryPressureConfig(root, transport)
				spool := heterolog.DefaultSpoolConfig()
				spool.RunnerConfig.LokiConfig.Runner.Interval = time.Hour
				opts := []heterolog.Option{
					heterolog.WithSpool(spool),
					heterolog.WithMemoryPressureMonitor(monitor),
					heterolog.WithObservers(nil, meter),
				}

				const message = "memory pressure policy entry"
				if kind == "sync" {
					logger, err := heterolog.New(ctx, cfg, opts...)
					if err != nil {
						t.Fatal(err)
					}
					defer logger.Close()
					// The entry stays buffered; its send and failure handling run at Close.
					if err := logger.Info(ctx, message); err != nil {
						t.Fatal(err)
					}
					requireMemoryOverloadCloseResult(t, logger.Close(), pressure.critical)
				} else {
					asyncCfg := heterolog.DefaultAsyncConfig()
					asyncCfg.Stdout = nil
					asyncCfg.LogOutputDestinations = cfg.LogOutputDestinations
					asyncCfg.Loki, asyncCfg.LokiClient = cfg.Loki, cfg.LokiClient
					logger, err := heterolog.NewAsync(ctx, asyncCfg, opts...)
					if err != nil {
						t.Fatal(err)
					}
					defer logger.Close()
					logger.Info(ctx, message)
					requireMemoryOverloadCloseResult(t, logger.Close(), pressure.critical)
				}

				if transport.calls.Load() == 0 {
					t.Fatal("backend send was not attempted")
				}
				if monitor.checks.Load() == 0 {
					t.Error("configured memory monitor was not consulted")
				}
				// WithMemoryPressureMonitor borrows an already-started monitor.
				// Neither construction nor shutdown may take over its lifetime.
				if starts, resets := monitor.starts.Load(), monitor.resets.Load(); starts != 0 || resets != 0 {
					t.Errorf("caller-owned monitor was started %d times and reset %d times", starts, resets)
				}
				meter.mu.Lock()
				drops := meter.totals["logger_failure_overload_drop_total"]
				meter.mu.Unlock()
				wantDrops := int64(0)
				if pressure.critical {
					wantDrops = 1
				}
				if drops != wantDrops {
					t.Errorf("overload drop notification = %d, want %d", drops, wantDrops)
				}

				// Inspect durable payloads after Close has drained the accepted
				// write. A metric alone cannot prove whether the record survived.
				for _, state := range []pkgspool.State{
					pkgspool.StateActive, pkgspool.StateProcessing, pkgspool.StateDone,
					pkgspool.StateDead, pkgspool.StateQuarantine,
				} {
					records := readRuntimeMemorySpoolRecords(t, filepath.Join(root, "memory-test", "spool", "loki", string(state)))
					wantCount := 0
					if !pressure.critical && state == pkgspool.StateActive {
						wantCount = 1
					}
					if len(records) != wantCount {
						t.Errorf("%s spool records = %d, want %d", state, len(records), wantCount)
					}
					for _, record := range records {
						var payload struct {
							Message string `json:"message"`
						}
						if err := json.Unmarshal(record.Payload, &payload); err != nil {
							t.Fatal(err)
						}
						if payload.Message != message {
							t.Errorf("saved message = %q, want %q", payload.Message, message)
						}
					}
				}
			})
		}
	}
}

// requireMemoryOverloadCloseResult checks the Close that flushes the buffered
// entry. When the overload policy drops the failed entry, its only copy is lost,
// so Close reports ErrLogDropped together with the backend's delivery cause.
func requireMemoryOverloadCloseResult(t *testing.T, err error, dropped bool) {
	t.Helper()
	if !dropped {
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	if !errors.Is(err, heterolog.ErrLogDropped) {
		t.Fatalf("Close() error = %v, want ErrLogDropped for the entry dropped by the overload policy", err)
	}
	var status *loki.HTTPStatusError
	if !errors.As(err, &status) || status.Code != http.StatusServiceUnavailable {
		t.Fatalf("Close() error = %v, want the Loki 503 cause alongside ErrLogDropped", err)
	}
}

func runtimeMemoryPressureConfig(root string, transport http.RoundTripper) heterolog.Config {
	cfg := heterolog.DefaultConfig()
	cfg.Stdout = nil
	cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputLoki}
	cfg.LokiClient = &loki.Config{
		Endpoint: "http://loki.invalid",
		HTTPClient: &http.Client{
			Transport: transport,
			Timeout:   time.Second,
		},
	}
	cfg.Loki = &pkglogger.LokiConfig{
		LokiLoggerBaseConfig: pkglogger.LokiLoggerBaseConfig{
			RuntimeConfig: pkglogger.RuntimeConfig{AppName: "memory-test", LogLevel: heterolog.LevelInfo},
			LogBodyConfig: pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano},
			// Keep the entry buffered until Close so the result is settled before
			// inspecting the spool, independently of timer scheduling.
			FlushInterval: time.Hour, MaxEntries: 100, BufferFlushBytes: 1 << 20,
			FallbackQueueMaxEntries: 100, MaxLineBytes: 1 << 16, MaxPushRequestBytes: 1 << 20,
		},
		LokiFlushManagerConfig: pkglogger.LokiFlushManagerConfig{
			Resource: pkglogger.Resource{ServiceName: "memory-test"},
			FailureStoreConfig: durable.FailureStoreConfig{
				MaxConcurrent: 1,
				DurableType:   []durable.DurableType{durable.DurableTypeSpool},
				DurableStoreConfig: durable.DurableStoreConfig{
					SpoolConfig: durable.SpoolConfig{
						SpoolPath: root, Target: "memory-test", Dir: "loki",
						OverloadPolicyConfig: durable.SpoolOverloadPolicyConfig{
							UseMemoryPressureForOverload: true,
							OverloadPolicy:               durable.OverloadPolicyDrop,
						},
					},
				},
			},
		},
	}
	return cfg
}

func readRuntimeMemorySpoolRecords(t *testing.T, root string) []pkgspool.SpoolRecord {
	t.Helper()
	var records []pkgspool.SpoolRecord
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			if path == root && errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".hlogspool" {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		decoder := json.NewDecoder(file)
		for {
			var record pkgspool.SpoolRecord
			if err := decoder.Decode(&record); err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
			records = append(records, record)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return records
}

// spoolHoldsRecordForTest reports whether root holds a spool file in any state.
// The loggers in these tests save accepted logs only after sending them, and the
// spool runner sends only saved records, so a request that arrives while root
// holds a record is the runner's replay. The runner's first pass does not wait
// for Interval and can replay records saved before Close. It reports walk
// errors with t.Errorf, so HTTP handlers and transports can call it.
func spoolHoldsRecordForTest(t testing.TB, root string) bool {
	found := false
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			// The spool may not exist yet or may change its directories during the walk.
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if !entry.IsDir() && filepath.Ext(path) == ".hlogspool" {
			found = true
			return filepath.SkipAll
		}
		return nil
	})
	if err != nil {
		t.Errorf("inspect spool below %q: %v", root, err)
	}
	return found
}
