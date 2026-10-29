package bootstrap_test

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

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
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

func TestMemoryPressureMonitorControlsFailureOverload(t *testing.T) {
	// 同期と非同期は、backendの依存を別々に作ります。Delivery loggerは、失敗のspoolも、
	// その過負荷の方針も持ちません。
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
				meter := observertest.NewMeter()
				cfg := runtimeMemoryPressureConfig(root, transport)
				spool := bootstrap.DefaultSpoolConfig()
				spool.RunnerConfig.LokiConfig.Runner.Interval = time.Hour
				opts := []loggerruntime.Option{
					loggerruntime.WithSpool(spool),
					loggerruntime.WithMemoryPressureMonitor(monitor),
					loggerruntime.WithObservers(nil, meter),
				}

				const message = "memory pressure policy entry"
				if kind == "sync" {
					logger, err := bootstrap.New(ctx, cfg, opts...)
					if err != nil {
						t.Fatal(err)
					}
					defer logger.Close()
					// logはbufferに残り、その送信と失敗の処理はCloseで行います。
					if err := logger.Info(ctx, message); err != nil {
						t.Fatal(err)
					}
					requireMemoryOverloadCloseResult(t, logger.Close(), pressure.critical)
				} else {
					asyncCfg := bootstrap.DefaultAsyncConfig()
					asyncCfg.Stdout = nil
					asyncCfg.LogOutputDestinations = cfg.LogOutputDestinations
					asyncCfg.Loki, asyncCfg.LokiClient = cfg.Loki, cfg.LokiClient
					logger, err := bootstrap.NewAsync(ctx, asyncCfg, opts...)
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
				// WithMemoryPressureMonitorは、起動済みの監視を借ります。作成時も終了時も、
				// その寿命を引き継いではいけません。
				if starts, resets := monitor.starts.Load(), monitor.resets.Load(); starts != 0 || resets != 0 {
					t.Errorf("caller-owned monitor was started %d times and reset %d times", starts, resets)
				}
				drops := meter.CountMatching("heterolog_failure_records_total", "action", "dropped")
				wantDrops := int64(0)
				if pressure.critical {
					wantDrops = 1
				}
				if drops != wantDrops {
					t.Errorf("overload drop notification = %d, want %d", drops, wantDrops)
				}

				// Closeが受け付けた書き込みを出し切った後に、durableなpayloadを確かめます。
				// metricだけでは、記録が残ったかどうかを示せません。
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

// requireMemoryOverloadCloseResultは、buffer内のlogを書き出すCloseの結果を
// 確かめます。過負荷の方針が失敗したlogを捨てると、唯一の写しが失われるため、Closeは
// ErrLogDroppedを、backendの送信の原因と一緒に報告します。
func requireMemoryOverloadCloseResult(t *testing.T, err error, dropped bool) {
	t.Helper()
	if !dropped {
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	if !errors.Is(err, pkglogger.ErrLogDropped) {
		t.Fatalf("Close() error = %v, want ErrLogDropped for the entry dropped by the overload policy", err)
	}
	var status *loki.HTTPStatusError
	if !errors.As(err, &status) || status.Code != http.StatusServiceUnavailable {
		t.Fatalf("Close() error = %v, want the Loki 503 cause alongside ErrLogDropped", err)
	}
}

func runtimeMemoryPressureConfig(root string, transport http.RoundTripper) pkglogger.LoggerConfig {
	cfg := bootstrap.DefaultConfig()
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
			RuntimeConfig: pkglogger.RuntimeConfig{AppName: "memory-test", LogLevel: pkglogger.LogLevelInfo},
			LogBodyConfig: pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano},
			// timerのschedulingに左右されず、spoolを確かめる前に結果を確定させるため、Close
			// までlogをbufferに残します。
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

// spoolHoldsRecordForTestは、rootがどれかの状態のspool fileを持っているかを返します。
// これらの試験のloggerは、受け付けたlogを送った後にだけ保存し、spoolの再送処理は保存
// した記録だけを送ります。そのため、rootが記録を持っている間に届いた要求は、再送処理
// による再送です。再送処理の最初の巡回はIntervalを待たず、Close前に保存した記録を
// 再送することがあります。走査のerrorはt.Errorfで報告するため、HTTPのhandlerや
// transportから呼べます。
func spoolHoldsRecordForTest(t testing.TB, root string) bool {
	found := false
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			// spoolはまだ存在しないことや、走査の途中でdirectoryが変わることがあります。
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
