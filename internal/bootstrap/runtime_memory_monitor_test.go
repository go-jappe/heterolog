package bootstrap_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	memorymonitor "github.com/go-jappe/heterolog/internal/observer/memory/pressure"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// runtimeMemoryProviderは、pkg/memorypressureのproviderと同じく固定の読み取り値を
// 返し、監視が読んだ回数を数えます。
type runtimeMemoryProvider struct {
	pressure memorypressure.MemoryPressure
	reads    atomic.Int32
}

func (p *runtimeMemoryProvider) ReadMemoryPressure(context.Context) (memorypressure.MemoryPressure, error) {
	p.reads.Add(1)
	return p.pressure, nil
}

func TestNewMemoryPressureMonitorRejectsInvalidInput(t *testing.T) {
	provider := &runtimeMemoryProvider{}
	valid := memorypressure.CriticalPolicy{CriticalRatio: 0.9}
	for _, tc := range []struct {
		name     string
		provider memorypressure.Provider
		policy   memorypressure.CriticalPolicy
		interval time.Duration
	}{
		{"nil provider", nil, valid, time.Second},
		{"zero poll interval", provider, valid, 0},
		{"no threshold", provider, memorypressure.CriticalPolicy{ConsecutiveCriticalHits: 1}, time.Second},
		// 比率の代わりに百分率を指定すると、決して達しません。
		{"ratio above one", provider, memorypressure.CriticalPolicy{CriticalRatio: 90}, time.Second},
		{"negative consecutive hits", provider, memorypressure.CriticalPolicy{CriticalRatio: 0.9, ConsecutiveCriticalHits: -1}, time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			monitor, err := memorymonitor.New(tc.provider, tc.policy, tc.interval)
			if !errors.Is(err, memorymonitor.ErrInvalidMonitorConfig) || monitor != nil {
				t.Fatalf("NewMemoryPressureMonitor() = (%v, %v), want ErrInvalidMemoryPressureMonitor", monitor, err)
			}
		})
	}
}

func TestNewMemoryPressureMonitorDrivesFailureOverload(t *testing.T) {
	for _, tc := range []struct {
		name     string
		reading  memorypressure.MemoryPressure
		critical bool
	}{
		{name: "healthy", reading: memorypressure.MemoryPressure{
			Ratio: 0.5, LimitBytes: 1 << 30, UsedBytes: 512 << 20, AvailableBytes: 512 << 20, Reliable: true,
		}},
		{name: "critical", critical: true, reading: memorypressure.MemoryPressure{
			Ratio: 0.95, LimitBytes: 1 << 30, UsedBytes: 973 << 20, AvailableBytes: 51 << 20, Reliable: true,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			provider := &runtimeMemoryProvider{pressure: tc.reading}
			// 比率だけの方針は、pkg/memorypressureに従います。比率以上の信頼できる最初の
			// 読み取り値で危険になります。
			monitor, err := memorymonitor.New(provider, memorypressure.CriticalPolicy{CriticalRatio: 0.9}, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			monitorCtx, stopMonitor := context.WithCancel(ctx)
			defer stopMonitor()
			if err := monitor.Start(monitorCtx); err != nil {
				t.Fatal(err)
			}
			if critical, err := monitor.IsCritical(ctx); err != nil || critical != tc.critical {
				t.Fatalf("IsCritical() = (%t, %v), want %t", critical, err, tc.critical)
			}

			root := t.TempDir()
			transport := &runtimeMemoryFailureTransport{}
			cfg := runtimeMemoryPressureConfig(root, transport)
			// degradeの方針は、過負荷の失敗をspoolではなくstdoutに残すため、logを失わずに
			// 過負荷の判断を確かめられます。
			overload := &cfg.Loki.FailureStoreConfig.DurableStoreConfig.SpoolConfig.OverloadPolicyConfig
			overload.OverloadPolicy = durable.OverloadPolicyDegrade
			overload.SpillDestination = durable.SpillDestinationStdout
			spool := bootstrap.DefaultSpoolConfig()
			spool.RunnerConfig.LokiConfig.Runner.Interval = time.Hour
			const message = "constructed memory monitor entry"
			output := testutil.CaptureStdout(t, func() {
				logger, err := bootstrap.New(ctx, cfg, loggerruntime.WithSpool(spool), loggerruntime.WithMemoryPressureMonitor(monitor))
				if err != nil {
					t.Fatal(err)
				}
				defer logger.Close()
				if err := logger.Info(ctx, message); err != nil {
					t.Fatal(err)
				}
				if err := logger.Close(); err != nil {
					t.Fatal(err)
				}
			})

			if transport.calls.Load() == 0 {
				t.Fatal("backend send was not attempted")
			}
			active := readRuntimeMemorySpoolRecords(t, filepath.Join(root, "memory-test", "spool", "loki", string(pkgspool.StateActive)))
			spilled := strings.Contains(output, "stdout_degrade") && strings.Contains(output, message)
			if tc.critical {
				if len(active) != 0 || !spilled {
					t.Errorf("critical reading: active spool records = %d, stdout spill = %t; want the entry degraded to stdout", len(active), spilled)
				}
			} else if len(active) != 1 || spilled {
				t.Errorf("healthy reading: active spool records = %d, stdout spill = %t; want the entry in the spool", len(active), spilled)
			}
			// loggerは監視を借りるだけです。改めてpollingせず、呼び出し元が始めた状態も
			// resetしません。
			if reads := provider.reads.Load(); reads != 1 {
				t.Errorf("provider reads = %d, want only the read made by Start", reads)
			}
			if snapshot := monitor.Last(); snapshot.LastReadAt.IsZero() || snapshot.LastCritical != tc.critical {
				t.Errorf("logger changed the caller-owned monitor state: %+v", snapshot)
			}
		})
	}
}

func TestNewMemoryPressureMonitorPollsUntilStartContextEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		provider := &runtimeMemoryProvider{pressure: memorypressure.MemoryPressure{Ratio: 0.95, Reliable: true}}
		monitor, err := memorymonitor.New(provider,
			memorypressure.CriticalPolicy{CriticalRatio: 0.9, ConsecutiveCriticalHits: 2}, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		ctx, stop := context.WithCancel(context.Background())
		defer stop()
		if err := monitor.Start(ctx); err != nil {
			t.Fatal(err)
		}
		// Startは一度読みます。危険と報告するには、候補が2回続く必要があります。
		if critical, err := monitor.IsCritical(ctx); err != nil || critical {
			t.Fatalf("IsCritical() after the first candidate = (%t, %v), want false", critical, err)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if critical, err := monitor.IsCritical(ctx); err != nil || !critical {
			t.Fatalf("IsCritical() after the second candidate = (%t, %v), want true", critical, err)
		}
		// 呼び出し元は、Startのcontextを終わらせて監視を止めます。
		stop()
		synctest.Wait()
		reads := provider.reads.Load()
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if got := provider.reads.Load(); got != reads {
			t.Fatalf("monitor kept polling after its context ended: reads %d -> %d", reads, got)
		}
	})
}
