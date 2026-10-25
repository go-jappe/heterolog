package heterolog_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// runtimeMemoryProvider returns a fixed reading, as a pkg/memorypressure
// provider would, and counts how often the monitor reads it.
type runtimeMemoryProvider struct {
	pressure memorypressure.MemoryPressure
	reads    atomic.Int32
}

func (p *runtimeMemoryProvider) ReadMemoryPressure(context.Context) (memorypressure.MemoryPressure, error) {
	p.reads.Add(1)
	return p.pressure, nil
}

func TestPublicNewMemoryPressureMonitorRejectsInvalidInput(t *testing.T) {
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
		// A percentage instead of a ratio could never be reached.
		{"ratio above one", provider, memorypressure.CriticalPolicy{CriticalRatio: 90}, time.Second},
		{"negative consecutive hits", provider, memorypressure.CriticalPolicy{CriticalRatio: 0.9, ConsecutiveCriticalHits: -1}, time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			monitor, err := heterolog.NewMemoryPressureMonitor(tc.provider, tc.policy, tc.interval)
			if !errors.Is(err, heterolog.ErrInvalidMemoryPressureMonitor) || monitor != nil {
				t.Fatalf("NewMemoryPressureMonitor() = (%v, %v), want ErrInvalidMemoryPressureMonitor", monitor, err)
			}
		})
	}
}

func TestPublicNewMemoryPressureMonitorDrivesFailureOverload(t *testing.T) {
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
			// A ratio-only policy follows pkg/memorypressure: the first reliable
			// reading at or above the ratio is critical.
			monitor, err := heterolog.NewMemoryPressureMonitor(provider, memorypressure.CriticalPolicy{CriticalRatio: 0.9}, time.Hour)
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
			// The degrade policy keeps an overloaded failure on stdout instead of the
			// spool, so the overload decision is visible without losing the entry.
			overload := &cfg.Loki.FailureStoreConfig.DurableStoreConfig.SpoolConfig.OverloadPolicyConfig
			overload.OverloadPolicy = durable.OverloadPolicyDegrade
			overload.SpillDestination = durable.SpillDestinationStdout
			spool := heterolog.DefaultSpoolConfig()
			spool.RunnerConfig.LokiConfig.Runner.Interval = time.Hour
			const message = "constructed memory monitor entry"
			output := captureRuntimeStdout(t, func() {
				logger, err := heterolog.New(ctx, cfg, heterolog.WithSpool(spool), heterolog.WithMemoryPressureMonitor(monitor))
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
			// The logger only borrows the monitor: it neither polls again nor
			// resets the state the caller started.
			if reads := provider.reads.Load(); reads != 1 {
				t.Errorf("provider reads = %d, want only the read made by Start", reads)
			}
			if snapshot := monitor.Last(); snapshot.LastReadAt.IsZero() || snapshot.LastCritical != tc.critical {
				t.Errorf("logger changed the caller-owned monitor state: %+v", snapshot)
			}
		})
	}
}

func TestPublicNewMemoryPressureMonitorPollsUntilStartContextEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		provider := &runtimeMemoryProvider{pressure: memorypressure.MemoryPressure{Ratio: 0.95, Reliable: true}}
		monitor, err := heterolog.NewMemoryPressureMonitor(provider,
			memorypressure.CriticalPolicy{CriticalRatio: 0.9, ConsecutiveCriticalHits: 2}, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		ctx, stop := context.WithCancel(context.Background())
		defer stop()
		if err := monitor.Start(ctx); err != nil {
			t.Fatal(err)
		}
		// Start reads once; two consecutive candidates are required.
		if critical, err := monitor.IsCritical(ctx); err != nil || critical {
			t.Fatalf("IsCritical() after the first candidate = (%t, %v), want false", critical, err)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if critical, err := monitor.IsCritical(ctx); err != nil || !critical {
			t.Fatalf("IsCritical() after the second candidate = (%t, %v), want true", critical, err)
		}
		// Ending the Start context is how the caller stops the monitor.
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
