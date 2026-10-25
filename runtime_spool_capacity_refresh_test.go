package heterolog_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog"
)

func TestPublicSpoolCapacityRefreshWithNonpositiveInterval(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second} {
		t.Run(interval.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				root := t.TempDir()
				transport := &runtimeMemoryFailureTransport{}
				cfg := runtimeMemoryPressureConfig(root, transport)
				cfg.Loki.NoBulkFlushModeEnabled = true
				overload := &cfg.Loki.FailureStoreConfig.DurableStoreConfig.SpoolConfig.OverloadPolicyConfig
				overload.UseMemoryPressureForOverload = false
				overload.MaxDirSize = 1
				overload.MaxCallsSinceScan = 0
				overload.StatsScanInterval = interval
				spool := heterolog.DefaultSpoolConfig()
				spool.RunnerConfig.LokiConfig.Runner.Interval = time.Hour
				meter := &runtimeObserverMeter{}
				logger, err := heterolog.New(ctx, cfg, heterolog.WithSpool(spool), heterolog.WithObservers(nil, meter))
				if err != nil {
					t.Fatal(err)
				}
				defer logger.Close()
				// The spool runner's first pass starts without waiting for Interval.
				// Let it find the empty spool before the failures below are saved, so
				// it cannot replay them and add HTTP requests; its next pass is an
				// hour of bubble time away.
				synctest.Wait()

				// The first failure fills the initially empty spool. The next may
				// use its cached size while initiating an asynchronous refresh, so
				// it is either stored or dropped. A dropped entry's only copy is
				// lost, and the synchronous API reports it with ErrLogDropped.
				droppedReturns := 0
				for index, message := range []string{"initial failure", "failure during refresh"} {
					err := logger.Info(ctx, message)
					switch {
					case index > 0 && errors.Is(err, heterolog.ErrLogDropped):
						droppedReturns++
					case err != nil:
						t.Fatal(err)
					}
					synctest.Wait()
				}
				// Every scan started above has completed. A subsequent failure
				// must now honor the known full spool and the configured drop policy.
				if err := logger.Info(ctx, "failure after refresh"); !errors.Is(err, heterolog.ErrLogDropped) {
					t.Fatalf("Info after refresh = %v, want ErrLogDropped from the full spool", err)
				}
				droppedReturns++
				if err := logger.Close(); err != nil {
					t.Fatal(err)
				}
				records := readRuntimeMemorySpoolRecords(t, root)
				if len(records) < 1 || len(records) > 2 {
					t.Fatalf("spool records = %d, want only failures accepted before capacity refresh", len(records))
				}
				messages := make(map[string]int)
				for _, record := range records {
					var payload struct {
						Message string `json:"message"`
					}
					if err := json.Unmarshal(record.Payload, &payload); err != nil {
						t.Fatal(err)
					}
					messages[payload.Message]++
				}
				if messages["initial failure"] != 1 || messages["failure during refresh"] != len(records)-1 {
					t.Fatalf("spooled messages = %v, want accepted failures unchanged and no post-refresh failure", messages)
				}
				meter.mu.Lock()
				drops := meter.totals["logger_failure_overload_drop_total"]
				meter.mu.Unlock()
				if wantDrops := int64(3 - len(records)); drops != wantDrops || transport.calls.Load() != 3 {
					t.Fatalf("overload drops = %d, HTTP requests = %d, want %d and 3", drops, transport.calls.Load(), wantDrops)
				}
				// Each dropped entry, and only a dropped entry, returned ErrLogDropped.
				if wantReturns := 3 - len(records); droppedReturns != wantReturns {
					t.Fatalf("ErrLogDropped returns = %d, want one per dropped entry (%d)", droppedReturns, wantReturns)
				}
			})
		})
	}
}
