package loggerruntime_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestSpoolCapacityRefreshWithNonpositiveInterval(t *testing.T) {
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
				spool := loggerruntime.DefaultSpoolConfig()
				spool.RunnerConfig.LokiConfig.Runner.Interval = time.Hour
				meter := observertest.NewMeter()
				logger, err := loggerruntime.New(ctx, cfg, loggerruntime.WithSpool(spool), loggerruntime.WithObservers(nil, meter))
				if err != nil {
					t.Fatal(err)
				}
				defer logger.Close()
				// spoolの再送処理の最初の巡回は、Intervalを待たずに始まります。下の失敗を保存する
				// 前に空のspoolを見つけさせ、それらを再送してHTTPの要求を増やさないようにします。
				// 次の巡回は、bubbleの時間で1時間後です。
				synctest.Wait()

				// 最初の失敗で、空だったspoolが満杯になります。次の失敗は、非同期の更新を始める間に
				// cacheした大きさを使うことがあるため、保存されるか捨てられるかのどちらかです。捨てた
				// logは唯一の写しが失われ、同期のAPIはErrLogDroppedで報告します。
				droppedReturns := 0
				for index, message := range []string{"initial failure", "failure during refresh"} {
					err := logger.Info(ctx, message)
					switch {
					case index > 0 && errors.Is(err, pkglogger.ErrLogDropped):
						droppedReturns++
					case err != nil:
						t.Fatal(err)
					}
					synctest.Wait()
				}
				// 上で始めた走査はすべて終わりました。以後の失敗は、満杯と分かったspoolと、
				// 設定したdropの方針に従う必要があります。
				if err := logger.Info(ctx, "failure after refresh"); !errors.Is(err, pkglogger.ErrLogDropped) {
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
				drops := meter.CountMatching("heterolog_failure_records_total", "action", "dropped")
				if wantDrops := int64(3 - len(records)); drops != wantDrops || transport.calls.Load() != 3 {
					t.Fatalf("overload drops = %d, HTTP requests = %d, want %d and 3", drops, transport.calls.Load(), wantDrops)
				}
				// 捨てたlogだけが、それぞれErrLogDroppedを返しました。
				if wantReturns := 3 - len(records); droppedReturns != wantReturns {
					t.Fatalf("ErrLogDropped returns = %d, want one per dropped entry (%d)", droppedReturns, wantReturns)
				}
			})
		})
	}
}
