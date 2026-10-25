package fsarchivehandler

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// 空き容量は構築時に解決したarchive rootで確認し、構築時に検証した閾値と比較します。
// 閾値ちょうどの空き容量は保存可能で、閾値を下回った場合だけ過負荷です。
func TestArchiveDiskSpaceThresholdUsesResolvedArchiveRoot(t *testing.T) {
	now := time.Date(2026, 9, 27, 11, 30, 0, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.lifecycleMode = pkgfsstore.FileLifecycleModeDisable
	})
	minimum := pipeline.handler.Config.OverloadPolicyConfig().MinAvailableDiskSpace()
	var probed []string
	free := minimum
	pipeline.handler.availableDiskSpace = func(directory string) (uint64, error) {
		probed = append(probed, directory)
		return free, nil
	}

	overloaded, err := pipeline.handler.isStorageOverloaded(context.Background())
	if err != nil || overloaded {
		t.Fatalf("isStorageOverloaded() at threshold = (%v, %v), want (false, nil)", overloaded, err)
	}
	free = minimum - 1
	overloaded, err = pipeline.handler.isStorageOverloaded(context.Background())
	if err != nil || !overloaded {
		t.Fatalf("isStorageOverloaded() below threshold = (%v, %v), want (true, nil)", overloaded, err)
	}
	if len(probed) != 2 || probed[0] != pipeline.handler.archiveRoot || probed[1] != pipeline.handler.archiveRoot {
		t.Fatalf("probed directories = %v, want resolved archive root %q twice", probed, pipeline.handler.archiveRoot)
	}
}

// 空き容量の取得を注入しないhandlerはOSの空き容量を確認します。取得できない場合は
// 保存可能とも容量不足とも判断せず、取得エラーを返します。
func TestArchiveDefaultDiskSpaceProbeReportsUnavailableDirectory(t *testing.T) {
	cfg := newValidArchiveHandlerConfig(t)
	handler := FsArchiveHandler{
		Config:      cfg,
		archiveRoot: filepath.Join(t.TempDir(), "missing"),
	}
	overloaded, err := handler.isStorageOverloaded(context.Background())
	if !errors.Is(err, os.ErrNotExist) || overloaded {
		t.Fatalf("isStorageOverloaded() = (%v, %v), want (false, missing directory error)", overloaded, err)
	}
}

func TestArchiveDiskSpaceProbeFailureRejectsSaveAndRecovers(t *testing.T) {
	for _, withReport := range []bool{false, true} {
		name := "HandleFailure"
		if withReport {
			name = "HandleFailureWithReport"
		}
		t.Run(name, func(t *testing.T) {
			now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
			var pipeline *FailureArchivePipeline
			var notified []error
			var reentryErr error
			pipeline = newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
				cfg.lifecycleMode = pkgfsstore.FileLifecycleModeDisable
			}, InternalLogHandlers{OnError: func(
				_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter,
			) {
				notified = append(notified, err)
				if pipeline == nil || !pipeline.handler.Fsstore.Mu.TryLock() {
					reentryErr = errors.New("OnError called before construction or while file lock is held")
					return
				}
				pipeline.handler.Fsstore.Mu.Unlock()
				// 容量確認に失敗しても、通知から同じwriterの状態照会へ再入できます。
				_, reentryErr = pipeline.ActiveArchivePath(failurearchive.FailureCategoryRetryable)
			}})
			probeErr := fmt.Errorf("probe failure archive directory: %w", os.ErrNotExist)
			probeFails := true
			pipeline.handler.availableDiskSpace = func(string) (uint64, error) {
				if probeFails {
					return 0, probeErr
				}
				return testArchiveAvailableDiskSpace, nil
			}
			evacuationCalls := 0
			pipeline.handler.OverloadHandler = reportOverloadHandlerFunc(func(context.Context, loggerfailure.SendResult) error {
				evacuationCalls++
				return nil
			})
			result := retryableArchiveResult(now, "disk-probe-failure")
			call := func() (pkglogger.DeliveryReport, error) {
				if withReport {
					return pipeline.HandleFailureWithReport(context.Background(), result)
				}
				return pkglogger.DeliveryReport{}, pipeline.HandleFailure(context.Background(), result)
			}

			report, err := call()
			if !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("save error = %v, want missing probe directory error", err)
			}
			if len(notified) != 1 || !errors.Is(notified[0], os.ErrNotExist) {
				t.Fatalf("OnError = %v, want exactly one disk probe failure", notified)
			}
			if reentryErr != nil {
				t.Fatalf("OnError reentry: %v", reentryErr)
			}
			if evacuationCalls != 0 {
				t.Fatalf("overload handler calls = %d, want 0 for unknown disk capacity", evacuationCalls)
			}
			if records := readArchiveRecords(t, pipeline.handler.Fsstore.ActiveFilePathRetryable); len(records) != 0 {
				t.Fatalf("archive records = %d, want 0 after failed capacity check", len(records))
			}
			if withReport {
				if len(report.Durables) != 1 || report.Durables[0].Status != pkglogger.DurableFailed ||
					!errors.Is(report.Durables[0].Err, os.ErrNotExist) || len(report.Evacuations) != 0 {
					t.Fatalf("report = %+v, want failed archive and no evacuation", report)
				}
			}

			// 前回の判定失敗は保持しません。次の保存で確認できれば通常どおり書き込みます。
			probeFails = false
			report, err = call()
			if err != nil {
				t.Fatalf("save after probe recovery: %v", err)
			}
			if len(notified) != 1 || evacuationCalls != 0 {
				t.Fatalf("after recovery notifications=%d evacuations=%d, want 1 and 0", len(notified), evacuationCalls)
			}
			if records := readArchiveRecords(t, pipeline.handler.Fsstore.ActiveFilePathRetryable); len(records) != 1 {
				t.Fatalf("archive records = %d, want only the recovered save", len(records))
			}
			if withReport && (len(report.Durables) != 1 || report.Durables[0].Status != pkglogger.DurableSucceeded) {
				t.Fatalf("recovered report = %+v, want archive success", report)
			}
		})
	}
}
