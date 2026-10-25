package spoolstorehandler

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/overload"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/overload/provider"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
)

// recordingRetryAfterRecorderは、送信先全体の待機期限として渡された時刻を記録します。
type recordingRetryAfterRecorder struct {
	mu        sync.Mutex
	deadlines []time.Time
}

func (r *recordingRetryAfterRecorder) RecordRetryAfter(notBefore time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deadlines = append(r.deadlines, notBefore)
}

func (r *recordingRetryAfterRecorder) recorded() []time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Time(nil), r.deadlines...)
}

// requireRecordedRetryAfterDeadlineは、一回の送信失敗の期限がちょうど一回、wantとして記録されたことを確認します。
func requireRecordedRetryAfterDeadline(t *testing.T, recorder *recordingRetryAfterRecorder, want time.Time) {
	t.Helper()
	deadlines := recorder.recorded()
	if len(deadlines) != 1 || !deadlines[0].Equal(want) {
		t.Fatalf("recorded Retry-After deadlines = %v, want exactly [%s]", deadlines, want)
	}
}

// newRetryAfterRecordingHandlerは、通常の保存またはoverload時の処置を選んだhandlerへ、送信先全体の
// 待機期限を記録するrecorderを構築中に設定します。overloadedならmemory逼迫として過負荷にします。
// handlerSetsには構築時に渡す内部ログの通知先を一つまで指定できます。
func newRetryAfterRecordingHandler(
	t *testing.T,
	now time.Time,
	policy durable.OverloadPolicy,
	overloaded bool,
	degradeStdout *bytes.Buffer,
	handlerSets ...InternalLogHandlers,
) (*SpoolStoreHandler, *recordingRetryAfterRecorder) {
	t.Helper()
	if len(handlerSets) > 1 {
		t.Fatalf("internal log handler sets = %d, want at most one", len(handlerSets))
	}
	internalLogHandlers := InternalLogHandlers{}
	if len(handlerSets) == 1 {
		internalLogHandlers = handlerSets[0]
	}
	cfg := testOverloadPolicyConfig{
		overloadPolicy:               policy,
		spillDestination:             durable.SpillDestinationStdout,
		useMemoryPressureForOverload: true,
	}
	var overloadHandler overload.Handler = provider.NewOverloadDropHandler(observer.NopMeter{})
	if policy == durable.OverloadPolicyDegrade {
		if degradeStdout == nil {
			t.Fatal("degrade policy needs a writer that receives the evacuated failures")
		}
		var err error
		overloadHandler, err = provider.NewDegradeHandlerWithStdout(cfg, degradeStdout)
		if err != nil {
			t.Fatalf("NewDegradeHandlerWithStdout error = %v", err)
		}
	}
	handler := newTestHandlerWithOverloadHandler(
		t, now, cfg, &recordingMemoryMonitor{critical: overloaded}, overloadHandler, internalLogHandlers,
	)
	t.Cleanup(func() {
		if err := shutdownSpoolHandlerForTest(handler); err != nil {
			t.Errorf("Shutdown error = %v", err)
		}
	})
	recorder := &recordingRetryAfterRecorder{}
	handler.SetRetryAfterRecorder(recorder)
	return handler, recorder
}

// 通常送信が受け取ったRetry-Afterは、spoolへ保存する場合も、過負荷でdrop・degradeして保存しない場合も、
// 応答を受け取った失敗時刻から数えた期限を送信先全体の待機期限として一度だけ記録する。
// 同じ失敗の複数recordが期限を持つ場合は、記録済みの期限を短縮しないよう最も遅い期限を記録する。
func TestHandleFailureRecordsLatestRetryAfterDeadline(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		name       string
		policy     durable.OverloadPolicy
		overloaded bool
		exported   bool
	}{
		{name: "stored", policy: durable.OverloadPolicyDrop},
		{name: "stored through StoreSendResultByState", policy: durable.OverloadPolicyDrop, exported: true},
		{name: "dropped on overload", policy: durable.OverloadPolicyDrop, overloaded: true},
		{name: "degraded on overload", policy: durable.OverloadPolicyDegrade, overloaded: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var stdout bytes.Buffer
			handler, recorder := newRetryAfterRecordingHandler(t, now, tt.policy, tt.overloaded, &stdout)
			// 応答を受け取ってから保存するまでに時間が経っても、期限は失敗時刻から数えます。
			failedAt := now.Add(-10 * time.Second)
			shorter := failedRecord("shorter", "http_throttle", failedAt)
			shorter.RetryAfter = 30 * time.Second
			longer := failedRecord("longer", "http_throttle", failedAt)
			longer.RetryAfter = 45 * time.Second
			result := loggerfailure.SendResult{
				Meta:             loggerfailure.Meta{Backend: "loki", RequestID: "retry-after-recorder", AttemptedAt: failedAt, InputCount: 2},
				RetryableRecords: []loggerfailure.FailedRecord{shorter, longer},
			}

			var err error
			if tt.exported {
				err = handler.StoreSendResultByState(context.Background(), result)
			} else {
				err = handler.HandleFailure(context.Background(), result)
			}

			// 前提として、選んだ処置で失敗を扱ったことを確認します。
			records := readSpoolRecords(t, handler.Store.RootDir.ActiveDir().Path())
			switch {
			case !tt.overloaded:
				if err != nil {
					t.Fatalf("store error = %v, want nil", err)
				}
				assertRecordCount(t, records, 2, "active")
			case tt.policy == durable.OverloadPolicyDrop:
				if !errors.Is(err, pkglogger.ErrLogDropped) {
					t.Fatalf("HandleFailure error = %v, want ErrLogDropped", err)
				}
				assertRecordCount(t, records, 0, "active")
			default:
				if err != nil {
					t.Fatalf("HandleFailure error = %v, want nil after degrading to stdout", err)
				}
				assertRecordCount(t, records, 0, "active")
				if !bytes.Contains(stdout.Bytes(), []byte(`"requestID":"retry-after-recorder"`)) {
					t.Fatalf("degraded stdout = %q, want the evacuated send failure", stdout.String())
				}
			}
			requireRecordedRetryAfterDeadline(t, recorder, failedAt.Add(45*time.Second))
		})
	}
}

// recordに失敗時刻がない場合は、spoolへ保存するrecordと同じく送信の試行時刻から数え、
// どちらもない場合は保存時刻から数える。
func TestHandleFailureRetryAfterDeadlineFallsBackToAttemptTimeAndClock(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	attemptedAt := now.Add(-20 * time.Second)
	for _, tt := range []struct {
		name        string
		attemptedAt time.Time
		want        time.Time
	}{
		{name: "attempt time", attemptedAt: attemptedAt, want: attemptedAt.Add(30 * time.Second)},
		{name: "clock", want: now.Add(30 * time.Second)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, recorder := newRetryAfterRecordingHandler(t, now, durable.OverloadPolicyDrop, false, nil)
			record := failedRecord("without-failed-at", "http_throttle", time.Time{})
			record.RetryAfter = 30 * time.Second
			if err := handler.HandleFailure(context.Background(), loggerfailure.SendResult{
				Meta:             loggerfailure.Meta{Backend: "loki", RequestID: "retry-after-fallback", AttemptedAt: tt.attemptedAt},
				RetryableRecords: []loggerfailure.FailedRecord{record},
			}); err != nil {
				t.Fatalf("HandleFailure error = %v", err)
			}
			requireRecordedRetryAfterDeadline(t, recorder, tt.want)
		})
	}
}

// 上限を超えるRetry-Afterは、recordへ保存する期限と同じく保存時刻から上限までに短縮して記録する。
// 上限は同じspoolを再送するrunnerから受け取った値で、0なら既定の1時間を使う。
func TestHandleFailureCapsRecordedRetryAfterDeadline(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		name          string
		maxRetryAfter time.Duration
		overloaded    bool
		want          time.Time
	}{
		{name: "configured maximum", maxRetryAfter: 20 * time.Minute, want: now.Add(20 * time.Minute)},
		{name: "default maximum", want: now.Add(time.Hour)},
		{name: "dropped on overload", maxRetryAfter: 20 * time.Minute, overloaded: true, want: now.Add(20 * time.Minute)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, recorder := newRetryAfterRecordingHandler(t, now, durable.OverloadPolicyDrop, tt.overloaded, nil)
			handler.SetMaxRetryAfter(tt.maxRetryAfter)
			failedAt := now.Add(-time.Second)
			throttled := failedRecord("throttled", "http_throttle", failedAt)
			throttled.RetryAfter = 3 * time.Hour
			within := failedRecord("within", "http_throttle", failedAt)
			within.RetryAfter = 10 * time.Minute
			err := handler.HandleFailure(context.Background(), loggerfailure.SendResult{
				Meta:             loggerfailure.Meta{Backend: "loki", RequestID: "retry-after-cap", AttemptedAt: failedAt},
				RetryableRecords: []loggerfailure.FailedRecord{within, throttled},
			})
			if tt.overloaded {
				if !errors.Is(err, pkglogger.ErrLogDropped) {
					t.Fatalf("HandleFailure error = %v, want ErrLogDropped", err)
				}
			} else if err != nil {
				t.Fatalf("HandleFailure error = %v", err)
			}
			requireRecordedRetryAfterDeadline(t, recorder, tt.want)
		})
	}
}

// Retry-Afterを指定したretryable recordがない送信失敗では、送信先全体の待機期限を記録しない。
func TestHandleFailureDoesNotRecordRetryAfterDeadlineWithoutRetryAfter(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	handler, recorder := newRetryAfterRecordingHandler(t, now, durable.OverloadPolicyDrop, false, nil)
	if err := handler.HandleFailure(context.Background(), loggerfailure.SendResult{
		Meta:             loggerfailure.Meta{Backend: "loki", RequestID: "without-retry-after", AttemptedAt: now},
		RetryableRecords: []loggerfailure.FailedRecord{failedRecord("unrestricted", "http_status", now)},
	}); err != nil {
		t.Fatalf("HandleFailure error = %v", err)
	}
	if deadlines := recorder.recorded(); len(deadlines) != 0 {
		t.Fatalf("recorded Retry-After deadlines = %v, want none", deadlines)
	}
}

// 上限を超えるRetry-Afterは、過負荷でdrop・degradeしてspoolへ保存しない場合や、activeへの保存に
// 失敗した場合も、記録した期限の短縮としてspoolへ保存する場合と同じ内容を一度だけ内部ログへ警告する。
// spoolへ保存する場合は、recordと期限の両方を短縮しても警告を重ねない。上限以下の指定は警告しない。
func TestHandleFailureWarnsOnceWhenRetryAfterDeadlineIsCapped(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	const maxRetryAfter = 20 * time.Minute
	for _, tt := range []struct {
		name       string
		policy     durable.OverloadPolicy
		overloaded bool
		exported   bool
		withinCap  bool
		// saveCanceledは、activeへの保存の直前にcontextを取り消し、保存を失敗させます。
		saveCanceled bool
	}{
		{name: "stored", policy: durable.OverloadPolicyDrop},
		{name: "stored through StoreSendResultByState", policy: durable.OverloadPolicyDrop, exported: true},
		{name: "dropped on overload", policy: durable.OverloadPolicyDrop, overloaded: true},
		{name: "degraded on overload", policy: durable.OverloadPolicyDegrade, overloaded: true},
		{name: "dropped within the maximum", policy: durable.OverloadPolicyDrop, overloaded: true, withinCap: true},
		{name: "active save failed", policy: durable.OverloadPolicyDrop, saveCanceled: true},
		{name: "active save failed through StoreSendResultByState", policy: durable.OverloadPolicyDrop, exported: true, saveCanceled: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var (
				mu       sync.Mutex
				warnings []*logging.LogEntry
				stdout   bytes.Buffer
			)
			handler, recorder := newRetryAfterRecordingHandler(t, now, tt.policy, tt.overloaded, &stdout, InternalLogHandlers{
				OnWarn: func(_ context.Context, entry *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
					mu.Lock()
					defer mu.Unlock()
					warnings = append(warnings, entry)
				},
			})
			handler.SetMaxRetryAfter(maxRetryAfter)
			failedAt := now.Add(-time.Second)
			longest := failedRecord("longest", "http_throttle", failedAt)
			longest.RetryAfter = 3*time.Hour + time.Second
			longer := failedRecord("longer", "http_throttle", failedAt)
			longer.RetryAfter = 2*time.Hour + time.Second
			within := failedRecord("within", "http_throttle", failedAt)
			within.RetryAfter = 10*time.Minute + time.Second
			// 最も長い指定を先頭以外に置き、最初に短縮したrecordではなく最も長い指定を通知することも確かめます。
			records := []loggerfailure.FailedRecord{longer, longest, within}
			wantDeadline := now.Add(maxRetryAfter)
			if tt.withinCap {
				records = []loggerfailure.FailedRecord{within}
				wantDeadline = now.Add(10 * time.Minute)
			}
			result := loggerfailure.SendResult{
				Meta:             loggerfailure.Meta{Backend: "loki", RequestID: "retry-after-cap-warning", AttemptedAt: failedAt},
				RetryableRecords: records,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.saveCanceled {
				if tt.exported {
					// StoreSendResultByStateは過負荷を判定せず、activeへの保存から始めます。
					cancel()
				} else {
					// 保存先の準備と過負荷の判定を終えた後、activeへの保存の直前に取り消します。
					handler.dirStatsScanner = func(spooldir.StateDir) (DirStats, error) {
						cancel()
						return DirStats{}, nil
					}
				}
			}
			var err error
			if tt.exported {
				err = handler.StoreSendResultByState(ctx, result)
			} else {
				err = handler.HandleFailure(ctx, result)
			}
			switch {
			case tt.saveCanceled:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("send failure handling error = %v, want the canceled active save", err)
				}
				// 前提として、activeへ何も保存していないことを確認します。
				assertRecordCount(t, readSpoolRecords(t, handler.Store.RootDir.ActiveDir().Path()), 0, "active")
			case tt.overloaded && tt.policy == durable.OverloadPolicyDrop:
				if !errors.Is(err, pkglogger.ErrLogDropped) {
					t.Fatalf("HandleFailure error = %v, want ErrLogDropped", err)
				}
			case err != nil:
				t.Fatalf("send failure handling error = %v, want nil", err)
			}
			requireRecordedRetryAfterDeadline(t, recorder, wantDeadline)

			mu.Lock()
			defer mu.Unlock()
			if tt.withinCap {
				if len(warnings) != 0 {
					t.Fatalf("warnings = %d, want none within the maximum", len(warnings))
				}
				return
			}
			if len(warnings) != 1 {
				t.Fatalf("warnings = %d, want exactly one per send failure", len(warnings))
			}
			for key, want := range map[string]any{
				"backend":               "loki",
				"request_id":            "retry-after-cap-warning",
				"requested_retry_after": (3 * time.Hour).String(),
				"max_retry_after":       maxRetryAfter.String(),
				"not_before":            now.Add(maxRetryAfter),
				"record_count":          2,
			} {
				if got := warnings[0].Fields[key]; got != want {
					t.Fatalf("warning field %s = %v, want %v", key, got, want)
				}
			}
		})
	}
}
