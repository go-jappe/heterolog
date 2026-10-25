package spoolstorehandler

import (
	"context"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
)

// 送信元がRetry-Afterを引き継いだrecordは、FailedAtにRetryAfterを加えた時刻まで
// 再送しないようNotBeforeを保存する。FailedAtがない場合は失敗時刻と同じMeta.AttemptedAtから
// 数え、Retry-Afterのないrecordは従来どおり制限なしで保存する。
func TestStoreSendResultByStateStoresRetryAfterAsNotBefore(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	handler := newTestHandler(t, now, testOverloadPolicyConfig{})
	failedAt := now.Add(-time.Minute)
	metaAttemptedAt := now.Add(-2 * time.Minute)

	throttled := failedRecord("throttled", "http_throttle", failedAt)
	throttled.RetryAfter = 31 * time.Second
	withoutFailedAt := failedRecord("without-failed-at", "http_throttle", time.Time{})
	withoutFailedAt.RetryAfter = 30 * time.Second
	unrestricted := failedRecord("unrestricted", "http_status", failedAt)
	result := loggerfailure.SendResult{
		Meta: loggerfailure.Meta{
			Backend:     "loki",
			RequestID:   "retry-after-request",
			AttemptedAt: metaAttemptedAt,
		},
		RetryableRecords: []loggerfailure.FailedRecord{throttled, withoutFailedAt, unrestricted},
	}

	if err := handler.StoreSendResultByState(context.Background(), result); err != nil {
		t.Fatalf("StoreSendResultByState returned error: %v", err)
	}

	records := readSpoolRecords(t, handler.Store.RootDir.ActiveDir().Path())
	assertRecordCount(t, records, 3, "active")
	wants := []time.Time{
		failedAt.Add(31 * time.Second),
		metaAttemptedAt.Add(30 * time.Second),
		{},
	}
	for i, want := range wants {
		if !records[i].NotBefore.Equal(want) {
			t.Fatalf("records[%d].NotBefore = %s, want %s", i, records[i].NotBefore, want)
		}
	}
}

// 通常送信が上限を超えるRetry-Afterを受け取った場合は、保存時刻から上限までの待機に短縮し、
// 同じ送信失敗に由来する短縮を最も長い指定で一度だけOnWarnへ通知する。上限以下の指定は変えない。
func TestStoreSendResultByStateCapsRetryAfterAndWarnsOnce(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name          string
		maxRetryAfter time.Duration
		wantMax       time.Duration
	}{
		{name: "configured maximum", maxRetryAfter: time.Hour, wantMax: time.Hour},
		{name: "default maximum", wantMax: time.Hour},
		{name: "shorter maximum", maxRetryAfter: 20 * time.Minute, wantMax: 20 * time.Minute},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
			handler := newTestHandler(t, now, testOverloadPolicyConfig{})
			handler.SetMaxRetryAfter(tt.maxRetryAfter)
			var warnings []*logging.LogEntry
			handler.OnWarn = func(_ context.Context, entry *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
				warnings = append(warnings, entry)
			}
			failedAt := now.Add(-time.Second)
			longest := failedRecord("longest", "http_throttle", failedAt)
			longest.RetryAfter = 3*time.Hour + time.Second
			longer := failedRecord("longer", "http_throttle", failedAt)
			longer.RetryAfter = 2*time.Hour + time.Second
			within := failedRecord("within", "http_throttle", failedAt)
			within.RetryAfter = 10*time.Minute + time.Second
			result := loggerfailure.SendResult{
				Meta: loggerfailure.Meta{
					Backend:     "loki",
					RequestID:   "retry-after-cap",
					AttemptedAt: failedAt,
				},
				// 最も長い指定を先頭以外に置き、最初に短縮したrecordではなく最も長い指定を通知することも確かめます。
				RetryableRecords: []loggerfailure.FailedRecord{longer, longest, within},
			}

			if err := handler.StoreSendResultByState(context.Background(), result); err != nil {
				t.Fatalf("StoreSendResultByState returned error: %v", err)
			}

			records := readSpoolRecords(t, handler.Store.RootDir.ActiveDir().Path())
			assertRecordCount(t, records, 3, "active")
			capped := now.Add(tt.wantMax)
			for i, want := range []time.Time{capped, capped, now.Add(10 * time.Minute)} {
				if !records[i].NotBefore.Equal(want) {
					t.Fatalf("records[%d].NotBefore = %s, want %s", i, records[i].NotBefore, want)
				}
			}
			if len(warnings) != 1 {
				t.Fatalf("warnings = %d, want one notification per send failure", len(warnings))
			}
			fields := warnings[0].Fields
			for key, want := range map[string]any{
				"backend":               "loki",
				"request_id":            "retry-after-cap",
				"requested_retry_after": (3 * time.Hour).String(),
				"max_retry_after":       tt.wantMax.String(),
				"not_before":            capped,
				"record_count":          2,
			} {
				if got := fields[key]; got != want {
					t.Fatalf("warning field %s = %v, want %v", key, got, want)
				}
			}
		})
	}
}

func TestStoreSendResultByStateDoesNotWarnWithinMaximum(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	handler := newTestHandler(t, now, testOverloadPolicyConfig{})
	handler.SetMaxRetryAfter(time.Hour)
	warned := false
	handler.OnWarn = func(context.Context, *logging.LogEntry, observer.Tracer, observer.Meter) { warned = true }
	record := failedRecord("at-maximum", "http_throttle", now)
	record.RetryAfter = time.Hour

	if err := handler.StoreSendResultByState(context.Background(), loggerfailure.SendResult{
		Meta:             loggerfailure.Meta{Backend: "loki", AttemptedAt: now},
		RetryableRecords: []loggerfailure.FailedRecord{record},
	}); err != nil {
		t.Fatalf("StoreSendResultByState returned error: %v", err)
	}

	records := readSpoolRecords(t, handler.Store.RootDir.ActiveDir().Path())
	assertRecordCount(t, records, 1, "active")
	if want := now.Add(time.Hour); !records[0].NotBefore.Equal(want) {
		t.Fatalf("NotBefore = %s, want %s", records[0].NotBefore, want)
	}
	if warned {
		t.Fatal("a Retry-After within the maximum must not be reported")
	}
}
