package spoolstorehandler

import (
	"context"
	"testing"
	"time"

	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
)

func TestStoreSendResultByStateCountsSpooledAndFailedRecords(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 29, 1, 2, 3, 0, time.UTC)
	handler := newTestHandler(t, now, testOverloadPolicyConfig{})
	meter := observertest.NewMeter()
	handler.Meter = meter
	result := loggerfailure.SendResult{
		Meta: loggerfailure.Meta{Backend: "opensearch", AttemptedAt: now},
		RetryableRecords: []loggerfailure.FailedRecord{
			failedRecord("retry-id", "es_rejected_execution_exception", now),
		},
		DeadRecords: []loggerfailure.FailedRecord{
			failedRecord("dead-id", "cluster_block_exception", now),
		},
		// payloadのないrecordはspool recordへ変換できず、保存できません。
		QuarantineRecords: []loggerfailure.FailedRecord{{FailureCode: testMapperParsingFailureCode}},
	}

	if err := handler.StoreSendResultByState(context.Background(), result); err == nil {
		t.Fatal("StoreSendResultByState succeeded, want the payload conversion error")
	}

	want := map[string]int64{
		observertest.Key(telemetry.FailureRecords, telemetry.AttrBackend, "opensearch", telemetry.AttrAction, telemetry.ActionSpooled): 2,
		observertest.Key(telemetry.FailureRecords, telemetry.AttrBackend, "opensearch", telemetry.AttrAction, telemetry.ActionFailed):  1,
	}
	got := meter.Counters()
	for key, count := range want {
		if got[key] != count {
			t.Errorf("%s = %d, want %d; counters = %v", key, got[key], count, got)
		}
	}
	if len(got) != len(want) {
		t.Errorf("counters = %v, want only %v", got, want)
	}
}
