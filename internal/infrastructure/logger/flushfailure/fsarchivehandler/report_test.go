package fsarchivehandler

import (
	"context"
	"errors"
	"testing"
	"time"

	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

type reportOverloadHandlerFunc func(context.Context, loggerfailure.SendResult) error

func (f reportOverloadHandlerFunc) Handle(
	ctx context.Context,
	result loggerfailure.SendResult,
) error {
	return f(ctx, result)
}

func TestHandleFailureWithReportArchivesRecordAndReportsDurableSuccess(t *testing.T) {
	now := time.Date(2026, 7, 18, 1, 0, 0, 0, time.UTC)
	handler := newTestArchiveHandler(
		t,
		now,
		nil,
	)
	result := loggerfailure.SendResult{
		Meta: loggerfailure.Meta{
			Backend:     "loki",
			RequestID:   "request-report",
			AttemptedAt: now,
			InputCount:  1,
		},
		RetryableRecords: []loggerfailure.FailedRecord{
			archiveFailedRecord("report-record", now, 429, "too_many_requests"),
		},
	}

	report, err := handler.HandleFailureWithReport(context.Background(), result)
	if err != nil {
		t.Fatalf("HandleFailureWithReport() error = %v", err)
	}
	if len(report.Targets) != 0 {
		t.Fatalf("Targets = %+v, want empty", report.Targets)
	}
	if len(report.Durables) != 1 {
		t.Fatalf("Durables = %+v, want one result", report.Durables)
	}
	durableResult := report.Durables[0]
	if durableResult.Backend != "loki" ||
		durableResult.Kind != pkglogger.DurableArchive ||
		durableResult.Status != pkglogger.DurableSucceeded ||
		durableResult.Err != nil {
		t.Fatalf("DurableResult = %+v, want succeeded loki archive", durableResult)
	}

	records := readArchiveRecords(t, handler.handler.Fsstore.ActiveFilePathRetryable)
	assertArchiveRecordCount(t, records, 1, "retryable report")
	assertArchiveRecord(
		t,
		records[0],
		archiveRecordFailureCategoryRetryable,
		now,
		"request-report",
		"",
		"",
		"report-record",
		429,
	)
}

func TestHandleFailureWithReportReportsArchiveWriteFailure(t *testing.T) {
	handler := newTestArchiveHandler(
		t,
		time.Date(2026, 7, 18, 1, 5, 0, 0, time.UTC),
		nil,
	)
	closeRetryableArchiveHandleForWriteFailure(t, handler)

	report, err := handler.HandleFailureWithReport(
		context.Background(),
		loggerfailure.SendResult{
			Meta: loggerfailure.Meta{Backend: "opensearch"},
			RetryableRecords: []loggerfailure.FailedRecord{{
				Payload: map[string]any{"message": "archive failure"},
			}},
		},
	)
	if err == nil {
		t.Fatal("HandleFailureWithReport() error = nil, want archive write error")
	}
	if len(report.Durables) != 1 {
		t.Fatalf("Durables = %+v, want one failed result", report.Durables)
	}
	durableResult := report.Durables[0]
	if durableResult.Backend != "opensearch" ||
		durableResult.Kind != pkglogger.DurableArchive ||
		durableResult.Status != pkglogger.DurableFailed ||
		!errors.Is(durableResult.Err, err) {
		t.Fatalf("DurableResult = %+v, want failed opensearch archive", durableResult)
	}
}

// Delivery APIでは、drop policyの適用をEvacuationDroppedの成功した処置として報告します。
// 同期APIが返すErrLogDroppedを、退避の失敗やerrorとして重ねて返しません。
func TestHandleFailureWithReportReportsOverloadDrop(t *testing.T) {
	handler := newTestArchiveHandler(
		t,
		time.Date(2026, 7, 18, 1, 10, 0, 0, time.UTC),
		func(cfg *testArchiveConfig) {
			cfg.overload.overloadPolicy = pkgdurable.OverloadPolicyDrop
			cfg.overload.minAvailableDiskSpace = ^uint64(0)
		},
	)

	report, err := handler.HandleFailureWithReport(
		context.Background(),
		loggerfailure.SendResult{
			RetryableRecords: []loggerfailure.FailedRecord{{
				Payload: map[string]any{"message": "drop on overload"},
			}},
		},
	)
	if err != nil {
		t.Fatalf("HandleFailureWithReport() error = %v", err)
	}
	if len(report.Durables) != 0 {
		t.Fatalf("Durables = %+v, want empty on overload", report.Durables)
	}
	if len(report.Evacuations) != 1 {
		t.Fatalf("Evacuations = %+v, want one result", report.Evacuations)
	}
	evacuation := report.Evacuations[0]
	if evacuation.Kind != pkglogger.EvacuationDropped ||
		evacuation.Status != pkglogger.EvacuationSucceeded ||
		evacuation.Err != nil {
		t.Fatalf("EvacuationResult = %+v, want succeeded drop", evacuation)
	}
}

func TestHandleOverloadWithReportReportsEvacuationFailure(t *testing.T) {
	overloadFailure := errors.New("overload evacuation failed")
	tests := []struct {
		name            string
		overloadHandler reportOverloadHandlerFunc
		wantErr         error
	}{
		{
			name:    "handler is not configured",
			wantErr: errNilOverloadHandler,
		},
		{
			name: "handler failed",
			overloadHandler: func(context.Context, loggerfailure.SendResult) error {
				return overloadFailure
			},
			wantErr: overloadFailure,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := newTestArchiveHandler(
				t,
				time.Date(2026, 7, 18, 1, 12, 0, 0, time.UTC),
				nil,
			)
			if tt.overloadHandler == nil {
				handler.handler.OverloadHandler = nil
			} else {
				handler.handler.OverloadHandler = tt.overloadHandler
			}

			report, err := handler.handler.handleOverloadWithReport(
				context.Background(),
				loggerfailure.SendResult{},
			)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("handleOverloadWithReport() error = %v, want %v", err, tt.wantErr)
			}
			if len(report.Evacuations) != 1 {
				t.Fatalf("Evacuations = %+v, want one failed result", report.Evacuations)
			}
			evacuation := report.Evacuations[0]
			if evacuation.Kind != pkglogger.EvacuationDropped ||
				evacuation.Status != pkglogger.EvacuationFailed ||
				evacuation.Err != err {
				t.Fatalf("EvacuationResult = %+v, want failed drop with returned error", evacuation)
			}
		})
	}
}

func TestHandleFailureWithReportWithNoFailuresReturnsEmptyReport(t *testing.T) {
	handler := newTestArchiveHandler(
		t,
		time.Date(2026, 7, 18, 1, 15, 0, 0, time.UTC),
		nil,
	)

	report, err := handler.HandleFailureWithReport(
		context.Background(),
		loggerfailure.SendResult{},
	)
	if err != nil {
		t.Fatalf("HandleFailureWithReport() error = %v", err)
	}
	if len(report.Targets) != 0 || len(report.Durables) != 0 || len(report.Evacuations) != 0 {
		t.Fatalf("report = %+v, want empty", report)
	}
}
