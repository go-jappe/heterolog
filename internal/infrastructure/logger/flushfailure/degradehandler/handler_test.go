package degradehandler

import (
	"context"
	"errors"
	"testing"

	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

type recordingDelegate struct {
	calls int
	err   error
}

func (d *recordingDelegate) Handle(context.Context, loggerfailure.SendResult) error {
	d.calls++
	return d.err
}

func TestHandleFailureDelegatesDirectEvacuation(t *testing.T) {
	tests := []struct {
		name        string
		delegateErr error
	}{
		{name: "succeeded"},
		{name: "failed", delegateErr: errors.New("write failed")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delegate := &recordingDelegate{err: tt.delegateErr}
			handler := newHandler(delegate)

			err := handler.handleFailure(
				context.Background(),
				loggerfailure.SendResult{},
			)
			if !errors.Is(err, tt.delegateErr) {
				t.Fatalf("handleFailure error = %v, want %v", err, tt.delegateErr)
			}
			if delegate.calls != 1 {
				t.Fatalf("delegate calls = %d, want 1", delegate.calls)
			}
		})
	}
}

func TestNilHandlerReturnsExplicitError(t *testing.T) {
	var handler *Handler

	if err := handler.handleFailure(context.Background(), loggerfailure.SendResult{}); !errors.Is(err, ErrNilHandler) {
		t.Fatalf("handleFailure error = %v, want ErrNilHandler", err)
	}
}

func TestHandleFailureWithReportReturnsEvacuationResult(t *testing.T) {
	delegateErr := errors.New("spill failed")
	tests := []struct {
		name        string
		delegateErr error
		wantStatus  pkglogger.EvacuationStatus
	}{
		{
			name:       "succeeded",
			wantStatus: pkglogger.EvacuationSucceeded,
		},
		{
			name:        "failed",
			delegateErr: delegateErr,
			wantStatus:  pkglogger.EvacuationFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delegate := &recordingDelegate{err: tt.delegateErr}
			handler := newHandler(delegate)

			report, err := handler.HandleFailureWithReport(
				context.Background(),
				loggerfailure.SendResult{
					RetryableRecords: []loggerfailure.FailedRecord{{
						Payload: map[string]any{"message": "evacuate"},
					}},
				},
			)
			if !errors.Is(err, tt.delegateErr) {
				t.Fatalf("HandleFailureWithReport() error = %v, want %v", err, tt.delegateErr)
			}
			if delegate.calls != 1 {
				t.Fatalf("delegate calls = %d, want 1", delegate.calls)
			}
			if len(report.Targets) != 0 || len(report.Durables) != 0 {
				t.Fatalf("report = %+v, want evacuation only", report)
			}
			if len(report.Evacuations) != 1 {
				t.Fatalf("Evacuations = %+v, want one result", report.Evacuations)
			}
			evacuation := report.Evacuations[0]
			if evacuation.Kind != pkglogger.EvacuationSpilled ||
				evacuation.Status != tt.wantStatus ||
				!errors.Is(evacuation.Err, tt.delegateErr) {
				t.Fatalf(
					"EvacuationResult = %+v, want kind=%s status=%s err=%v",
					evacuation,
					pkglogger.EvacuationSpilled,
					tt.wantStatus,
					tt.delegateErr,
				)
			}
		})
	}
}

func TestHandleFailureWithReportWithNoFailuresReturnsEmptyReport(t *testing.T) {
	delegate := &recordingDelegate{}
	handler := newHandler(delegate)

	report, err := handler.HandleFailureWithReport(
		context.Background(),
		loggerfailure.SendResult{},
	)
	if err != nil {
		t.Fatalf("HandleFailureWithReport() error = %v", err)
	}
	if delegate.calls != 0 {
		t.Fatalf("delegate calls = %d, want 0", delegate.calls)
	}
	if len(report.Targets) != 0 || len(report.Durables) != 0 || len(report.Evacuations) != 0 {
		t.Fatalf("report = %+v, want empty", report)
	}
}
