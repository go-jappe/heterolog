package failure_test

import (
	"errors"
	"testing"

	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
)

func TestSendResultCountsAndFailurePredicates(t *testing.T) {
	failedRecord := loggerfailure.FailedRecord{
		Payload: map[string]any{"message": "failed"},
	}

	tests := []struct {
		name             string
		result           loggerfailure.SendResult
		hasFailedRecords bool
		hasSendFailure   bool
		failedCount      int
		successCount     int
	}{
		{
			name: "empty",
		},
		{
			name: "all succeeded",
			result: loggerfailure.SendResult{
				Meta: loggerfailure.Meta{InputCount: 3},
			},
			successCount: 3,
		},
		{
			name: "terminal only failure confirms no successes",
			result: loggerfailure.SendResult{
				Meta:          loggerfailure.Meta{InputCount: 3},
				TerminalError: errors.New("connection reset"),
			},
			hasSendFailure: true,
		},
		{
			name: "partial failure",
			result: loggerfailure.SendResult{
				Meta:             loggerfailure.Meta{InputCount: 3},
				DeadRecords:      []loggerfailure.FailedRecord{failedRecord},
				RetryableRecords: []loggerfailure.FailedRecord{failedRecord},
			},
			hasFailedRecords: true,
			hasSendFailure:   true,
			failedCount:      2,
			successCount:     1,
		},
		{
			name: "terminal failure with records remains fail closed",
			result: loggerfailure.SendResult{
				Meta:             loggerfailure.Meta{InputCount: 3},
				TerminalError:    errors.New("final response unavailable"),
				RetryableRecords: []loggerfailure.FailedRecord{failedRecord},
			},
			hasFailedRecords: true,
			hasSendFailure:   true,
			failedCount:      1,
		},
		{
			name: "failed records exceed input count",
			result: loggerfailure.SendResult{
				Meta:             loggerfailure.Meta{InputCount: 1},
				DeadRecords:      []loggerfailure.FailedRecord{failedRecord},
				RetryableRecords: []loggerfailure.FailedRecord{failedRecord},
			},
			hasFailedRecords: true,
			hasSendFailure:   true,
			failedCount:      2,
		},
		{
			name: "negative input count",
			result: loggerfailure.SendResult{
				Meta: loggerfailure.Meta{InputCount: -1},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.result.HasFailedRecords(); got != test.hasFailedRecords {
				t.Fatalf("HasFailedRecords() = %t, want %t", got, test.hasFailedRecords)
			}
			if got := test.result.HasSendFailure(); got != test.hasSendFailure {
				t.Fatalf("HasSendFailure() = %t, want %t", got, test.hasSendFailure)
			}
			if got := test.result.FailedCount(); got != test.failedCount {
				t.Fatalf("FailedCount() = %d, want %d", got, test.failedCount)
			}
			if got := test.result.SuccessCount(); got != test.successCount {
				t.Fatalf("SuccessCount() = %d, want %d", got, test.successCount)
			}
		})
	}
}
