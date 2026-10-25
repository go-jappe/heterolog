package logger

import "testing"

func TestLoggerContractKindCanonicalIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		contractKind LoggerContractKind
		wantID       string
		wantDisplay  string
	}{
		{name: "sync", contractKind: LoggerContractSync, wantID: "logger", wantDisplay: "logger"},
		{name: "async", contractKind: LoggerContractAsyncBestEffort, wantID: "async_logger", wantDisplay: "async logger"},
		{name: "delivery", contractKind: LoggerContractDelivery, wantID: "delivery_logger", wantDisplay: "delivery logger"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := test.contractKind.String(); got != test.wantID {
				t.Fatalf("LoggerContractKind.String() = %q, want %q", got, test.wantID)
			}
			if got := test.contractKind.DisplayName(); got != test.wantDisplay {
				t.Fatalf("LoggerContractKind.DisplayName() = %q, want %q", got, test.wantDisplay)
			}
		})
	}
}
