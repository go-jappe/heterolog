package timeoutbudget

import (
	"errors"
	"testing"
)

func TestParseTimeoutBudgetPolicy(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  TimeoutBudgetPolicy
	}{
		{name: "empty defaults to must_fit", value: "", want: TimeoutBudgetMustFit},
		{name: "must fit", value: "must_fit", want: TimeoutBudgetMustFit},
		{name: "may cutoff", value: "may_cutoff", want: TimeoutBudgetMayCutoff},
		{name: "unbounded", value: "unbounded", want: TimeoutBudgetUnbounded},
		{name: "trim and lower", value: " May_Cutoff ", want: TimeoutBudgetMayCutoff},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTimeoutBudgetPolicy(tt.value)
			if err != nil {
				t.Fatalf("ParseTimeoutBudgetPolicy(%q) error = %v", tt.value, err)
			}
			if got != tt.want {
				t.Fatalf("ParseTimeoutBudgetPolicy(%q) = %q, want %q", tt.value, got, tt.want)
			}
		})
	}
}

func TestParseTimeoutBudgetPolicyRejectsUnknownValue(t *testing.T) {
	if _, err := ParseTimeoutBudgetPolicy("forever"); !errors.Is(err, ErrInvalidTimeoutBudgetPolicy) {
		t.Fatalf("ParseTimeoutBudgetPolicy() error = %v, want %v", err, ErrInvalidTimeoutBudgetPolicy)
	}
}
