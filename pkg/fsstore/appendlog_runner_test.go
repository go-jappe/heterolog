package fsstore

import (
	"errors"
	"testing"
	"time"
)

func TestValidateAppendLogRunnerConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  AppendLogRunnerConfig
		wantErr error
	}{
		{
			name:    "zero interval",
			wantErr: ErrInvalidAppendLogRunnerInterval,
		},
		{
			name: "supervised stop on error",
			config: AppendLogRunnerConfig{
				Interval:    time.Second,
				StopOnError: true,
				Supervisor: AppendLogRunnerSupervisorConfig{
					Enabled:      true,
					RestartDelay: time.Second,
					MaxRestarts:  3,
				},
			},
		},
		{
			name:    "negative interval",
			config:  AppendLogRunnerConfig{Interval: -time.Second},
			wantErr: ErrInvalidAppendLogRunnerInterval,
		},
		{
			name:   "unsupervised stop on error is handled by caller",
			config: AppendLogRunnerConfig{Interval: time.Second, StopOnError: true},
		},
		{
			name: "zero restart delay",
			config: AppendLogRunnerConfig{Interval: time.Second, Supervisor: AppendLogRunnerSupervisorConfig{
				Enabled:     true,
				MaxRestarts: 1,
			}},
			wantErr: ErrInvalidAppendLogSupervisorRestartDelay,
		},
		{
			name: "negative restart delay",
			config: AppendLogRunnerConfig{Interval: time.Second, Supervisor: AppendLogRunnerSupervisorConfig{
				Enabled:      true,
				RestartDelay: -time.Second,
				MaxRestarts:  1,
			}},
			wantErr: ErrInvalidAppendLogSupervisorRestartDelay,
		},
		{
			name: "zero max restarts",
			config: AppendLogRunnerConfig{Interval: time.Second, Supervisor: AppendLogRunnerSupervisorConfig{
				Enabled:      true,
				RestartDelay: time.Second,
			}},
			wantErr: ErrInvalidAppendLogSupervisorMaxRestarts,
		},
		{
			name: "negative max restarts",
			config: AppendLogRunnerConfig{Interval: time.Second, Supervisor: AppendLogRunnerSupervisorConfig{
				Enabled:      true,
				RestartDelay: time.Second,
				MaxRestarts:  -1,
			}},
			wantErr: ErrInvalidAppendLogSupervisorMaxRestarts,
		},
		{
			name: "disabled supervisor ignores its unused values",
			config: AppendLogRunnerConfig{Interval: time.Second, Supervisor: AppendLogRunnerSupervisorConfig{
				RestartDelay: -time.Second,
				MaxRestarts:  -1,
			}},
		},
	}

	for _, testCase := range tests {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateAppendLogRunnerConfig(testCase.config)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("ValidateAppendLogRunnerConfig() error = %v, want %v", err, testCase.wantErr)
			}
		})
	}
}
