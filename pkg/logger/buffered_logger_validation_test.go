package logger

import (
	"testing"
	"time"
)

func TestValidateBufferedFlushInterval(t *testing.T) {
	for _, tt := range []struct {
		name     string
		interval time.Duration
		wantErr  bool
	}{
		{name: "positive", interval: time.Second},
		{name: "negative", interval: -time.Nanosecond, wantErr: true},
		{name: "zero", wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBufferedFlushInterval(tt.interval)
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("ValidateBufferedFlushInterval() error = %v, want nil", err)
				}
				return
			}
			fieldErr := findFieldError(err, ConfigFieldFlushInterval, ErrNonPositiveSetting)
			if fieldErr == nil || fieldErr.Value != tt.interval {
				t.Fatalf("ValidateBufferedFlushInterval() error = %v, want non-positive FlushInterval %s", err, tt.interval)
			}
		})
	}
}
