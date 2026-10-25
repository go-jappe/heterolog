package duration

import (
	"testing"
	"time"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestLogLevelForElapsed(t *testing.T) {
	for _, test := range []struct {
		name    string
		elapsed time.Duration
		want    pkglogger.LogLevel
	}{
		{name: "below warn", elapsed: 0, want: pkglogger.LogLevelDebug},
		{name: "equal warn", elapsed: 10 * time.Millisecond, want: pkglogger.LogLevelDebug},
		{name: "above warn", elapsed: 10*time.Millisecond + time.Nanosecond, want: pkglogger.LogLevelWarn},
		{name: "equal error", elapsed: 20 * time.Millisecond, want: pkglogger.LogLevelWarn},
		{name: "above error", elapsed: 20*time.Millisecond + time.Nanosecond, want: pkglogger.LogLevelError},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := LogLevelForElapsed(test.elapsed, 20*time.Millisecond, 10*time.Millisecond); got != test.want {
				t.Fatalf("level = %v, want %v", got, test.want)
			}
		})
	}
}
