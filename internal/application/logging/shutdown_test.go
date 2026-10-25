package logging

import (
	"errors"
	"testing"
)

type noForceLogger struct{}

func (noForceLogger) Close() error { return nil }

func TestForceCloseRejectsUnsupportedLogger(t *testing.T) {
	if err := ForceClose(noForceLogger{}); !errors.Is(err, ErrForceCloseUnsupported) {
		t.Fatal(err)
	}
	if err := ForceClose(nil); err != nil {
		t.Fatal(err)
	}
}
