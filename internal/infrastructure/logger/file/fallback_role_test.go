package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestResolveInternalLogFileRole(t *testing.T) {
	tests := []struct {
		name          string
		separateError bool
		separateDebug bool
		wantError     loggerType
		wantDebug     loggerType
	}{
		{
			name:      "neither separated",
			wantError: DefaultLogger,
			wantDebug: DefaultLogger,
		},
		{
			name:          "error only",
			separateError: true,
			wantError:     ErrorLogger,
			wantDebug:     DefaultLogger,
		},
		{
			name:          "debug only",
			separateDebug: true,
			wantError:     DefaultLogger,
			wantDebug:     DebugLogger,
		},
		{
			name:          "error and debug",
			separateError: true,
			separateDebug: true,
			wantError:     ErrorLogger,
			wantDebug:     DebugLogger,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "fallback-role")
			cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = tt.separateError
			cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = tt.separateDebug
			adapter := NewConfigAdapter(cfg)

			levelTests := []struct {
				name  string
				level pkglogger.LogLevel
				want  loggerType
			}{
				{name: "error", level: pkglogger.LogLevelError, want: tt.wantError},
				{name: "debug", level: pkglogger.LogLevelDebug, want: tt.wantDebug},
				{name: "warn", level: pkglogger.LogLevelWarn, want: DefaultLogger},
				{name: "info", level: pkglogger.LogLevelInfo, want: DefaultLogger},
			}
			for _, levelTest := range levelTests {
				t.Run(levelTest.name, func(t *testing.T) {
					if got := resolveInternalLogFileRole(adapter.Fsstore().Layout(), levelTest.level); got != levelTest.want {
						t.Fatalf(
							"resolveInternalLogFileRole() = %q, want %q",
							got,
							levelTest.want,
						)
					}
				})
			}
		})
	}
}

func TestFallbackHandlerRecordsInternalLogRegardlessOfSourceRole(t *testing.T) {
	root := t.TempDir()
	cfg := newTestFileLoggerConfig(root, "logs", "fallback-self-role")
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
	adapter := NewConfigAdapter(cfg)
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}
	ctx := context.Background()
	loggerSet, err := NewLoggerSet(adapter, tracer, meter)
	if err != nil {
		t.Fatalf("NewLoggerSet(): %v", err)
	}
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "loggerSet Close", func() closerForTest { return loggerSet }, true)
	handler := loggerSet.newErrorFallbackHandler()

	handler(
		ctx,
		errors.New("same role"),
		&logging.LogEntry{
			Level:   pkglogger.LogLevelError,
			Message: "same role is written once",
			Source:  fileLoggerInternalSource(DefaultLogger),
		},
		tracer,
		meter,
	)
	handler(
		ctx,
		errors.New("different role"),
		&logging.LogEntry{
			Level:   pkglogger.LogLevelError,
			Message: "must be written to default role",
			Source:  fileLoggerInternalSource(DebugLogger),
		},
		tracer,
		meter,
	)

	logDir := filepath.Join(
		root,
		cfg.FsstoreConfig.Layout.Target,
		cfg.FsstoreConfig.Layout.LogDir,
	)
	matches, err := filepath.Glob(filepath.Join(logDir, "*.log"))
	if err != nil {
		t.Fatalf("glob fallback files: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("fallback files = %v, want exactly one default-role file", matches)
	}
	content, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("read fallback file: %v", err)
	}
	if !strings.Contains(string(content), "same role is written once") {
		t.Fatalf("same-role internal log was not written: %s", content)
	}
	if !strings.Contains(string(content), "must be written to default role") {
		t.Fatalf("different-role fallback entry was not written: %s", content)
	}
}
