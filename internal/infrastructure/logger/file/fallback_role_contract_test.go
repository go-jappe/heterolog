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

func TestFallbackFileRoleContract(t *testing.T) {
	tests := []struct {
		name          string
		separateError bool
		separateDebug bool
	}{
		{name: "all events use default file"},
		{name: "only error uses separate file", separateError: true},
		{name: "only debug uses separate file", separateDebug: true},
		{
			name:          "error and debug use their separate files",
			separateError: true,
			separateDebug: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const (
				target = "fallback-role-contract"
				logDir = "logs"
			)

			root := t.TempDir()
			cfg := newTestFileLoggerConfig(root, logDir, target)
			cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = tt.separateError
			cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = tt.separateDebug
			adapter := NewConfigAdapter(cfg)
			ctx := context.Background()
			tracer := observer.NopTracer{}
			meter := observer.NopMeter{}
			loggerSet, err := NewLoggerSet(adapter, tracer, meter)
			if err != nil {
				t.Fatalf("NewLoggerSet(): %v", err)
			}
			closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "loggerSet Close", func() closerForTest { return loggerSet }, true)

			messages := map[loggerType]string{
				ErrorLogger:   "error-role-contract-event",
				DebugLogger:   "debug-role-contract-event",
				DefaultLogger: "default-role-contract-event",
			}
			infoMessage := "info-default-role-contract-event"

			loggerSet.newErrorFallbackHandler()(
				ctx,
				errors.New("error fallback trigger"),
				&logging.LogEntry{
					Level:   pkglogger.LogLevelError,
					Message: messages[ErrorLogger],
				},
				tracer,
				meter,
			)
			loggerSet.newDebugFallbackHandler()(
				ctx,
				&logging.LogEntry{
					Level:   pkglogger.LogLevelDebug,
					Message: messages[DebugLogger],
				},
				tracer,
				meter,
			)
			loggerSet.newWarnFallbackHandler()(
				ctx,
				&logging.LogEntry{
					Level:   pkglogger.LogLevelWarn,
					Message: messages[DefaultLogger],
				},
				tracer,
				meter,
			)
			loggerSet.newInfoFallbackHandler()(
				ctx,
				&logging.LogEntry{
					Level:   pkglogger.LogLevelInfo,
					Message: infoMessage,
				},
				tracer,
				meter,
			)

			contents := readFallbackRoleContents(t, root, target, logDir, cfg)
			wantErrorRole := DefaultLogger
			if tt.separateError {
				wantErrorRole = ErrorLogger
			}
			wantDebugRole := DefaultLogger
			if tt.separateDebug {
				wantDebugRole = DebugLogger
			}

			assertMessageExistsOnlyInFallbackRole(
				t,
				contents,
				messages[ErrorLogger],
				wantErrorRole,
			)
			assertMessageExistsOnlyInFallbackRole(
				t,
				contents,
				messages[DebugLogger],
				wantDebugRole,
			)
			assertMessageExistsOnlyInFallbackRole(
				t,
				contents,
				messages[DefaultLogger],
				DefaultLogger,
			)
			assertMessageExistsOnlyInFallbackRole(
				t,
				contents,
				infoMessage,
				DefaultLogger,
			)

			if got := len(contents[ErrorLogger]); got != boolToInt(tt.separateError) {
				t.Fatalf("error role file count = %d, want %d", got, boolToInt(tt.separateError))
			}
			if got := len(contents[DebugLogger]); got != boolToInt(tt.separateDebug) {
				t.Fatalf("debug role file count = %d, want %d", got, boolToInt(tt.separateDebug))
			}
			if got := len(contents[DefaultLogger]); got != 1 {
				t.Fatalf("default role file count = %d, want 1", got)
			}
		})
	}
}

func TestLoggerSetSharesOnlyTheResolvedPhysicalFileRole(t *testing.T) {
	tests := []struct {
		name          string
		separateError bool
		separateDebug bool
		wantLoggers   int
	}{
		{name: "one shared default logger", wantLoggers: 1},
		{
			name:          "error has an independent logger",
			separateError: true,
			wantLoggers:   2,
		},
		{
			name:          "debug has an independent logger",
			separateDebug: true,
			wantLoggers:   2,
		},
		{
			name:          "all physical roles are independent",
			separateError: true,
			separateDebug: true,
			wantLoggers:   3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "logger-role-contract")
			cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = tt.separateError
			cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = tt.separateDebug
			adapter := NewConfigAdapter(cfg)
			ctx := context.Background()
			tracer := observer.NopTracer{}
			meter := observer.NopMeter{}

			loggerSet, err := NewLoggerSet(adapter, tracer, meter)
			if err != nil {
				t.Fatalf("NewLoggerSet(): %v", err)
			}
			closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "loggerSet Close", func() closerForTest { return loggerSet }, true)

			defaultLogger, err := loggerSet.NewFileLogger(ctx)
			if err != nil {
				t.Fatalf("LoggerSet.NewFileLogger(): %v", err)
			}
			defaultFile := defaultLogger.Fsstore.File
			if defaultFile == nil {
				t.Fatal("default physical file is nil")
			}

			// fallback handlerから先にroleを使用し、その後constructorから取得しても
			// 同じ物理loggerと*os.Fileが返ることを確認します。
			loggerSet.newErrorFallbackHandler()(
				ctx,
				errors.New("error fallback trigger"),
				&logging.LogEntry{
					Level:   pkglogger.LogLevelError,
					Message: "logger-set-error-event",
				},
				tracer,
				meter,
			)
			loggerSet.newDebugFallbackHandler()(
				ctx,
				&logging.LogEntry{
					Level:   pkglogger.LogLevelDebug,
					Message: "logger-set-debug-event",
				},
				tracer,
				meter,
			)

			errorLogger, err := loggerSet.NewErrorFileLogger(ctx)
			if err != nil {
				t.Fatalf("LoggerSet.NewErrorFileLogger(): %v", err)
			}
			debugLogger, err := loggerSet.NewDebugFileLogger(ctx)
			if err != nil {
				t.Fatalf("LoggerSet.NewDebugFileLogger(): %v", err)
			}

			assertLoggerIdentity(
				t,
				"default and error",
				defaultLogger,
				errorLogger,
				!tt.separateError,
			)
			assertLoggerIdentity(
				t,
				"default and debug",
				defaultLogger,
				debugLogger,
				!tt.separateDebug,
			)
			if tt.separateError && tt.separateDebug {
				assertLoggerIdentity(t, "error and debug", errorLogger, debugLogger, false)
			}
			if got := len(loggerSet.loggers); got != tt.wantLoggers {
				t.Fatalf("physical logger count = %d, want %d", got, tt.wantLoggers)
			}
			if defaultLogger.Fsstore.File != defaultFile {
				t.Fatal("fallback handling replaced the already-open default *os.File")
			}
		})
	}
}

func TestFileLoggerErrorWithStackUsesOnlyResolvedErrorRole(t *testing.T) {
	tests := []struct {
		name          string
		separateError bool
		separateDebug bool
		wantRole      loggerType
	}{
		{name: "default handles error", wantRole: DefaultLogger},
		{name: "error is separated", separateError: true, wantRole: ErrorLogger},
		{
			name:          "debug is separated but error stays default",
			separateDebug: true,
			wantRole:      DefaultLogger,
		},
		{
			name:          "error and debug are separated",
			separateError: true,
			separateDebug: true,
			wantRole:      ErrorLogger,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const (
				target  = "error-with-stack-role"
				logDir  = "logs"
				message = "stack entry must have one physical role"
			)
			root := t.TempDir()
			cfg := newTestFileLoggerConfig(root, logDir, target)
			cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = tt.separateError
			cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = tt.separateDebug

			loggerSet, err := NewLoggerSet(
				NewConfigAdapter(cfg),
				observer.NopTracer{},
				observer.NopMeter{},
			)
			if err != nil {
				t.Fatalf("NewLoggerSet(): %v", err)
			}
			closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "loggerSet Close", func() closerForTest { return loggerSet }, true)

			ctx := context.Background()
			loggers := make([]*FileLogger, 0, 3)
			defaultLogger, err := loggerSet.NewFileLogger(ctx)
			if err != nil {
				t.Fatalf("LoggerSet.NewFileLogger(): %v", err)
			}
			loggers = append(loggers, defaultLogger)
			if tt.separateError {
				errorLogger, err := loggerSet.NewErrorFileLogger(ctx)
				if err != nil {
					t.Fatalf("LoggerSet.NewErrorFileLogger(): %v", err)
				}
				loggers = append(loggers, errorLogger)
			}
			if tt.separateDebug {
				debugLogger, err := loggerSet.NewDebugFileLogger(ctx)
				if err != nil {
					t.Fatalf("LoggerSet.NewDebugFileLogger(): %v", err)
				}
				loggers = append(loggers, debugLogger)
			}

			for _, logger := range loggers {
				if err := logger.testErrorWithStack(ctx, message, errors.New("stack cause")); err != nil {
					t.Fatalf("%s ErrorWithStack(): %v", logger.logger, err)
				}
			}

			contents := readFallbackRoleContents(t, root, target, logDir, cfg)
			assertMessageExistsOnlyInFallbackRole(t, contents, message, tt.wantRole)
		})
	}
}

func assertLoggerIdentity(
	t *testing.T,
	name string,
	left *FileLogger,
	right *FileLogger,
	wantSame bool,
) {
	t.Helper()

	if gotSame := left == right; gotSame != wantSame {
		t.Fatalf("%s logger identity same = %t, want %t", name, gotSame, wantSame)
	}
	if left.Fsstore.File == nil || right.Fsstore.File == nil {
		t.Fatalf("%s contains a nil physical file", name)
	}
	if gotSame := left.Fsstore.File == right.Fsstore.File; gotSame != wantSame {
		t.Fatalf("%s *os.File identity same = %t, want %t", name, gotSame, wantSame)
	}
}

func readFallbackRoleContents(
	t *testing.T,
	root string,
	target string,
	defaultDir string,
	cfg pkglogger.FileLoggerConfig,
) map[loggerType][]string {
	t.Helper()

	patterns := map[loggerType]string{
		DefaultLogger: filepath.Join(
			root,
			target,
			defaultDir,
			target+"_*.log",
		),
		ErrorLogger: filepath.Join(
			root,
			target,
			cfg.FsstoreConfig.Layout.ErrorLogDir,
			cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix+target+"_*.log",
		),
		DebugLogger: filepath.Join(
			root,
			target,
			defaultDir,
			cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix+target+"_*.log",
		),
	}

	contents := make(map[loggerType][]string, len(patterns))
	for role, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatalf("glob %s role files with %q: %v", role, pattern, err)
		}
		for _, path := range matches {
			content, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s role file %q: %v", role, path, err)
			}
			contents[role] = append(contents[role], string(content))
		}
	}
	return contents
}

func assertMessageExistsOnlyInFallbackRole(
	t *testing.T,
	contents map[loggerType][]string,
	message string,
	wantRole loggerType,
) {
	t.Helper()

	for _, role := range []loggerType{DefaultLogger, ErrorLogger, DebugLogger} {
		found := false
		for _, content := range contents[role] {
			if strings.Contains(content, message) {
				found = true
				break
			}
		}
		if role == wantRole && !found {
			t.Fatalf("message %q was not written to %s role", message, wantRole)
		}
		if role != wantRole && found {
			t.Fatalf("message %q leaked to %s role; want only %s", message, role, wantRole)
		}
	}
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
