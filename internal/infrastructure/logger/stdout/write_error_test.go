package stdout_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/stdout"
	stdouterrors "github.com/go-jappe/heterolog/internal/infrastructure/logger/stdout/errors"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestStdoutHandlerReturnsWriteFailureAfterNotifying(t *testing.T) {
	for _, level := range []pkglogger.LogLevel{pkglogger.LogLevelInfo, pkglogger.LogLevelError} {
		t.Run(level.String(), func(t *testing.T) {
			var notified error
			logger, err := stdout.NewStdoutLogger(context.Background(), stdout.NewConfigAdapter(testStdoutConfig()), nil, nil, nil,
				stdout.WithInternalLogHandlers(func(_ context.Context, err error, _ *infralogging.LogEntry, _ observer.Tracer, _ observer.Meter) {
					notified = err
				}, nil, nil, nil))
			if err != nil {
				t.Fatal(err)
			}
			closed, err := os.CreateTemp(t.TempDir(), "closed-output")
			if err != nil {
				t.Fatal(err)
			}
			if err := closed.Close(); err != nil {
				t.Fatal(err)
			}
			originalOut, originalErr := os.Stdout, os.Stderr
			if level == pkglogger.LogLevelInfo {
				os.Stdout = closed
			} else {
				os.Stderr = closed
			}
			defer func() { os.Stdout, os.Stderr = originalOut, originalErr }()
			err = logger.HandleLogEntry(context.Background(), &infralogging.LogEntry{Level: level, Message: "failed write"})
			os.Stdout, os.Stderr = originalOut, originalErr
			var internalErr *stdouterrors.StdoutLoggerInternalError
			if !errors.Is(err, os.ErrClosed) || !errors.As(err, &internalErr) {
				t.Fatalf("write error = %v", err)
			}
			if notified != err {
				t.Fatalf("notifier received %v, caller received %v", notified, err)
			}
		})
	}
}
