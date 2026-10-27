package loggerruntime_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestRuntimeReturnsStdoutAndStderrWriteErrors(t *testing.T) {
	for _, stream := range []string{"stdout", "stderr"} {
		t.Run(stream, func(t *testing.T) {
			logger, err := loggerruntime.New(context.Background(), loggerruntime.DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			defer logger.Close()
			closed, err := os.CreateTemp(t.TempDir(), "closed-output")
			if err != nil {
				t.Fatal(err)
			}
			if err := closed.Close(); err != nil {
				t.Fatal(err)
			}
			if stream == "stdout" {
				original := os.Stdout
				os.Stdout = closed
				defer func() { os.Stdout = original }()
				err = logger.Info(context.Background(), "failed stdout entry")
				os.Stdout = original
			} else {
				original := os.Stderr
				os.Stderr = closed
				defer func() { os.Stderr = original }()
				err = logger.Warn(context.Background(), "failed stderr entry")
				os.Stderr = original
			}
			var failures *pkglogger.MultiLoggerError
			if !errors.Is(err, os.ErrClosed) || !errors.As(err, &failures) {
				t.Fatalf("write error not preserved: %v", err)
			}
			if len(failures.Failures) != 1 {
				t.Fatalf("expected one handler failure: %v", failures)
			}
		})
	}
}
