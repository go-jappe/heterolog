// Run with: go run ./examples/slog
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/go-jappe/heterolog"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (err error) {
	ctx := context.Background()
	cfg := heterolog.DefaultConfig()
	cfg.Stdout.RuntimeConfig.LogLevel = heterolog.LevelDebug
	runtime, err := heterolog.New(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err = errors.Join(err, runtime.Shutdown(shutdownCtx))
	}()

	logger := slog.New(heterolog.NewSlogHandler(runtime, &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: true,
	}))
	logger.InfoContext(ctx, "service started", "service", "example", "version", "dev")
	logger.With("component", "worker").WithGroup("job").DebugContext(ctx,
		"job completed", "id", 42, "elapsed", 20*time.Millisecond)
	return nil
}
