package bootstrap_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestFileRotationKeepsOutputInSelectedDestination(t *testing.T) {
	for _, kind := range []string{"sync", "async", "delivery"} {
		for _, rotate := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rotate=%t", kind, rotate), func(t *testing.T) {
				root := t.TempDir()
				cfg := directoryWhitespaceConfig(root, "file-output", "logs")
				cfg.MaxLogEntryBytes = 2 << 20
				cfg.File.FsstoreConfig.Lifecycle.EnableRotate = rotate
				cfg.File.FsstoreConfig.Lifecycle.RotationType = pkgfsstore.FileRotationDaily
				// 対応している大きさの上限と十分な量のdataを使い、実際のrotationと、通常の
				// 大きさの確認の経路を通します。
				first := "first file record " + strings.Repeat("x", 1<<20)
				const second = "second file record after size boundary"
				output := testutil.CaptureStdout(t, func() {
					write, closeLogger := fileOutputWriter(t, kind, cfg)
					t.Cleanup(func() { _ = closeLogger() })
					for _, message := range []string{first, second} {
						if err := write(message); err != nil {
							t.Fatal(err)
						}
					}
					if err := closeLogger(); err != nil {
						t.Fatal(err)
					}
				})
				if output != "" {
					t.Errorf("file-only runtime wrote to stdout: %q", output)
				}
				paths, err := filepath.Glob(filepath.Join(root, "file-output", "logs", "*.log"))
				if err != nil {
					t.Fatal(err)
				}
				wantFiles := 1
				if rotate {
					wantFiles = 2
				}
				if len(paths) != wantFiles {
					t.Fatalf("log files = %v, want %d", paths, wantFiles)
				}
				var contents strings.Builder
				for _, path := range paths {
					body, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					contents.Write(body)
				}
				for _, message := range []string{first, second} {
					if got := strings.Count(contents.String(), message); got != 1 {
						t.Errorf("message occurrences in selected files = %d, want 1", got)
					}
				}
			})
		}
		t.Run(kind+"/stdout selected", func(t *testing.T) {
			const message = "explicit stdout destination"
			output := testutil.CaptureStdout(t, func() {
				write, closeLogger := fileOutputWriter(t, kind, loggerruntime.DefaultConfig())
				t.Cleanup(func() { _ = closeLogger() })
				if err := write(message); err != nil {
					t.Fatal(err)
				}
				if err := closeLogger(); err != nil {
					t.Fatal(err)
				}
			})
			if got := strings.Count(output, message); got != 1 {
				t.Fatalf("selected stdout contains message %d times, want 1: %q", got, output)
			}
		})
	}
}

func fileOutputWriter(t *testing.T, kind string, cfg pkglogger.LoggerConfig) (func(string) error, func() error) {
	t.Helper()
	ctx := context.Background()
	switch kind {
	case "sync":
		logger, err := bootstrap.New(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		return func(message string) error { return logger.Info(ctx, message) }, logger.Close
	case "async":
		async := loggerruntime.DefaultAsyncConfig()
		async.LogOutputDestinations, async.Stdout, async.File = cfg.LogOutputDestinations, cfg.Stdout, cfg.File
		async.MaxLogEntryBytes, async.MaxParallel = cfg.MaxLogEntryBytes, 1
		logger, err := bootstrap.NewAsync(ctx, async)
		if err != nil {
			t.Fatal(err)
		}
		return func(message string) error { logger.Info(ctx, message); return nil }, logger.Close
	case "delivery":
		delivery := loggerruntime.DefaultDeliveryConfig()
		delivery.LogOutputDestinations, delivery.MaxLogEntryBytes = cfg.LogOutputDestinations, cfg.MaxLogEntryBytes
		if cfg.Stdout == nil {
			delivery.Stdout = nil
		}
		if cfg.File != nil {
			file := cfg.File
			delivery.File = &pkglogger.DeliveryFileLoggerConfig{
				FsstoreConfig: pkgfsstore.DeliveryFileWriteConfig{
					Layout: file.FsstoreConfig.Layout,
					Rotation: pkgfsstore.FileRotationConfig{
						EnableRotate: file.FsstoreConfig.Lifecycle.EnableRotate,
						RotationType: file.FsstoreConfig.Lifecycle.RotationType,
						MaxFileSize:  file.FsstoreConfig.Lifecycle.MaxFileSize,
					},
					Duration: file.FsstoreConfig.Duration,
					Retry:    file.FsstoreConfig.Retry,
				},
				TimeConfig:    file.TimeConfig,
				LogBodyConfig: file.LogBodyConfig,
			}
		}
		logger, err := bootstrap.NewDelivery(ctx, delivery)
		if err != nil {
			t.Fatal(err)
		}
		return func(message string) error {
			report, err := logger.Info(ctx, message)
			if err == nil && (len(report.Targets) != 1 || report.Targets[0].Status != pkglogger.DeliverySucceeded) {
				return fmt.Errorf("unexpected delivery result: %+v", report)
			}
			return err
		}, logger.Close
	default:
		t.Fatalf("unknown logger kind: %s", kind)
		return nil, nil
	}
}
