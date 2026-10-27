package loggerruntime_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestDirectoryOwnershipPreservesLiteralWhitespace(t *testing.T) {
	for _, tt := range []struct {
		name, first, second string
		conflict            bool
		trailingSpace       bool
	}{
		{name: "distinct ordinary directories", first: "logs", second: "other"},
		{name: "ordinary child overlap", first: "logs", second: "logs/child", conflict: true},
		{name: "ordinary parent overlap", first: "logs/child", second: "logs", conflict: true},
		{name: "leading space is distinct", first: " logs", second: "logs"},
		{name: "interior space is distinct", first: "log files", second: "logfiles"},
		{name: "trailing space is distinct", first: "logs ", second: "logs", trailingSpace: true},
		{name: "trailing space child overlap", first: "logs ", second: "logs /child", conflict: true, trailingSpace: true},
		{name: "trailing space parent overlap", first: "logs /child", second: "logs ", conflict: true, trailingSpace: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.trailingSpace && runtime.GOOS == "windows" {
				t.Skip("Windows ordinary paths do not preserve trailing-space directory names")
			}
			root, target := t.TempDir(), "directory-ownership"
			first, err := loggerruntime.New(context.Background(), directoryWhitespaceConfig(root, target, tt.first))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := first.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := first.Info(context.Background(), "first directory entry"); err != nil {
				t.Fatal(err)
			}
			firstDir := filepath.Join(root, target, tt.first)
			assertDirectoryMessage(t, firstDir, "first directory entry", "second directory entry")

			second, err := loggerruntime.New(context.Background(), directoryWhitespaceConfig(root, target, tt.second))
			if second != nil {
				t.Cleanup(func() {
					if err := second.Close(); err != nil {
						t.Error(err)
					}
				})
			}
			if tt.conflict {
				if !errors.Is(err, directorylease.ErrDirectoryLeaseConflict) {
					t.Fatalf("actual parent/child ownership overlap accepted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("distinct physical directories rejected: %v", err)
			}
			if err := second.Info(context.Background(), "second directory entry"); err != nil {
				t.Fatal(err)
			}
			// 両方のleaseが有効な間に、文字どおりの二つの保存先へ実際に書いたことと、もう
			// 一方のloggerのlogが混ざっていないことを確かめます。
			assertDirectoryMessage(t, firstDir, "first directory entry", "second directory entry")
			assertDirectoryMessage(t, filepath.Join(root, target, tt.second), "second directory entry", "first directory entry")
		})
	}
}

func assertDirectoryMessage(t *testing.T, dir, want, unwanted string) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil || len(files) != 1 {
		t.Fatalf("literal directory %q: log files = %v, error = %v", dir, files, err)
	}
	contents, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), want) || strings.Contains(string(contents), unwanted) {
		t.Fatalf("literal directory %q: contents = %q, want %q only", dir, contents, want)
	}
}

func directoryWhitespaceConfig(root, target, dir string) pkglogger.LoggerConfig {
	cfg := loggerruntime.DefaultConfig()
	cfg.Stdout = nil
	cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
	cfg.File = &pkglogger.FileLoggerConfig{
		LifecycleMode: pkgfsstore.FileLifecycleModeDisable,
		RuntimeConfig: pkglogger.RuntimeConfig{LogLevel: pkglogger.LogLevelInfo},
		TimeConfig:    pkglogger.TimeConfig{TimeZone: "UTC"},
		LogBodyConfig: pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano},
		FsstoreConfig: pkgfsstore.Config{
			Layout: pkgfsstore.FsstoreConfig{
				LogPath: root, Target: target, LogDir: dir,
				FileNameFormat: pkgfsstore.FileNameFormat, FileDateFormat: pkgfsstore.FileDateFormat,
			},
			Lifecycle: pkgfsstore.FileLifecycleConfig{
				MaxFileSize: 1 << 20, MaxFileBackups: 2, MaxFileAge: 1, MaxDirSize: 10 << 20,
			},
			Retry: pkgfsstore.RetryConfig{MaxWriteRetries: 0},
		},
	}
	return cfg
}
