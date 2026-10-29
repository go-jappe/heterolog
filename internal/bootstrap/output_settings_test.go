package bootstrap_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func levelPointer(level pkglogger.LogLevel) *pkglogger.LogLevel { return &level }

// 稼働中に出力先のlevelを変えると、次のlogから反映します。
func TestUpdateOutputSettingsChangesTheLevelOfARunningLogger(t *testing.T) {
	ctx := context.Background()
	var settings map[pkglogger.LogOutputDestination]pkglogger.OutputSettings
	output := testutil.CaptureStdout(t, func() {
		runtime, err := bootstrap.New(ctx, loggerruntime.DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.Debug(ctx, "debug before the change"); err != nil {
			t.Errorf("Debug: %v", err)
		}
		if _, err := runtime.UpdateOutputSettings(map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
			pkglogger.LogOutputStdout: {Level: levelPointer(pkglogger.LogLevelDebug)},
		}); err != nil {
			t.Fatalf("UpdateOutputSettings: %v", err)
		}
		if err := runtime.Debug(ctx, "debug after the change"); err != nil {
			t.Errorf("Debug: %v", err)
		}
		settings = runtime.OutputSettings()
		if err := runtime.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	if strings.Contains(output, "debug before the change") {
		t.Errorf("stdout recorded a DEBUG entry before the level was lowered:\n%s", output)
	}
	if !strings.Contains(output, "debug after the change") {
		t.Errorf("stdout did not record a DEBUG entry after the level was lowered:\n%s", output)
	}
	if got := settings[pkglogger.LogOutputStdout].Level; got != pkglogger.LogLevelDebug || len(settings) != 1 {
		t.Errorf("OutputSettings() = %+v, want only stdout at DEBUG", settings)
	}
}

// 非同期のloggerでも同じく反映します。非同期のloggerはlogを後から処理し、処理する時点の
// 設定を使うため、変更の後に記録したlogだけで確かめます。
func TestUpdateOutputSettingsChangesTheLevelOfARunningAsyncLogger(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		name    string
		level   pkglogger.LogLevel
		log     func(*loggerruntime.AsyncRuntime)
		message string
		written bool
	}{
		{
			name: "lowered", level: pkglogger.LogLevelDebug, message: "async debug after lowering", written: true,
			log: func(runtime *loggerruntime.AsyncRuntime) { runtime.Debug(ctx, "async debug after lowering") },
		},
		{
			name: "raised", level: pkglogger.LogLevelError, message: "async info after raising", written: false,
			log: func(runtime *loggerruntime.AsyncRuntime) { runtime.Info(ctx, "async info after raising") },
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			output := testutil.CaptureStdout(t, func() {
				runtime, err := bootstrap.NewAsync(ctx, loggerruntime.DefaultAsyncConfig())
				if err != nil {
					t.Fatal(err)
				}
				if _, err := runtime.UpdateOutputSettings(map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
					pkglogger.LogOutputStdout: {Level: levelPointer(tt.level)},
				}); err != nil {
					t.Fatalf("UpdateOutputSettings: %v", err)
				}
				tt.log(runtime)
				if err := runtime.Close(); err != nil {
					t.Errorf("Close: %v", err)
				}
			})
			if got := strings.Contains(output, tt.message); got != tt.written {
				t.Errorf("stdout contains %q = %t, want %t:\n%s", tt.message, got, tt.written, output)
			}
		})
	}
}

// fileの出力先も、稼働中のlevelの変更に従います。
func TestUpdateOutputSettingsChangesTheLevelOfAFileOutput(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cfg := directoryWhitespaceConfig(root, "live-settings", "logs")
	runtime, err := bootstrap.New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Info(ctx, "info before the change"); err != nil {
		t.Fatalf("Info: %v", err)
	}
	if _, err := runtime.UpdateOutputSettings(map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
		pkglogger.LogOutputFile: {Level: levelPointer(pkglogger.LogLevelError)},
	}); err != nil {
		t.Fatalf("UpdateOutputSettings: %v", err)
	}
	if err := runtime.Info(ctx, "info after the change"); err != nil {
		t.Fatalf("Info: %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	contents := readFileLoggerOutputForTest(t, filepath.Join(root, "live-settings", "logs"))
	if !strings.Contains(contents, "info before the change") || strings.Contains(contents, "info after the change") {
		t.Errorf("file output = %q, want only the INFO entry written before the level was raised", contents)
	}
}

// samplingも稼働中に変更でき、無効に戻せば全件を記録します。
func TestUpdateOutputSettingsChangesSampling(t *testing.T) {
	ctx := context.Background()
	output := testutil.CaptureStdout(t, func() {
		runtime, err := bootstrap.New(ctx, loggerruntime.DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		// INFOの割合を最大にすると、1件だけ記録される確率は無視できるほど小さくなります。
		// 既定で有効なrequest IDでの選択は無効にし、INFOの割合だけで決まるようにします。
		on, off, one, maxRate := true, false, 1, 1<<31-1
		if _, err := runtime.UpdateOutputSettings(map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
			pkglogger.LogOutputStdout: {Sampling: &pkglogger.SamplingChange{
				IsSamplingEnabled: &on, EnableLoggingByRequestID: &off,
				SamplingDebugRate: &one, SamplingInfoRate: &maxRate, SamplingWarnRate: &one, SamplingErrorRate: &one,
			}},
		}); err != nil {
			t.Fatalf("UpdateOutputSettings(sampling on): %v", err)
		}
		if err := runtime.Info(ctx, "info while sampling"); err != nil {
			t.Errorf("Info: %v", err)
		}
		if _, err := runtime.UpdateOutputSettings(map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
			pkglogger.LogOutputStdout: {Sampling: &pkglogger.SamplingChange{IsSamplingEnabled: &off}},
		}); err != nil {
			t.Fatalf("UpdateOutputSettings(sampling off): %v", err)
		}
		if err := runtime.Info(ctx, "info after sampling"); err != nil {
			t.Errorf("Info: %v", err)
		}
		if err := runtime.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	if strings.Contains(output, "info while sampling") || !strings.Contains(output, "info after sampling") {
		t.Errorf("stdout = %q, want the entry dropped while sampling and recorded after it was turned off", output)
	}
}

// readFileLoggerOutputForTestは、directoryにあるlog fileの内容を連結して返します。
func readFileLoggerOutputForTest(t *testing.T, directory string) string {
	t.Helper()
	var contents strings.Builder
	err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		contents.Write(data)
		return nil
	})
	if err != nil {
		t.Fatalf("read file output: %v", err)
	}
	return contents.String()
}
