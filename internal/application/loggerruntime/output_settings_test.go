package loggerruntime_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func levelPointer(level pkglogger.LogLevel) *pkglogger.LogLevel { return &level }

// 稼働中に出力先のlevelを変えると、次のlogから反映します。
func TestUpdateOutputSettingsChangesTheLevelOfARunningLogger(t *testing.T) {
	ctx := context.Background()
	var settings map[pkglogger.LogOutputDestination]pkglogger.OutputSettings
	output := testutil.CaptureStdout(t, func() {
		runtime, err := loggerruntime.New(ctx, loggerruntime.DefaultConfig())
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
				runtime, err := loggerruntime.NewAsync(ctx, loggerruntime.DefaultAsyncConfig())
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
	runtime, err := loggerruntime.New(ctx, cfg)
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
		runtime, err := loggerruntime.New(ctx, loggerruntime.DefaultConfig())
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

// 設定にLiveを渡した場合はそれを共有し、どちらから変えても同じ値になります。
func TestNewUsesTheLiveSettingsOfTheConfig(t *testing.T) {
	ctx := context.Background()
	live, err := pkglogger.NewLiveSettings(pkglogger.LogLevelWarn, pkglogger.SamplingConfig{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := loggerruntime.DefaultConfig()
	cfg.Stdout.RuntimeConfig.Live = live
	runtime, err := loggerruntime.New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	if got := runtime.OutputSettings()[pkglogger.LogOutputStdout].Level; got != pkglogger.LogLevelWarn {
		t.Fatalf("stdout level = %v, want WARN from the Live settings", got)
	}
	if _, err := runtime.UpdateOutputSettings(map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
		pkglogger.LogOutputStdout: {Level: levelPointer(pkglogger.LogLevelError)},
	}); err != nil {
		t.Fatal(err)
	}
	if got := live.Level(); got != pkglogger.LogLevelError {
		t.Errorf("Live level = %v after UpdateOutputSettings, want ERROR", got)
	}
}

// OutputLiveSettingsは、管理用APIが共有を確かめられるよう、出力先が実際に使う
// LiveSettingsを返します。設定で渡したものはそのまま、渡さない出力先には専用に作った
// ものを返します。
func TestOutputLiveSettingsReturnsTheLiveSettingsInUse(t *testing.T) {
	ctx := context.Background()
	live, err := pkglogger.NewLiveSettings(pkglogger.LogLevelWarn, pkglogger.SamplingConfig{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := loggerruntime.DefaultConfig()
	cfg.Stdout.RuntimeConfig.Live = live
	runtime, err := loggerruntime.New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	if got := runtime.OutputLiveSettings(); len(got) != 1 || got[pkglogger.LogOutputStdout] != live {
		t.Errorf("Runtime.OutputLiveSettings() = %v, want only the Live of the stdout configuration", got)
	}

	async, err := loggerruntime.NewAsync(ctx, loggerruntime.DefaultAsyncConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = async.Close() })
	created := async.OutputLiveSettings()[pkglogger.LogOutputStdout]
	if created == nil || created == live {
		t.Fatalf("AsyncRuntime.OutputLiveSettings()[stdout] = %p, want its own LiveSettings", created)
	}
	if err := created.Set(pkglogger.LogLevelError, created.Sampling()); err != nil {
		t.Fatal(err)
	}
	if got := async.OutputSettings()[pkglogger.LogOutputStdout].Level; got != pkglogger.LogLevelError {
		t.Errorf("stdout level = %v after setting the returned LiveSettings, want ERROR", got)
	}
}

// 別々のRuntimeも、共有するLiveSettingsの読み取りから反映までを直列化します。
func TestRuntimesSharingLiveSettingsKeepConcurrentPartialUpdates(t *testing.T) {
	previous := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	initial := pkglogger.SamplingConfig{IsSamplingEnabled: true,
		SamplingDebugRate: 1, SamplingInfoRate: 1, SamplingWarnRate: 1, SamplingErrorRate: 1}
	live, err := pkglogger.NewLiveSettings(pkglogger.LogLevelInfo, initial)
	if err != nil {
		t.Fatal(err)
	}
	cfg := loggerruntime.DefaultConfig()
	cfg.Stdout.RuntimeConfig.Live = live
	var runtimes [2]*loggerruntime.Runtime
	for i := range runtimes {
		runtimes[i], err = loggerruntime.New(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = runtimes[i].Close() })
	}
	info, warn := 3, 4
	changes := [2]pkglogger.SamplingChange{{SamplingInfoRate: &info}, {SamplingWarnRate: &warn}}
	type result struct {
		applied pkglogger.AppliedOutputSettings
		err     error
	}
	for attempt := 0; attempt < 10000; attempt++ {
		if err := live.Set(pkglogger.LogLevelInfo, initial); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		results := make(chan result, 2)
		for i, logger := range runtimes {
			go func() {
				<-start
				applied, err := logger.UpdateOutputSettings(map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
					pkglogger.LogOutputStdout: {Sampling: &changes[i]},
				})
				results <- result{applied[pkglogger.LogOutputStdout], err}
			}()
		}
		close(start)
		first, second := <-results, <-results
		if first.err != nil || second.err != nil {
			t.Fatalf("UpdateOutputSettings: %v, %v", first.err, second.err)
		}
		got := live.Sampling()
		if got.SamplingInfoRate != info || got.SamplingWarnRate != warn {
			t.Fatalf("attempt %d: lost a successful change: %+v", attempt, got)
		}
		// Completion order may differ from update order, but the transitions must
		// form one history rather than both claiming the same starting state.
		if first.applied.After != second.applied.Before && second.applied.After != first.applied.Before {
			t.Fatalf("attempt %d: inconsistent transitions: %+v, %+v", attempt, first.applied, second.applied)
		}
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
