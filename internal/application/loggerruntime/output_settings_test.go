package loggerruntime_test

import (
	"context"
	"runtime"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func levelPointer(level pkglogger.LogLevel) *pkglogger.LogLevel { return &level }

// 設定にLiveを渡した場合はそれを共有し、どちらから変えても同じ値になります。
func TestNewUsesTheLiveSettingsOfTheConfig(t *testing.T) {
	ctx := context.Background()
	live, err := pkglogger.NewLiveSettings(pkglogger.LogLevelWarn, pkglogger.SamplingConfig{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := loggerruntime.DefaultConfig()
	cfg.Stdout.RuntimeConfig.Live = live
	runtime, err := loggerruntime.New(ctx, &recordingBuilder{}, cfg)
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
	runtime, err := loggerruntime.New(ctx, &recordingBuilder{}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	if got := runtime.OutputLiveSettings(); len(got) != 1 || got[pkglogger.LogOutputStdout] != live {
		t.Errorf("Runtime.OutputLiveSettings() = %v, want only the Live of the stdout configuration", got)
	}

	async, err := loggerruntime.NewAsync(ctx, &recordingBuilder{}, loggerruntime.DefaultAsyncConfig())
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
		runtimes[i], err = loggerruntime.New(context.Background(), &recordingBuilder{}, cfg)
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
		// 終わる順は反映の順と違うことがあります。それでも二つの変化は一続きの履歴になり、
		// 両方が同じ変更前の状態から始まったことにはなりません。
		if first.applied.After != second.applied.Before && second.applied.After != first.applied.Before {
			t.Fatalf("attempt %d: inconsistent transitions: %+v, %+v", attempt, first.applied, second.applied)
		}
	}
}
