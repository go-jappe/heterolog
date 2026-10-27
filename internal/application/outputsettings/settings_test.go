package outputsettings

import (
	"errors"
	"testing"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func newRegistryForTest(t *testing.T) (*Registry, *pkglogger.LiveSettings) {
	t.Helper()
	live, err := pkglogger.NewLiveSettings(pkglogger.LogLevelInfo, pkglogger.SamplingConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return NewRegistry("test logger", map[pkglogger.LogOutputDestination]*pkglogger.LiveSettings{
		pkglogger.LogOutputStdout: live,
	}), live
}

// Updateは出力先のLiveSettingsを変え、Snapshotは変えた後の値を返します。
func TestRegistryUpdateChangesTheLiveSettings(t *testing.T) {
	registry, live := newRegistryForTest(t)
	sampling := pkglogger.SamplingConfig{IsSamplingEnabled: true, SamplingDebugRate: 1, SamplingInfoRate: 5, SamplingWarnRate: 1, SamplingErrorRate: 1}
	if err := registry.Update(map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
		pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug), Sampling: &sampling},
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if live.Level() != pkglogger.LogLevelDebug || live.Sampling() != sampling {
		t.Errorf("LiveSettings = %v, %+v; want DEBUG with the new sampling", live.Level(), live.Sampling())
	}
	if got := registry.Snapshot()[pkglogger.LogOutputStdout]; got.Level != pkglogger.LogLevelDebug || got.Sampling != sampling {
		t.Errorf("Snapshot() = %+v, want the updated settings", got)
	}
	// 省略した項目は今の値のままです。
	if err := registry.Update(map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
		pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelWarn)},
	}); err != nil {
		t.Fatalf("Update(level only): %v", err)
	}
	if live.Level() != pkglogger.LogLevelWarn || live.Sampling() != sampling {
		t.Errorf("LiveSettings = %v, %+v; want WARN with the previous sampling", live.Level(), live.Sampling())
	}
}

// 一つでも不正な変更があれば、どの変更も反映しません。
func TestRegistryUpdateAppliesNothingWhenAChangeIsInvalid(t *testing.T) {
	registry, live := newRegistryForTest(t)
	for _, tt := range []struct {
		name    string
		changes map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange
		want    error
	}{
		{"unknown output", map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
			pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug)},
			pkglogger.LogOutputFile:   {Level: level(pkglogger.LogLevelDebug)},
		}, pkglogger.ErrUnknownOutput},
		{"invalid level", map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
			pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevel(42))},
		}, pkglogger.ErrInvalidLogLevel},
		{"invalid sampling rate", map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
			pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug), Sampling: &pkglogger.SamplingConfig{IsSamplingEnabled: true}},
		}, pkglogger.ErrInvalidSamplingRate},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := registry.Update(tt.changes); !errors.Is(err, tt.want) {
				t.Fatalf("Update() = %v, want %v", err, tt.want)
			}
			if live.Level() != pkglogger.LogLevelInfo || live.Sampling().IsSamplingEnabled {
				t.Errorf("LiveSettings = %v, %+v after a rejected update, want the original INFO without sampling", live.Level(), live.Sampling())
			}
		})
	}
}

// nilのRegistry（設定を持たないlogger）は、空の設定を返し、どの変更もErrUnknownOutputです。
func TestNilRegistryHasNoOutputs(t *testing.T) {
	var registry *Registry
	if got := registry.Snapshot(); len(got) != 0 {
		t.Errorf("Snapshot() = %v, want empty", got)
	}
	if err := registry.Update(map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
		pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug)},
	}); !errors.Is(err, pkglogger.ErrUnknownOutput) {
		t.Errorf("Update() = %v, want ErrUnknownOutput", err)
	}
}
