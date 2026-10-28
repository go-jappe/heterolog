package outputsettings

import (
	"errors"
	"sync"
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
	if _, err := registry.Update(map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
		pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug), Sampling: &pkglogger.SamplingChange{
			IsSamplingEnabled: ptr(true), SamplingDebugRate: ptr(1), SamplingInfoRate: ptr(5), SamplingWarnRate: ptr(1), SamplingErrorRate: ptr(1),
		}},
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
	if _, err := registry.Update(map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
		pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelWarn)},
	}); err != nil {
		t.Fatalf("Update(level only): %v", err)
	}
	if live.Level() != pkglogger.LogLevelWarn || live.Sampling() != sampling {
		t.Errorf("LiveSettings = %v, %+v; want WARN with the previous sampling", live.Level(), live.Sampling())
	}
}

// Updateは、変えた出力先ごとに、反映の直前と直後の設定を返します。直後の設定は、反映した
// 後のLiveSettingsから読み直した値です。
func TestRegistryUpdateReturnsTheSettingsRightBeforeAndAfterTheChange(t *testing.T) {
	registry, live := newRegistryForTest(t)
	applied, err := registry.Update(map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
		pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug), Sampling: &pkglogger.SamplingChange{SamplingInfoRate: ptr(7)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := pkglogger.AppliedOutputSettings{
		Before: pkglogger.OutputSettings{Level: pkglogger.LogLevelInfo},
		After:  pkglogger.OutputSettings{Level: pkglogger.LogLevelDebug, Sampling: pkglogger.SamplingConfig{SamplingInfoRate: 7}},
	}
	if len(applied) != 1 || applied[pkglogger.LogOutputStdout] != want {
		t.Fatalf("Update() = %+v, want only stdout with %+v", applied, want)
	}
	if got := (pkglogger.OutputSettings{Level: live.Level(), Sampling: live.Sampling()}); got != want.After {
		t.Errorf("LiveSettings = %+v, want the returned after settings %+v", got, want.After)
	}
}

// samplingの変更は指定した項目だけを変え、ほかの項目は反映する時点の値のままです。
func TestRegistryUpdateChangesOnlyTheSamplingFieldsItSets(t *testing.T) {
	registry, live := newRegistryForTest(t)
	for _, change := range []pkglogger.SamplingChange{
		{IsSamplingEnabled: ptr(true), EnableLoggingByRequestID: ptr(true), LoggingByRequestIDRate: ptr(3),
			SamplingDebugRate: ptr(1), SamplingInfoRate: ptr(1), SamplingWarnRate: ptr(1), SamplingErrorRate: ptr(1)},
		{SamplingInfoRate: ptr(10)},
		{SamplingWarnRate: ptr(20)},
	} {
		if _, err := registry.Update(map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
			pkglogger.LogOutputStdout: {Sampling: &change},
		}); err != nil {
			t.Fatalf("Update(%+v): %v", change, err)
		}
	}
	want := pkglogger.SamplingConfig{IsSamplingEnabled: true, EnableLoggingByRequestID: true, LoggingByRequestIDRate: 3,
		SamplingDebugRate: 1, SamplingInfoRate: 10, SamplingWarnRate: 20, SamplingErrorRate: 1}
	if got := live.Sampling(); got != want || live.Level() != pkglogger.LogLevelInfo {
		t.Errorf("LiveSettings = %v, %+v; want INFO with %+v", live.Level(), got, want)
	}
}

// 別の項目を変える同時のUpdateは、どれも反映され、互いを打ち消しません。
func TestRegistryConcurrentUpdatesOfDifferentSamplingFieldsAllApply(t *testing.T) {
	registry, live := newRegistryForTest(t)
	changes := []pkglogger.SamplingChange{
		{SamplingDebugRate: ptr(2)},
		{SamplingInfoRate: ptr(3)},
		{SamplingWarnRate: ptr(4)},
		{SamplingErrorRate: ptr(5)},
		{LoggingByRequestIDRate: ptr(6)},
	}
	start := make(chan struct{})
	errs := make(chan error, len(changes))
	var wg sync.WaitGroup
	for _, change := range changes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := registry.Update(map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
				pkglogger.LogOutputStdout: {Sampling: &change},
			})
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
	}
	want := pkglogger.SamplingConfig{SamplingDebugRate: 2, SamplingInfoRate: 3, SamplingWarnRate: 4, SamplingErrorRate: 5, LoggingByRequestIDRate: 6}
	if got := live.Sampling(); got != want {
		t.Errorf("Sampling() = %+v, want every concurrent change %+v", got, want)
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
			pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug), Sampling: &pkglogger.SamplingChange{IsSamplingEnabled: ptr(true)}},
		}, pkglogger.ErrInvalidSamplingRate},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := registry.Update(tt.changes); !errors.Is(err, tt.want) {
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
	if _, err := registry.Update(map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
		pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug)},
	}); !errors.Is(err, pkglogger.ErrUnknownOutput) {
		t.Errorf("Update() = %v, want ErrUnknownOutput", err)
	}
}
