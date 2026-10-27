package outputsettings

import (
	"errors"
	"testing"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// registryControllerは、RegistryをControllerとして使う試験用の型です。
type registryController struct{ *Registry }

func (c registryController) OutputSettings() map[pkglogger.LogOutputDestination]pkglogger.OutputSettings {
	return c.Snapshot()
}

func (c registryController) UpdateOutputSettings(changes map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange) error {
	return c.Update(changes)
}

// failingControllerは、検証を通った変更の反映に失敗するControllerです。
type failingController struct{ registryController }

func (failingController) UpdateOutputSettings(map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange) error {
	return errors.New("applying failed")
}

func newControllerForTest(t *testing.T) registryController {
	t.Helper()
	live, err := pkglogger.NewLiveSettings(pkglogger.LogLevelInfo, pkglogger.SamplingConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return registryController{NewRegistry("test logger", map[pkglogger.LogOutputDestination]*pkglogger.LiveSettings{
		pkglogger.LogOutputStdout: live,
	})}
}

func level(l pkglogger.LogLevel) *pkglogger.LogLevel { return &l }

func ptr[T any](v T) *T { return &v }

func TestNewServiceRequiresALogger(t *testing.T) {
	if _, err := NewService(nil); !errors.Is(err, ErrNoLoggers) {
		t.Errorf("NewService(nil) error = %v, want ErrNoLoggers", err)
	}
}

// Updateは全loggerの変更を検証してから反映し、反映した変更を名前と出力先の順に返します。
func TestServiceUpdateAppliesAndReportsEveryChange(t *testing.T) {
	a, b := newControllerForTest(t), newControllerForTest(t)
	service, err := NewService(map[string]pkglogger.SettingsController{"a": a, "b": b})
	if err != nil {
		t.Fatal(err)
	}
	events, err := service.Update(map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
		"b": {pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelError)}},
		"a": {pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug)}},
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if len(events) != 2 || events[0].Logger != "a" || events[1].Logger != "b" ||
		events[0].Before.Level != pkglogger.LogLevelInfo || events[0].After.Level != pkglogger.LogLevelDebug ||
		events[1].After.Level != pkglogger.LogLevelError {
		t.Errorf("events = %+v, want a then b with their before and after levels", events)
	}
	if got := service.Settings()["a"][pkglogger.LogOutputStdout].Level; got != pkglogger.LogLevelDebug {
		t.Errorf("a level = %v, want DEBUG", got)
	}
}

// 一つでも不正な変更があれば、別のloggerの有効な変更も反映しません。
func TestServiceUpdateAppliesNothingWhenAnyChangeIsInvalid(t *testing.T) {
	a, b := newControllerForTest(t), newControllerForTest(t)
	service, err := NewService(map[string]pkglogger.SettingsController{"a": a, "b": b})
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name    string
		changes map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange
		want    error
	}{
		{"invalid level", map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
			"a": {pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug)}},
			"b": {pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevel(42))}},
		}, pkglogger.ErrInvalidLogLevel},
		{"invalid sampling", map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
			"a": {pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug)}},
			"b": {pkglogger.LogOutputStdout: {Sampling: &pkglogger.SamplingChange{IsSamplingEnabled: ptr(true)}}},
		}, pkglogger.ErrInvalidSamplingRate},
		{"unknown logger", map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
			"a":       {pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug)}},
			"missing": {pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug)}},
		}, ErrUnknownLogger},
		{"unknown output", map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
			"a": {pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug)}},
			"b": {pkglogger.LogOutputFile: {Level: level(pkglogger.LogLevelDebug)}},
		}, pkglogger.ErrUnknownOutput},
	} {
		t.Run(tt.name, func(t *testing.T) {
			events, err := service.Update(tt.changes)
			if !errors.Is(err, tt.want) || len(events) != 0 {
				t.Fatalf("Update() = %+v, %v; want no events and %v", events, err, tt.want)
			}
			for name, outputs := range service.Settings() {
				if got := outputs[pkglogger.LogOutputStdout].Level; got != pkglogger.LogLevelInfo {
					t.Errorf("%s level = %v after a rejected update, want INFO", name, got)
				}
			}
		})
	}
}

// 検証の後で反映に失敗した場合は、それまでに反映した変更とErrApplyFailedを返します。
func TestServiceUpdateReportsAFailureAfterValidation(t *testing.T) {
	a := newControllerForTest(t)
	b := failingController{newControllerForTest(t)}
	service, err := NewService(map[string]pkglogger.SettingsController{"a": a, "b": b})
	if err != nil {
		t.Fatal(err)
	}
	events, err := service.Update(map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
		"a": {pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug)}},
		"b": {pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug)}},
	})
	if !errors.Is(err, ErrApplyFailed) || len(events) != 1 || events[0].Logger != "a" {
		t.Errorf("Update() = %+v, %v; want the change of a and ErrApplyFailed", events, err)
	}
}
