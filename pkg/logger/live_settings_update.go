package logger

import (
	"errors"
	"fmt"
	"maps"
	"slices"
)

// UpdateLiveSettings validates and applies a batch of partial settings changes.
// It serializes the entire operation with Set and other batches, even when
// outputs belong to different runtimes. On validation failure nothing changes.
// source identifies the configuration in validation errors. Unknown destinations
// return ErrUnknownOutput. Aliases of the same LiveSettings are merged in
// destination order and return the same original and final settings.
// Each value in outputs must be a non-nil LiveSettings. The caller must not
// modify outputs or changes while this function runs.
func UpdateLiveSettings(source string, outputs map[LogOutputDestination]*LiveSettings, changes map[LogOutputDestination]OutputSettingsChange) (map[LogOutputDestination]AppliedOutputSettings, error) {
	liveSettingsUpdateMu.Lock()
	defer liveSettingsUpdateMu.Unlock()
	type plan struct {
		destination LogOutputDestination
		live        *LiveSettings
		before      OutputSettings
	}
	plans := make([]plan, 0, len(changes))
	staged := make(map[*LiveSettings]OutputSettings)
	var errs []error
	// 毎回同じ順で確かめ、errorを同じ順で報告します。
	for _, destination := range slices.Sorted(maps.Keys(changes)) {
		live, ok := outputs[destination]
		if !ok {
			errs = append(errs, fmt.Errorf("%w: %q", ErrUnknownOutput, destination))
			continue
		}
		before := currentLiveSettings(live)
		base, ok := staged[live]
		if !ok {
			base = before
		}
		next := applyLiveSettingsChange(base, changes[destination])
		if err := validateLiveSettings(source, destination, next); err != nil {
			errs = append(errs, err)
			continue
		}
		staged[live] = next
		plans = append(plans, plan{destination: destination, live: live, before: before})
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	for live, next := range staged {
		live.store(next)
	}
	applied := make(map[LogOutputDestination]AppliedOutputSettings, len(plans))
	for _, p := range plans {
		applied[p.destination] = AppliedOutputSettings{Before: p.before, After: currentLiveSettings(p.live)}
	}
	return applied, nil
}

// currentLiveSettingsは、levelとsamplingの両方を読みます。liveSettingsUpdateMuを持って
// 呼びます。
func currentLiveSettings(live *LiveSettings) OutputSettings {
	return OutputSettings{Level: live.Level(), Sampling: live.Sampling()}
}

// applyLiveSettingsChangeは、currentにchangeを当てた設定を返します。changeが指定しない
// 項目は、currentの値のままです。
func applyLiveSettingsChange(current OutputSettings, change OutputSettingsChange) OutputSettings {
	if change.Level != nil {
		current.Level = *change.Level
	}
	if s := change.Sampling; s != nil {
		setIfPresent(&current.Sampling.IsSamplingEnabled, s.IsSamplingEnabled)
		setIfPresent(&current.Sampling.EnableLoggingByRequestID, s.EnableLoggingByRequestID)
		setIfPresent(&current.Sampling.LoggingByRequestIDRate, s.LoggingByRequestIDRate)
		setIfPresent(&current.Sampling.SamplingDebugRate, s.SamplingDebugRate)
		setIfPresent(&current.Sampling.SamplingInfoRate, s.SamplingInfoRate)
		setIfPresent(&current.Sampling.SamplingWarnRate, s.SamplingWarnRate)
		setIfPresent(&current.Sampling.SamplingErrorRate, s.SamplingErrorRate)
	}
	return current
}

func setIfPresent[T any](target *T, value *T) {
	if value != nil {
		*target = *value
	}
}

// validateLiveSettingsは、設定がpkg/loggerの規則を満たすかを確かめます。errorには、設定の
// 出所のsourceと出力先を含めます。
func validateLiveSettings(source string, destination LogOutputDestination, settings OutputSettings) error {
	return errors.Join(
		ValidateLogLevel(source, destination, settings.Level),
		ValidateSamplingConfig(source, destination, settings.Sampling),
	)
}
