// Package outputsettingsは、稼働中のloggerの出力先ごとのlog levelとsampling設定を、
// 読み取り・変更する処理を提供します。値は各出力先が持つpkg/loggerのLiveSettingsに
// 保持し、出力先はlogごとに読むため、変更は次のlogから反映します。変更はメモリ上
// だけに保持します。
package outputsettings

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// Registryは、一つのloggerの出力先ごとのLiveSettingsを保持し、Controllerの処理を行います。
type Registry struct {
	// labelは、errorに含めるloggerの種類です。
	label string
	// muは変更を直列化し、一回の変更の全項目を反映してから次の変更が今の値を検証するように
	// します。変更で指定していない項目は、muの中で読んだ今の値を使うため、同時の変更が
	// 互いを打ち消しません。
	mu      sync.Mutex
	outputs map[pkglogger.LogOutputDestination]*pkglogger.LiveSettings
}

// NewRegistryは、outputsの出力先を扱うRegistryを返します。labelはerrorに含める
// loggerの種類です。outputsは複製して保持します。
func NewRegistry(label string, outputs map[pkglogger.LogOutputDestination]*pkglogger.LiveSettings) *Registry {
	return &Registry{label: label, outputs: maps.Clone(outputs)}
}

// Snapshotは、各出力先の現在の設定を返します。nilのRegistryは空のmapを返します。
func (r *Registry) Snapshot() map[pkglogger.LogOutputDestination]pkglogger.OutputSettings {
	result := map[pkglogger.LogOutputDestination]pkglogger.OutputSettings{}
	if r == nil {
		return result
	}
	for destination, live := range r.outputs {
		result[destination] = current(live)
	}
	return result
}

// Updateは、すべての変更を検証してから反映し、反映した出力先ごとに、反映の直前と直後の
// 設定を返します。どちらもmuの中で読むため、別の変更を含みません。直後の設定は、計算した
// 値ではなく、反映した後のLiveSettingsから読み直した値です。一つでも不正な変更があれば、
// どれも反映しません。loggerが使っていない出力先はErrUnknownOutput、不正な値は
// pkg/loggerのConfigFieldErrorです。nilのRegistryは、どの出力先も持たないものとして
// 扱います。
func (r *Registry) Update(changes map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange) (map[pkglogger.LogOutputDestination]pkglogger.AppliedOutputSettings, error) {
	if r == nil {
		r = &Registry{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	type plan struct {
		destination pkglogger.LogOutputDestination
		live        *pkglogger.LiveSettings
		before      pkglogger.OutputSettings
		next        pkglogger.OutputSettings
	}
	plans := make([]plan, 0, len(changes))
	var errs []error
	// 毎回同じ順で確かめ、errorを同じ順で報告します。
	for _, destination := range slices.Sorted(maps.Keys(changes)) {
		live, ok := r.outputs[destination]
		if !ok {
			errs = append(errs, fmt.Errorf("%w: %q", pkglogger.ErrUnknownOutput, destination))
			continue
		}
		before := current(live)
		next := apply(before, changes[destination])
		if err := validate(r.label, destination, next); err != nil {
			errs = append(errs, err)
			continue
		}
		plans = append(plans, plan{destination: destination, live: live, before: before, next: next})
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	applied := make(map[pkglogger.LogOutputDestination]pkglogger.AppliedOutputSettings, len(plans))
	for _, p := range plans {
		if err := p.live.Set(p.next.Level, p.next.Sampling); err != nil {
			return applied, err
		}
		applied[p.destination] = pkglogger.AppliedOutputSettings{Before: p.before, After: current(p.live)}
	}
	return applied, nil
}

// currentは、LiveSettingsの今の設定を返します。
func current(live *pkglogger.LiveSettings) pkglogger.OutputSettings {
	return pkglogger.OutputSettings{Level: live.Level(), Sampling: live.Sampling()}
}

// applyは、currentにchangeを適用した設定を返します。changeで指定していないsamplingの
// 項目は、currentの値のままです。
func apply(current pkglogger.OutputSettings, change pkglogger.OutputSettingsChange) pkglogger.OutputSettings {
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

// validateは、出力先の設定がpkg/loggerの規則を満たすかを確かめます。sourceはerrorに
// 含める設定の出所です。
func validate(source string, destination pkglogger.LogOutputDestination, settings pkglogger.OutputSettings) error {
	return errors.Join(
		pkglogger.ValidateLogLevel(source, destination, settings.Level),
		pkglogger.ValidateSamplingConfig(source, destination, settings.Sampling),
	)
}
