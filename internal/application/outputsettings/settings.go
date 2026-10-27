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
		result[destination] = pkglogger.OutputSettings{Level: live.Level(), Sampling: live.Sampling()}
	}
	return result
}

// Updateは、すべての変更を検証してから反映します。一つでも不正な変更があれば、どれも
// 反映しません。loggerが使っていない出力先はErrUnknownOutput、不正な値はpkg/loggerの
// ConfigFieldErrorです。nilのRegistryは、どの出力先も持たないものとして扱います。
func (r *Registry) Update(changes map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange) error {
	if r == nil {
		r = &Registry{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	type plan struct {
		live     *pkglogger.LiveSettings
		settings pkglogger.OutputSettings
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
		next := apply(pkglogger.OutputSettings{Level: live.Level(), Sampling: live.Sampling()}, changes[destination])
		if err := validate(r.label, destination, next); err != nil {
			errs = append(errs, err)
			continue
		}
		plans = append(plans, plan{live: live, settings: next})
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	for _, p := range plans {
		if err := p.live.Set(p.settings.Level, p.settings.Sampling); err != nil {
			return err
		}
	}
	return nil
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
