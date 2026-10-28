// Package outputsettingsは、稼働中のloggerの出力先ごとのlog levelとsampling設定を、
// 読み取り・変更する処理を提供します。値は各出力先が持つpkg/loggerのLiveSettingsに
// 保持し、出力先はlogごとに読むため、変更は次のlogから反映します。変更はメモリ上
// だけに保持します。
package outputsettings

import (
	"maps"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// Registryは、一つのloggerの出力先ごとのLiveSettingsを保持し、Controllerの処理を行います。
type Registry struct {
	// labelは、errorに含めるloggerの種類です。
	label   string
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

// OutputLiveSettingsは、出力先ごとのLiveSettingsを複製したmapで返します。nilの
// Registryは空のmapを返します。
func (r *Registry) OutputLiveSettings() map[pkglogger.LogOutputDestination]*pkglogger.LiveSettings {
	result := map[pkglogger.LogOutputDestination]*pkglogger.LiveSettings{}
	if r == nil {
		return result
	}
	maps.Copy(result, r.outputs)
	return result
}

// Updateは、すべての変更を検証してから反映し、反映した出力先ごとに、反映の直前と直後の
// 設定を返します。共有するLiveSettingsの変更も直列化し、別の変更を含めません。直後の
// 設定は、計算した値ではなく、反映した後のLiveSettingsから読み直した値です。一つでも
// 不正な変更があれば、どれも反映しません。loggerが使っていない出力先はErrUnknownOutput、
// 不正な値はpkg/loggerのConfigFieldErrorです。nilのRegistryは、どの出力先も持たないもの
// として扱います。
func (r *Registry) Update(changes map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange) (map[pkglogger.LogOutputDestination]pkglogger.AppliedOutputSettings, error) {
	if r == nil {
		r = &Registry{}
	}
	return pkglogger.UpdateLiveSettings(r.label, r.outputs, changes)
}

// currentは、LiveSettingsの今の設定を返します。
func current(live *pkglogger.LiveSettings) pkglogger.OutputSettings {
	return pkglogger.OutputSettings{Level: live.Level(), Sampling: live.Sampling()}
}
