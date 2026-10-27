package loggerruntime

import (
	"errors"

	"github.com/go-jappe/heterolog/internal/application/outputsettings"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

var (
	_ pkglogger.SettingsController = (*Runtime)(nil)
	_ pkglogger.SettingsController = (*AsyncRuntime)(nil)
)

// attachOutputSettingsは、通常の出力先それぞれに、設定がまだ持っていなければ専用の
// LiveSettingsを持たせ、それらを返します。呼び出し元の設定を変えないよう、設定の
// pointerを複製に置き換えます。内部logの通知だけに使う出力先は、固定の設定のままです。
func attachOutputSettings(
	label string,
	destinations pkglogger.LogOutputDestinations,
	file **pkglogger.FileLoggerConfig,
	stdout **pkglogger.StdoutLoggerConfig,
	slack **pkglogger.SlackLoggerConfig,
	openSearch **pkglogger.OpenSearchConfig,
	loki **pkglogger.LokiConfig,
	cloudWatchLogs **pkglogger.CloudWatchLogsConfig,
	clickHouse **pkglogger.ClickHouseConfig,
) (*outputsettings.Registry, error) {
	outputs := map[pkglogger.LogOutputDestination]*pkglogger.LiveSettings{}
	enabled := func(destination pkglogger.LogOutputDestination) bool {
		return len(destinations) == 0 || destinations.Has(destination)
	}
	attach := func(destination pkglogger.LogOutputDestination, runtime *pkglogger.RuntimeConfig, sampling pkglogger.SamplingConfig) error {
		if runtime.Live == nil {
			live, err := pkglogger.NewLiveSettings(runtime.LogLevel, sampling)
			if err != nil {
				return err
			}
			runtime.Live = live
		}
		outputs[destination] = runtime.Live
		return nil
	}
	var errs []error
	if *file != nil && enabled(pkglogger.LogOutputFile) {
		copied := **file
		errs = append(errs, attach(pkglogger.LogOutputFile, &copied.RuntimeConfig, copied.SamplingConfig))
		*file = &copied
	}
	if *stdout != nil && enabled(pkglogger.LogOutputStdout) {
		copied := **stdout
		errs = append(errs, attach(pkglogger.LogOutputStdout, &copied.RuntimeConfig, copied.SamplingConfig))
		*stdout = &copied
	}
	if *slack != nil && enabled(pkglogger.LogOutputSlack) {
		copied := **slack
		errs = append(errs, attach(pkglogger.LogOutputSlack, &copied.RuntimeConfig, copied.SamplingConfig))
		*slack = &copied
	}
	if *openSearch != nil && enabled(pkglogger.LogOutputOpenSearch) {
		copied := **openSearch
		errs = append(errs, attach(pkglogger.LogOutputOpenSearch, &copied.RuntimeConfig, copied.SamplingConfig))
		*openSearch = &copied
	}
	if *loki != nil && enabled(pkglogger.LogOutputLoki) {
		copied := **loki
		errs = append(errs, attach(pkglogger.LogOutputLoki, &copied.RuntimeConfig, copied.SamplingConfig))
		*loki = &copied
	}
	if *cloudWatchLogs != nil && enabled(pkglogger.LogOutputCloudWatchLogs) {
		copied := **cloudWatchLogs
		errs = append(errs, attach(pkglogger.LogOutputCloudWatchLogs, &copied.RuntimeConfig, copied.SamplingConfig))
		*cloudWatchLogs = &copied
	}
	if *clickHouse != nil && enabled(pkglogger.LogOutputClickHouse) {
		copied := **clickHouse
		errs = append(errs, attach(pkglogger.LogOutputClickHouse, &copied.RuntimeConfig, copied.SamplingConfig))
		*clickHouse = &copied
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return outputsettings.NewRegistry(label, outputs), nil
}

// OutputSettingsは、loggerの各出力先の現在の設定を返します。内部logの通知だけに使う
// 出力先は含みません。
func (r *Runtime) OutputSettings() map[pkglogger.LogOutputDestination]pkglogger.OutputSettings {
	return r.outputs.Snapshot()
}

// UpdateOutputSettingsは、稼働中のloggerの出力先のlevelとsamplingを変えます。各変更は
// 反映する時点の設定に適用するため、別の項目への同時の変更が互いを打ち消しません。すべての
// 変更を先に確かめ、一つでも不正な変更があれば、どれも反映しません。loggerが使って
// いない出力先はpkg/loggerのErrUnknownOutput、不正な値はpkg/loggerの
// ConfigFieldErrorで報告します。
// 変更は次のlogから反映し、processが終了すると残りません。
func (r *Runtime) UpdateOutputSettings(changes map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange) error {
	return r.outputs.Update(changes)
}

// OutputSettingsは、Runtime.OutputSettingsと同じく、loggerの各出力先の現在の設定を
// 返します。
func (r *AsyncRuntime) OutputSettings() map[pkglogger.LogOutputDestination]pkglogger.OutputSettings {
	return r.outputs.Snapshot()
}

// UpdateOutputSettingsは、Runtime.UpdateOutputSettingsと同じく、稼働中のloggerの
// 出力先のlevelとsamplingを変えます。非同期loggerは、logを処理する時点の設定を使います。
func (r *AsyncRuntime) UpdateOutputSettings(changes map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange) error {
	return r.outputs.Update(changes)
}
