package outputsettings

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

var (
	// ErrNoLoggersは、Serviceに管理するloggerが一つもないことを表します。
	ErrNoLoggers = errors.New("no loggers to manage")
	// ErrUnknownLoggerは、Serviceにない名前のloggerへの変更であることを表します。
	ErrUnknownLogger = errors.New("unknown logger")
	// ErrApplyFailedは、検証を通った変更を反映できなかったことを表します。検証の後で
	// 別の変更と競合した場合などに生じ、それより前のloggerの変更は反映済みです。
	ErrApplyFailed = errors.New("output settings change could not be applied")
)

// Serviceは、名前を付けた複数のloggerの出力先の設定を、まとめて読み取り・変更します。
// 管理用APIの処理です。
type Service struct {
	loggers map[string]pkglogger.SettingsController
}

// NewServiceは、loggersを扱うServiceを返します。loggersは複製して保持します。
// loggerが一つもない場合はErrNoLoggersを返します。
func NewService(loggers map[string]pkglogger.SettingsController) (*Service, error) {
	if len(loggers) == 0 {
		return nil, ErrNoLoggers
	}
	return &Service{loggers: maps.Clone(loggers)}, nil
}

// Eventは、反映した一つの出力先の変更です。
type Event struct {
	Logger string
	Output pkglogger.LogOutputDestination
	Before pkglogger.OutputSettings
	After  pkglogger.OutputSettings
}

// Settingsは、loggerの名前ごとに、各出力先の現在の設定を返します。
func (s *Service) Settings() map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettings {
	result := make(map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettings, len(s.loggers))
	for name, logger := range s.loggers {
		result[name] = logger.OutputSettings()
	}
	return result
}

// Updateは、全loggerの全変更を検証してから反映し、反映した変更をloggerの名前と出力先の
// 順に返します。一つでも不正な変更があれば、どれも反映しません。Serviceにないloggerは
// ErrUnknownLogger、loggerが使っていない出力先はErrUnknownOutput、不正な値は
// pkg/loggerのConfigFieldErrorです。検証の後で反映に失敗した場合は、それまでに反映した
// 変更と、ErrApplyFailedを包んだerrorを返します。
func (s *Service) Update(changes map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange) ([]Event, error) {
	type plan struct {
		name    string
		logger  pkglogger.SettingsController
		before  map[pkglogger.LogOutputDestination]pkglogger.OutputSettings
		changes map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange
	}
	var plans []plan
	for _, name := range slices.Sorted(maps.Keys(changes)) {
		logger, ok := s.loggers[name]
		if !ok {
			return nil, fmt.Errorf("%w: %q", ErrUnknownLogger, name)
		}
		before := logger.OutputSettings()
		for _, output := range slices.Sorted(maps.Keys(changes[name])) {
			current, ok := before[output]
			if !ok {
				return nil, fmt.Errorf("logger %q: %w: %q", name, pkglogger.ErrUnknownOutput, output)
			}
			if err := validate(name, output, apply(current, changes[name][output])); err != nil {
				return nil, err
			}
		}
		plans = append(plans, plan{name: name, logger: logger, before: before, changes: changes[name]})
	}
	var events []Event
	for _, p := range plans {
		if err := p.logger.UpdateOutputSettings(p.changes); err != nil {
			return events, fmt.Errorf("%w: logger %q: %w", ErrApplyFailed, p.name, err)
		}
		after := p.logger.OutputSettings()
		for _, output := range slices.Sorted(maps.Keys(p.changes)) {
			events = append(events, Event{Logger: p.name, Output: output, Before: p.before[output], After: after[output]})
		}
	}
	return events, nil
}
