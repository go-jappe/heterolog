package logger

import (
	"errors"
	"fmt"
	"time"

	pkgslack "github.com/go-jappe/heterolog/pkg/slack"
)

func validateLoggerSlackBackendConfig(
	sourceLabel string,
	cfg *SlackLoggerConfig,
) error {
	return errors.Join(
		validateSlackLoggerConfig(sourceLabel, cfg.MaxLogsPerNotification, cfg.Intervals, cfg.MaxRestThreshold),
		validateSlackBackend(sourceLabel, cfg.Token, cfg.Channel),
	)
}

func validateDeliverySlackBackendConfig(
	sourceLabel string,
	cfg *DeliverySlackLoggerConfig,
) error {
	return validateSlackBackend(sourceLabel, cfg.Token, cfg.Channel)
}

func validateSlackBackend(sourceLabel, token, channel string) error {
	if err := pkgslack.ValidateConfig(pkgslack.Config{
		Token:   token,
		Channel: channel,
	}); err != nil {
		return backendRuleError(
			sourceLabel, LogOutputSlack, ConfigFieldClient, nil, ErrInvalidClientConfig,
			"slack config is invalid", err,
		)
	}
	return nil
}

// ValidateSlackMaxLogsPerNotification validates that the flush threshold on
// the number of logs accumulated into one Slack notification is positive. A
// value of 0 or less is reported as a ConfigFieldError with the kind
// ErrNonPositiveSetting.
func ValidateSlackMaxLogsPerNotification(maxLogs int) error {
	if maxLogs <= 0 {
		return settingError(
			ConfigFieldMaxLogsPerNotification, ErrNonPositiveSetting, maxLogs,
			"max logs per notification must be greater than zero: %d", maxLogs,
		)
	}
	return nil
}

// SlackIntervalError reports the position of a non-positive wait time in the
// Intervals of a Slack logger. It is the cause of the ConfigFieldError that
// ValidateSlackIntervals returns; retrieved with errors.As, its Index
// identifies the invalid element.
type SlackIntervalError struct {
	// Index is the zero-based position of the invalid wait time in Intervals.
	Index int
	// Interval is the wait time at that position.
	Interval time.Duration
}

// Error returns a message that shows the position and the value of the
// invalid wait time.
func (e *SlackIntervalError) Error() string {
	return fmt.Sprintf("interval at index %d must be positive: %s", e.Index, e.Interval)
}

// ValidateSlackIntervals validates the wait times that a buffered Slack logger
// applies in order. An empty list is reported as a ConfigFieldError with the
// kind ErrEmptySetting, and a wait time of 0 or less as one with the kind
// ErrNonPositiveSetting. The ConfigFieldError for a wait time of 0 or less has
// as its cause a SlackIntervalError that holds the position.
func ValidateSlackIntervals(intervals []time.Duration) error {
	if len(intervals) == 0 {
		return settingError(ConfigFieldIntervals, ErrEmptySetting, nil, "intervals must not be empty")
	}
	for index, interval := range intervals {
		if interval <= 0 {
			intervalErr := &SlackIntervalError{Index: index, Interval: interval}
			return &ConfigFieldError{
				Field:  ConfigFieldIntervals,
				Detail: intervalErr.Error(),
				Value:  interval,
				Err:    ErrNonPositiveSetting,
				cause:  intervalErr,
			}
		}
	}
	return nil
}

// ValidateSlackMaxRestThreshold validates the pause in log arrivals after which
// the Slack notification interval is reset. A value of 0 or less is reported
// as a ConfigFieldError with the kind ErrNonPositiveSetting.
func ValidateSlackMaxRestThreshold(threshold time.Duration) error {
	if threshold <= 0 {
		return settingError(
			ConfigFieldMaxRestThreshold, ErrNonPositiveSetting, threshold,
			"max rest threshold must be positive: %s", threshold,
		)
	}
	return nil
}

// validateSlackLoggerConfigは、buffered Slack loggerの通知件数・待機時間・休止時間を構築前に
// 検証します。不正な項目は、他のbackend設定と同じくsourceのSlack設定（SourceとBackendを持つ
// ConfigFieldError）として報告し、待機時間の不正はSlackIntervalErrorで位置を保ちます。
func validateSlackLoggerConfig(
	sourceLabel string,
	maxLogs int,
	intervals []time.Duration,
	maxRestThreshold time.Duration,
) error {
	return errors.Join(
		wrapBackendSettingErrors(
			sourceLabel, LogOutputSlack, "logger config", ValidateSlackMaxLogsPerNotification(maxLogs),
		),
		wrapBackendSettingErrors(
			sourceLabel, LogOutputSlack, "logger config", ValidateSlackIntervals(intervals),
		),
		wrapBackendSettingErrors(
			sourceLabel, LogOutputSlack, "logger config", ValidateSlackMaxRestThreshold(maxRestThreshold),
		),
	)
}
