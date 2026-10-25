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

// ValidateSlackMaxLogsPerNotificationは、一回のSlack通知へ蓄積するlog件数の
// flush閾値が正の値であることを検証します。0以下の値はErrNonPositiveSettingを持つ
// ConfigFieldErrorです。
func ValidateSlackMaxLogsPerNotification(maxLogs int) error {
	if maxLogs <= 0 {
		return settingError(
			ConfigFieldMaxLogsPerNotification, ErrNonPositiveSetting, maxLogs,
			"max logs per notification must be greater than zero: %d", maxLogs,
		)
	}
	return nil
}

// SlackIntervalErrorは、Slack loggerのIntervalsのうち正でない待機時間の位置を表します。
// ValidateSlackIntervalsが返すConfigFieldErrorの原因で、errors.Asで取り出すとIndexで
// 不正な要素を特定できます。
type SlackIntervalError struct {
	// Indexは、Intervalsの中で不正な待機時間の位置です（先頭は0）。
	Index int
	// Intervalは、その位置の待機時間です。
	Interval time.Duration
}

// Errorは、不正な待機時間の位置と値を表すmessageを返します。
func (e *SlackIntervalError) Error() string {
	return fmt.Sprintf("interval at index %d must be positive: %s", e.Index, e.Interval)
}

// ValidateSlackIntervalsはbuffered Slack loggerが順に適用する待機時間を検証します。
// 空の一覧はErrEmptySetting、0以下の待機時間はErrNonPositiveSettingを持つ
// ConfigFieldErrorです。0以下の待機時間のConfigFieldErrorは、その位置を表す
// SlackIntervalErrorを原因として持ちます。
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

// ValidateSlackMaxRestThresholdはSlackの通知間隔をリセットするまでのログ到着の休止時間を
// 検証します。0以下の値はErrNonPositiveSettingを持つConfigFieldErrorです。
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
