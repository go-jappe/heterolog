package slack

import (
	"errors"
	"fmt"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgslack "github.com/go-jappe/heterolog/pkg/slack"
)

// ErrNonPositiveTimeoutは、Slack APIへの送信timeoutが0以下であることを表します。
var ErrNonPositiveTimeout = errors.New("slack timeout must be positive")

type slackDestinationConfigProvider interface {
	Token() string
	Channel() string
	Timeout() time.Duration
}

// validateSlackDestinationConfigはlogger固有のproviderを、Slack clientが受け取る
// 共通設定へ変換して検証します。通常・Delivery・fallbackで同じ契約を使用します。
func validateSlackDestinationConfig(cfg slackDestinationConfigProvider) error {
	if cfg == nil {
		return pkgslack.ValidateConfig(pkgslack.Config{})
	}
	if cfg.Timeout() <= 0 {
		return fmt.Errorf("%w: %s", ErrNonPositiveTimeout, cfg.Timeout())
	}
	return pkgslack.ValidateConfig(pkgslack.Config{
		Token:   cfg.Token(),
		Channel: cfg.Channel(),
	})
}

// validateSlackBufferedConfigは通常・fallback Slack loggerがbufferを安全に初期化し、
// 件数閾値で通知できる設定であることを検証します。Delivery loggerはbufferを持たないため
// destination設定だけを検証します。
func validateSlackBufferedConfig(cfg SlackLoggerConfigProvider) error {
	if err := validateSlackDestinationConfig(cfg); err != nil {
		return err
	}
	if err := pkglogger.ValidateSlackMaxLogsPerNotification(cfg.MaxLogsPerNotification()); err != nil {
		return fmt.Errorf("slack logger config is invalid: %w", err)
	}
	if err := pkglogger.ValidateSlackIntervals(cfg.Intervals()); err != nil {
		return fmt.Errorf("slack logger %w", err)
	}
	if err := pkglogger.ValidateSlackMaxRestThreshold(cfg.MaxRestThreshold()); err != nil {
		return fmt.Errorf("slack logger %w", err)
	}
	if err := sampling.ValidateSamplingConfigProviderFor(pkglogger.LogOutputSlack, cfg.Sampling()); err != nil {
		return fmt.Errorf("slack logger sampling config: %w", err)
	}
	return nil
}
