package cloudwatchlogs

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
)

// NewConfiguredTimestampFormatterはCloudWatch Logs loggerの時刻設定から、event本文の
// 表示時刻に使用するtimezoneと表示形式を作ります。PutLogEventsのevent timestampは
// OccurredAtから生成し、log stream切替はformatterのLocationを明示的に使用します。
func NewConfiguredTimestampFormatter(
	cfg CloudWatchLogsConfigProvider,
) (timestamp.Formatter, error) {
	if cfg == nil {
		return timestamp.Formatter{}, fmt.Errorf("cloudwatch logs logger config is nil")
	}

	format := strings.TrimSpace(cfg.LogBody().LogLineDateFormat())
	if format == "" {
		// 直接構築されたzero-value設定でも、時刻文字列が空になることを防ぎます。
		format = time.RFC3339
	}
	configuredFormatter, err := logging.NewTimestampFormatter(
		cfg.Time().EnableLocalTime(),
		cfg.Time().TimeZone(),
		format,
	)
	if err != nil {
		return timestamp.Formatter{}, fmt.Errorf("create cloudwatch logs timestamp formatter: %w", err)
	}
	return configuredFormatter, nil
}
