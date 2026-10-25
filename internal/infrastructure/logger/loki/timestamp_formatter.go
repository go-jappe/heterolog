package loki

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
)

// NewConfiguredTimestampFormatterはLoki loggerの設定から、送信line内の表示時刻に
// 使用するtimezoneと表示形式を作ります。Loki protocol timestampはOccurredAtから
// 直接生成し、この文字列を再利用しません。
func NewConfiguredTimestampFormatter(cfg LokiConfigProvider) (timestamp.Formatter, error) {
	if cfg == nil {
		return timestamp.Formatter{}, fmt.Errorf("loki logger config is nil")
	}

	format := strings.TrimSpace(cfg.LogBody().LogLineDateFormat())
	if format == "" {
		format = time.RFC3339
	}
	configuredFormatter, err := logging.NewTimestampFormatter(
		cfg.Time().EnableLocalTime(),
		cfg.Time().TimeZone(),
		format,
	)
	if err != nil {
		return timestamp.Formatter{}, fmt.Errorf("create loki timestamp formatter: %w", err)
	}
	return configuredFormatter, nil
}
