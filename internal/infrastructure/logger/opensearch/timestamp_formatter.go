package opensearch

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
)

// NewConfiguredTimestampFormatterはOpenSearch loggerの設定から、document内の
// 表示時刻に使用するtimezoneと表示形式を作ります。現在時刻は取得しません。
func NewConfiguredTimestampFormatter(
	cfg OpenSearchLoggerConfigProvider,
) (timestamp.Formatter, error) {
	if cfg == nil {
		return timestamp.Formatter{}, fmt.Errorf("opensearch logger config is nil")
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
		return timestamp.Formatter{}, fmt.Errorf("create opensearch timestamp formatter: %w", err)
	}
	return configuredFormatter, nil
}
