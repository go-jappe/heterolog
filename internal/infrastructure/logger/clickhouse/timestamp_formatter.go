package clickhouse

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
)

// NewConfiguredTimestampFormatterはClickHouse loggerの設定から、recordのLineに含める
// 表示時刻のtimezoneと表示形式を作ります。record.TimestampはOccurredAtから直接生成し、
// この文字列をprotocol timestampへ再利用しません。
func NewConfiguredTimestampFormatter(cfg ClickHouseConfigProvider) (timestamp.Formatter, error) {
	if cfg == nil {
		return timestamp.Formatter{}, fmt.Errorf("clickhouse logger config is nil")
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
		return timestamp.Formatter{}, fmt.Errorf("create clickhouse timestamp formatter: %w", err)
	}
	return configuredFormatter, nil
}
