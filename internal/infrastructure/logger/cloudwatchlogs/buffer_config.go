package cloudwatchlogs

import (
	"fmt"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// validateCloudWatchLogsBufferConfigはlogical bufferの送信開始条件と、
// fallback専用hard queue上限の関係を検証します。PutLogEventsの物理上限は
// request plannerが独立して守るため、ここでは10,000件や1 MiBと比較しません。
// 規則はpkg/loggerのValidateCloudWatchLogsBufferConfigと共有し、不正な項目は
// pkglogger.ConfigFieldErrorとして判別できます。
func validateCloudWatchLogsBufferConfig(cfg CloudWatchLogsConfigProvider) error {
	if cfg == nil {
		return fmt.Errorf("cloudwatch logs logger config is nil")
	}
	return pkglogger.ValidateCloudWatchLogsBufferConfig(
		cfg.BufferFlushMaxEvents(),
		cfg.BufferFlushBytes(),
		cfg.FallbackQueueMaxEvents(),
	)
}
