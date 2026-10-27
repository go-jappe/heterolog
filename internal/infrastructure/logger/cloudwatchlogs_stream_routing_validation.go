package logger

import (
	"errors"
	"fmt"
	"time"

	infracloudwatchlogs "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// validateLoggerCloudWatchLogsStreamRoutingは、sync/async loggerが実際に構築し得る
// CloudWatch Logs保存先について、application側のstream分割粒度と、suffixを付けた
// 実stream名を検証します。通常出力だけでなくinternal log専用保存先も対象です。
// CloudWatch Logsを全く構築しない設定では、未使用の粒度を検証しません。
func validateLoggerCloudWatchLogsStreamRouting(
	source string,
	cfg pkglogger.LoggerConfig,
	granularity pkgcloudwatchlogs.StreamRouting,
) error {
	var streamNames []string
	if cfg.CloudWatchLogs != nil &&
		pkglogger.LoggerConfigRequiresBaseBackend(cfg, pkglogger.LogOutputCloudWatchLogs) {
		streamNames = append(
			streamNames,
			cfg.CloudWatchLogs.CloudWatchLogsFlushManagerConfig.LogStreamName,
		)
	}
	if cfg.InternalLogDestinations != nil &&
		cfg.InternalLogDestinations.CloudWatchLogs != nil {
		streamNames = append(
			streamNames,
			cfg.InternalLogDestinations.CloudWatchLogs.
				CloudWatchLogsFlushManagerConfig.LogStreamName,
		)
	}
	return validateCloudWatchLogsStreamRouting(source, streamNames, granularity)
}

// validateDeliveryLoggerCloudWatchLogsStreamRoutingはDelivery loggerの通常出力と
// internal log専用保存先について、実stream名まで検証します。
func validateDeliveryLoggerCloudWatchLogsStreamRouting(
	source string,
	cfg pkglogger.DeliveryLoggerConfig,
	granularity pkgcloudwatchlogs.StreamRouting,
) error {
	var streamNames []string
	if cfg.CloudWatchLogs != nil &&
		pkglogger.DeliveryLoggerConfigRequiresBaseBackend(cfg, pkglogger.LogOutputCloudWatchLogs) {
		streamNames = append(
			streamNames,
			cfg.CloudWatchLogs.DeliveryCloudWatchLogsFlushManagerConfig.LogStreamName,
		)
	}
	if cfg.InternalLogDestinations != nil &&
		cfg.InternalLogDestinations.CloudWatchLogs != nil {
		streamNames = append(
			streamNames,
			cfg.InternalLogDestinations.CloudWatchLogs.
				DeliveryCloudWatchLogsFlushManagerConfig.LogStreamName,
		)
	}
	return validateCloudWatchLogsStreamRouting(source, streamNames, granularity)
}

// ErrUnsupportedStreamRoutingGranularityは、CloudWatch Logsのstream分割粒度が
// none、monthly、daily、hourlyのいずれでもないことを表します。
var ErrUnsupportedStreamRoutingGranularity = errors.New("unsupported cloudwatch logs stream routing granularity")

// CloudWatchLogsStreamRoutingErrorは、CloudWatch Logsのstream routingを構築できないことを
// 表します。Sourceは設定の出所、Granularityは分割粒度です。StreamNameは実stream名の
// 検証に使った基準stream名で、分割粒度が未対応の場合は空です。Errは元の原因で、
// 実stream名の不正ではpkg/cloudwatchlogsのsentinel（ErrLogStreamNameTooLongなど）を含みます。
type CloudWatchLogsStreamRoutingError struct {
	Source      string
	Granularity pkgcloudwatchlogs.StreamRouting
	StreamName  string
	Err         error
}

func (e *CloudWatchLogsStreamRoutingError) Error() string {
	return fmt.Sprintf("%s CloudWatch Logs stream routing: %v", e.Source, e.Err)
}

func (e *CloudWatchLogsStreamRoutingError) Unwrap() error { return e.Err }

// unsupportedStreamRoutingGranularityErrorは、streamroutingの検証errorを文言のまま保ち、
// ErrUnsupportedStreamRoutingGranularityとしても判定できるようにします。
type unsupportedStreamRoutingGranularityError struct {
	err error
}

func (e unsupportedStreamRoutingGranularityError) Error() string { return e.err.Error() }

func (e unsupportedStreamRoutingGranularityError) Unwrap() []error {
	return []error{ErrUnsupportedStreamRoutingGranularity, e.err}
}

func validateCloudWatchLogsStreamRouting(
	source string,
	streamNames []string,
	granularity pkgcloudwatchlogs.StreamRouting,
) error {
	if len(streamNames) == 0 {
		return nil
	}
	if err := granularity.Validate(); err != nil {
		return &CloudWatchLogsStreamRoutingError{
			Source:      source,
			Granularity: granularity,
			Err:         unsupportedStreamRoutingGranularityError{err: err},
		}
	}
	validationTime := time.Date(2006, time.January, 2, 15, 0, 0, 0, time.UTC)
	for _, streamName := range streamNames {
		if _, err := infracloudwatchlogs.ResolveLogStreamName(
			streamName,
			granularity,
			validationTime,
		); err != nil {
			return &CloudWatchLogsStreamRoutingError{
				Source:      source,
				Granularity: granularity,
				StreamName:  streamName,
				Err:         err,
			}
		}
	}
	return nil
}
