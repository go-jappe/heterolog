package cloudwatchlogs

import (
	"time"

	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	loggertime "github.com/go-jappe/heterolog/internal/infrastructure/logger/time"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// CloudWatchLogsFlushManagerRuntimeConfigProvider は通常 logger と delivery logger が共有する送信設定です。
type CloudWatchLogsFlushManagerRuntimeConfigProvider interface {
	LogGroupName() string
	LogStreamName() string
	LogStreamRoutingGranularity() pkgcloudwatchlogs.StreamRouting
	PostWarnThresholdDuration() time.Duration
	PostErrorThresholdDuration() time.Duration
	PutLatencyEnabled() bool
}

// CloudWatchLogsFlushManagerConfigProvider は通常 logger の durable 設定を含む provider です。
type CloudWatchLogsFlushManagerConfigProvider interface {
	CloudWatchLogsFlushManagerRuntimeConfigProvider
	FailureStoreConfig() loggerdurable.FailureStoreConfigProvider
}

// DeliveryCloudWatchLogsFlushManagerConfigProvider はDelivery専用のfailure store設定を含むproviderです。
// 1件送信に失敗した場合は退避の完了を同期的に待ち、その結果をDeliveryReportへ含めます。
type DeliveryCloudWatchLogsFlushManagerConfigProvider interface {
	CloudWatchLogsFlushManagerRuntimeConfigProvider
	DeliveryFailureStoreConfig() loggerdurable.DeliveryFailureStoreConfigProvider
}

// CloudWatchLogsConfigProvider は logger 本体の runtime、buffer、sampling、handler 設定を抽象化します。
type CloudWatchLogsConfigProvider interface {
	Time() loggertime.TimeConfigProvider
	Runtime() RuntimeConfigProvider
	LogBody() LogBodyConfigProvider
	// FlushIntervalはbufferを時間経過でflushする間隔です。正の値が必須です。
	FlushInterval() time.Duration
	// BufferFlushMaxEventsはbufferの件数で早期flushするsoft thresholdです。
	// PutLogEventsの物理request件数上限とは独立しています。
	BufferFlushMaxEvents() int
	// BufferFlushBytesはformat後messageのUTF-8 byte数にAWS規定の
	// event overheadを加えた概算合計で早期flushするsoft thresholdです。
	BufferFlushBytes() int
	// FallbackQueueMaxEventsはinternal-log fallbackが保持するhard queue件数です。
	FallbackQueueMaxEvents() int
	NoBulkFlushModeEnabled() bool
	Sampling() sampling.SamplingConfigProvider
	Handler() handler.ConfigProvider
}

// RuntimeConfigProvider は runtime 名や log level など、出力可否と metadata に関わる設定です。
type RuntimeConfigProvider interface {
	AppName() string
	IsDebug() bool
	Target() string
	LogLevel() pkglogger.LogLevel
}

// LogBodyConfigProvider は log 本文と stack trace の整形に使う設定です。
type LogBodyConfigProvider interface {
	LogLineDateFormat() string
	ErrorTraceDepth() int
	ErrorTraceDepthDebugMode() int
}
