package clickhouse

import (
	"time"

	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	loggertime "github.com/go-jappe/heterolog/internal/infrastructure/logger/time"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// ClickHouseFlushManagerRuntimeConfigProvider は通常・Deliveryで共有する
// ClickHouse送信時のruntime設定です。failure store設定は各経路のproviderが所有します。
type ClickHouseFlushManagerRuntimeConfigProvider interface {
	PostWarnThresholdDuration() time.Duration
	PostErrorThresholdDuration() time.Duration
	InsertLatencyEnabled() bool
}

type ClickHouseFlushManagerConfigProvider interface {
	ClickHouseFlushManagerRuntimeConfigProvider
	FailureStoreConfig() loggerdurable.FailureStoreConfigProvider
}

type DeliveryClickHouseFlushManagerConfigProvider interface {
	ClickHouseFlushManagerRuntimeConfigProvider
	DeliveryFailureStoreConfig() loggerdurable.DeliveryFailureStoreConfigProvider
}

type ClickHouseConfigProvider interface {
	Time() loggertime.TimeConfigProvider
	Runtime() RuntimeConfigProvider
	LogBody() LogBodyConfigProvider
	// FlushIntervalはbufferを時間経過でflushする間隔です。正の値が必須です。
	FlushInterval() time.Duration
	MaxEntries() int
	// BufferFlushBytesはbuffer内の未圧縮JSONEachRow byte数による早期flush閾値です。
	BufferFlushBytes() int
	// FallbackQueueMaxEntriesはfallback handlerが保持できるentry件数のhard上限です。
	FallbackQueueMaxEntries() int
	// MaxInsertRequestBytesは一回の物理HTTP INSERTに許容する未圧縮JSONEachRowのhard limitです。
	MaxInsertRequestBytes() int
	Sampling() sampling.SamplingConfigProvider
	Handler() handler.ConfigProvider
}

type RuntimeConfigProvider interface {
	AppName() string
	IsDebug() bool
	Target() string
	LogLevel() pkglogger.LogLevel
}

type LogBodyConfigProvider interface {
	LogLineDateFormat() string
	ErrorTraceDepth() int
	ErrorTraceDepthDebugMode() int
}
