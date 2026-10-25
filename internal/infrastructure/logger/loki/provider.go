package loki

import (
	"time"

	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	loggertime "github.com/go-jappe/heterolog/internal/infrastructure/logger/time"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// LokiFlushManagerRuntimeConfigProvider は通常 logger と delivery logger が共有する送信遅延監視設定です。
// HTTP再試行はlogbackend/loki/wrapperのRetryConfigProviderが所有します。
type LokiFlushManagerRuntimeConfigProvider interface {
	PostWarnThresholdDuration() time.Duration
	PostErrorThresholdDuration() time.Duration
	PushLatencyEnabled() bool
	StaticLabels() map[string]string
}

// LokiFlushManagerConfigProvider は通常 logger の durable 設定を含む provider です。
type LokiFlushManagerConfigProvider interface {
	LokiFlushManagerRuntimeConfigProvider
	FailureStoreConfig() loggerdurable.FailureStoreConfigProvider
}

// DeliveryLokiFlushManagerConfigProvider はDelivery専用のfailure store設定を含むproviderです。
// 1件送信に失敗した場合は退避の完了を同期的に待ち、その結果をDeliveryReportへ含めます。
type DeliveryLokiFlushManagerConfigProvider interface {
	LokiFlushManagerRuntimeConfigProvider
	DeliveryFailureStoreConfig() loggerdurable.DeliveryFailureStoreConfigProvider
}

type LokiConfigProvider interface {
	Time() loggertime.TimeConfigProvider
	Runtime() RuntimeConfigProvider
	LogBody() LogBodyConfigProvider
	// FlushIntervalはbufferを時間経過でflushする間隔です。正の値が必須です。
	FlushInterval() time.Duration
	MaxEntries() int
	// BufferFlushBytesはformatter済みlineのUTF-8 byte概算合計で早期flushする
	// soft thresholdです。JSON escapeやPush envelopeを含むhard request上限は
	// MaxPushRequestBytesとbackend plannerが別途担当します。
	BufferFlushBytes() int
	// FallbackQueueMaxEntriesはinternal-log fallbackが保持するhard queue件数です。
	// MaxEntriesのbatch wake閾値とは独立しています。
	FallbackQueueMaxEntries() int
	MaxLineBytes() int
	MaxPushRequestBytes() int
	NoBulkFlushModeEnabled() bool
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
