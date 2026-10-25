package opensearch

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	loggertime "github.com/go-jappe/heterolog/internal/infrastructure/logger/time"
	"github.com/go-jappe/heterolog/pkg/logger"
	"time"
)

type OpenSearchFlushManagerRuntimeConfigProvider interface {
	Index() string
	IsDebug() bool
	UseOpenTelemetryFormat() bool
	PostWarnThresholdDuration() time.Duration
	PostErrorThresholdDuration() time.Duration
	IndexLatencyEnabled() bool
}

// OpenSearchBulkRetryConfigProviderは、bulk応答で失敗した項目の再送設定を提供します。
// 送信先indexや遅延通知などの共通設定は含みません。
type OpenSearchBulkRetryConfigProvider interface {
	// MaxRetryAttemptsは初回を含む総送信試行回数です。
	MaxRetryAttempts() int
	// RetryWaitTimeはretryableと分類されたBulk itemを次に送信するまでの待機時間です。
	RetryWaitTime() time.Duration
}

// OpenSearchBulkFlushManagerConfigProviderは、bulk送信の構築時に必要な共通設定と
// 項目ごとの再送設定をまとめます。1件ずつ送信するDelivery OpenSearchは実装しません。
type OpenSearchBulkFlushManagerConfigProvider interface {
	OpenSearchFlushManagerRuntimeConfigProvider
	OpenSearchBulkRetryConfigProvider
}

type OpenSearchFlushManagerConfigProvider interface {
	OpenSearchBulkFlushManagerConfigProvider
	FailureStoreConfig() durable.FailureStoreConfigProvider
}

type DeliveryOpenSearchFlushManagerConfigProvider interface {
	OpenSearchFlushManagerRuntimeConfigProvider
	DeliveryFailureStoreConfig() durable.DeliveryFailureStoreConfigProvider
}

type OpenSearchLoggerConfigProvider interface {
	Time() loggertime.TimeConfigProvider
	Runtime() RuntimeConfigProvider
	LogBody() LogBodyConfigProvider
	// FlushIntervalはbufferを時間経過でflushする間隔です。正の値が必須です。
	FlushInterval() time.Duration
	MaxBulkDocument() int
	// BufferFlushBytesはbuffer内のOpenSearch document JSON概算byte数が到達したら
	// intervalやdocument件数を待たずflushを開始するsoft thresholdです。
	BufferFlushBytes() int
	// MaxBulkRequestBytesはPlanBulkRequestsが一つのBulk requestへ割り当てる
	// gzip圧縮前NDJSON bodyのhard limitです。Index APIには適用しません。
	MaxBulkRequestBytes() int
	NoBulkFlushModeEnabled() bool //　bulk送信を使わず必ず一件づつ送信する（高負荷・ログ保証）
	Sampling() sampling.SamplingConfigProvider
	Handler() handler.ConfigProvider
}

type RuntimeConfigProvider interface {
	AppName() string // appname
	IsDebug() bool
	Target() string
	LogLevel() logger.LogLevel
}

type LogBodyConfigProvider interface {
	LogLineDateFormat() string
	ErrorTraceDepth() int
	ErrorTraceDepthDebugMode() int
}

type OpenSearchFlushManagerResourceProvider interface {
	ServiceName() string
	ServiceVersion() string
	HostName() string
	OSType() string
	Environment() string
	RuntimeType() string
	RuntimeInstanceID() string
	CloudProvider() string
	CloudProviderRegion() string
}
