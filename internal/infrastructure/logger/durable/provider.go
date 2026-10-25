package durable

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	spoolstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	loggerhandler "github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	spoolstoreconfig "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/store"
	"github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

type FailureStoreConfigProvider interface {
	EnableDegrade() bool
	MaxConcurrent() int
	DurableType() []durable.DurableType
	SpillType() []durable.SpillType
	DurableStoreConfig() DurableStoreConfigProvider
}

type DeliveryFailureStoreConfigProvider interface {
	EnableDegrade() bool
	DurableType() []durable.DurableType
	SpillType() []durable.SpillType
	DeliveryDurableStoreConfig() DeliveryDurableStoreConfigProvider
}

type DurableStoreConfigProvider interface {
	FileArchiveConfig() FileArchiveConfigProvider
	SpoolConfig() SpoolConfigProvider
}

// SpoolConfigProviderはspool保存先、過負荷判定、内部ログの設定を
// flush failure factoryへ提供します。
type SpoolConfigProvider interface {
	spoolstore.SpoolStoreConfigProvider
	OverloadPolicyConfig() spoolstoreconfig.SpoolStoreOverloadPolicyConfigProvider
	Handler() loggerhandler.ConfigProvider
}

// FileArchiveConfigProviderは保存・保持設定に、送信・checkpoint・過負荷判定・
// runner実行設定と内部ログの通知設定を組み合わせます。
// loggerはLifecycleModeに従い、writerと必要なrunnerを構築します。
type FileArchiveConfigProvider interface {
	failurearchive.FileArchiveConfigProvider
	LifecycleRunnerConfig() fsstore.AppendLogRunnerConfig
	ObjectStorageUpload() objectstorage.UploadTargetConfig
	ArchiveCheckpoint() fsstore.ArchiveCheckpointConfig
	OverloadPolicyConfig() FileArchiveOverloadPolicyConfigProvider
	Handler() loggerhandler.ConfigProvider
	IsDebug() bool
	ErrorTraceDepth() int
	ErrorTraceDepthDebugMode() int
}

// FileArchiveOverloadPolicyConfigProviderはarchive保存時の過負荷判定と退避方法を指定します。
type FileArchiveOverloadPolicyConfigProvider interface {
	OverloadPolicy() durable.OverloadPolicy
	SpillDestination() durable.SpillDestination
	MinAvailableDiskSpace() uint64
}

type DeliveryDurableStoreConfigProvider interface {
	FileArchiveConfig() FileArchiveConfigProvider
}
