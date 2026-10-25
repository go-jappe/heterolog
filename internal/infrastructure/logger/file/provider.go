package file

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	loggertime "github.com/go-jappe/heterolog/internal/infrastructure/logger/time"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

type FileLoggerConfigProvider interface {
	Time() loggertime.TimeConfigProvider
	Runtime() RuntimeProvider
	LogBody() LogBodyConfigProvider
	Fsstore() appendlog.ConfigProvider
	// Overloadはfile loggerの書き込み可否と退避方針であり、appendlog storeの設定ではありません。
	Overload() FileOverloadConfigProvider
	ObjectStorageUpload() objectstorage.LogFileUploadConfigProvider
	ArchiveCheckpoint() pkgfsstore.ArchiveCheckpointConfig
	LifecycleMode() pkgfsstore.FileLifecycleMode
	Sampling() sampling.SamplingConfigProvider
	Handler() handler.ConfigProvider
}

// FileOverloadConfigProviderはfile loggerの書き込み前の空き容量判定と退避方針を提供します。
type FileOverloadConfigProvider interface {
	OverloadPolicy() pkgfsstore.OverloadPolicy
	SpillDestination() pkgfsstore.SpillDestination
	MinAvailableDiskSpace() uint64
}

type RuntimeProvider interface {
	IsDebug() bool
	Target() string
	LogLevel() logger.LogLevel
}

type LogBodyConfigProvider interface {
	LogLineDateFormat() string
	ErrorTraceDepth() int
	ErrorTraceDepthDebugMode() int
}
