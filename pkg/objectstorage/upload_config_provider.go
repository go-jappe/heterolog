package objectstorage

import (
	"time"

	"github.com/go-jappe/heterolog/pkg/failurestore"
)

// LogFileUploadConfigProvider provides the upload settings used by the normal,
// error and debug log series. Consumers do not receive the concrete
// configuration struct; they obtain the values they need from the provider of
// each log series.
type LogFileUploadConfigProvider interface {
	Enabled() bool
	Default() UploadTargetConfigProvider
	Error() UploadTargetConfigProvider
	Debug() UploadTargetConfigProvider
	CompressedLogFileUploadSpool() CompressedLogFileUploadSpoolConfigProvider
	UsesCompressedLogFileUploadSpool(separateError, separateDebug bool) bool
	CompressedLogFileUploadSpoolServices(separateError, separateDebug bool) []UploadTarget
}

// UploadTargetConfigProvider provides the destinations of one log series and
// the evacuation settings used when an upload fails.
type UploadTargetConfigProvider interface {
	Enabled() bool
	Targets() []UploadTarget
	S3DestinationConfigs() []S3ConfigProvider
	CompressedLogFileUploadFailureStore() CompressedLogFileUploadFailureStoreConfigProvider
	CompressedLogFileUploadSpool() CompressedLogFileUploadSpoolConfigProvider
	Has(UploadTarget) bool
	CompressedLogFileUploadSpoolServices() []UploadTarget
}

// S3ConfigProvider provides the connection, authentication and storage
// location settings of one S3 destination.
type S3ConfigProvider interface {
	Region() string
	Endpoint() string
	AccessKeyID() string
	SecretAccessKey() string
	SessionToken() string
	Bucket() string
	Prefix() string
	ForcePathStyle() bool
	Timeout() time.Duration
}

// CompressedLogFileUploadFailureStoreConfigProvider provides the evacuation
// method and the storage location for compressed logs whose upload failed.
type CompressedLogFileUploadFailureStoreConfigProvider interface {
	DurableType() []failurestore.DurableType
	FileArchiveConfig() CompressedLogFileUploadFailureArchiveConfigProvider
}

// CompressedLogFileUploadFailureArchiveConfigProvider provides the storage
// location and the retention settings for compressed logs that are kept
// without being retried.
type CompressedLogFileUploadFailureArchiveConfigProvider interface {
	RootPath() string
	LoggerContract() string
	LifecycleInterval() time.Duration
	RetryablePurge() CompressedLogFileUploadFailureArchivePurgeConfig
	PermanentPurge() CompressedLogFileUploadFailureArchivePurgeConfig
}
