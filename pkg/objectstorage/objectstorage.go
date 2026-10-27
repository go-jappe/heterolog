package objectstorage

import (
	"slices"
	"time"

	"github.com/go-jappe/heterolog/pkg/failurestore"
)

type UploadTarget string

const (
	UploadTargetS3 UploadTarget = "s3"
)

// DestinationID stably identifies one physical destination in checkpoints and
// upload results. It is generated not only from the kind of provider but from
// an identity that includes the endpoint, the bucket, the prefix and the other
// values that determine the actual storage location. Credentials and timeout
// settings do not change the destination, so they are not included.
type DestinationID string

type UploadTargetConfig struct {
	Enabled bool
	Targets []UploadTarget
	S3      S3Config
	// S3Destinations lists the S3 destinations. When it is set, it replaces
	// the single destination in S3. Each element has its own connection
	// target, credentials, bucket and prefix, and progress is managed per
	// destination.
	// S3 is used only when S3Destinations is nil. An explicitly empty list is
	// a configuration error.
	S3Destinations []S3Config
	// CompressedLogFileUploadFailureStore selects the durable store to which a
	// compressed log file whose direct upload to object storage could not be
	// completed is handed over before the producer releases it. When the
	// selection is empty or none, the producer keeps the local file.
	CompressedLogFileUploadFailureStore CompressedLogFileUploadFailureStoreConfig
	// CompressedLogFileUploadSpool configures the retry spool that this route
	// uses. The backend delivery failure archives and the ordinary logs of the
	// same logger owner use the same runtime when their storage locations and
	// operational settings match.
	CompressedLogFileUploadSpool CompressedLogFileUploadSpoolConfigProvider
}

type LogFileUploadConfig struct {
	Enabled bool
	Default UploadTargetConfig
	Error   UploadTargetConfig
	Debug   UploadTargetConfig
	// CompressedLogFileUploadSpool configures the retry spool that the roles
	// use in common. Whether a runtime is shared is decided by matching storage
	// locations and operational settings, not by the identity of the provider.
	CompressedLogFileUploadSpool CompressedLogFileUploadSpoolConfigProvider
}

// UsesCompressedLogFileUploadSpool reports whether the shared compressed-log
// spool is selected, judging only from the upload settings of the roles that
// are actually used.
func (cfg LogFileUploadConfig) UsesCompressedLogFileUploadSpool(separateError, separateDebug bool) bool {
	if !cfg.Enabled {
		return false
	}
	targets := []UploadTargetConfig{cfg.Default}
	if separateError {
		targets = append(targets, cfg.Error)
	}
	if separateDebug {
		targets = append(targets, cfg.Debug)
	}
	for _, target := range targets {
		if target.Enabled && slices.Contains(target.CompressedLogFileUploadFailureStore.DurableType, failurestore.DurableTypeSpool) {
			return true
		}
	}
	return false
}

type S3Config struct {
	// Region is used for SigV4 signing with leading and trailing white space
	// removed. Its case is not changed. Destination IDs treat regions that
	// differ only in case as the same storage location.
	Region string
	// Endpoint is an http or https S3-compatible endpoint. When it is empty,
	// the AWS S3 endpoint of Region is used. A query or a fragment is not
	// accepted.
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Bucket          string
	Prefix          string
	ForcePathStyle  bool
	// Timeout is the limit of the S3 HTTP client. Zero is the sentinel for an
	// unset value, which selects the default timeout; a negative value is
	// invalid.
	Timeout time.Duration
}

// S3DestinationConfigs returns the S3 destinations actually used, in
// configuration order. Do not modify the returned slice.
func (cfg UploadTargetConfig) S3DestinationConfigs() []S3Config {
	if cfg.S3Destinations != nil {
		return cfg.S3Destinations
	}
	return []S3Config{cfg.S3}
}

func (cfg UploadTargetConfig) Has(target UploadTarget) bool {
	for _, current := range cfg.Targets {
		if current == target {
			return true
		}
	}
	return false
}
