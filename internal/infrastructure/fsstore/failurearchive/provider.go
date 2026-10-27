package failurearchive

import (
	"time"

	"github.com/go-jappe/heterolog/pkg/fsstore"
)

// FileArchiveConfigProviderはfailure archiveの保存先・rotation・保持処理の設定です。
// 内部ログの通知先やstack trace設定はlogger層で組み合わせます。
type FileArchiveConfigProvider interface {
	Target() string // appName または backend 固有の target 名
	// LifecycleModeはrotation後のcompress、upload、purgeをwriter自身、
	// 専用runner、またはどちらでも実行しないかを選択します。
	LifecycleMode() fsstore.FileLifecycleMode
	RotationType() fsstore.FileRotationType
	RotationDayOfWeek() time.Weekday
	FileNameFormat() string
	FileDateFormat() string
	MaxFileSize() uint64
	UsageCacheTTL() time.Duration
	MaxDirSize() uint64
	MaxFileAge() int
	MaxFileBackups() int
	EnableFileCompress() bool
	CompressFormat() fsstore.CompressFormat
	ArchivePath() string      // ex.)/var/lib/{target}　(linuxの場合の例)
	Dir() string              // ex.)/var/lib/{target}/{dir}/
	DirCaseRetryable() string // ex.)/var/lib/{target}/{dir}/{retryable}
	DirCasePermanent() string // ex.)/var/lib/{target}/{dir}/{permanent}
	EnableRotate() bool
	MaxWriteRetries() int
	FileOperationRetryWaitTime() time.Duration
}

// CompressionCodecProviderは、組み込み以外の圧縮形式をprogrammaticに設定した
// FileArchiveConfigProviderだけが実装するoptional契約です。
type CompressionCodecProvider interface {
	CompressionCodec() fsstore.CompressionCodec
}
