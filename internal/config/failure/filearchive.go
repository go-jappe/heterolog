package failure

import (
	"time"

	"github.com/go-jappe/heterolog/pkg/fsstore"
)

const (
	DirCaseRetryable = "retryable" // /var/lib/{target}/{backend_failure_archive}/retryable
	DirCasePermanent = "permanent" // /var/lib/{target}/{backend_failure_archive}/permanent

	// backend送信失敗を保存するJSONLのdefaultサイズ。
	DefaultFileArchiveMaxSizeMB = 50

	// failure archiveのローテーション設定。
	DefaultFileArchiveRotateEnabled      = true
	DefaultAsyncFileArchiveRotateEnabled = true
	// failure archiveは未送信の失敗データを保持するため、既定では世代数・日数・容量の
	// いずれでも削除しません（0は上限なし）。ディスクの枯渇は、空き容量が
	// HETEROLOG_MIN_AVAILABLE_DISK_SPACE_GBを下回った新しい失敗を過負荷policyで扱うことで防ぎます。
	// fileArchiveファイル保持最大世代数。0は上限なし
	DefaultFileArchiveMaxBackups      = 0
	DefaultAsyncFileArchiveMaxBackups = 0
	// fileArchiveファイル最大保持日数。0は上限なし
	DefaultFileArchiveMaxAge      = 0
	DefaultAsyncFileArchiveMaxAge = 0
	// failure archiveディレクトリの合計サイズ上限。0は上限なしで、0以外では超過分を古い順に削除します。
	DefaultFileArchiveMaxDirSizeMB      = 0
	DefaultAsyncFileArchiveMaxDirSizeMB = 0
	// fileArchiveのローテート時に古いファイルを圧縮
	DefaultFileArchiveCompressEnabled      = true
	DefaultAsyncFileArchiveCompressEnabled = true

	// fileArchiveファイル圧縮形式
	DefaultFileArchiveCompressFormat      = fsstore.CompressZstd
	DefaultAsyncFileArchiveCompressFormat = fsstore.CompressZstd

	// failure archiveは従来、rotation後のcompress、upload、purgeを
	// handlerの書込み経路から実行していたため、未指定時はinlineを維持します。
	DefaultFileArchiveLifecycleMode = fsstore.FileLifecycleModeInline

	// デフォルトのログファイルのローテーション頻度
	DefaultFileArchiveRotation               = fsstore.FileRotationDaily
	DefaultFileArchiveRotationDayOfWeek      = time.Sunday
	DefaultAsyncFileArchiveRotation          = fsstore.FileRotationDaily
	DefaultAsyncFileArchiveRotationDayOfWeek = time.Sunday
)
