package spoollifecycle

import (
	"time"
)

const (
	// doneは再送に成功したrecordなので、保持期間・容量を超えた分を既定で削除します。
	DefaultEnablePurgeDone  = true
	DefaultDoneMaxFileAge   = 30
	DefaultDoneMaxDirSizeMB = 2048
	// deadは配送不能が確定したrecord、quarantineは成否を確認できないrecordです。
	// 確認前に暗黙に削除しないよう、公開APIのDefaultSpoolConfigと同じく既定では削除せず、
	// HETEROLOG_ENABLE_PURGE_*_SPOOL_DEAD・HETEROLOG_ENABLE_PURGE_*_SPOOL_QUARANTINEを明示した場合だけ削除します。
	// 保持期間・容量の既定値は、削除を有効にした場合に使います。
	DefaultEnablePurgeDead          = false
	DefaultDeadMaxFileAges          = 30
	DefaultDeadMaxDirSizes          = 2048
	DefaultEnablePurgeQuarantine    = false
	DefaultQuarantineMaxFileAge     = 30
	DefaultQuarantineMaxDirSize     = 2048
	DefaultSizePurgeInterval        = 60 * time.Second
	DefaultEnableSizeEstimateCache  = true
	DefaultSizeFullScanEveryNChecks = 65536
	DefaultSizeFullScanMaxInterval  = 5 * time.Minute
)
