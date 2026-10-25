package failure

import "time"

const (
	// overload時の処理
	DefaultSpoolOverloadPolicy      = OverloadPolicyDrop
	DefaultAsyncSpoolOverloadPolicy = OverloadPolicyDrop

	// active ディレクトリ最大サイズ(overload policy)
	// 当閾値を超過した場合、spoolには受け入れない
	DefaultSpoolActiveMaxDirSizeMB      = 1024
	DefaultAsyncSpoolActiveMaxDirSizeMB = 1024

	// activeの最古データに対するWarn開始日数。保存可否には影響しない。
	DefaultSpoolActiveBacklogWarnAfterDays      = 120
	DefaultAsyncSpoolActiveBacklogWarnAfterDays = 120

	// sync async 共通。小さいVMやcontainerでも書き込みを止めないよう1GiBにします。
	DefaultMinAvailableDiskSpaceGB = 1

	DefaultMaxSpoolScannedCacheUsageCount = 100000
	DefaultSpoolCacheLifetime             = 30 * time.Second

	DefaultUseMemoryPressureForOverload = false
)
