package spoollifecycle

import "time"

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

type LifecycleConfig struct {
	EnablePurgeDone bool
	// EnablePurgeDoneがtrueの場合は正の値が必須です。
	DoneMaxFileAge int
	// EnablePurgeDoneがtrueの場合は正の値が必須です。
	DoneMaxDirSize uint64

	EnablePurgeDead bool
	// EnablePurgeDeadがtrueの場合は正の値が必須です。
	DeadMaxFileAge int
	// EnablePurgeDeadがtrueの場合は正の値が必須です。
	DeadMaxDirSize uint64

	EnablePurgeQuarantine bool
	// EnablePurgeQuarantineがtrueの場合は正の値が必須です。
	QuarantineMaxFileAge int
	// EnablePurgeQuarantineがtrueの場合は正の値が必須です。
	QuarantineMaxDirSize uint64

	// by-size purge を走らせる最小間隔。
	// 正の値が必須です。
	SizePurgeInterval time.Duration

	// true の場合、by-size purge の判定に size 推定値を使う。
	//
	// 推定値が max dir size 以下であれば full scan を省略できる。
	// ただし、推定値のみを使用するケースは、dir sizeが　max dir sizeを超えないと推定されるケースだけである
	// 削除が必要そうな場合は必ず filesystem を full scan し、
	// 実測した総サイズを基準に purge する。
	EnableSizeEstimateCache bool

	// size check を何回行うごとに full scan を行うか。
	// EnableSizeEstimateCacheがtrueの場合は正の値が必須です。
	// 1の場合は毎回full scanします。
	SizeFullScanEveryNChecks int

	// full scan を最低でもこの間隔では実行する。
	// EnableSizeEstimateCacheがtrueの場合は正の値が必須です。
	SizeFullScanMaxInterval time.Duration
}
