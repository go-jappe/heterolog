package store

import (
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"time"
)

// SpoolStoreOverloadPolicyConfigProviderは、active spoolへの保存可否を決める
// overload設定と、保存可否には影響しない滞留Warn設定を提供します。
type SpoolStoreOverloadPolicyConfigProvider interface {
	OverloadPolicy() durable.OverloadPolicy     // drop or spill
	SpillDestination() durable.SpillDestination // spoolに受け入れられない(spill)場合の二次退避先　s3 stdoutなど

	// 前回の統計 scan 以降、この回数を超えて overload 判定が呼ばれたら、
	// StatsScanInterval の期限前でも active ディレクトリ統計を再 scan する。
	// 0 なら呼び出し回数による再 scan は無効。
	MaxCallsSinceScan() uint64

	// overload 判定用に保持している active ディレクトリ統計の有効期間。
	// この期間を過ぎたら、次回判定時に統計を再 scan する。
	// 0 以下なら毎回再 scan する。
	StatsScanInterval() time.Duration

	// MaxDirSizeをactiveディレクトリサイズが超えた場合は、spoolへの保存を拒否する。
	MaxDirSize() uint64

	// ActiveBacklogWarnAfterDaysはactiveの最古データに対する滞留Warn開始日数。
	// 0なら警告無効。超過してもoverloadとはみなさず、spoolへの保存は継続する。
	ActiveBacklogWarnAfterDays() int

	// MinAvailableDiskSpaceを利用可能容量が下回った場合は、spoolへの保存を拒否する。
	MinAvailableDiskSpace() uint64

	UseMemoryPressureForOverload() bool // メモリ逼迫率に応じてOverload判定を行うか

	// メモリ逼迫率がこの値以上なら、単発の critical 条件に該当する。
	// 0 以下ならこの条件は無効。
	MemoryCriticalRatio() float64

	// 利用可能メモリがこの値以下なら、単発の critical 条件に該当する。
	// 0 ならこの条件は無効。
	MinAvailableMemoryBytes() uint64

	// 単発の critical 条件にこの回数連続で該当した場合に、
	// 実際の memory critical 状態として扱う。
	// 1 以下なら 1 回の該当で critical とみなす。
	MemoryCriticalConsecutiveHits() int
}
