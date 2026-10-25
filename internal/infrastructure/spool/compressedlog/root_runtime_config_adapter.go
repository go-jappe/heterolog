package compressedlog

import (
	"time"

	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// rootRuntimeConfigAdapterは構築時に読み取った周期・保持設定を値として固定します。
// RootRuntime・runner・purgeは同じadapterを参照し、元providerを読み直しません。
// 値の補完や検証はせず、検証はvalidateRootRuntimeConfigと各利用先が行います。
type rootRuntimeConfigAdapter struct {
	runnerInterval             time.Duration
	lifecycleInterval          time.Duration
	circuitBreakerOpenCooldown time.Duration
	maxRetryAfter              time.Duration
	done                       statePurgeConfigSnapshot
	dead                       statePurgeConfigSnapshot
	quarantine                 statePurgeConfigSnapshot
}

var _ rootRuntimeConfigProvider = rootRuntimeConfigAdapter{}

func newRootRuntimeConfigAdapter(config ConfigProvider) rootRuntimeConfigAdapter {
	return rootRuntimeConfigAdapter{
		runnerInterval:             config.RunnerInterval(),
		lifecycleInterval:          config.LifecycleInterval(),
		circuitBreakerOpenCooldown: config.CircuitBreakerOpenCooldown(),
		maxRetryAfter:              config.MaxRetryAfter(),
		done:                       snapshotStatePurgeConfig(config.DonePurge()),
		dead:                       snapshotStatePurgeConfig(config.DeadPurge()),
		quarantine:                 snapshotStatePurgeConfig(config.QuarantinePurge()),
	}
}

// IntervalはRunnerConfigProviderの再送周期を返します。
func (a rootRuntimeConfigAdapter) Interval() time.Duration { return a.runnerInterval }

// PurgeIntervalは、Runnerが再送とは別周期でterminal領域をpurgeする間隔です。
func (a rootRuntimeConfigAdapter) PurgeInterval() time.Duration {
	return a.lifecycleInterval
}

func (a rootRuntimeConfigAdapter) CircuitBreakerOpenCooldown() time.Duration {
	return a.circuitBreakerOpenCooldown
}

// MaxRetryAfterは、upload先がRetry-Afterで指定した宛先別の再送待機の上限です。
func (a rootRuntimeConfigAdapter) MaxRetryAfter() time.Duration {
	return a.maxRetryAfter
}

func (a rootRuntimeConfigAdapter) DonePurge() objectstoragespool.StatePurgeConfigProvider {
	return a.done.provider()
}

func (a rootRuntimeConfigAdapter) DeadPurge() objectstoragespool.StatePurgeConfigProvider {
	return a.dead.provider()
}

func (a rootRuntimeConfigAdapter) QuarantinePurge() objectstoragespool.StatePurgeConfigProvider {
	return a.quarantine.provider()
}

// CapacityPurgeMinimumFileAgeは保存直後のpurgeを避けるため、purge一周期分を返します。
// lifecycleIntervalと同じ値なので、別フィールドには保持しません。
func (a rootRuntimeConfigAdapter) CapacityPurgeMinimumFileAge() time.Duration {
	return a.lifecycleInterval
}

// statePurgeConfigSnapshotはproviderから取得した保持設定を固定します。
// 同じ保存先を共有する場合、無効なstateの設定値も含めて一致を確認します。
type statePurgeConfigSnapshot struct {
	present        bool
	enabled        bool
	maxFileAgeDays int
	maxDirSize     uint64
}

func snapshotStatePurgeConfig(config objectstorage.CompressedLogFileUploadSpoolPurgeConfigProvider) statePurgeConfigSnapshot {
	if config == nil {
		return statePurgeConfigSnapshot{}
	}
	return statePurgeConfigSnapshot{
		present: true, enabled: config.Enabled(), maxFileAgeDays: config.MaxFileAgeDays(), maxDirSize: config.MaxDirSize(),
	}
}

func (s statePurgeConfigSnapshot) provider() objectstoragespool.StatePurgeConfigProvider {
	if !s.present {
		return nil
	}
	return s
}

func (s statePurgeConfigSnapshot) Enabled() bool       { return s.enabled }
func (s statePurgeConfigSnapshot) MaxFileAgeDays() int { return s.maxFileAgeDays }
func (s statePurgeConfigSnapshot) MaxDirSize() uint64  { return s.maxDirSize }
