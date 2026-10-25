package objectstorage

import "time"

type compressedLogFileUploadSpoolConfigAdapter struct {
	config CompressedLogFileUploadSpoolConfig
}

// NewCompressedLogFileUploadSpoolConfigAdapterは、構築側の設定をコピーしてproviderへ変換します。
// 既定値の補完や検証は行いません。変換後に元の設定を書き換えてもproviderには反映しません。
func NewCompressedLogFileUploadSpoolConfigAdapter(config CompressedLogFileUploadSpoolConfig) CompressedLogFileUploadSpoolConfigProvider {
	return compressedLogFileUploadSpoolConfigAdapter{config: config}
}

func (a compressedLogFileUploadSpoolConfigAdapter) SpoolPath() string {
	return a.config.SpoolPath
}

func (a compressedLogFileUploadSpoolConfigAdapter) Target() string {
	return a.config.Target
}

func (a compressedLogFileUploadSpoolConfigAdapter) LoggerContract() string {
	return a.config.LoggerContract
}

func (a compressedLogFileUploadSpoolConfigAdapter) RunnerInterval() time.Duration {
	return a.config.RunnerInterval
}

func (a compressedLogFileUploadSpoolConfigAdapter) LifecycleInterval() time.Duration {
	return a.config.LifecycleInterval
}

func (a compressedLogFileUploadSpoolConfigAdapter) CircuitBreakerOpenCooldown() time.Duration {
	return a.config.CircuitBreakerOpenCooldown
}

func (a compressedLogFileUploadSpoolConfigAdapter) MaxRetryAfter() time.Duration {
	return a.config.MaxRetryAfter
}

func (a compressedLogFileUploadSpoolConfigAdapter) DonePurge() CompressedLogFileUploadSpoolPurgeConfigProvider {
	return compressedLogFileUploadSpoolPurgeConfigAdapter{config: a.config.Done}
}

func (a compressedLogFileUploadSpoolConfigAdapter) DeadPurge() CompressedLogFileUploadSpoolPurgeConfigProvider {
	return compressedLogFileUploadSpoolPurgeConfigAdapter{config: a.config.Dead}
}

func (a compressedLogFileUploadSpoolConfigAdapter) QuarantinePurge() CompressedLogFileUploadSpoolPurgeConfigProvider {
	return compressedLogFileUploadSpoolPurgeConfigAdapter{config: a.config.Quarantine}
}

type compressedLogFileUploadSpoolPurgeConfigAdapter struct {
	config CompressedLogFileUploadSpoolPurgeConfig
}

func (a compressedLogFileUploadSpoolPurgeConfigAdapter) Enabled() bool {
	return a.config.Enabled
}

func (a compressedLogFileUploadSpoolPurgeConfigAdapter) MaxFileAgeDays() int {
	return a.config.MaxFileAgeDays
}

func (a compressedLogFileUploadSpoolPurgeConfigAdapter) MaxDirSize() uint64 {
	return a.config.MaxDirSize
}
