package objectstorage

import "time"

// CompressedLogFileUploadSpoolConfigProviderは、圧縮ログの再送用spoolの設定を供給します。
// 設定の具体型と保持方法は構築側が決めます。共有するruntimeは解決済みの保存先で識別し、
// このproviderの値やポインタを識別子には使いません。
type CompressedLogFileUploadSpoolConfigProvider interface {
	SpoolPath() string
	Target() string
	LoggerContract() string
	RunnerInterval() time.Duration
	LifecycleInterval() time.Duration
	CircuitBreakerOpenCooldown() time.Duration
	// MaxRetryAfterは、upload先がRetry-Afterで指定した宛先別の再送待機の上限です。
	MaxRetryAfter() time.Duration
	DonePurge() CompressedLogFileUploadSpoolPurgeConfigProvider
	DeadPurge() CompressedLogFileUploadSpoolPurgeConfigProvider
	QuarantinePurge() CompressedLogFileUploadSpoolPurgeConfigProvider
}

// CompressedLogFileUploadSpoolPurgeConfigProviderは、再送処理を終えた一つのstateの保持設定です。
type CompressedLogFileUploadSpoolPurgeConfigProvider interface {
	Enabled() bool
	MaxFileAgeDays() int
	MaxDirSize() uint64
}
