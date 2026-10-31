package config

import (
	"errors"
	"time"

	pkgenv "github.com/go-jappe/heterolog/pkg/env"
)

// AdminServerConfigは、稼働中に出力先の設定を変更する管理用APIの、管理用サーバーの
// 環境設定です。Addrが空なら管理用サーバーを起動しません。値の検証と起動は、
// 公開APIのLoadEnvConfigとStartAdminServerが行います。Tokenは秘密のため、表示しないでください。
type AdminServerConfig struct {
	Addr        string
	Token       string
	TLSCertFile string
	TLSKeyFile  string
	Status      AdminStatusConfig
}

// AdminStatusConfigは、管理用APIのGET /statusの環境設定です。DirectoryScanIntervalが0なら
// directoryのscanをしません。percentは0から100です。
type AdminStatusConfig struct {
	DirectoryScanInterval        time.Duration
	DiskFreeWarningPercent       float64
	DiskFreeCriticalPercent      float64
	DirectorySizeWarningPercent  float64
	BackendFailureWarningPercent float64
	SlowRequest                  time.Duration
}

// 管理用APIのstatusの既定値です。pkg/adminのDefaultStatusConfigと同じ値で、環境設定では
// scanの間隔を0にして無効にできます。
const (
	defaultAdminStatusScanInterval       = 5 * time.Minute
	defaultAdminStatusDiskFreeWarning    = 10.0
	defaultAdminStatusDiskFreeCritical   = 5.0
	defaultAdminStatusDirectorySizeWarn  = 90.0
	defaultAdminStatusBackendFailureWarn = 10.0
	defaultAdminStatusSlowRequest        = 5 * time.Second
)

func loadAdminServerConfig(env pkgenv.Lookup) (AdminServerConfig, error) {
	scanInterval, scanIntervalErr := env.GetEnvDuration("HETEROLOG_ADMIN_STATUS_DIRECTORY_SCAN_INTERVAL_SECOND", defaultAdminStatusScanInterval)
	diskWarning, diskWarningErr := env.GetEnvFloat64("HETEROLOG_ADMIN_STATUS_DISK_FREE_WARNING_PERCENT", defaultAdminStatusDiskFreeWarning)
	diskCritical, diskCriticalErr := env.GetEnvFloat64("HETEROLOG_ADMIN_STATUS_DISK_FREE_CRITICAL_PERCENT", defaultAdminStatusDiskFreeCritical)
	dirWarning, dirWarningErr := env.GetEnvFloat64("HETEROLOG_ADMIN_STATUS_DIRECTORY_SIZE_WARNING_PERCENT", defaultAdminStatusDirectorySizeWarn)
	failureWarning, failureWarningErr := env.GetEnvFloat64("HETEROLOG_ADMIN_STATUS_BACKEND_FAILURE_WARNING_PERCENT", defaultAdminStatusBackendFailureWarn)
	slowRequest, slowRequestErr := env.GetEnvDuration("HETEROLOG_ADMIN_STATUS_SLOW_REQUEST_SECOND", defaultAdminStatusSlowRequest)
	return AdminServerConfig{
		Addr:        env.GetEnvString("HETEROLOG_ADMIN_ADDR", ""),
		Token:       env.GetEnvString("HETEROLOG_ADMIN_TOKEN", ""),
		TLSCertFile: env.GetEnvString("HETEROLOG_ADMIN_TLS_CERT_FILE", ""),
		TLSKeyFile:  env.GetEnvString("HETEROLOG_ADMIN_TLS_KEY_FILE", ""),
		Status: AdminStatusConfig{
			DirectoryScanInterval:        scanInterval,
			DiskFreeWarningPercent:       diskWarning,
			DiskFreeCriticalPercent:      diskCritical,
			DirectorySizeWarningPercent:  dirWarning,
			BackendFailureWarningPercent: failureWarning,
			SlowRequest:                  slowRequest,
		},
	}, errors.Join(scanIntervalErr, diskWarningErr, diskCriticalErr, dirWarningErr, failureWarningErr, slowRequestErr)
}
