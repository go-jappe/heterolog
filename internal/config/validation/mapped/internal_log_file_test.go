package mappedvalidation

import (
	"testing"
	"time"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestValidateMappedInternalLogFileUsesLoggerLifecycleContract(t *testing.T) {
	err := validateMappedInternalLogFileConfig(
		"logger",
		&pkglogger.InternalLogDestinationConfig{
			File: &pkglogger.FileLoggerConfig{
				LifecycleMode: pkgfsstore.FileLifecycleModeInline,
				FsstoreConfig: pkgfsstore.Config{
					Layout:    pkgfsstore.FsstoreConfig{LogDir: "."},
					Lifecycle: pkgfsstore.FileLifecycleConfig{UsageCacheTTL: time.Minute},
				},
				ArchiveCheckpointConfig: pkgfsstore.ArchiveCheckpointConfig{
					Directory:    "/var/lib/heterolog/archive-checkpoints",
					SourcePrefix: "internal_log_file_validation",
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("internal-log file with inline lifecycle mode error = %v", err)
	}
}

func TestValidateMappedDeliveryInternalLogFileUsesDeliveryLifecycleContract(
	t *testing.T,
) {
	err := validateMappedDeliveryInternalLogFileConfig(
		"delivery logger",
		&pkglogger.DeliveryInternalLogDestinationConfig{
			File: &pkglogger.DeliveryFileLoggerConfig{
				LifecycleMode: pkgfsstore.FileLifecycleModeInline,
			},
		},
	)
	// 設定したmodeはinlineだけなので、このsentinelはinlineを拒否したことを表します。
	requireConfigError(
		t,
		err,
		"delivery logger internal log file",
		ConfigSectionLifecycle,
		pkgfsstore.ErrUnsupportedDeliveryFileLifecycleMode,
	)
}
