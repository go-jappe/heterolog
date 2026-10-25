package lifecycle

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestDeliveryLifecycleFactoryRejectsInvalidDefaultPurgeBeforeSideEffects(t *testing.T) {
	for _, separated := range []bool{false, true} {
		for _, invalid := range []struct {
			name  string
			apply func(*pkgfsstore.DeliveryFileLifecycleConfig)
		}{
			{"backups", func(cfg *pkgfsstore.DeliveryFileLifecycleConfig) { cfg.MaxFileBackups = 0 }},
			{"age", func(cfg *pkgfsstore.DeliveryFileLifecycleConfig) { cfg.MaxFileAge = 0 }},
			{"capacity", func(cfg *pkgfsstore.DeliveryFileLifecycleConfig) { cfg.MaxDirSize = 1 }},
		} {
			name := "default-only/" + invalid.name
			if separated {
				name = "separated/" + invalid.name
			}
			t.Run(name, func(t *testing.T) {
				logCfg := rolePurgeRunnerTestConfig(t)
				logCfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = separated
				logCfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = separated
				if separated {
					// error/debugは有効な個別設定を持ち、default系列の誤りを経由せずに検出します。
					logCfg.FsstoreConfig.Lifecycle.ErrorPurge = &pkgfsstore.FilePurgeConfig{MaxFileAge: 30, MaxFileBackups: 6, MaxDirSize: 20 << 20}
					logCfg.FsstoreConfig.Lifecycle.DebugPurge = &pkgfsstore.FilePurgeConfig{MaxFileAge: 30, MaxFileBackups: 6, MaxDirSize: 20 << 20}
				}
				cfg := deliveryRolePurgeTestConfig(logCfg)
				invalid.apply(&cfg.FsstoreConfig.Lifecycle)

				lifecycles, err := NewDeliveryFileLifecycles(DeliveryFileOptions{Config: cfg})
				require.Nil(t, lifecycles)
				require.ErrorIs(t, err, ErrInvalidDefaultLogPurgeConfiguration)
				require.NotErrorIs(t, err, ErrInvalidRoleLogPurgeConfiguration)
				// default系列のどの保持設定を拒否したかを、項目ごとのsentinelで確認します。
				requireOnlyPurgeFieldRejected(t, err, invalid.name)
				entries, readErr := os.ReadDir(cfg.FsstoreConfig.Layout.LogPath)
				require.NoError(t, readErr)
				require.Empty(t, entries, "invalid retention must not create logs or checkpoint stores")
			})
		}
	}
}
