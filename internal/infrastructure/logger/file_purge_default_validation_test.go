package logger

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// Delivery writerは保持設定を持たないため、runnerへ渡すdefault系列の保持設定も
// 構築前に検証します。分離が無効でdefault系列だけを使う場合や、error/debugが
// 有効な個別設定を持つ場合も、default系列の誤りとして拒否します。
func TestDeliveryLoggerRejectsInvalidDefaultPurgeBeforeIO(t *testing.T) {
	for _, internal := range []bool{false, true} {
		label := "delivery logger file"
		if internal {
			label = "delivery logger internal log file"
		}
		for _, separated := range []bool{false, true} {
			for _, invalid := range []struct {
				name  string
				apply func(*pkgfsstore.FileLifecycleConfig)
				want  error
			}{
				{"backups", func(cfg *pkgfsstore.FileLifecycleConfig) { cfg.MaxFileBackups = 0 }, fsstore.ErrInvalidMaxFileBackups},
				{"age", func(cfg *pkgfsstore.FileLifecycleConfig) { cfg.MaxFileAge = 0 }, fsstore.ErrInvalidMaxFileAge},
				{"capacity", func(cfg *pkgfsstore.FileLifecycleConfig) { cfg.MaxDirSize = 1 }, fsstore.ErrInvalidMaxDirSize},
			} {
				name := label + "/default-only/" + invalid.name
				if separated {
					name = label + "/separated/" + invalid.name
				}
				t.Run(name, func(t *testing.T) {
					root := filepath.Join(t.TempDir(), "uncreated")
					file := rolePurgeConstructionFileConfig(root)
					file.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = separated
					file.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = separated
					if separated {
						file.FsstoreConfig.Lifecycle.ErrorPurge = &pkgfsstore.FilePurgeConfig{MaxFileBackups: 5, MaxFileAge: 5, MaxDirSize: 50 << 20}
						file.FsstoreConfig.Lifecycle.DebugPurge = &pkgfsstore.FilePurgeConfig{MaxFileBackups: 5, MaxFileAge: 5, MaxDirSize: 50 << 20}
					}
					invalid.apply(&file.FsstoreConfig.Lifecycle)
					cfg := pkglogger.LoggerConfig{
						Timeout: time.Second, MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes, File: file,
					}
					if internal {
						cfg.InternalLogDestinations = &pkglogger.InternalLogDestinationConfig{File: file}
						cfg.File = rolePurgeConstructionFileConfig(root)
						cfg.File.FsstoreConfig.Layout.LogDir = "base"
					}

					err := constructRolePurgeTestLogger(t, "delivery logger", cfg)
					// default系列だけを一件拒否し、有効なerror/debugの個別設定は拒否しません。
					requireSingleFilePurgeConfigError(
						t, err, label,
						deliveryFilePurgeConfigFields[appendlog.PurgeRoleDefault], appendlog.PurgeRoleDefault, invalid.want,
					)
					if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("constructor created filesystem resources: stat error = %v", err)
					}
				})
			}
		}
	}
}
