package config

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

func TestInternalLogErrorDirectoryMappingAndClaims(t *testing.T) {
	const baseErrorDir = "./base-errors"
	tests := []struct {
		name          string
		separateError bool
		handlerOnly   bool
		override      logconfig.InternalLogDestinationConfig
		wantDedicated bool
	}{
		{
			name:     "disabled error directory override shares base",
			override: logconfig.InternalLogDestinationConfig{FileErrorLogDir: "./internal-errors"},
		},
		{
			name: "disabled error directory override accompanies active destination change",
			override: logconfig.InternalLogDestinationConfig{
				FileLogDir: "internal-logs", FileErrorLogDir: "./internal-errors",
			},
			wantDedicated: true,
		},
		{
			name:          "disabled error directory is inherited with active destination change",
			override:      logconfig.InternalLogDestinationConfig{FileLogDir: "internal-logs"},
			wantDedicated: true,
		},
		{
			name:          "enabled error directory override creates dedicated destination",
			separateError: true,
			handlerOnly:   true,
			override:      logconfig.InternalLogDestinationConfig{FileErrorLogDir: "./internal-errors"},
			wantDedicated: true,
		},
		{
			name:          "enabled identical error directory shares base",
			separateError: true,
			override:      logconfig.InternalLogDestinationConfig{FileErrorLogDir: "base-errors"},
		},
	}
	for _, contract := range []pkglogger.LoggerContractKind{
		pkglogger.LoggerContractSync, pkglogger.LoggerContractAsyncBestEffort, pkglogger.LoggerContractDelivery,
	} {
		for _, tt := range tests {
			t.Run(string(contract)+"/"+tt.name, func(t *testing.T) {
				cfg := mapperValidationTestConfig()
				cfg.RuntimeConfig.Target = "app"
				cfg.FsstoreConfig.LogPath = filepath.Join(t.TempDir(), "logs")
				cfg.FsstoreConfig.AppStatePath = t.TempDir()
				configureDebugDirectoryModeContract(&cfg, contract, logconfig.FileSystemStoreConfig{
					LogDir: "base-logs", LifecycleMode: pkgfsstore.FileLifecycleModeDisable,
				}, tt.override, !tt.handlerOnly)
				// 既存の共有fixtureに、今回検証するerror roleの生の設定を追加します。
				var sourceErrorDir *string
				var sourceOverride *logconfig.InternalLogDestinationConfig
				switch contract {
				case pkglogger.LoggerContractSync:
					cfg.Logger.FileSystemStoreConfig.IsSeparateErrorLogsEnabled = tt.separateError
					sourceErrorDir = &cfg.Logger.FileSystemStoreConfig.ErrorLogDir
					sourceOverride = &cfg.Logger.InternalLogDestinationConfig
				case pkglogger.LoggerContractAsyncBestEffort:
					cfg.AsyncBestEffortMultiLoggerConfig.FileSystemStoreConfig.IsSeparateErrorLogsEnabled = tt.separateError
					sourceErrorDir = &cfg.AsyncBestEffortMultiLoggerConfig.FileSystemStoreConfig.ErrorLogDir
					sourceOverride = &cfg.AsyncBestEffortMultiLoggerConfig.InternalLogDestinationConfig
				case pkglogger.LoggerContractDelivery:
					cfg.DeliveryLogger.FileSystemStoreConfig.IsSeparateErrorLogsEnabled = tt.separateError
					sourceErrorDir = &cfg.DeliveryLogger.FileSystemStoreConfig.ErrorLogDir
					sourceOverride = &cfg.DeliveryLogger.InternalLogDestinationConfig
				}
				*sourceErrorDir = baseErrorDir

				baseLayout, internalLayout := debugDirectoryModeMappedLayouts(cfg, contract)
				require.Equal(t, baseErrorDir, baseLayout.ErrorLogDir, "過去raw回復用の元設定を失わない")
				require.Equal(t, tt.separateError, baseLayout.IsSeparateErrorLogsEnabled)
				require.Equal(t, tt.wantDedicated, internalLayout != nil)
				if internalLayout != nil {
					wantErrorDir := baseErrorDir
					if tt.override.FileErrorLogDir != "" {
						wantErrorDir = tt.override.FileErrorLogDir
					}
					require.Equal(t, wantErrorDir, internalLayout.ErrorLogDir, "比較用の除外を実設定へ書き戻さない")
					require.Equal(t, tt.separateError, internalLayout.IsSeparateErrorLogsEnabled)
					if tt.override.FileLogDir != "" {
						require.Equal(t, tt.override.FileLogDir, internalLayout.LogDir)
					}
				}

				claims, err := cfg.collectStorageDirectoryClaims()
				require.NoError(t, err)
				require.NoError(t, validateStorageDirectoryClaims(claims), "無効なerror directoryだけで別ownerとの競合を作らない")
				internalSource := loggerInternalLogOutputSource(contract, logconfig.LogOutputFile)
				defaultClaims := storageClaimsBySource(claims, internalSource+".default")
				if tt.wantDedicated {
					require.Len(t, defaultClaims, 1)
					if tt.separateError {
						errorClaims := storageClaimsBySource(claims, internalSource+".error")
						require.Len(t, errorClaims, 1)
						wantPath, err := canonicalStoragePath(filepath.Join(cfg.FsstoreConfig.LogPath, "app", tt.override.FileErrorLogDir))
						require.NoError(t, err)
						require.Equal(t, wantPath, errorClaims[0].path)
					}
				} else {
					for _, claim := range claims {
						require.False(t, strings.HasPrefix(claim.source, internalSource+"."), "base共有時にinternal用の所有権を追加しない")
					}
				}
				require.Equal(t, baseErrorDir, *sourceErrorDir)
				require.Equal(t, tt.override, *sourceOverride)
			})
		}
	}
}
