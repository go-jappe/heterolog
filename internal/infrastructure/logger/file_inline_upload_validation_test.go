package logger

import (
	"context"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	objectstoragevalidation "github.com/go-jappe/heterolog/pkg/objectstorage/validation"

	"github.com/stretchr/testify/require"
)

func TestDirectLoggerConstructorsRejectInvalidInlineFileUpload(t *testing.T) {
	tests := []struct {
		name   string
		config func(*testing.T) pkglogger.LoggerConfig
	}{
		{
			name: "normal destination",
			config: func(t *testing.T) pkglogger.LoggerConfig {
				return pkglogger.LoggerConfig{
					LogOutputDestinations: pkglogger.LogOutputDestinations{
						pkglogger.LogOutputFile,
					},
					File: invalidInlineUploadFileConfig(t, "normal-file"),
				}
			},
		},
		{
			name: "handler-only destination",
			config: func(t *testing.T) pkglogger.LoggerConfig {
				stdoutCfg := newActivationStdoutConfig()
				stdoutCfg.HandlerConfig = pkgloggerhandler.HandlerConfig{
					OnErrorConfig: []string{pkglogger.LogOutputFile.String()},
				}
				return pkglogger.LoggerConfig{
					LogOutputDestinations: pkglogger.LogOutputDestinations{
						pkglogger.LogOutputStdout,
					},
					Stdout: stdoutCfg,
					File:   invalidInlineUploadFileConfig(t, "handler-only-file"),
				}
			},
		},
		{
			name: "dedicated internal log destination",
			config: func(t *testing.T) pkglogger.LoggerConfig {
				stdoutCfg := newActivationStdoutConfig()
				stdoutCfg.HandlerConfig = pkgloggerhandler.HandlerConfig{
					OnErrorConfig: []string{pkglogger.LogOutputFile.String()},
				}
				return pkglogger.LoggerConfig{
					LogOutputDestinations: pkglogger.LogOutputDestinations{
						pkglogger.LogOutputStdout,
					},
					Stdout: stdoutCfg,
					InternalLogDestinations: &pkglogger.InternalLogDestinationConfig{
						File: invalidInlineUploadFileConfig(t, "internal-log-file"),
					},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.config(t)
			// 検証対象のupload先以外は有効にし、upload先の不備だけで拒否されることを確認します。
			cfg.Timeout = time.Second
			cfg.MaxLogEntryBytes = pkglogger.DefaultMaxLogEntryBytes

			syncLogger, err := NewLogger(
				context.Background(),
				cfg,
				pkgcloudwatchlogs.StreamRoutingNone,
				nil,
				nil,
				observer.NopTracer{},
				observer.NopMeter{},
			)

			requireUnsupportedUploadTarget(t, err, "unsupported")
			require.Nil(t, syncLogger)

			asyncCfg := asyncConfigFromLoggerConfig(cfg)
			asyncCfg.MaxParallel = 1
			asyncLogger, err := NewAsyncLogger(
				context.Background(),
				asyncCfg,
				pkgcloudwatchlogs.StreamRoutingNone,
				nil,
				nil,
				observer.NopTracer{},
				observer.NopMeter{},
			)

			requireUnsupportedUploadTarget(t, err, "unsupported")
			require.Nil(t, asyncLogger)
		})
	}
}

// requireUnsupportedUploadTargetは、errが対応するproviderのないupload targetを報告することを
// 確認します。
func requireUnsupportedUploadTarget(t *testing.T, err error, target objectstorage.UploadTarget) {
	t.Helper()
	require.ErrorIs(t, err, objectstoragevalidation.ErrUnsupportedUploadTarget)
	var targetErr *objectstoragevalidation.UnsupportedUploadTargetError
	require.ErrorAs(t, err, &targetErr)
	require.Equal(t, target, targetErr.Target)
}

func invalidInlineUploadFileConfig(
	t *testing.T,
	target string,
) *pkglogger.FileLoggerConfig {
	t.Helper()
	cfg := newActivationFileConfig(t, target)
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{
		Enabled: true,
		Default: objectstorage.UploadTargetConfig{
			Enabled: true,
			Targets: []objectstorage.UploadTarget{"unsupported"},
		},
	}
	return cfg
}

// asyncConfigFromLoggerConfigは同じbackend構成をasync設定へ写します。LoggerConfigに
// 対応する値がないMaxParallelは、呼び出し側で有効な値を指定してください。
func asyncConfigFromLoggerConfig(
	cfg pkglogger.LoggerConfig,
) pkglogger.AsyncBestEffortMultiLoggerConfig {
	return pkglogger.AsyncBestEffortMultiLoggerConfig{
		LogOutputDestinations:   cfg.LogOutputDestinations,
		Timeout:                 cfg.Timeout,
		MaxLogEntryBytes:        cfg.MaxLogEntryBytes,
		File:                    cfg.File,
		Stdout:                  cfg.Stdout,
		Slack:                   cfg.Slack,
		OpenSearch:              cfg.OpenSearch,
		OpenSearchClient:        cfg.OpenSearchClient,
		Loki:                    cfg.Loki,
		LokiClient:              cfg.LokiClient,
		CloudWatchLogs:          cfg.CloudWatchLogs,
		CloudWatchLogsClient:    cfg.CloudWatchLogsClient,
		ClickHouse:              cfg.ClickHouse,
		ClickHouseClient:        cfg.ClickHouseClient,
		InternalLogDestinations: cfg.InternalLogDestinations,
		Resource:                cfg.Resource,
	}
}
