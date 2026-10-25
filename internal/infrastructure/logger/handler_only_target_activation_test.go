package logger

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	filelogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/file"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	stdoutlogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/stdout"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"

	"github.com/stretchr/testify/require"
)

func TestHandlerOnlyTargetDoesNotStartNormalSyncOrAsyncChild(t *testing.T) {
	targets := []pkglogger.LogOutputDestination{
		pkglogger.LogOutputFile,
		pkglogger.LogOutputStdout,
		pkglogger.LogOutputSlack,
		pkglogger.LogOutputOpenSearch,
		pkglogger.LogOutputLoki,
		pkglogger.LogOutputCloudWatchLogs,
		pkglogger.LogOutputClickHouse,
	}

	for _, mode := range []string{"sync", "async"} {
		for _, target := range targets {
			t.Run(mode+"/"+target.String(), func(t *testing.T) {
				cfg, sourceName, expectedType := newHandlerOnlyLoggerConfig(t, target)
				if mode == "async" {
					cfg = (&pkglogger.AsyncBestEffortMultiLoggerConfig{
						LogOutputDestinations: cfg.LogOutputDestinations,
						File:                  cfg.File,
						Stdout:                cfg.Stdout,
						Slack:                 cfg.Slack,
						OpenSearch:            cfg.OpenSearch,
						OpenSearchClient:      cfg.OpenSearchClient,
						Loki:                  cfg.Loki,
						LokiClient:            cfg.LokiClient,
						CloudWatchLogs:        cfg.CloudWatchLogs,
						CloudWatchLogsClient:  cfg.CloudWatchLogsClient,
						ClickHouse:            cfg.ClickHouse,
						ClickHouseClient:      cfg.ClickHouseClient,
						Resource:              cfg.Resource,
					}).ToLoggerConfig()
				}

				fileLoggerSet, err := newFileLoggerSet(cfg.File, observer.NopTracer{}, observer.NopMeter{})
				require.NoError(t, err)
				if fileLoggerSet != nil {
					t.Cleanup(func() { require.NoError(t, fileLoggerSet.Close()) })
				}

				stdoutTarget, err := newStdoutNotificationTarget(cfg)
				require.NoError(t, err)
				available := getHandler(cfg, streamrouting.None, fileLoggerSet, stdoutTarget)
				requireSelectedErrorTarget(t, cfg, sourceName, target.String(), available)

				children, _, err := buildLoggers(
					context.Background(),
					cfg,
					streamrouting.None,
					nil,
					observer.NopTracer{},
					observer.NopMeter{},
					fileLoggerSet,
					nil,
					nil,
					pkglogger.LoggerContractSync,
					nil,
				)
				require.NoError(t, err)
				require.Len(t, children, 1)
				require.Equal(t, expectedType, fmt.Sprintf("%T", children[0]))
			})
		}
	}
}

func TestHandlerOnlyTargetDoesNotStartNormalDeliveryChild(t *testing.T) {
	targets := []pkglogger.LogOutputDestination{
		pkglogger.LogOutputFile,
		pkglogger.LogOutputStdout,
		pkglogger.LogOutputSlack,
		pkglogger.LogOutputOpenSearch,
		pkglogger.LogOutputLoki,
		pkglogger.LogOutputCloudWatchLogs,
		pkglogger.LogOutputClickHouse,
	}

	for _, target := range targets {
		t.Run(target.String(), func(t *testing.T) {
			cfg, sourceName, expectedType := newHandlerOnlyDeliveryConfig(t, target)
			fileLoggerSet, err := newDeliveryFileLoggerSet(
				cfg.File,
				observer.NopTracer{},
				observer.NopMeter{},
			)
			require.NoError(t, err)
			if fileLoggerSet != nil {
				t.Cleanup(func() { require.NoError(t, fileLoggerSet.Close()) })
			}

			stdoutTarget, err := newDeliveryStdoutNotificationTarget(cfg)
			require.NoError(t, err)
			available := getDeliveryHandler(cfg, streamrouting.None, fileLoggerSet, stdoutTarget)
			requireSelectedDeliveryErrorTarget(t, cfg, sourceName, target.String(), available)

			children, _, err := buildDeliveryLoggers(
				context.Background(),
				cfg,
				streamrouting.None,
				observer.NopTracer{},
				observer.NopMeter{},
				fileLoggerSet,
				nil,
				nil,
			)
			require.NoError(t, err)
			require.Len(t, children, 1)
			require.Equal(t, expectedType, fmt.Sprintf("%T", children[0].Logger))
			// 通常出力のchildは、multi loggerが作る送達結果もchild自身の結果と同じ論理出力名で
			// 報告します（handler専用のtargetはchildになりません）。
			require.Equal(t, sourceName, children[0].Backend)
		})
	}
}

func TestBuildDeliveryLoggersFileSplitFlagsAreIndependent(t *testing.T) {
	tests := []struct {
		name          string
		separateError bool
		separateDebug bool
	}{
		{name: "neither"},
		{name: "error only", separateError: true},
		{name: "debug only", separateDebug: true},
		{name: "both", separateError: true, separateDebug: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fileCfg := newCloseOwnershipDeliveryFileConfig(t.TempDir(), "delivery-split-flags")
			fileCfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = tt.separateError
			fileCfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = tt.separateDebug
			cfg := pkglogger.DeliveryLoggerConfig{
				LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
				File:                  fileCfg,
			}
			fileLoggerSet, err := newDeliveryFileLoggerSet(
				cfg.File,
				observer.NopTracer{},
				observer.NopMeter{},
			)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, fileLoggerSet.Close()) })

			children, _, err := buildDeliveryLoggers(
				context.Background(),
				cfg,
				streamrouting.None,
				observer.NopTracer{},
				observer.NopMeter{},
				fileLoggerSet,
				nil,
				nil,
			)
			require.NoError(t, err)
			require.Len(t, children, 1, "physical file roles must remain one logical delivery target")
			require.Equal(t, pkglogger.LogOutputFile.String(), children[0].Backend)
		})
	}
}

func TestDeliveryHandlerOnlyFileIsLazyAndOwnedByTopLevelLogger(t *testing.T) {
	cfg, _, _ := newHandlerOnlyDeliveryConfig(t, pkglogger.LogOutputFile)
	root := cfg.File.FsstoreConfig.Layout.LogPath
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}
	topLevel, err := NewDeliveryLogger(context.Background(), cfg, streamrouting.None, tracer, meter)
	require.NoError(t, err)

	owned, ok := topLevel.(*topLevelDeliveryLoggerRuntime)
	require.True(t, ok)
	require.Empty(t, logFilesUnder(t, root), "handler-only delivery file must stay lazy")

	available := getDeliveryHandler(cfg, streamrouting.None, owned.fileLoggerSet, owned.internalLogDestinations.stdoutNotificationTarget)
	sourceCfg := stdoutlogger.NewDeliveryConfigAdapter(*cfg.Stdout)
	selected := selectConfiguredHandlers(available, sourceCfg.Handler())
	require.Len(t, selected.OnError, 1)
	selected.OnError[0].Handler(
		context.Background(),
		fmt.Errorf("delivery stdout internal failure"),
		&logging.LogEntry{
			Level:   pkglogger.LogLevelError,
			Message: "delivery handler-only file write",
			Source:  pkglogger.LogOutputStdout.String(),
		},
		tracer,
		meter,
	)

	fileLogger, err := owned.fileLoggerSet.NewFileLogger(context.Background())
	require.NoError(t, err)
	content, err := os.ReadFile(fileLogger.Fsstore.ActiveFilePath)
	require.NoError(t, err)
	require.Contains(t, string(content), "delivery handler-only file write")

	require.NoError(t, owned.Close())
	require.Nil(t, fileLogger.Fsstore.File)
}

func newHandlerOnlyLoggerConfig(
	t *testing.T,
	target pkglogger.LogOutputDestination,
) (pkglogger.LoggerConfig, string, string) {
	t.Helper()
	cfg := pkglogger.LoggerConfig{
		Timeout:          time.Second,
		MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
	}
	handlerCfg := pkgloggerhandler.HandlerConfig{OnErrorConfig: []string{target.String()}}
	if target == pkglogger.LogOutputStdout {
		cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
		cfg.File = newActivationFileConfig(t, "sync-source-file")
		cfg.File.HandlerConfig = handlerCfg
		cfg.Stdout = newActivationStdoutConfig()
		return cfg, pkglogger.LogOutputFile.String(), "*file.FileLogger"
	}

	cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
	cfg.Stdout = newActivationStdoutConfig()
	cfg.Stdout.HandlerConfig = handlerCfg
	addLoggerHandlerCandidate(t, &cfg, target)
	return cfg, pkglogger.LogOutputStdout.String(), "*stdout.StdoutLogger"
}

func newHandlerOnlyDeliveryConfig(
	t *testing.T,
	target pkglogger.LogOutputDestination,
) (pkglogger.DeliveryLoggerConfig, string, string) {
	t.Helper()
	cfg := pkglogger.DeliveryLoggerConfig{
		Timeout:          time.Second,
		MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
		InternalLogBatchConfig: pkglogger.DeliveryInternalLogBatchConfig{
			OpenSearch: pkglogger.DeliveryInternalLogOpenSearchBatchConfig{
				FlushInterval:       time.Hour,
				MaxBulkDocument:     1,
				BufferFlushBytes:    testOpenSearchBufferFlushBytes,
				MaxBulkRequestBytes: testOpenSearchMaxBulkRequestBytes,
			},
			Loki: pkglogger.DeliveryInternalLogLokiBatchConfig{
				FlushInterval:           time.Hour,
				MaxEntries:              1,
				BufferFlushBytes:        1,
				FallbackQueueMaxEntries: 1,
			},
			CloudWatchLogs: pkglogger.DeliveryInternalLogCloudWatchLogsBatchConfig{
				FlushInterval:          time.Hour,
				BufferFlushMaxEvents:   1,
				BufferFlushBytes:       1,
				FallbackQueueMaxEvents: 1,
			},
			Slack: pkglogger.DeliveryInternalLogSlackBatchConfig{
				Intervals:              []time.Duration{time.Hour},
				MaxLogsPerNotification: 1,
				MaxRestThreshold:       time.Hour,
			},
		},
	}
	handlerCfg := pkgloggerhandler.HandlerConfig{OnErrorConfig: []string{target.String()}}
	if target == pkglogger.LogOutputStdout {
		cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
		cfg.File = newCloseOwnershipDeliveryFileConfig(t.TempDir(), "delivery-source-file")
		cfg.File.HandlerConfig = handlerCfg
		cfg.Stdout = newActivationDeliveryStdoutConfig()
		return cfg, pkglogger.LogOutputFile.String(), "*file.deliveryRoleLogger"
	}

	cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
	cfg.Stdout = newActivationDeliveryStdoutConfig()
	cfg.Stdout.HandlerConfig = handlerCfg
	addDeliveryHandlerCandidate(t, &cfg, target)
	return cfg, pkglogger.LogOutputStdout.String(), "*stdout.DeliveryStdoutLogger"
}

func addLoggerHandlerCandidate(
	t *testing.T,
	cfg *pkglogger.LoggerConfig,
	target pkglogger.LogOutputDestination,
) {
	t.Helper()
	switch target {
	case pkglogger.LogOutputFile:
		cfg.File = newActivationFileConfig(t, "handler-file")
	case pkglogger.LogOutputSlack:
		cfg.Slack = &pkglogger.SlackLoggerConfig{
			Token:                  "test-token",
			Channel:                "test-channel",
			Timeout:                time.Second,
			Intervals:              []time.Duration{time.Hour},
			MaxLogsPerNotification: 1,
			MaxRestThreshold:       time.Hour,
		}
	case pkglogger.LogOutputOpenSearch:
		cfg.OpenSearch = &pkglogger.OpenSearchConfig{
			OpenSearchLoggerBaseConfig: pkglogger.OpenSearchLoggerBaseConfig{
				FlushInterval:       time.Hour,
				MaxBulkDocument:     1,
				BufferFlushBytes:    testOpenSearchBufferFlushBytes,
				MaxBulkRequestBytes: testOpenSearchMaxBulkRequestBytes,
			},
			OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
				Index: "test-logs",
			},
		}
		cfg.OpenSearch.OpenSearchFlushManagerConfig.Resource = activationResource()
		cfg.OpenSearchClient = validActivationOpenSearchClient()
	case pkglogger.LogOutputLoki:
		cfg.Loki = &pkglogger.LokiConfig{
			LokiLoggerBaseConfig: pkglogger.LokiLoggerBaseConfig{
				FlushInterval:           time.Hour,
				MaxEntries:              1,
				BufferFlushBytes:        directRetryLokiBufferFlushBytes,
				FallbackQueueMaxEntries: directRetryLokiFallbackQueueMaxEntries,
				MaxLineBytes:            directRetryLokiMaxLineBytes,
				MaxPushRequestBytes:     directRetryLokiMaxPushRequestBytes,
			},
		}
		cfg.Loki.LokiFlushManagerConfig.Resource = activationResource()
		cfg.LokiClient = &pkgloki.Config{Endpoint: "http://localhost:3100"}
	case pkglogger.LogOutputCloudWatchLogs:
		cfg.CloudWatchLogs = &pkglogger.CloudWatchLogsConfig{
			CloudWatchLogsLoggerBaseConfig: pkglogger.CloudWatchLogsLoggerBaseConfig{
				FlushInterval:          time.Hour,
				BufferFlushMaxEvents:   1,
				BufferFlushBytes:       1,
				FallbackQueueMaxEvents: 1,
			},
			CloudWatchLogsFlushManagerConfig: pkglogger.CloudWatchLogsFlushManagerConfig{
				LogGroupName:  "test-group",
				LogStreamName: "test-stream",
			},
		}
		cfg.CloudWatchLogs.CloudWatchLogsFlushManagerConfig.Resource = activationResource()
		cfg.CloudWatchLogsClient = validActivationCloudWatchLogsClient()
	case pkglogger.LogOutputClickHouse:
		cfg.ClickHouse = &pkglogger.ClickHouseConfig{
			ClickHouseLoggerBaseConfig: pkglogger.ClickHouseLoggerBaseConfig{
				FlushInterval:           time.Hour,
				MaxEntries:              1,
				BufferFlushBytes:        directRetryClickHouseBufferFlushBytes,
				FallbackQueueMaxEntries: 1,
				MaxInsertRequestBytes:   directRetryClickHouseMaxInsertRequestBytes,
			},
		}
		cfg.ClickHouse.ClickHouseFlushManagerConfig.Resource = activationResource()
		cfg.ClickHouseClient = &pkgclickhouse.Config{
			Endpoint: "http://localhost:8123",
			Database: "default",
			Table:    "logs",
		}
	default:
		t.Fatalf("unsupported handler target %q", target)
	}
}

func addDeliveryHandlerCandidate(
	t *testing.T,
	cfg *pkglogger.DeliveryLoggerConfig,
	target pkglogger.LogOutputDestination,
) {
	t.Helper()
	switch target {
	case pkglogger.LogOutputFile:
		cfg.File = newCloseOwnershipDeliveryFileConfig(t.TempDir(), "delivery-handler-file")
	case pkglogger.LogOutputSlack:
		cfg.Slack = &pkglogger.DeliverySlackLoggerConfig{
			Token:   "test-token",
			Channel: "test-channel",
			Timeout: time.Second,
		}
	case pkglogger.LogOutputOpenSearch:
		cfg.OpenSearch = &pkglogger.DeliveryOpenSearchConfig{
			DeliveryOpenSearchLoggerBaseConfig: pkglogger.DeliveryOpenSearchLoggerBaseConfig{},
			DeliveryOpenSearchFlushManagerConfig: pkglogger.DeliveryOpenSearchFlushManagerConfig{
				Index: "test-logs",
			},
		}
		cfg.OpenSearch.DeliveryOpenSearchFlushManagerConfig.Resource = activationResource()
		cfg.OpenSearchClient = validActivationOpenSearchClient()
	case pkglogger.LogOutputLoki:
		cfg.Loki = &pkglogger.DeliveryLokiConfig{}
		cfg.Loki.DeliveryLokiFlushManagerConfig.Resource = activationResource()
		cfg.LokiClient = &pkgloki.Config{Endpoint: "http://localhost:3100"}
	case pkglogger.LogOutputCloudWatchLogs:
		cfg.CloudWatchLogs = &pkglogger.DeliveryCloudWatchLogsConfig{
			DeliveryCloudWatchLogsFlushManagerConfig: pkglogger.DeliveryCloudWatchLogsFlushManagerConfig{
				LogGroupName:  "test-group",
				LogStreamName: "test-stream",
			},
		}
		cfg.CloudWatchLogs.DeliveryCloudWatchLogsFlushManagerConfig.Resource = activationResource()
		cfg.CloudWatchLogsClient = validActivationCloudWatchLogsClient()
	case pkglogger.LogOutputClickHouse:
		cfg.ClickHouse = &pkglogger.DeliveryClickHouseConfig{
			DeliveryClickHouseLoggerBaseConfig: pkglogger.DeliveryClickHouseLoggerBaseConfig{
				FlushInterval:           time.Hour,
				MaxEntries:              1,
				BufferFlushBytes:        directRetryClickHouseBufferFlushBytes,
				FallbackQueueMaxEntries: 1,
				MaxInsertRequestBytes:   directRetryClickHouseMaxInsertRequestBytes,
			},
		}
		cfg.ClickHouse.DeliveryClickHouseFlushManagerConfig.Resource = activationResource()
		cfg.ClickHouseClient = &pkgclickhouse.Config{
			Endpoint: "http://localhost:8123",
			Database: "default",
			Table:    "logs",
		}
	default:
		t.Fatalf("unsupported delivery handler target %q", target)
	}
}

func validActivationOpenSearchClient() *pkgopensearch.Config {
	return &pkgopensearch.Config{
		Endpoint: "http://localhost:9200",
		AuthConfig: pkgopensearch.AuthConfig{
			Type: pkgopensearch.AuthTypeNone,
		},
	}
}

func validActivationCloudWatchLogsClient() *pkgcloudwatchlogs.Config {
	return &pkgcloudwatchlogs.Config{
		Region:          "ap-northeast-1",
		LogGroupName:    "test-group",
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
	}
}

func activationResource() pkglogger.Resource {
	return pkglogger.Resource{Cloud: &pkglogger.CloudResourceInfo{}}
}

func requireSelectedErrorTarget(
	t *testing.T,
	cfg pkglogger.LoggerConfig,
	sourceName string,
	targetName string,
	available handlerStruct,
) {
	t.Helper()
	var selected handlerStruct
	switch sourceName {
	case pkglogger.LogOutputFile.String():
		selected = selectConfiguredHandlers(
			available,
			filelogger.NewConfigAdapter(*cfg.File).Handler(),
		)
	case pkglogger.LogOutputStdout.String():
		selected = selectConfiguredHandlers(
			available,
			stdoutlogger.NewConfigAdapter(*cfg.Stdout).Handler(),
		)
	default:
		t.Fatalf("unsupported source %q", sourceName)
	}
	require.Len(t, selected.OnError, 1)
	require.Equal(t, targetName, selected.OnError[0].Name)
}

func requireSelectedDeliveryErrorTarget(
	t *testing.T,
	cfg pkglogger.DeliveryLoggerConfig,
	sourceName string,
	targetName string,
	available handlerStruct,
) {
	t.Helper()
	var selected handlerStruct
	switch sourceName {
	case pkglogger.LogOutputFile.String():
		selected = selectConfiguredHandlers(
			available,
			filelogger.NewDeliveryFileLoggerConfigAdapter(*cfg.File).Handler(),
		)
	case pkglogger.LogOutputStdout.String():
		selected = selectConfiguredHandlers(
			available,
			stdoutlogger.NewDeliveryConfigAdapter(*cfg.Stdout).Handler(),
		)
	default:
		t.Fatalf("unsupported delivery source %q", sourceName)
	}
	require.Len(t, selected.OnError, 1)
	require.Equal(t, targetName, selected.OnError[0].Name)
}

func newActivationStdoutConfig() *pkglogger.StdoutLoggerConfig {
	return &pkglogger.StdoutLoggerConfig{
		RuntimeConfig: pkglogger.RuntimeConfig{LogLevel: pkglogger.LogLevelDebug},
		LogBodyConfig: pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano},
	}
}

func newActivationDeliveryStdoutConfig() *pkglogger.DeliveryStdoutLoggerConfig {
	return &pkglogger.DeliveryStdoutLoggerConfig{
		LogBodyConfig: pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano},
	}
}

func newActivationFileConfig(t *testing.T, target string) *pkglogger.FileLoggerConfig {
	t.Helper()
	return &pkglogger.FileLoggerConfig{
		FsstoreConfig: pkgfsstore.Config{
			Layout: pkgfsstore.FsstoreConfig{
				Target:         target,
				LogPath:        t.TempDir(),
				LogDir:         "logs",
				ErrorLogDir:    "logs_error",
				FileNameFormat: pkgfsstore.FileNameFormat,
				FileDateFormat: pkgfsstore.FileDateFormat,
			},
			Lifecycle: pkgfsstore.FileLifecycleConfig{
				EnableRotate:   false,
				RotationType:   pkgfsstore.FileRotationDaily,
				MaxFileBackups: infrafsstore.MinAllowedMaxFileBackups,
				MaxFileAge:     infrafsstore.MinAllowedMaxFileAge,
				MaxFileSize:    infrafsstore.MinAllowedFileSizeBytes,
				MaxDirSize:     infrafsstore.MinAllowedMaxDirSize,
				UsageCacheTTL:  time.Hour,
				CompressFormat: pkgfsstore.CompressGzip,
			},
			Duration: pkgfsstore.DurationConfig{
				CompressWarnThresholdDuration:   time.Millisecond,
				CompressErrorThresholdDuration:  time.Millisecond,
				WriteFileWarnThresholdDuration:  time.Millisecond,
				WriteFileErrorThresholdDuration: time.Millisecond,
			},
			Retry: pkgfsstore.RetryConfig{
				MaxWriteAttempts:           1,
				FileOperationRetryWaitTime: time.Millisecond,
			},
		},
		RuntimeConfig: pkglogger.RuntimeConfig{
			AppName:  "handler-activation-test",
			Target:   target,
			LogLevel: pkglogger.LogLevelDebug,
		},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat:        time.RFC3339Nano,
			ErrorTraceDepth:          1,
			ErrorTraceDepthDebugMode: 1,
		},
		SamplingConfig: pkglogger.SamplingConfig{
			LoggingByRequestIDRate: 1,
			SamplingDebugRate:      1,
			SamplingInfoRate:       1,
			SamplingWarnRate:       1,
			SamplingErrorRate:      1,
		},
	}
}
