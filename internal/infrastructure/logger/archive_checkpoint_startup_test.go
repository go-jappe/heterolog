package logger

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestPrepareLoggerArchiveCheckpointDirectoryUsesOnlyEnabledFileUpload(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func(*pkglogger.LoggerConfig)
		want   bool
	}{
		{name: "inline", want: true},
		{name: "implicit inline", mutate: func(cfg *pkglogger.LoggerConfig) { cfg.File.LifecycleMode = "" }, want: true},
		{name: "runner", mutate: func(cfg *pkglogger.LoggerConfig) { cfg.File.LifecycleMode = pkgfsstore.FileLifecycleModeRunner }, want: true},
		{name: "lifecycle disabled", mutate: func(cfg *pkglogger.LoggerConfig) { cfg.File.LifecycleMode = pkgfsstore.FileLifecycleModeDisable }},
		{name: "upload disabled", mutate: func(cfg *pkglogger.LoggerConfig) { cfg.File.ObjectStorageUploadConfig.Enabled = false }},
		{name: "all roles disabled", mutate: func(cfg *pkglogger.LoggerConfig) { cfg.File.ObjectStorageUploadConfig.Default.Enabled = false }},
		{name: "unused file", mutate: func(cfg *pkglogger.LoggerConfig) {
			cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
			cfg.Stdout = newActivationStdoutConfig()
		}},
		{name: "handler-only file", mutate: func(cfg *pkglogger.LoggerConfig) {
			cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
			cfg.Stdout = newActivationStdoutConfig()
			cfg.Stdout.HandlerConfig.OnErrorConfig = []string{"file"}
		}, want: true},
		{name: "error upload without separate error", mutate: func(cfg *pkglogger.LoggerConfig) {
			cfg.File.ObjectStorageUploadConfig.Error = cfg.File.ObjectStorageUploadConfig.Default
			cfg.File.ObjectStorageUploadConfig.Default.Enabled = false
		}},
		{name: "separate error upload", mutate: func(cfg *pkglogger.LoggerConfig) {
			cfg.File.ObjectStorageUploadConfig.Error = cfg.File.ObjectStorageUploadConfig.Default
			cfg.File.ObjectStorageUploadConfig.Default.Enabled = false
			cfg.File.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
		}, want: true},
		{name: "debug upload without separate debug", mutate: func(cfg *pkglogger.LoggerConfig) {
			cfg.File.ObjectStorageUploadConfig.Debug = cfg.File.ObjectStorageUploadConfig.Default
			cfg.File.ObjectStorageUploadConfig.Default.Enabled = false
		}},
		{name: "separate debug upload", mutate: func(cfg *pkglogger.LoggerConfig) {
			cfg.File.ObjectStorageUploadConfig.Debug = cfg.File.ObjectStorageUploadConfig.Default
			cfg.File.ObjectStorageUploadConfig.Default.Enabled = false
			cfg.File.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = true
		}, want: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "checkpoints")
			cfg := pkglogger.LoggerConfig{
				LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
				File:                  checkpointStartupFileConfig(t, directory),
			}
			if testCase.mutate != nil {
				testCase.mutate(&cfg)
			}
			require.NoError(t, prepareLoggerArchiveCheckpointDirectory(cfg))
			if testCase.want {
				require.DirExists(t, directory)
			} else {
				require.NoDirExists(t, directory)
			}
		})
	}
}

func TestPrepareLoggerArchiveCheckpointDirectoryUsesDedicatedInternalFile(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("upload=%t", enabled), func(t *testing.T) {
			root := t.TempDir()
			unusedBaseDirectory := filepath.Join(root, "unused-base", "checkpoints")
			internalDirectory := filepath.Join(root, "internal", "checkpoints")
			cfg, _, _ := newHandlerOnlyLoggerConfig(t, pkglogger.LogOutputFile)
			cfg.File = checkpointStartupFileConfig(t, unusedBaseDirectory)
			internalFile := checkpointStartupFileConfig(t, internalDirectory)
			internalFile.ObjectStorageUploadConfig.Enabled = enabled
			cfg.InternalLogDestinations = &pkglogger.InternalLogDestinationConfig{File: internalFile}
			require.NoError(t, prepareLoggerArchiveCheckpointDirectory(cfg))
			require.NoDirExists(t, unusedBaseDirectory, "a dedicated internal file replaces the unused base file")
			if enabled {
				require.DirExists(t, internalDirectory)
			} else {
				require.NoDirExists(t, internalDirectory)
			}
		})
	}
}

func TestPrepareDeliveryArchiveCheckpointDirectoryRequiresRunnerAndEnabledUpload(t *testing.T) {
	for _, mode := range []pkgfsstore.FileLifecycleMode{"", pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner, pkgfsstore.FileLifecycleModeDisable} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("mode=%s/upload=%t", mode, enabled), func(t *testing.T) {
				directory := filepath.Join(t.TempDir(), "checkpoints")
				file := checkpointStartupDeliveryFileConfig(t, directory)
				file.LifecycleMode = mode
				file.LifecycleRunnerSettings.ObjectStorageUploadConfig.Enabled = enabled
				cfg := pkglogger.DeliveryLoggerConfig{
					LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}, File: file,
				}
				require.NoError(t, prepareDeliveryArchiveCheckpointDirectory(cfg))
				if enabled && mode == pkgfsstore.FileLifecycleModeRunner {
					require.DirExists(t, directory)
				} else {
					require.NoDirExists(t, directory)
				}
			})
		}
	}
}

func TestPrepareArchiveCheckpointDirectoryUsesOnlySelectedBackendArchives(t *testing.T) {
	for _, backend := range []pkglogger.LogOutputDestination{pkglogger.LogOutputOpenSearch, pkglogger.LogOutputLoki, pkglogger.LogOutputCloudWatchLogs, pkglogger.LogOutputClickHouse} {
		for _, delivery := range []bool{false, true} {
			for _, state := range []string{"enabled", "runner", "implicit inline", "upload disabled", "lifecycle disabled", "archive unselected", "backend unused"} {
				t.Run(fmt.Sprintf("%s/delivery=%t/%s", backend, delivery, state), func(t *testing.T) {
					directory := filepath.Join(t.TempDir(), "checkpoints")
					mode := pkgfsstore.FileLifecycleModeInline
					if state == "runner" {
						mode = pkgfsstore.FileLifecycleModeRunner
					} else if state == "implicit inline" {
						mode = ""
					} else if state == "lifecycle disabled" {
						mode = pkgfsstore.FileLifecycleModeDisable
					}
					selected := []durable.DurableType{durable.DurableTypeFileArchive}
					if state == "archive unselected" {
						selected = []durable.DurableType{durable.DurableTypeNone}
					}
					upload := checkpointStartupUploadConfig()
					upload.Enabled = state != "upload disabled"
					checkpoint := pkgfsstore.ArchiveCheckpointConfig{Directory: directory, SourcePrefix: backend.String()}
					outputs := pkglogger.LogOutputDestinations{backend}
					if state == "backend unused" {
						outputs = pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
					}
					if delivery {
						failureStore := durable.DeliveryFailureStoreConfig{
							DurableType: selected,
							FileArchiveConfig: durable.DeliveryFileArchiveConfig{
								LifecycleMode: mode, ObjectStorageUploadConfig: upload, ArchiveCheckpointConfig: checkpoint,
							},
						}
						cfg := pkglogger.DeliveryLoggerConfig{LogOutputDestinations: outputs, Stdout: newActivationDeliveryStdoutConfig()}
						setCheckpointStartupDeliveryBackend(&cfg, backend, failureStore)
						require.NoError(t, prepareDeliveryArchiveCheckpointDirectory(cfg))
					} else {
						failureStore := durable.FailureStoreConfig{
							DurableType: selected,
							DurableStoreConfig: durable.DurableStoreConfig{FileArchiveConfig: durable.FileArchiveConfig{
								LifecycleMode: mode, ObjectStorageUploadConfig: upload, ArchiveCheckpointConfig: checkpoint,
							}},
						}
						cfg := pkglogger.LoggerConfig{LogOutputDestinations: outputs, Stdout: newActivationStdoutConfig()}
						setCheckpointStartupBackend(&cfg, backend, failureStore)
						require.NoError(t, prepareLoggerArchiveCheckpointDirectory(cfg))
					}
					if state == "enabled" || state == "runner" || state == "implicit inline" {
						require.DirExists(t, directory)
					} else {
						require.NoDirExists(t, directory)
					}
				})
			}
		}
	}
}

func TestPrepareArchiveCheckpointDirectoryRejectsInvalidUploadBeforeCreatingDirectory(t *testing.T) {
	for _, delivery := range []bool{false, true} {
		for _, backendArchive := range []bool{false, true} {
			t.Run(fmt.Sprintf("delivery=%t/backendArchive=%t", delivery, backendArchive), func(t *testing.T) {
				directory := filepath.Join(t.TempDir(), "checkpoints")
				upload := checkpointStartupUploadConfig()
				upload.Targets = []objectstorage.UploadTarget{"unsupported"}
				checkpoint := pkgfsstore.ArchiveCheckpointConfig{Directory: directory, SourcePrefix: "invalid-upload"}
				var err error
				if delivery {
					cfg := pkglogger.DeliveryLoggerConfig{LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}}
					if backendArchive {
						cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
						setCheckpointStartupDeliveryBackend(&cfg, pkglogger.LogOutputOpenSearch, durable.DeliveryFailureStoreConfig{
							DurableType: []durable.DurableType{durable.DurableTypeFileArchive},
							FileArchiveConfig: durable.DeliveryFileArchiveConfig{
								LifecycleMode: pkgfsstore.FileLifecycleModeInline, ObjectStorageUploadConfig: upload, ArchiveCheckpointConfig: checkpoint,
							},
						})
					} else {
						cfg.File = checkpointStartupDeliveryFileConfig(t, directory)
						cfg.File.LifecycleRunnerSettings.ObjectStorageUploadConfig.Default = upload
					}
					err = prepareDeliveryArchiveCheckpointDirectory(cfg)
				} else {
					cfg := pkglogger.LoggerConfig{LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}}
					if backendArchive {
						cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
						setCheckpointStartupBackend(&cfg, pkglogger.LogOutputOpenSearch, durable.FailureStoreConfig{
							DurableType: []durable.DurableType{durable.DurableTypeFileArchive},
							DurableStoreConfig: durable.DurableStoreConfig{FileArchiveConfig: durable.FileArchiveConfig{
								LifecycleMode: pkgfsstore.FileLifecycleModeInline, ObjectStorageUploadConfig: upload, ArchiveCheckpointConfig: checkpoint,
							}},
						})
					} else {
						cfg.File = checkpointStartupFileConfig(t, directory)
						cfg.File.ObjectStorageUploadConfig.Default = upload
					}
					err = prepareLoggerArchiveCheckpointDirectory(cfg)
				}
				requireUnsupportedUploadTarget(t, err, "unsupported")
				require.NoDirExists(t, directory, "invalid upload configuration must be rejected before filesystem preparation")
			})
		}
	}
}

func TestLoggerConstructorsPrepareCheckpointDirectoryWithoutMain(t *testing.T) {
	for _, contract := range []string{"sync", "async", "delivery"} {
		for _, internal := range []bool{false, true} {
			for _, blocked := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/internal=%t/blocked=%t", contract, internal, blocked), func(t *testing.T) {
					root := t.TempDir()
					logRoot := filepath.Join(root, "logs")
					directory := filepath.Join(root, "checkpoints")
					if blocked {
						parent := filepath.Join(root, "not-a-directory")
						require.NoError(t, os.WriteFile(parent, []byte("preserve"), 0o600))
						directory = filepath.Join(parent, "checkpoints")
					}
					// 空の保存先から構築だけを行い、rotation・圧縮file生成・uploadは起こしません。
					// 設定も実S3ではなくloopbackのdummy宛先に限定します。
					var created io.Closer
					var err error
					if contract == "delivery" {
						file := checkpointStartupDeliveryFileConfig(t, directory)
						file.FsstoreConfig.Layout.LogPath = logRoot
						cfg := pkglogger.DeliveryLoggerConfig{
							LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
							Timeout:               time.Second, MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes, File: file,
						}
						if internal {
							cfg.File = nil
							cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
							cfg.Stdout = newActivationDeliveryStdoutConfig()
							cfg.Stdout.HandlerConfig.OnErrorConfig = []string{"file"}
							cfg.InternalLogDestinations = &pkglogger.DeliveryInternalLogDestinationConfig{File: file}
						}
						created, err = NewDeliveryLogger(context.Background(), cfg, pkgcloudwatchlogs.StreamRoutingNone, observer.NopTracer{}, observer.NopMeter{})
					} else {
						file := checkpointStartupFileConfig(t, directory)
						file.FsstoreConfig.Layout.LogPath = logRoot
						cfg := pkglogger.LoggerConfig{
							LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
							Timeout:               time.Second, MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes, File: file,
						}
						if internal {
							cfg.File = nil
							cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
							cfg.Stdout = newActivationStdoutConfig()
							cfg.Stdout.HandlerConfig.OnErrorConfig = []string{"file"}
							cfg.InternalLogDestinations = &pkglogger.InternalLogDestinationConfig{File: file}
						}
						if contract == "sync" {
							created, err = NewLogger(context.Background(), cfg, pkgcloudwatchlogs.StreamRoutingNone, nil, nil, observer.NopTracer{}, observer.NopMeter{})
						} else {
							asyncCfg := asyncConfigFromLoggerConfig(cfg)
							asyncCfg.MaxLogEntryBytes, asyncCfg.MaxParallel = cfg.MaxLogEntryBytes, 1
							created, err = NewAsyncLogger(context.Background(), asyncCfg, pkgcloudwatchlogs.StreamRoutingNone, nil, nil, observer.NopTracer{}, observer.NopMeter{})
						}
					}
					if blocked {
						var checkpointErr *ArchiveCheckpointDirectoryError
						require.ErrorAs(t, err, &checkpointErr)
						require.Equal(t, directory, checkpointErr.Directory)
						require.Nil(t, created)
						require.NoDirExists(t, logRoot, "checkpoint preparation must fail before log files are opened")
					} else {
						require.NoError(t, err)
						require.NotNil(t, created)
						t.Cleanup(func() { require.NoError(t, created.Close()) })
						require.DirExists(t, directory, "the public constructor must prepare the directory without main")
						require.NoError(t, created.Close())
					}
				})
			}
		}
	}
}

func checkpointStartupFileConfig(t *testing.T, directory string) *pkglogger.FileLoggerConfig {
	t.Helper()
	cfg := runnerFileConfigForInternalLogRuntime(t.TempDir(), "checkpoint-startup")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.LifecycleHandlerConfig = pkgloggerhandler.HandlerConfig{}
	cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{Enabled: true, Default: checkpointStartupUploadConfig()}
	cfg.ArchiveCheckpointConfig = pkgfsstore.ArchiveCheckpointConfig{Directory: directory, SourcePrefix: "logger-startup"}
	return cfg
}

func checkpointStartupDeliveryFileConfig(t *testing.T, directory string) *pkglogger.DeliveryFileLoggerConfig {
	t.Helper()
	cfg := runnerDeliveryFileConfigForInternalLogRuntime(t.TempDir(), "checkpoint-startup")
	cfg.LifecycleRunnerSettings.LifecycleHandlerConfig = pkgloggerhandler.HandlerConfig{}
	cfg.LifecycleRunnerSettings.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{Enabled: true, Default: checkpointStartupUploadConfig()}
	cfg.LifecycleRunnerSettings.ArchiveCheckpointConfig = pkgfsstore.ArchiveCheckpointConfig{Directory: directory, SourcePrefix: "delivery-startup"}
	return cfg
}

func checkpointStartupUploadConfig() objectstorage.UploadTargetConfig {
	return objectstorage.UploadTargetConfig{
		Enabled: true, Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
		S3: objectstorage.S3Config{
			Region: "us-east-1", Bucket: "checkpoint-tests", Prefix: "logs", AccessKeyID: "test", SecretAccessKey: "test",
			Endpoint: "http://127.0.0.1:1", ForcePathStyle: true,
		},
	}
}

func setCheckpointStartupBackend(cfg *pkglogger.LoggerConfig, backend pkglogger.LogOutputDestination, failureStore durable.FailureStoreConfig) {
	switch backend {
	case pkglogger.LogOutputOpenSearch:
		cfg.OpenSearch = &pkglogger.OpenSearchConfig{OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{FailureStoreConfig: failureStore}}
	case pkglogger.LogOutputLoki:
		cfg.Loki = &pkglogger.LokiConfig{LokiFlushManagerConfig: pkglogger.LokiFlushManagerConfig{FailureStoreConfig: failureStore}}
	case pkglogger.LogOutputCloudWatchLogs:
		cfg.CloudWatchLogs = &pkglogger.CloudWatchLogsConfig{CloudWatchLogsFlushManagerConfig: pkglogger.CloudWatchLogsFlushManagerConfig{FailureStoreConfig: failureStore}}
	case pkglogger.LogOutputClickHouse:
		cfg.ClickHouse = &pkglogger.ClickHouseConfig{ClickHouseFlushManagerConfig: pkglogger.ClickHouseFlushManagerConfig{FailureStoreConfig: failureStore}}
	}
}

func setCheckpointStartupDeliveryBackend(cfg *pkglogger.DeliveryLoggerConfig, backend pkglogger.LogOutputDestination, failureStore durable.DeliveryFailureStoreConfig) {
	switch backend {
	case pkglogger.LogOutputOpenSearch:
		cfg.OpenSearch = &pkglogger.DeliveryOpenSearchConfig{DeliveryOpenSearchFlushManagerConfig: pkglogger.DeliveryOpenSearchFlushManagerConfig{FailureStoreConfig: failureStore}}
	case pkglogger.LogOutputLoki:
		cfg.Loki = &pkglogger.DeliveryLokiConfig{DeliveryLokiFlushManagerConfig: pkglogger.DeliveryLokiFlushManagerConfig{FailureStoreConfig: failureStore}}
	case pkglogger.LogOutputCloudWatchLogs:
		cfg.CloudWatchLogs = &pkglogger.DeliveryCloudWatchLogsConfig{DeliveryCloudWatchLogsFlushManagerConfig: pkglogger.DeliveryCloudWatchLogsFlushManagerConfig{FailureStoreConfig: failureStore}}
	case pkglogger.LogOutputClickHouse:
		cfg.ClickHouse = &pkglogger.DeliveryClickHouseConfig{DeliveryClickHouseFlushManagerConfig: pkglogger.DeliveryClickHouseFlushManagerConfig{FailureStoreConfig: failureStore}}
	}
}
