package logging_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/config/failure"
	configspoolrunner "github.com/go-jappe/heterolog/internal/config/spool/spoolrunner"
	configspoolsupervisor "github.com/go-jappe/heterolog/internal/config/spool/spoolsupervisor"
	infralogger "github.com/go-jappe/heterolog/internal/infrastructure/logger"
	memorymonitor "github.com/go-jappe/heterolog/internal/observer/memory/pressure"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

const (
	loggingIntegrationTarget                        = "logging-integration"
	loggingIntegrationOpenSearchBufferFlushBytes    = 5 * 1024 * 1024
	loggingIntegrationOpenSearchMaxBulkRequestBytes = 15 * 1024 * 1024
)

func TestLoggingOpenSearchSpoolOverflowWithMemoryMonitor(t *testing.T) {
	tests := []struct {
		name           string
		pressure       memorypressure.MemoryPressure
		wantSpoolRetry bool
		wantDrop       bool
	}{
		{
			name: "non critical memory stores retryable failures and runner resends them",
			pressure: memorypressure.MemoryPressure{
				Ratio:          0.20,
				AvailableBytes: 1024 * 1024 * 1024,
				LimitBytes:     2 * 1024 * 1024 * 1024,
				UsedBytes:      1024 * 1024 * 1024,
				Source:         memorypressure.SourceLinuxProcMem,
				Reliable:       true,
			},
			wantSpoolRetry: true,
		},
		{
			name: "critical memory drops retryable failures by overflow policy",
			pressure: memorypressure.MemoryPressure{
				Ratio:          0.95,
				AvailableBytes: 64 * 1024 * 1024,
				LimitBytes:     2 * 1024 * 1024 * 1024,
				UsedBytes:      1900 * 1024 * 1024,
				Source:         memorypressure.SourceLinuxProcMem,
				Reliable:       true,
			},
			wantDrop: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			meter := newRecordingMeter()
			server := newBulkFailureServer(t, []bulkFailureStatus{
				{status: http.StatusTooManyRequests, errorType: "es_rejected_execution_exception", reason: "test queue is full"},
				{status: http.StatusTooManyRequests, errorType: "es_rejected_execution_exception", reason: "test queue is full"},
			})
			defer server.Close()

			dirs := durableTestDirs{
				spoolBase:   t.TempDir(),
				archiveBase: t.TempDir(),
			}
			monitor := startedMemoryMonitor(t, tt.pressure)
			loggerCfg := durableOpenSearchLoggerConfig(
				server.URL,
				dirs,
				[]durable.DurableType{failure.DurableTypeSpool},
			)

			logger, err := infralogger.NewLogger(
				ctx,
				loggerCfg,
				pkgcloudwatchlogs.StreamRoutingNone,
				integrationSpoolConfig(),
				monitor,
				observer.NopTracer{},
				meter,
			)
			if err != nil {
				t.Fatalf("failed to create logger: %v", err)
			}
			defer closeIntegrationLogger(t, logger)

			writeTwoBulkLogs(t, ctx, logger, "memory-overflow", tt.wantDrop)
			logBulkRequests(t, server)

			activeRoot := filepath.Join(dirs.spoolBase, loggingIntegrationTarget, "spool", "opensearch", "active")
			if tt.wantSpoolRetry {
				// paired runnerはactive fileを直ちにprocessingへ移し得るため、特定stateの
				// 一瞬のfile存在ではなく、spool経由の再送requestを観測する。
				bodies := waitForRequestBodies(t, "OpenSearch bulk", server.Bodies, server.Recorded(), 2)
				logBulkRequests(t, server)
				if retried := strings.Join(bodies[1:], "\n"); !strings.Contains(retried, "memory-overflow") {
					t.Fatalf("spool retry request does not contain memory-overflow log: %s", retried)
				}
				return
			}

			if tt.wantDrop {
				// Closeは受理済みのfailure処理を待つため、その後の件数と保存内容が最終です。
				// 再送処理がactiveから移したfileも含め、spoolのどの状態にも保存していないことを
				// 確認します。
				closeIntegrationLogger(t, logger)
				if drops := meter.counterValue("logger_failure_overload_drop_total"); drops < 1 {
					t.Fatalf("overload drop counter = %d, want at least 1", drops)
				}
				spoolRoot := filepath.Dir(activeRoot)
				if lines := readLogLines(t, spoolRoot, ".hlogspool"); len(lines) != 0 {
					logIOLines(t, "unexpected spool lines", lines, 3)
					t.Fatalf("spool lines under %s = %d, want 0 for entries dropped by memory overflow", spoolRoot, len(lines))
				}
			}
		})
	}
}

func TestLoggingOpenSearchDurableArchiveOnPermanentFailure(t *testing.T) {
	ctx := context.Background()
	meter := newRecordingMeter()
	server := newOpenSearchFailureServer(t, bulkFailureStatus{
		status:    http.StatusBadRequest,
		errorType: "mapper_parsing_exception",
		reason:    "test mapping rejected the log",
	})
	defer server.Close()

	dirs := durableTestDirs{
		spoolBase:   t.TempDir(),
		archiveBase: t.TempDir(),
	}
	monitor := startedMemoryMonitor(t, memorypressure.MemoryPressure{
		Ratio:          0.10,
		AvailableBytes: 1536 * 1024 * 1024,
		LimitBytes:     2 * 1024 * 1024 * 1024,
		UsedBytes:      512 * 1024 * 1024,
		Source:         memorypressure.SourceLinuxProcMem,
		Reliable:       true,
	})
	loggerCfg := durableOpenSearchLoggerConfig(
		server.URL,
		dirs,
		[]durable.DurableType{failure.DurableTypeFsstore},
	)
	loggerCfg.OpenSearch.OpenSearchLoggerBaseConfig.NoBulkFlushModeEnabled = true
	loggerCfg.OpenSearch.OpenSearchLoggerBaseConfig.MaxBulkDocument = 1

	logger, err := infralogger.NewLogger(
		ctx,
		loggerCfg,
		pkgcloudwatchlogs.StreamRoutingNone,
		nil,
		monitor,
		observer.NopTracer{},
		meter,
	)
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}
	// archive fileとworkerを解放してから一時directoryを削除します。
	defer closeIntegrationLogger(t, logger)

	writeTwoBulkLogs(t, ctx, logger, "durable-archive", false)
	logOpenSearchRequests(t, server)

	permanentRoot := filepath.Join(
		dirs.archiveBase,
		loggingIntegrationTarget,
		"opensearch_failure_archives",
		"permanent",
	)
	// Closeは受理済みのfailure処理（archiveへの保存）を待つため、その後の内容が最終です。
	closeIntegrationLogger(t, logger)
	lines := readLogLines(t, permanentRoot, ".jsonl")
	if len(lines) < 2 {
		t.Fatalf("permanent archive lines under %s = %d, want at least 2", permanentRoot, len(lines))
	}
	logIOLines(t, "permanent archive", lines, 2)
	assertLinesContain(t, lines, "durable-archive")
	assertLinesContain(t, lines, "mapper_parsing_exception")
}

func TestAsyncLoggingOpenSearchFailureSpoolIsResentEndToEnd(t *testing.T) {
	ctx := context.Background()
	meter := newRecordingMeter()
	server := newOpenSearchFailureServer(t, bulkFailureStatus{
		status:    http.StatusTooManyRequests,
		errorType: "es_rejected_execution_exception",
		reason:    "async test queue is full",
	})
	defer server.Close()

	dirs := durableTestDirs{
		spoolBase:   t.TempDir(),
		archiveBase: t.TempDir(),
	}
	monitor := startedMemoryMonitor(t, memorypressure.MemoryPressure{
		Ratio:          0.10,
		AvailableBytes: 1536 * 1024 * 1024,
		LimitBytes:     2 * 1024 * 1024 * 1024,
		UsedBytes:      512 * 1024 * 1024,
		Source:         memorypressure.SourceLinuxProcMem,
		Reliable:       true,
	})
	loggerCfg := asyncDurableOpenSearchLoggerConfig(
		server.URL,
		dirs,
		[]durable.DurableType{failure.DurableTypeSpool},
	)

	logger, err := infralogger.NewAsyncLogger(
		ctx,
		loggerCfg,
		pkgcloudwatchlogs.StreamRoutingNone,
		integrationSpoolConfig(),
		monitor,
		observer.NopTracer{},
		meter,
	)
	if err != nil {
		t.Fatalf("failed to create async logger: %v", err)
	}
	defer closeIntegrationAsyncLogger(t, logger)

	writeAsyncLog(t, ctx, logger, "async-spool log 1", "async-spool-request")

	bodies := waitForRequestBodies(t, "OpenSearch document", server.Bodies, server.Recorded(), 2)
	logOpenSearchRequests(t, server)
	if retried := strings.Join(bodies[1:], "\n"); !strings.Contains(retried, "async-spool log 1") {
		t.Fatalf("async spool retry request does not contain expected log: %s", retried)
	}
}

func TestDeliveryLoggingOpenSearchReportsTargetFailureAndArchivesInternallyEndToEnd(t *testing.T) {
	ctx := context.Background()
	meter := newRecordingMeter()
	server := newOpenSearchFailureServer(t, bulkFailureStatus{
		status:    http.StatusBadRequest,
		errorType: "mapper_parsing_exception",
		reason:    "delivery test mapping rejected the log",
	})
	defer server.Close()

	dirs := durableTestDirs{
		spoolBase:   t.TempDir(),
		archiveBase: t.TempDir(),
	}
	loggerCfg := deliveryDurableOpenSearchLoggerConfig(server.URL, dirs)

	logger, err := infralogger.NewDeliveryLogger(
		ctx,
		loggerCfg,
		pkgcloudwatchlogs.StreamRoutingNone,
		observer.NopTracer{},
		meter,
	)
	if err != nil {
		t.Fatalf("failed to create delivery logger: %v", err)
	}
	// archive fileとworkerを解放してから一時directoryを削除します。
	defer closeIntegrationDeliveryLogger(t, logger)

	report, err := logger.Info(
		ctx,
		"delivery-archive log 1",
		pkglogger.WithRequestID("delivery-archive-request"),
	)
	if err == nil {
		t.Fatal("delivery logger.Info error = nil, want target delivery failure")
	}
	if len(report.Targets) != 1 {
		t.Fatalf("delivery logger.Info target count = %d, want 1", len(report.Targets))
	}
	if report.Targets[0].Status != pkglogger.DeliveryFailed {
		t.Fatalf("delivery logger.Info target status = %s, want %s", report.Targets[0].Status, pkglogger.DeliveryFailed)
	}

	logOpenSearchRequests(t, server)

	permanentRoot := filepath.Join(
		dirs.archiveBase,
		loggingIntegrationTarget,
		"delivery_opensearch_failure_archives",
		"permanent",
	)
	// Closeは受理済みのfailure処理（archiveへの保存）を待つため、その後の内容が最終です。
	closeIntegrationDeliveryLogger(t, logger)
	lines := readLogLines(t, permanentRoot, ".jsonl")
	if len(lines) < 1 {
		t.Fatalf("delivery permanent archive lines under %s = %d, want at least 1", permanentRoot, len(lines))
	}
	logIOLines(t, "delivery permanent archive", lines, 1)
	assertLinesContain(t, lines, "delivery-archive log 1")
	assertLinesContain(t, lines, "mapper_parsing_exception")
}

type durableTestDirs struct {
	spoolBase   string
	archiveBase string
}

func durableOpenSearchLoggerConfig(
	endpoint string,
	dirs durableTestDirs,
	durableTypes []durable.DurableType,
) pkglogger.LoggerConfig {
	runtimeCfg := pkglogger.RuntimeConfig{
		AppName:  loggingIntegrationTarget,
		IsDebug:  false,
		Target:   loggingIntegrationTarget,
		LogLevel: pkglogger.LogLevelInfo,
	}
	logBodyCfg := pkglogger.LogBodyConfig{
		LogLineDateFormat:        time.RFC3339Nano,
		ErrorTraceDepth:          4,
		ErrorTraceDepthDebugMode: 8,
	}
	samplingCfg := pkglogger.SamplingConfig{
		IsSamplingEnabled:        false,
		EnableLoggingByRequestID: false,
		LoggingByRequestIDRate:   1,
		SamplingDebugRate:        1,
		SamplingInfoRate:         1,
		SamplingWarnRate:         1,
		SamplingErrorRate:        1,
	}
	resource := pkglogger.Resource{
		ServiceName:    loggingIntegrationTarget,
		ServiceVersion: "test",
		HostName:       "localhost",
		OSType:         "linux",
		Environment:    "test",
		RuntimeType:    "go-test",
		Cloud: &pkglogger.CloudResourceInfo{
			RuntimeInstanceID: "go-test-1",
			Provider:          "local",
			Region:            "local",
		},
	}

	return pkglogger.LoggerConfig{
		Timeout:          2 * time.Second,
		MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
		OpenSearchClient: &pkgopensearch.Config{
			Endpoint:      endpoint,
			Timeout:       2 * time.Second,
			SkipVerifySSL: true,
			AuthConfig: pkgopensearch.AuthConfig{
				Type: pkgopensearch.AuthTypeNone,
			},
		},
		OpenSearch: &pkglogger.OpenSearchConfig{
			OpenSearchLoggerBaseConfig: pkglogger.OpenSearchLoggerBaseConfig{
				TimeConfig:          pkglogger.TimeConfig{IsLocalTimeEnabled: false, TimeZone: "UTC"},
				RuntimeConfig:       runtimeCfg,
				LogBodyConfig:       logBodyCfg,
				FlushInterval:       time.Hour,
				MaxBulkDocument:     2,
				BufferFlushBytes:    loggingIntegrationOpenSearchBufferFlushBytes,
				MaxBulkRequestBytes: loggingIntegrationOpenSearchMaxBulkRequestBytes,
				SamplingConfig:      samplingCfg,
			},
			OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
				Index:                      "logs-integration",
				IsDebug:                    false,
				UseOpenTelemetryFormat:     false,
				Resource:                   resource,
				PostWarnThresholdDuration:  time.Second,
				PostErrorThresholdDuration: 2 * time.Second,
				IndexLatencyEnabled:        false,
				MaxItemRetries:             0,
				RetryWaitTime:              time.Millisecond,
				FailureStoreConfig: durable.FailureStoreConfig{
					MaxConcurrent: 4,
					DurableType:   durableTypes,
					DurableStoreConfig: durable.DurableStoreConfig{
						FileArchiveConfig: durable.FileArchiveConfig{
							Target:                     loggingIntegrationTarget,
							MaxFileSize:                1024 * 1024,
							FailureFileArchivePath:     dirs.archiveBase,
							Dir:                        "opensearch_failure_archives",
							DirCaseRetryable:           "retryable",
							DirCasePermanent:           "permanent",
							FileNameFormat:             pkgfsstore.FailureArchiveFileNameFormat,
							FileDateFormat:             pkgfsstore.FileDateFormat,
							EnableRotate:               false,
							MaxFileBackups:             2,
							MaxFileAge:                 1,
							MaxDirSize:                 10 * 1024 * 1024,
							UsageCacheTTL:              time.Hour,
							EnableFileCompress:         false,
							CompressFormat:             pkgfsstore.CompressGzip,
							RotationType:               pkgfsstore.FileRotationDaily,
							RotationDayOfWeek:          time.Sunday,
							MaxWriteRetries:            0,
							FileOperationRetryWaitTime: time.Millisecond,
							OverloadPolicyConfig: durable.FileArchiveOverloadPolicyConfig{
								OverloadPolicy:        failure.OverloadPolicyDrop,
								SpillDestination:      failure.SpillDestinationStdout,
								MinAvailableDiskSpace: 100 * 1024 * 1024,
							},
							IsDebug:                  false,
							ErrorTraceDepth:          4,
							ErrorTraceDepthDebugMode: 8,
						},
						SpoolConfig: durable.SpoolConfig{
							SpoolPath: dirs.spoolBase,
							Target:    loggingIntegrationTarget,
							Dir:       "opensearch",
							OverloadPolicyConfig: durable.SpoolOverloadPolicyConfig{
								OverloadPolicy:                failure.OverloadPolicyDrop,
								SpillDestination:              failure.SpillDestinationStdout,
								MaxCallsSinceScan:             1,
								StatsScanInterval:             time.Millisecond,
								MaxDirSize:                    0,
								ActiveBacklogWarnAfterDays:    0,
								MinAvailableDiskSpace:         0,
								UseMemoryPressureForOverload:  true,
								MemoryCriticalRatio:           0.80,
								MinAvailableMemoryBytes:       128 * 1024 * 1024,
								MemoryCriticalConsecutiveHits: 1,
							},
						},
					},
				},
			},
		},
	}
}

func integrationSpoolConfig() *pkgspool.SpoolConfig {
	cfg := &pkgspool.SpoolConfig{}
	cfg.SupervisorConfig.RestartDelay = configspoolsupervisor.DefaultRestartDelay
	cfg.RunnerConfig.OpensearchConfig.Runner.Interval = configspoolrunner.DefaultRunnerInterval
	cfg.RunnerConfig.OpensearchConfig.Worker.Finalize.RetryDelay = time.Millisecond
	cfg.RunnerConfig.OpensearchConfig.Worker.Finalize.MaxEntries = 100
	cfg.RunnerConfig.OpensearchConfig.LifeCycle.SizePurgeInterval = time.Second
	return cfg
}

func asyncDurableOpenSearchLoggerConfig(
	endpoint string,
	dirs durableTestDirs,
	durableTypes []durable.DurableType,
) pkglogger.AsyncBestEffortMultiLoggerConfig {
	base := durableOpenSearchLoggerConfig(endpoint, dirs, durableTypes)
	base.OpenSearch.OpenSearchLoggerBaseConfig.MaxBulkDocument = 1
	base.OpenSearch.OpenSearchLoggerBaseConfig.FlushInterval = time.Hour
	base.OpenSearch.OpenSearchLoggerBaseConfig.NoBulkFlushModeEnabled = false
	base.OpenSearch.OpenSearchFlushManagerConfig.FailureStoreConfig.
		DurableStoreConfig.FileArchiveConfig.Dir = "async_opensearch_failure_archives"

	return pkglogger.AsyncBestEffortMultiLoggerConfig{
		Timeout:          2 * time.Second,
		MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
		MaxParallel:      4,
		OpenSearchClient: base.OpenSearchClient,
		OpenSearch:       base.OpenSearch,
		Resource:         base.Resource,
	}
}

func deliveryDurableOpenSearchLoggerConfig(
	endpoint string,
	dirs durableTestDirs,
) pkglogger.DeliveryLoggerConfig {
	base := durableOpenSearchLoggerConfig(
		endpoint,
		dirs,
		[]durable.DurableType{failure.DurableTypeFsstore},
	)
	baseArchive := base.OpenSearch.OpenSearchFlushManagerConfig.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig

	return pkglogger.DeliveryLoggerConfig{
		Timeout:          2 * time.Second,
		MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
		OpenSearchClient: base.OpenSearchClient,
		OpenSearch: &pkglogger.DeliveryOpenSearchConfig{
			DeliveryOpenSearchLoggerBaseConfig: pkglogger.DeliveryOpenSearchLoggerBaseConfig{
				TimeConfig: base.OpenSearch.OpenSearchLoggerBaseConfig.TimeConfig,
				RuntimeConfig: pkglogger.DeliveryRuntimeConfig{
					AppName: loggingIntegrationTarget,
					IsDebug: false,
					Target:  loggingIntegrationTarget,
				},
				LogBodyConfig: base.OpenSearch.OpenSearchLoggerBaseConfig.LogBodyConfig,
			},
			DeliveryOpenSearchFlushManagerConfig: pkglogger.DeliveryOpenSearchFlushManagerConfig{
				Index:                      base.OpenSearch.OpenSearchFlushManagerConfig.Index,
				IsDebug:                    false,
				UseOpenTelemetryFormat:     false,
				Resource:                   base.OpenSearch.OpenSearchFlushManagerConfig.Resource,
				PostWarnThresholdDuration:  time.Second,
				PostErrorThresholdDuration: 2 * time.Second,
				IndexLatencyEnabled:        false,
				FailureStoreConfig: durable.DeliveryFailureStoreConfig{
					DurableType: []durable.DurableType{failure.DurableTypeFsstore},
					FileArchiveConfig: durable.DeliveryFileArchiveConfig{
						Target:                     baseArchive.Target,
						MaxFileSize:                baseArchive.MaxFileSize,
						FailureFileArchivePath:     baseArchive.FailureFileArchivePath,
						Dir:                        "delivery_opensearch_failure_archives",
						DirCaseRetryable:           baseArchive.DirCaseRetryable,
						DirCasePermanent:           baseArchive.DirCasePermanent,
						FileNameFormat:             baseArchive.FileNameFormat,
						FileDateFormat:             baseArchive.FileDateFormat,
						EnableRotate:               baseArchive.EnableRotate,
						MaxFileBackups:             baseArchive.MaxFileBackups,
						MaxFileAge:                 baseArchive.MaxFileAge,
						MaxDirSize:                 baseArchive.MaxDirSize,
						UsageCacheTTL:              baseArchive.UsageCacheTTL,
						EnableFileCompress:         baseArchive.EnableFileCompress,
						CompressFormat:             baseArchive.CompressFormat,
						RotationType:               baseArchive.RotationType,
						RotationDayOfWeek:          baseArchive.RotationDayOfWeek,
						MaxWriteRetries:            baseArchive.MaxWriteRetries,
						FileOperationRetryWaitTime: baseArchive.FileOperationRetryWaitTime,
						OverloadPolicyConfig:       baseArchive.OverloadPolicyConfig,
						IsDebug:                    baseArchive.IsDebug,
						ErrorTraceDepth:            baseArchive.ErrorTraceDepth,
						ErrorTraceDepthDebugMode:   baseArchive.ErrorTraceDepthDebugMode,
					},
				},
			},
		},
		Resource: base.Resource,
	}
}

// writeTwoBulkLogsは二件のlogを書きます。MaxBulkDocument=2の設定では二件目でbulk送信が
// 起こるため、その送信失敗をoverload policyがdropした場合、二件目はErrLogDroppedを返します。
func writeTwoBulkLogs(t *testing.T, ctx context.Context, logger pkglogger.Logger, prefix string, wantSecondDropped bool) {
	t.Helper()

	if err := logger.Info(ctx, prefix+" log 1", pkglogger.WithRequestID(prefix+"-request")); err != nil {
		t.Fatalf("first logger.Info returned error: %v", err)
	}
	err := logger.Info(ctx, prefix+" log 2", pkglogger.WithRequestID(prefix+"-request"))
	switch {
	case wantSecondDropped && !errors.Is(err, pkglogger.ErrLogDropped):
		t.Fatalf("second logger.Info error = %v, want ErrLogDropped for the bulk dropped by overload policy", err)
	case !wantSecondDropped && err != nil:
		t.Fatalf("second logger.Info returned error: %v", err)
	}
}

func writeAsyncLog(
	t *testing.T,
	ctx context.Context,
	logger pkglogger.AsyncLogger,
	message string,
	requestID string,
) {
	t.Helper()

	logger.Info(ctx, message, pkglogger.WithRequestID(requestID))
}

// closeIntegrationLoggerは、loggerを停止の検出付きでCloseします。Closeは受理済みのfailure処理を
// 期限なく待つ場合があるため、上限（boundedwait.HangTimeout）を過ぎたらForceCloseして試験を
// 失敗させます。試験の本文では、Close後の件数と保存内容を最終の結果として確かめる前に呼びます。
func closeIntegrationLogger(t *testing.T, logger pkglogger.Logger) {
	t.Helper()

	closer, ok := logger.(applogging.CloseableLogger)
	if !ok {
		t.Errorf("logger does not implement CloseableLogger")
		return
	}
	if err := boundedwait.Close(t, "logger Close", closer.Close, boundedwait.ForceCloserOf(closer)); err != nil {
		t.Errorf("close logger: %v", err)
	}
}

// closeIntegrationAsyncLoggerは、closeIntegrationLoggerと同じ上限で非同期loggerをCloseします。
func closeIntegrationAsyncLogger(t *testing.T, logger pkglogger.AsyncLogger) {
	t.Helper()

	closer, ok := logger.(applogging.AsyncCloseableLogger)
	if !ok {
		t.Errorf("async logger does not implement AsyncCloseableLogger")
		return
	}
	if err := boundedwait.Close(t, "async logger Close", closer.Close, boundedwait.ForceCloserOf(closer)); err != nil {
		t.Errorf("close async logger: %v", err)
	}
}

// closeIntegrationDeliveryLoggerは、closeIntegrationLoggerと同じ上限でDelivery loggerをCloseします。
func closeIntegrationDeliveryLogger(t *testing.T, logger applogging.DeliveryClosableLogger) {
	t.Helper()

	if err := boundedwait.Close(t, "delivery logger Close", logger.Close, boundedwait.ForceCloserOf(logger)); err != nil {
		t.Errorf("close delivery logger: %v", err)
	}
}

type staticMemoryReader struct {
	pressure memorypressure.MemoryPressure
}

func (r staticMemoryReader) ReadMemoryPressure(ctx context.Context) (memorypressure.MemoryPressure, error) {
	_ = ctx
	return r.pressure, nil
}

func startedMemoryMonitor(t *testing.T, pressure memorypressure.MemoryPressure) memorypressure.Monitor {
	t.Helper()

	monitor, err := memorymonitor.New(
		staticMemoryReader{pressure: pressure},
		memorypressure.CriticalPolicy{
			CriticalRatio:           0.80,
			MinAvailableBytes:       128 * 1024 * 1024,
			ConsecutiveCriticalHits: 1,
		},
		time.Hour,
	)
	if err != nil {
		t.Fatalf("failed to create memory monitor: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := monitor.Start(ctx); err != nil {
		t.Fatalf("failed to start memory monitor: %v", err)
	}

	snapshot := monitor.Last()
	t.Logf(
		"memory monitor snapshot: ratio=%.2f available=%d reliable=%t critical=%t",
		snapshot.Pressure.Ratio,
		snapshot.Pressure.AvailableBytes,
		snapshot.Pressure.Reliable,
		snapshot.LastCritical,
	)

	return monitor
}

type bulkFailureStatus struct {
	status    int
	errorType string
	reason    string
}

type bulkFailureServer struct {
	*httptest.Server
	mu     sync.Mutex
	bodies []string
	// recordedは、requestを記録するたびに待機中の試験を起こします（容量1で、通知を失いません）。
	recorded chan struct{}
}

func newBulkFailureServer(t *testing.T, statuses []bulkFailureStatus) *bulkFailureServer {
	t.Helper()
	if len(statuses) == 0 {
		t.Fatal("bulk failure server requires at least one status")
	}

	rec := &bulkFailureServer{recorded: make(chan struct{}, 1)}
	rec.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_bulk" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read bulk request body: %v", err)
			http.Error(w, "failed to read request body", http.StatusInternalServerError)
			return
		}
		rec.recordBody(string(body))

		requests, err := parseBulkRequests(body)
		if err != nil {
			// handlerはserverのgoroutineで動くため、t.Fatalではなく失敗を記録して応答します。
			t.Errorf("invalid bulk request: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		items := make([]map[string]any, 0, len(requests))
		for i, req := range requests {
			status := statuses[i%len(statuses)]
			result := map[string]any{
				"_index": req.index,
				"_id":    req.id,
				"status": status.status,
			}
			if status.status >= http.StatusMultipleChoices {
				result["error"] = map[string]any{
					"type":   status.errorType,
					"reason": status.reason,
				}
			} else {
				result["result"] = "created"
			}
			items = append(items, map[string]any{"create": result})
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"took":   1,
			"errors": true,
			"items":  items,
		}); err != nil {
			t.Errorf("failed to encode bulk response: %v", err)
		}
	}))

	return rec
}

type openSearchFailureServer struct {
	*httptest.Server
	mu     sync.Mutex
	bodies []string
	// recordedは、requestを記録するたびに待機中の試験を起こします（容量1で、通知を失いません）。
	recorded chan struct{}
}

// newOpenSearchFailureServerは通常loggerの1件送信とspool runnerのBulk再送を
// 同じfailure条件で受け付けます。これにより、最初の失敗からspool再送までを
// 一つの統合試験で追跡できます。
func newOpenSearchFailureServer(t *testing.T, status bulkFailureStatus) *openSearchFailureServer {
	t.Helper()

	rec := &openSearchFailureServer{recorded: make(chan struct{}, 1)}
	rec.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isDocumentRequest := strings.HasPrefix(r.URL.Path, "/logs-integration/_create/")
		isBulkRequest := r.URL.Path == "/_bulk"
		if !isDocumentRequest && !isBulkRequest {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read document request body: %v", err)
			http.Error(w, "failed to read request body", http.StatusInternalServerError)
			return
		}
		rec.recordBody(string(body))

		w.Header().Set("Content-Type", "application/json")
		if isBulkRequest {
			requests, err := parseBulkRequests(body)
			if err != nil {
				// handlerはserverのgoroutineで動くため、t.Fatalではなく失敗を記録して応答します。
				t.Errorf("invalid bulk request: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			items := make([]map[string]any, 0, len(requests))
			for _, request := range requests {
				result := map[string]any{
					"_index": request.index,
					"_id":    request.id,
					"status": status.status,
				}
				if status.status >= http.StatusMultipleChoices {
					result["error"] = map[string]any{
						"type":   status.errorType,
						"reason": status.reason,
					}
				} else {
					result["result"] = "created"
				}
				items = append(items, map[string]any{"create": result})
			}
			if err := json.NewEncoder(w).Encode(map[string]any{
				"took":   1,
				"errors": status.status >= http.StatusMultipleChoices,
				"items":  items,
			}); err != nil {
				t.Errorf("failed to encode bulk response: %v", err)
			}
			return
		}

		w.WriteHeader(status.status)
		if err := json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"type":   status.errorType,
				"reason": status.reason,
			},
			"status": status.status,
		}); err != nil {
			t.Errorf("failed to encode document response: %v", err)
		}
	}))

	return rec
}

func (s *openSearchFailureServer) recordBody(body string) {
	s.mu.Lock()
	s.bodies = append(s.bodies, body)
	s.mu.Unlock()
	signalRecordedRequest(s.recorded)
}

// Recordedは、requestを記録したことを知らせるchannelです。
func (s *openSearchFailureServer) Recorded() <-chan struct{} { return s.recorded }

func (s *openSearchFailureServer) Bodies() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]string, len(s.bodies))
	copy(out, s.bodies)
	return out
}

func (s *bulkFailureServer) recordBody(body string) {
	s.mu.Lock()
	s.bodies = append(s.bodies, body)
	s.mu.Unlock()
	signalRecordedRequest(s.recorded)
}

// Recordedは、requestを記録したことを知らせるchannelです。
func (s *bulkFailureServer) Recorded() <-chan struct{} { return s.recorded }

// signalRecordedRequestは、待機中の試験へrequestの記録を知らせます。未受信の通知が残っていれば
// 追加の通知は不要です（待つ側は受け取るたびに件数を確かめ直します）。
func signalRecordedRequest(recorded chan<- struct{}) {
	select {
	case recorded <- struct{}{}:
	default:
	}
}

func (s *bulkFailureServer) Bodies() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]string, len(s.bodies))
	copy(out, s.bodies)
	return out
}

type parsedBulkRequest struct {
	index string
	id    string
}

// parseBulkRequestsはHTTP handlerから呼ぶため、testを終了させずにerrorを返します。
func parseBulkRequests(body []byte) ([]parsedBulkRequest, error) {
	scanner := bufio.NewScanner(bytes.NewReader(body))
	var requests []parsedBulkRequest
	for lineNo := 0; scanner.Scan(); lineNo++ {
		if lineNo%2 == 1 {
			continue
		}

		var meta map[string]map[string]string
		if err := json.Unmarshal(scanner.Bytes(), &meta); err != nil {
			return nil, fmt.Errorf("unmarshal bulk meta line %d: %w", lineNo, err)
		}
		action := meta["create"]
		requests = append(requests, parsedBulkRequest{
			index: action["_index"],
			id:    action["_id"],
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan bulk request: %w", err)
	}
	if len(requests) == 0 {
		return nil, errors.New("bulk request did not contain any create actions")
	}
	return requests, nil
}

func logBulkRequests(t *testing.T, server *bulkFailureServer) {
	t.Helper()

	for i, body := range server.Bodies() {
		t.Logf("opensearch bulk request[%d]:\n%s", i, strings.TrimSpace(body))
	}
}

func logOpenSearchRequests(t *testing.T, server *openSearchFailureServer) {
	t.Helper()

	for i, body := range server.Bodies() {
		t.Logf("opensearch document request[%d]:\n%s", i, strings.TrimSpace(body))
	}
}

// integrationHangTimeoutは、fake serverへのrequestなどが届かない停止（hang）を検出するまで
// 待つ上限です。正常な処理に必要な時間の見積もりではありません。
const integrationHangTimeout = 10 * time.Second

// waitForRequestBodiesは、serverがwantAtLeast件以上のrequestを記録するまで、記録の通知を待ちます。
func waitForRequestBodies(
	t *testing.T,
	name string,
	bodies func() []string,
	recorded <-chan struct{},
	wantAtLeast int,
) []string {
	t.Helper()

	timer := time.NewTimer(integrationHangTimeout)
	defer timer.Stop()
	for {
		if recordedBodies := bodies(); len(recordedBodies) >= wantAtLeast {
			return recordedBodies
		}
		select {
		case <-recorded:
		case <-timer.C:
			t.Fatalf("%s requests = %d, want at least %d within %s", name, len(bodies()), wantAtLeast, integrationHangTimeout)
		}
	}
}

type ioLogLine struct {
	path string
	line string
}

// readLogLinesは、root配下のextのfileの行を読みます。loggerをCloseした後に呼び、受理済みの
// failure処理（archive・spoolへの保存）が終わった後の内容を確認します。
func readLogLines(t *testing.T, root, ext string) []ioLogLine {
	t.Helper()

	lines, err := collectLogLines(root, ext)
	if err != nil {
		t.Fatalf("failed to collect log lines under %s: %v", root, err)
	}
	return lines
}

func collectLogLines(root, ext string) ([]ioLogLine, error) {
	var lines []ioLogLine
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ext {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			lines = append(lines, ioLogLine{path: path, line: line})
		}
		return scanner.Err()
	})
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return lines, nil
}

func logIOLines(t *testing.T, label string, lines []ioLogLine, limit int) {
	t.Helper()

	if limit <= 0 || limit > len(lines) {
		limit = len(lines)
	}
	for i := 0; i < limit; i++ {
		t.Logf("%s[%d] path=%s line=%s", label, i, lines[i].path, lines[i].line)
	}
}

func assertLinesContain(t *testing.T, lines []ioLogLine, want string) {
	t.Helper()

	for _, line := range lines {
		if strings.Contains(line.line, want) {
			return
		}
	}
	t.Fatalf("no log line contains %q", want)
}

type recordingMeter struct {
	mu       sync.Mutex
	counters map[string]int64
}

func newRecordingMeter() *recordingMeter {
	return &recordingMeter{counters: make(map[string]int64)}
}

func (m *recordingMeter) Counter(name string) pkgobserver.Counter {
	return recordingCounter{meter: m, name: name}
}

func (m *recordingMeter) Gauge(name string) pkgobserver.Gauge {
	_ = name
	return observer.NopGauge{}
}

func (m *recordingMeter) add(name string, value int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[name] += value
}

func (m *recordingMeter) counterValue(name string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[name]
}

type recordingCounter struct {
	meter *recordingMeter
	name  string
}

func (c recordingCounter) Add(ctx context.Context, value int64, attrs ...pkgobserver.Attr) {
	_ = ctx
	_ = attrs
	c.meter.add(c.name, value)
}

var _ pkgobserver.Meter = (*recordingMeter)(nil)
