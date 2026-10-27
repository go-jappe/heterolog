package loki

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/config/failure"
	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	fsstorestore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	fsworker "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/worker"
	backendloki "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	infraspool "github.com/go-jappe/heterolog/internal/infrastructure/spool/log"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle"
	spoollifecyclereport "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle/report"
	spoolsenderloki "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/loki"
	spoolstore "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/store"
	spoolworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLokiLoggerFailureSpoolsActiveAndRunnerResendsToDone(t *testing.T) {
	ctx := context.Background()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}
	spoolCfg := lokiLoggerSpoolTestConfig{
		spoolPath: t.TempDir(),
		target:    "logging-integration",
		dir:       "loki",
	}
	clock := fixedLokiClock{t: time.Date(2026, 6, 23, 12, 34, 56, 0, time.UTC)}
	fmCfg := lokiSpoolRunnerFlushManagerConfig(spoolCfg)
	failureHandler, err := BuildFailureHandler(
		ctx,
		clock,
		fmCfg,
		nil,
		tracer,
		meter,
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	flushManager := newTestLokiFlushManagerWithFailureHandler(
		&recordingFlushManagerLokiClient{
			pushErr: &pkgloki.HTTPStatusError{
				Code:   http.StatusServiceUnavailable,
				Status: "503 Service Unavailable",
			},
		},
		failureHandler,
	)

	require.NoError(t, flushManager.Push(ctx, newTestLokiLogEntry(pkglogger.LogLevelInfo, "durable loki resend")))

	activeRecords := waitForLokiLoggerSpoolRecords(t, lokiLoggerSpoolStateRoot(spoolCfg, pkgspool.StateActive), 1)
	require.Len(t, activeRecords, 1)
	activeDoc := unmarshalLokiLoggerSpoolDocument(t, activeRecords[0])
	assert.Equal(t, "durable loki resend", activeDoc["message"])
	assert.Contains(t, activeDoc["line"], "durable loki resend")

	spoolStore, err := fsstorestore.NewSpoolStore(clock, spoolCfg, tracer, meter)
	require.NoError(t, err)
	stateStore, err := fsworker.NewStateStore(spoolCfg)
	require.NoError(t, err)
	resendClient := &recordingLokiSpoolResendClient{}
	spoolSender, err := spoolsenderloki.NewLokiSpoolSender(
		fixedLokiClock{t: clock.t.Add(5 * time.Minute)},
		resendClient,
		backendloki.PushLimits{
			MaxLineBytes:        256 * 1024,
			MaxPushRequestBytes: 4 * 1024 * 1024,
		},
	)
	require.NoError(t, err)
	spoolRunnerWorker, err := spoolworker.New(
		stateStore,
		lokiActiveSpoolWriter{store: spoolStore},
		spoolSender,
	)
	require.NoError(t, err)
	spoolRunnerWorker.TargetBackendService = "loki"

	lifecycleManager, err := lifecycle.NewManager(lokiSpoolLifecycleStoreStub{}, tracer, meter)
	require.NoError(t, err)
	runner, err := infraspool.NewRunner(
		spoolRunnerWorker,
		lifecycleManager,
		lokiRunnerConfigStub{},
		tracer,
		meter,
	)
	require.NoError(t, err)

	report, err := runner.RunOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, spoolworker.ProcessStatusCompleted, report.ProcessReport.Status)
	assert.Equal(t, 1, report.ProcessReport.RecordCount)
	assert.Equal(t, 1, report.ProcessReport.SuccessRecordCount)
	require.Equal(t, 1, resendClient.pushCalls)
	require.Len(t, resendClient.streams, 1)
	require.Len(t, resendClient.streams[0].Entries, 1)
	assert.Contains(t, resendClient.streams[0].Entries[0].Line, "durable loki resend")

	assert.Empty(t, readLokiLoggerSpoolRecords(t, lokiLoggerSpoolStateRoot(spoolCfg, pkgspool.StateActive)))
	assert.Empty(t, readLokiLoggerSpoolRecords(t, lokiLoggerSpoolStateRoot(spoolCfg, pkgspool.StateProcessing)))

	doneRecords := readLokiLoggerSpoolRecords(t, lokiLoggerSpoolStateRoot(spoolCfg, pkgspool.StateDone))
	require.Len(t, doneRecords, 1)
	doneDoc := unmarshalLokiLoggerSpoolDocument(t, doneRecords[0])
	assert.Equal(t, activeDoc, doneDoc)
	assert.False(t, doneRecords[0].FinalizedAt.IsZero())
	assert.False(t, doneRecords[0].FirstResendAttemptedAt.IsZero())
	assert.Equal(t, 1, doneRecords[0].ResendAttemptCount)
}

func TestLokiFlushManagerStoresRejectedRequestRecordsAsIndividualQuarantineSpoolRecords(t *testing.T) {
	ctx := context.Background()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}
	spoolCfg := lokiLoggerSpoolTestConfig{
		spoolPath: t.TempDir(),
		target:    "logging-isolation-integration",
		dir:       "loki",
	}
	clock := fixedLokiClock{t: time.Date(2026, 6, 23, 12, 34, 56, 0, time.UTC)}
	failureHandler, err := BuildFailureHandler(
		ctx,
		clock,
		lokiSpoolRunnerFlushManagerConfig(spoolCfg),
		nil,
		tracer,
		meter,
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, failureHandler.Drain(context.Background()))
	})

	client := &recordingFlushManagerLokiClient{
		pushErr: &pkgloki.HTTPStatusError{
			Code:   http.StatusBadRequest,
			Status: "400 Bad Request",
		},
	}
	flushManager := newTestLokiFlushManagerWithFailureHandler(
		client,
		failureHandler,
	)
	entries := []*logging.LogEntry{
		newTestLokiLogEntry(pkglogger.LogLevelInfo, "first rejected record"),
		newTestLokiLogEntry(pkglogger.LogLevelInfo, "second rejected record"),
	}

	require.NoError(t, flushManager.Flush(ctx, entries))
	require.Equal(t, 1, client.pushCalls, "400 response must not trigger record-isolation resends")

	// 400は入力不備または一部受理の可能性があるため、配送不能とせずquarantineへ保存します。
	quarantineRecords := waitForLokiLoggerSpoolRecords(
		t,
		lokiLoggerSpoolStateRoot(spoolCfg, pkgspool.StateQuarantine),
		len(entries),
	)
	require.Len(t, quarantineRecords, len(entries))
	assert.Empty(t, readLokiLoggerSpoolRecords(t, lokiLoggerSpoolStateRoot(spoolCfg, pkgspool.StateActive)))
	assert.Empty(t, readLokiLoggerSpoolRecords(t, lokiLoggerSpoolStateRoot(spoolCfg, pkgspool.StateDead)))
	messages := make(map[string]struct{}, len(quarantineRecords))
	for _, record := range quarantineRecords {
		doc := unmarshalLokiLoggerSpoolDocument(t, record)
		message, ok := doc["message"].(string)
		require.True(t, ok)
		messages[message] = struct{}{}
	}
	assert.Contains(t, messages, "first rejected record")
	assert.Contains(t, messages, "second rejected record")
}

func TestLokiFlushManagerStoresLocallyOversizedRecordInQuarantine(t *testing.T) {
	ctx := context.Background()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}
	spoolCfg := lokiLoggerSpoolTestConfig{
		spoolPath: t.TempDir(),
		target:    "logging-local-validation-integration",
		dir:       "loki",
	}
	clock := fixedLokiClock{t: time.Date(2026, 6, 23, 12, 34, 56, 0, time.UTC)}
	failureHandler, err := BuildFailureHandler(
		ctx,
		clock,
		lokiSpoolRunnerFlushManagerConfig(spoolCfg),
		nil,
		tracer,
		meter,
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, failureHandler.Drain(context.Background()))
	})

	client := &recordingFlushManagerLokiClient{}
	flushManager := newTestLokiFlushManagerWithFailureHandler(client, failureHandler)
	flushManager.LoggerBase.config = newTestLokiLoggerConfigWithPushLimits(8, 1024)

	require.NoError(t, flushManager.Push(
		ctx,
		newTestLokiLogEntry(pkglogger.LogLevelInfo, "locally oversized Loki record"),
	))
	require.Zero(t, client.pushCalls)

	quarantineRecords := waitForLokiLoggerSpoolRecords(
		t,
		lokiLoggerSpoolStateRoot(spoolCfg, pkgspool.StateQuarantine),
		1,
	)
	require.Len(t, quarantineRecords, 1)
	doc := unmarshalLokiLoggerSpoolDocument(t, quarantineRecords[0])
	assert.Equal(t, "locally oversized Loki record", doc["message"])
	assert.Empty(t, readLokiLoggerSpoolRecords(t, lokiLoggerSpoolStateRoot(spoolCfg, pkgspool.StateActive)))
	assert.Empty(t, readLokiLoggerSpoolRecords(t, lokiLoggerSpoolStateRoot(spoolCfg, pkgspool.StateDead)))
}

func lokiSpoolRunnerFlushManagerConfig(
	cfg lokiLoggerSpoolTestConfig,
) LokiFlushManagerConfigProvider {
	return NewFlushManagerConfigAdapter(pkglogger.LokiFlushManagerConfig{
		FailureStoreConfig: pkgdurable.FailureStoreConfig{
			MaxConcurrent: 1,
			DurableType:   []pkgdurable.DurableType{failure.DurableTypeSpool},
			DurableStoreConfig: pkgdurable.DurableStoreConfig{
				SpoolConfig: pkgdurable.SpoolConfig{
					SpoolPath: cfg.spoolPath,
					Target:    cfg.target,
					Dir:       cfg.dir,
					OverloadPolicyConfig: pkgdurable.SpoolOverloadPolicyConfig{
						OverloadPolicy: pkgdurable.OverloadPolicy("drop"),
					},
				},
			},
		},
	})
}

type lokiLoggerSpoolTestConfig struct {
	spoolPath string
	target    string
	dir       string
}

func (c lokiLoggerSpoolTestConfig) SpoolPath() string { return c.spoolPath }
func (c lokiLoggerSpoolTestConfig) Target() string    { return c.target }
func (c lokiLoggerSpoolTestConfig) Dir() string       { return c.dir }
func (c lokiLoggerSpoolTestConfig) FileNameFormat() string {
	return ""
}
func (c lokiLoggerSpoolTestConfig) FileDateFormat() string {
	return "20060102"
}
func (c lokiLoggerSpoolTestConfig) MaxWriteRetries() int {
	return 1
}
func (c lokiLoggerSpoolTestConfig) FileOperationRetryWaitTime() time.Duration {
	return time.Millisecond
}
func (c lokiLoggerSpoolTestConfig) OverloadPolicyConfig() spoolstore.SpoolStoreOverloadPolicyConfigProvider {
	return lokiLoggerSpoolOverloadConfig{}
}

type lokiLoggerSpoolOverloadConfig struct{}

func (lokiLoggerSpoolOverloadConfig) OverloadPolicy() pkgdurable.OverloadPolicy {
	return pkgdurable.OverloadPolicy("drop")
}
func (lokiLoggerSpoolOverloadConfig) SpillDestination() pkgdurable.SpillDestination {
	return pkgdurable.SpillDestination("stdout")
}
func (lokiLoggerSpoolOverloadConfig) MaxCallsSinceScan() uint64 { return 0 }
func (lokiLoggerSpoolOverloadConfig) StatsScanInterval() time.Duration {
	return 0
}
func (lokiLoggerSpoolOverloadConfig) MaxDirSize() uint64                 { return 0 }
func (lokiLoggerSpoolOverloadConfig) ActiveBacklogWarnAfterDays() int    { return 0 }
func (lokiLoggerSpoolOverloadConfig) MinAvailableDiskSpace() uint64      { return 0 }
func (lokiLoggerSpoolOverloadConfig) UseMemoryPressureForOverload() bool { return false }
func (lokiLoggerSpoolOverloadConfig) MemoryCriticalRatio() float64       { return 0 }
func (lokiLoggerSpoolOverloadConfig) MinAvailableMemoryBytes() uint64    { return 0 }
func (lokiLoggerSpoolOverloadConfig) MemoryCriticalConsecutiveHits() int { return 0 }

type lokiActiveSpoolWriter struct {
	store *fsstorestore.SpoolStore
}

func (w lokiActiveSpoolWriter) StoreActive(ctx context.Context, records []pkgspool.SpoolRecord) error {
	return w.store.StoreByState(ctx, pkgspool.StateActive, records)
}

type recordingLokiSpoolResendClient struct {
	pushCalls int
	streams   []pkgloki.Stream
}

func (c *recordingLokiSpoolResendClient) Push(ctx context.Context, streams []pkgloki.Stream) error {
	_ = ctx
	c.pushCalls++
	c.streams = append([]pkgloki.Stream(nil), streams...)
	return nil
}

type lokiRunnerConfigStub struct{}

func (lokiRunnerConfigStub) Interval() time.Duration { return time.Millisecond }
func (lokiRunnerConfigStub) StopOnError() bool       { return true }

type lokiSpoolLifecycleStoreStub struct{}

func (lokiSpoolLifecycleStoreStub) PurgeDoneIfNeed(
	ctx context.Context,
) (spoollifecyclereport.PurgeReport, error) {
	_ = ctx
	return spoollifecyclereport.PurgeReport{}, nil
}

func (lokiSpoolLifecycleStoreStub) PurgeDeadIfNeed(
	ctx context.Context,
) (spoollifecyclereport.PurgeReport, error) {
	_ = ctx
	return spoollifecyclereport.PurgeReport{}, nil
}

func (lokiSpoolLifecycleStoreStub) PurgeQuarantineIfNeed(
	ctx context.Context,
) (spoollifecyclereport.PurgeReport, error) {
	_ = ctx
	return spoollifecyclereport.PurgeReport{}, nil
}

func waitForLokiLoggerSpoolRecords(
	t *testing.T,
	root string,
	want int,
) []pkgspool.SpoolRecord {
	t.Helper()

	// 書込みの完了を待ちます。時間の上限はhangの検出だけに使います。
	deadline := time.Now().Add(lifecycleHangTimeout)
	for {
		records := readLokiLoggerSpoolRecords(t, root)
		if len(records) == want {
			return records
		}
		if time.Now().After(deadline) {
			require.Len(t, records, want)
			return records
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func readLokiLoggerSpoolRecords(t *testing.T, root string) []pkgspool.SpoolRecord {
	t.Helper()

	var records []pkgspool.SpoolRecord
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() || filepath.Ext(path) != spooldir.SpoolExt {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var record pkgspool.SpoolRecord
			if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
				return err
			}
			records = append(records, record)
		}
		return scanner.Err()
	})
	require.NoError(t, err)

	return records
}

func unmarshalLokiLoggerSpoolDocument(
	t *testing.T,
	record pkgspool.SpoolRecord,
) map[string]any {
	t.Helper()

	var doc map[string]any
	require.NoError(t, json.Unmarshal(record.Payload, &doc))
	return doc
}

func lokiLoggerSpoolStateRoot(
	cfg lokiLoggerSpoolTestConfig,
	state pkgspool.State,
) string {
	return filepath.Join(cfg.spoolPath, cfg.target, "spool", cfg.dir, string(state))
}

var _ spoolworker.ActiveSpoolWriter = lokiActiveSpoolWriter{}
var _ spoolsenderloki.Client = (*recordingLokiSpoolResendClient)(nil)
