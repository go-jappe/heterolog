package clickhouse

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
	infraclickhouse "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse"
	infraspool "github.com/go-jappe/heterolog/internal/infrastructure/spool/log"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle"
	spoollifecyclereport "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle/report"
	spoolsenderclickhouse "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/clickhouse"
	spoolstore "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/store"
	spoolworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClickHouseLoggerFailureSpoolsActiveAndRunnerResendsToDoneWithOriginalDeduplicationToken(t *testing.T) {
	ctx := context.Background()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}
	spoolCfg := clickHouseLoggerSpoolTestConfig{
		spoolPath: t.TempDir(),
		target:    "logging-integration",
		dir:       "clickhouse",
	}
	spoolClock := fixedClickHouseClock{
		t: time.Date(2026, 7, 14, 12, 34, 56, 0, time.UTC),
	}
	fmCfg := clickHouseSpoolRunnerFlushManagerConfig(spoolCfg)
	failureHandler, err := BuildFailureHandler(
		ctx,
		spoolClock,
		fmCfg,
		nil,
		tracer,
		meter,
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	initialClient := &recordingClickHouseClient{
		err: &pkgclickhouse.HTTPStatusError{
			Code:   http.StatusServiceUnavailable,
			Status: "503 Service Unavailable",
		},
	}
	loggerCfg := newTestClickHouseLoggerBaseConfig()
	loggerCfg.FlushInterval = time.Hour
	loggerCfg.MaxEntries = 1
	logger, err := NewClickHouseLogger(
		ctx,
		NewClickHouseLoggerConfigAdapter(loggerCfg),
		initialClient,
		fmCfg,
		newTestClickHouseResourceProvider(),
		failureHandler,
		tracer,
		meter,
		newTestClickHouseLoggerBase(),
		WithClock(fixedClickHouseClock{t: spoolClock.t.Add(-time.Minute)}),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, logger.Close(context.Background()))
	})

	require.NoError(t, handleTestInfo(ctx, logger, "durable clickhouse resend"))
	require.Equal(t, 1, initialClient.batchCount())
	require.Len(t, initialClient.insertOptions, 1)
	originalToken := initialClient.insertOptions[0].InsertDeduplicationToken
	require.NotEmpty(t, originalToken)

	activeRoot := clickHouseLoggerSpoolStateRoot(spoolCfg, pkgspool.StateActive)
	activeRecords := waitForClickHouseLoggerSpoolRecords(t, activeRoot, 1)
	require.Len(t, activeRecords, 1)
	activeDoc := unmarshalClickHouseLoggerSpoolDocument(t, activeRecords[0])
	assert.Equal(t, "durable clickhouse resend", activeDoc["message"])
	assert.Contains(t, activeDoc["line"], "durable clickhouse resend")
	assert.Equal(t, originalToken, activeDoc[pkgclickhouse.SettingInsertDeduplicationToken])

	spoolStore, err := fsstorestore.NewSpoolStore(spoolClock, spoolCfg, tracer, meter)
	require.NoError(t, err)
	stateStore, err := fsworker.NewStateStore(spoolCfg)
	require.NoError(t, err)
	resendClient := &recordingClickHouseClient{}
	spoolSender, err := spoolsenderclickhouse.NewClickHouseSpoolSender(
		fixedClickHouseClock{t: spoolClock.t.Add(5 * time.Minute)},
		resendClient,
		10*1024*1024,
	)
	require.NoError(t, err)
	spoolRunnerWorker, err := spoolworker.New(
		stateStore,
		clickHouseActiveSpoolWriter{store: spoolStore},
		spoolSender,
	)
	require.NoError(t, err)
	spoolRunnerWorker.TargetBackendService = "clickhouse"

	lifecycleManager, err := lifecycle.NewManager(clickHouseSpoolLifecycleStoreStub{}, tracer, meter)
	require.NoError(t, err)
	runner, err := infraspool.NewRunner(
		spoolRunnerWorker,
		lifecycleManager,
		clickHouseRunnerConfigStub{},
		tracer,
		meter,
	)
	require.NoError(t, err)

	report, err := runner.RunOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, spoolworker.ProcessStatusCompleted, report.ProcessReport.Status)
	assert.Equal(t, 1, report.ProcessReport.RecordCount)
	assert.Equal(t, 1, report.ProcessReport.SuccessRecordCount)
	require.Equal(t, 1, resendClient.batchCount())
	require.Len(t, resendClient.batches[0], 1)
	assert.Equal(t, "durable clickhouse resend", resendClient.batches[0][0].Message)
	require.Len(t, resendClient.insertOptions, 1)
	assert.Equal(t, originalToken, resendClient.insertOptions[0].InsertDeduplicationToken)

	assert.Empty(t, readClickHouseLoggerSpoolRecords(t, activeRoot))
	assert.Empty(t, readClickHouseLoggerSpoolRecords(
		t,
		clickHouseLoggerSpoolStateRoot(spoolCfg, pkgspool.StateProcessing),
	))

	doneRecords := readClickHouseLoggerSpoolRecords(
		t,
		clickHouseLoggerSpoolStateRoot(spoolCfg, pkgspool.StateDone),
	)
	require.Len(t, doneRecords, 1)
	doneDoc := unmarshalClickHouseLoggerSpoolDocument(t, doneRecords[0])
	assert.Equal(t, activeDoc, doneDoc)
	assert.Equal(t, originalToken, doneDoc[pkgclickhouse.SettingInsertDeduplicationToken])
	assert.False(t, doneRecords[0].FinalizedAt.IsZero())
	assert.False(t, doneRecords[0].FirstResendAttemptedAt.IsZero())
	assert.Equal(t, 1, doneRecords[0].ResendAttemptCount)
}

func TestClickHouseLoggerRecordFailureIsolatesReplayAndQuarantinesOnlyPoisonRecord(t *testing.T) {
	ctx := context.Background()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}
	spoolCfg := clickHouseLoggerSpoolTestConfig{
		spoolPath: t.TempDir(),
		target:    "logging-isolation-integration",
		dir:       "clickhouse",
	}
	spoolClock := fixedClickHouseClock{
		t: time.Date(2026, 8, 15, 12, 34, 56, 0, time.UTC),
	}
	fmCfg := clickHouseSpoolRunnerFlushManagerConfig(spoolCfg)
	failureHandler, err := BuildFailureHandler(
		ctx,
		spoolClock,
		fmCfg,
		nil,
		tracer,
		meter,
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	poisonErr := &pkgclickhouse.HTTPStatusError{
		Code:          http.StatusInternalServerError,
		Status:        "500 Internal Server Error",
		ExceptionType: pkgclickhouse.ErrorTypeCannotParseNumber,
	}
	initialClient := &recordingClickHouseClient{err: poisonErr}
	loggerCfg := newTestClickHouseLoggerBaseConfig()
	loggerCfg.FlushInterval = time.Hour
	loggerCfg.MaxEntries = 2
	logger, err := NewClickHouseLogger(
		ctx,
		NewClickHouseLoggerConfigAdapter(loggerCfg),
		initialClient,
		fmCfg,
		newTestClickHouseResourceProvider(),
		failureHandler,
		tracer,
		meter,
		newTestClickHouseLoggerBase(),
		WithClock(fixedClickHouseClock{t: spoolClock.t.Add(-time.Minute)}),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, logger.Close(context.Background()))
	})

	require.NoError(t, handleTestInfo(ctx, logger, "valid record"))
	require.NoError(t, handleTestInfo(ctx, logger, "poison record"))
	require.Equal(t, 1, initialClient.batchCount())
	rootToken := initialClient.insertOptions[0].InsertDeduplicationToken
	require.NotEmpty(t, rootToken)

	activeRoot := clickHouseLoggerSpoolStateRoot(spoolCfg, pkgspool.StateActive)
	activeRecords := waitForClickHouseLoggerSpoolRecords(t, activeRoot, 2)
	require.Len(t, activeRecords, 2)
	for _, record := range activeRecords {
		doc := unmarshalClickHouseLoggerSpoolDocument(t, record)
		assert.Equal(t, string(infraclickhouse.DurableReplayModeRecordIsolation), doc[infraclickhouse.DurablePayloadFieldReplayMode])
		assert.NotEmpty(t, doc[infraclickhouse.DurablePayloadFieldRecordID])
		assert.Equal(t, rootToken, doc[pkgclickhouse.SettingInsertDeduplicationToken])
	}

	spoolStore, err := fsstorestore.NewSpoolStore(spoolClock, spoolCfg, tracer, meter)
	require.NoError(t, err)
	stateStore, err := fsworker.NewStateStore(spoolCfg)
	require.NoError(t, err)
	resendClient := &selectiveClickHouseReplayClient{poisonMessage: "poison record", poisonErr: poisonErr}
	spoolSender, err := spoolsenderclickhouse.NewClickHouseSpoolSender(
		fixedClickHouseClock{t: spoolClock.t.Add(5 * time.Minute)},
		resendClient,
		10*1024*1024,
	)
	require.NoError(t, err)
	spoolRunnerWorker, err := spoolworker.New(
		stateStore,
		clickHouseActiveSpoolWriter{store: spoolStore},
		spoolSender,
	)
	require.NoError(t, err)
	spoolRunnerWorker.TargetBackendService = "clickhouse"

	lifecycleManager, err := lifecycle.NewManager(clickHouseSpoolLifecycleStoreStub{}, tracer, meter)
	require.NoError(t, err)
	runner, err := infraspool.NewRunner(
		spoolRunnerWorker,
		lifecycleManager,
		clickHouseRunnerConfigStub{},
		tracer,
		meter,
	)
	require.NoError(t, err)

	report, err := runner.RunOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, spoolworker.ProcessStatusCompleted, report.ProcessReport.Status)
	assert.Equal(t, 2, report.ProcessReport.RecordCount)
	assert.Equal(t, 1, report.ProcessReport.SuccessRecordCount)
	// 一件でも値を拒否されたrecordは入力不備として、配送不能とせずquarantineへ隔離します。
	assert.Equal(t, 1, report.ProcessReport.QuarantineRecordCount)
	assert.Zero(t, report.ProcessReport.DeadRecordCount)
	require.Len(t, resendClient.batches, 2)
	require.Len(t, resendClient.batches[0], 1)
	require.Len(t, resendClient.batches[1], 1)
	assert.NotEqual(t, rootToken, resendClient.options[0].InsertDeduplicationToken)
	assert.NotEqual(t, rootToken, resendClient.options[1].InsertDeduplicationToken)
	assert.NotEqual(t, resendClient.options[0].InsertDeduplicationToken, resendClient.options[1].InsertDeduplicationToken)

	assert.Empty(t, readClickHouseLoggerSpoolRecords(t, activeRoot))
	doneRecords := readClickHouseLoggerSpoolRecords(
		t,
		clickHouseLoggerSpoolStateRoot(spoolCfg, pkgspool.StateDone),
	)
	quarantineRecords := readClickHouseLoggerSpoolRecords(
		t,
		clickHouseLoggerSpoolStateRoot(spoolCfg, pkgspool.StateQuarantine),
	)
	require.Len(t, doneRecords, 1)
	require.Len(t, quarantineRecords, 1)
	assert.Empty(t, readClickHouseLoggerSpoolRecords(t, clickHouseLoggerSpoolStateRoot(spoolCfg, pkgspool.StateDead)))
	assert.Equal(t, "valid record", unmarshalClickHouseLoggerSpoolDocument(t, doneRecords[0])["message"])
	assert.Equal(t, "poison record", unmarshalClickHouseLoggerSpoolDocument(t, quarantineRecords[0])["message"])
}

type selectiveClickHouseReplayClient struct {
	poisonMessage string
	poisonErr     error
	batches       [][]pkgclickhouse.LogRecord
	options       []pkgclickhouse.InsertOptions
}

func (c *selectiveClickHouseReplayClient) InsertLogsWithOptions(
	_ context.Context,
	records []pkgclickhouse.LogRecord,
	opts pkgclickhouse.InsertOptions,
) (*pkgclickhouse.InsertResponse, error) {
	c.batches = append(c.batches, append([]pkgclickhouse.LogRecord(nil), records...))
	c.options = append(c.options, opts)
	if len(records) == 1 && records[0].Message == c.poisonMessage {
		return nil, c.poisonErr
	}
	return &pkgclickhouse.InsertResponse{StatusCode: http.StatusOK}, nil
}

func clickHouseSpoolRunnerFlushManagerConfig(
	cfg clickHouseLoggerSpoolTestConfig,
) ClickHouseFlushManagerConfigProvider {
	return NewFlushManagerConfigAdapter(pkglogger.ClickHouseFlushManagerConfig{
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

type clickHouseLoggerSpoolTestConfig struct {
	spoolPath string
	target    string
	dir       string
}

func (c clickHouseLoggerSpoolTestConfig) SpoolPath() string { return c.spoolPath }
func (c clickHouseLoggerSpoolTestConfig) Target() string    { return c.target }
func (c clickHouseLoggerSpoolTestConfig) Dir() string       { return c.dir }
func (c clickHouseLoggerSpoolTestConfig) FileNameFormat() string {
	return ""
}
func (c clickHouseLoggerSpoolTestConfig) FileDateFormat() string {
	return "20060102"
}
func (c clickHouseLoggerSpoolTestConfig) MaxWriteAttempts() int {
	return 1
}
func (c clickHouseLoggerSpoolTestConfig) FileOperationRetryWaitTime() time.Duration {
	return time.Millisecond
}
func (c clickHouseLoggerSpoolTestConfig) OverloadPolicyConfig() spoolstore.SpoolStoreOverloadPolicyConfigProvider {
	return clickHouseLoggerSpoolOverloadConfig{}
}

type clickHouseLoggerSpoolOverloadConfig struct{}

func (clickHouseLoggerSpoolOverloadConfig) OverloadPolicy() pkgdurable.OverloadPolicy {
	return pkgdurable.OverloadPolicy("drop")
}
func (clickHouseLoggerSpoolOverloadConfig) SpillDestination() pkgdurable.SpillDestination {
	return pkgdurable.SpillDestination("stdout")
}
func (clickHouseLoggerSpoolOverloadConfig) MaxCallsSinceScan() uint64 { return 0 }
func (clickHouseLoggerSpoolOverloadConfig) StatsScanInterval() time.Duration {
	return 0
}
func (clickHouseLoggerSpoolOverloadConfig) MaxDirSize() uint64                 { return 0 }
func (clickHouseLoggerSpoolOverloadConfig) ActiveBacklogWarnAfterDays() int    { return 0 }
func (clickHouseLoggerSpoolOverloadConfig) MinAvailableDiskSpace() uint64      { return 0 }
func (clickHouseLoggerSpoolOverloadConfig) UseMemoryPressureForOverload() bool { return false }
func (clickHouseLoggerSpoolOverloadConfig) MemoryCriticalRatio() float64       { return 0 }
func (clickHouseLoggerSpoolOverloadConfig) MinAvailableMemoryBytes() uint64    { return 0 }
func (clickHouseLoggerSpoolOverloadConfig) MemoryCriticalConsecutiveHits() int { return 0 }

type clickHouseActiveSpoolWriter struct {
	store *fsstorestore.SpoolStore
}

func (w clickHouseActiveSpoolWriter) StoreActive(
	ctx context.Context,
	records []pkgspool.SpoolRecord,
) error {
	return w.store.StoreByState(ctx, pkgspool.StateActive, records)
}

type clickHouseRunnerConfigStub struct{}

func (clickHouseRunnerConfigStub) Interval() time.Duration { return time.Millisecond }
func (clickHouseRunnerConfigStub) StopOnError() bool       { return true }

type clickHouseSpoolLifecycleStoreStub struct{}

func (clickHouseSpoolLifecycleStoreStub) PurgeDoneIfNeed(
	ctx context.Context,
) (spoollifecyclereport.PurgeReport, error) {
	_ = ctx
	return spoollifecyclereport.PurgeReport{}, nil
}

func (clickHouseSpoolLifecycleStoreStub) PurgeDeadIfNeed(
	ctx context.Context,
) (spoollifecyclereport.PurgeReport, error) {
	_ = ctx
	return spoollifecyclereport.PurgeReport{}, nil
}

func (clickHouseSpoolLifecycleStoreStub) PurgeQuarantineIfNeed(
	ctx context.Context,
) (spoollifecyclereport.PurgeReport, error) {
	_ = ctx
	return spoollifecyclereport.PurgeReport{}, nil
}

func waitForClickHouseLoggerSpoolRecords(
	t *testing.T,
	root string,
	want int,
) []pkgspool.SpoolRecord {
	t.Helper()

	// 書込みの完了を待ちます。時間の上限はhangの検出だけに使います。
	deadline := time.Now().Add(lifecycleHangTimeout)
	for {
		records := readClickHouseLoggerSpoolRecords(t, root)
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

func readClickHouseLoggerSpoolRecords(t *testing.T, root string) []pkgspool.SpoolRecord {
	t.Helper()

	var records []pkgspool.SpoolRecord
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != spooldir.SpoolExt {
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

func unmarshalClickHouseLoggerSpoolDocument(
	t *testing.T,
	record pkgspool.SpoolRecord,
) map[string]any {
	t.Helper()

	var doc map[string]any
	require.NoError(t, json.Unmarshal(record.Payload, &doc))
	return doc
}

func clickHouseLoggerSpoolStateRoot(
	cfg clickHouseLoggerSpoolTestConfig,
	state pkgspool.State,
) string {
	return filepath.Join(cfg.spoolPath, cfg.target, "spool", cfg.dir, string(state))
}

var _ spoolworker.ActiveSpoolWriter = clickHouseActiveSpoolWriter{}
var _ spoolsenderclickhouse.Client = (*recordingClickHouseClient)(nil)
