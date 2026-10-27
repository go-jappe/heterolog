package cloudwatchlogs

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/config/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	fsstorestore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	fsworker "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/worker"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/fsarchivehandler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/spoolstorehandler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	infraspool "github.com/go-jappe/heterolog/internal/infrastructure/spool/log"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle"
	spoollifecyclereport "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle/report"
	spoolsendercloudwatchlogs "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/cloudwatchlogs"
	spoolstore "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/store"
	spoolworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	infracloudwatchlogs "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudWatchLogsLoggerFailureSpoolsActiveAndRunnerResendsToDone(t *testing.T) {
	ctx := context.Background()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}
	spoolCfg := cloudWatchLogsLoggerSpoolTestConfig{
		spoolPath: t.TempDir(),
		target:    "logging-integration",
		dir:       "cloudwatchlogs",
	}
	clock := fixedCloudWatchLogsClock{t: time.Date(2026, 6, 23, 12, 34, 56, 0, time.UTC)}
	fmCfg := cloudWatchLogsSpoolRunnerFlushManagerConfig(spoolCfg)
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

	flushManager := newTestCloudWatchLogsFlushManager(
		&recordingCloudWatchLogsClient{
			err: &pkgcloudwatchlogs.HTTPStatusError{
				Code:   http.StatusTooManyRequests,
				Status: "429 Too Many Requests",
			},
		},
		failureHandler,
	)

	require.NoError(t, flushManager.Put(ctx, newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, "durable cloudwatch resend")))

	activeRecords := waitForCloudWatchLogsLoggerSpoolRecords(t, cloudWatchLogsLoggerSpoolStateRoot(spoolCfg, pkgspool.StateActive), 1)
	require.Len(t, activeRecords, 1)
	activeDoc := unmarshalCloudWatchLogsLoggerSpoolDocument(t, activeRecords[0])
	assert.Equal(t, "durable cloudwatch resend", activeDoc["message"])
	assert.Contains(t, activeDoc["line"], "durable cloudwatch resend")
	assert.Equal(t, "/app/test", activeDoc["log_group"])
	assert.Equal(t, "stream-a", activeDoc["log_stream"])

	spoolStore, err := fsstorestore.NewSpoolStore(clock, spoolCfg, tracer, meter)
	require.NoError(t, err)
	stateStore, err := fsworker.NewStateStore(spoolCfg)
	require.NoError(t, err)
	resendClient := &recordingCloudWatchLogsSpoolResendClient{}
	spoolSender, err := spoolsendercloudwatchlogs.NewCloudWatchLogsSpoolSender(
		fixedCloudWatchLogsClock{t: clock.t.Add(5 * time.Minute)},
		resendClient,
	)
	require.NoError(t, err)
	spoolRunnerWorker, err := spoolworker.New(
		stateStore,
		cloudWatchLogsActiveSpoolWriter{store: spoolStore},
		spoolSender,
	)
	require.NoError(t, err)
	spoolRunnerWorker.TargetBackendService = "cloudwatchlogs"

	lifecycleManager, err := lifecycle.NewManager(cloudWatchLogsSpoolLifecycleStoreStub{}, tracer, meter)
	require.NoError(t, err)
	runner, err := infraspool.NewRunner(
		spoolRunnerWorker,
		lifecycleManager,
		cloudWatchLogsRunnerConfigStub{},
		tracer,
		meter,
	)
	require.NoError(t, err)

	report, err := runner.RunOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, spoolworker.ProcessStatusCompleted, report.ProcessReport.Status)
	assert.Equal(t, 1, report.ProcessReport.RecordCount)
	assert.Equal(t, 1, report.ProcessReport.SuccessRecordCount)
	require.Equal(t, 1, resendClient.putCalls)
	require.Len(t, resendClient.events, 1)
	assert.Contains(t, resendClient.events[0].Message, "durable cloudwatch resend")

	assert.Empty(t, readCloudWatchLogsLoggerSpoolRecords(t, cloudWatchLogsLoggerSpoolStateRoot(spoolCfg, pkgspool.StateActive)))
	assert.Empty(t, readCloudWatchLogsLoggerSpoolRecords(t, cloudWatchLogsLoggerSpoolStateRoot(spoolCfg, pkgspool.StateProcessing)))

	doneRecords := readCloudWatchLogsLoggerSpoolRecords(t, cloudWatchLogsLoggerSpoolStateRoot(spoolCfg, pkgspool.StateDone))
	require.Len(t, doneRecords, 1)
	doneDoc := unmarshalCloudWatchLogsLoggerSpoolDocument(t, doneRecords[0])
	assert.Equal(t, activeDoc, doneDoc)
	assert.False(t, doneRecords[0].FinalizedAt.IsZero())
	assert.False(t, doneRecords[0].FirstResendAttemptedAt.IsZero())
	assert.Equal(t, 1, doneRecords[0].ResendAttemptCount)
}

func TestCloudWatchLogsFlushManagerStoresLocallyInvalidEventInQuarantine(t *testing.T) {
	ctx := context.Background()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}
	spoolCfg := cloudWatchLogsLoggerSpoolTestConfig{
		spoolPath: t.TempDir(),
		target:    "cloudwatchlogs-local-validation-integration",
		dir:       "cloudwatchlogs",
	}
	clock := fixedCloudWatchLogsClock{t: time.Date(2026, 6, 23, 12, 34, 56, 0, time.UTC)}
	failureHandler, err := BuildFailureHandler(
		ctx,
		clock,
		cloudWatchLogsSpoolRunnerFlushManagerConfig(spoolCfg),
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

	client := &recordingCloudWatchLogsClient{}
	flushManager := newTestCloudWatchLogsFlushManager(client, failureHandler)
	require.NoError(t, flushManager.Put(
		ctx,
		newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, ""),
	))
	require.Empty(t, client.calls)

	quarantineRecords := waitForCloudWatchLogsLoggerSpoolRecords(
		t,
		cloudWatchLogsLoggerSpoolStateRoot(spoolCfg, pkgspool.StateQuarantine),
		1,
	)
	require.Len(t, quarantineRecords, 1)
	doc := unmarshalCloudWatchLogsLoggerSpoolDocument(t, quarantineRecords[0])
	assert.Equal(t, "", doc["message"])
	assert.Empty(t, readCloudWatchLogsLoggerSpoolRecords(
		t,
		cloudWatchLogsLoggerSpoolStateRoot(spoolCfg, pkgspool.StateActive),
	))
	assert.Empty(t, readCloudWatchLogsLoggerSpoolRecords(
		t,
		cloudWatchLogsLoggerSpoolStateRoot(spoolCfg, pkgspool.StateDead),
	))
}

// Loggerが一回だけ渡したbuffer snapshotをFlushManager以下が複数のPutLogEvents requestへ
// 分割した場合も、requestごとのfailure handlingがそれぞれ一つのspool fileを作り、
// runnerがfile単位で一回ずつ再送することを実runtimeの接続で確認します。
func TestCloudWatchLogsFlushUsesRequestScopedSpoolFilesAndRunnerResendsEach(
	t *testing.T,
) {
	firstOccurrence := time.Date(2026, 8, 13, 12, 5, 0, 0, time.UTC)
	runCloudWatchLogsRequestScopedSpoolIntegration(
		t,
		"cloudwatchlogs-stream-boundary-integration",
		pkgcloudwatchlogs.StreamRoutingHourly,
		[]cloudWatchLogsRequestBoundaryEntry{
			{occurredAt: firstOccurrence, message: "hour 12 retryable"},
			{occurredAt: firstOccurrence.Add(time.Hour), message: "hour 13 retryable"},
		},
		[]string{
			"stream-a/2026/08/13/12",
			"stream-a/2026/08/13/13",
		},
	)
}

// 同一streamのeventがrequest byte上限によって二つのphysical requestへ分かれた場合も、
// failure handlingがrequest境界ごとにspool fileを作り、runnerが各fileを一回ずつ再送する
// ことを確認します。件数は二件、timestampも同一なので、分割理由はbyte上限だけです。
func TestCloudWatchLogsFlushSplitsSameStreamByRequestBytesIntoSpoolFilesAndResendsEach(
	t *testing.T,
) {
	occurrence := time.Date(2026, 8, 13, 12, 5, 0, 0, time.UTC)
	runCloudWatchLogsRequestScopedSpoolIntegration(
		t,
		"cloudwatchlogs-request-byte-boundary-integration",
		pkgcloudwatchlogs.StreamRoutingNone,
		[]cloudWatchLogsRequestBoundaryEntry{
			{occurredAt: occurrence, message: strings.Repeat("a", 600_000)},
			{occurredAt: occurrence, message: strings.Repeat("b", 600_000)},
		},
		[]string{"stream-a", "stream-a"},
	)
}

type cloudWatchLogsRequestBoundaryEntry struct {
	occurredAt time.Time
	message    string
}

// runCloudWatchLogsRequestScopedSpoolIntegrationは、一つのbuffer snapshotが二つの
// physical PutLogEvents requestへ分割される各条件について、初回送信からspool保存、
// runner再送、done遷移までの共通境界を実runtimeの接続で検証します。
func runCloudWatchLogsRequestScopedSpoolIntegration(
	t *testing.T,
	target string,
	routingGranularity pkgcloudwatchlogs.StreamRouting,
	entries []cloudWatchLogsRequestBoundaryEntry,
	wantStreams []string,
) {
	t.Helper()
	require.Len(t, entries, 2)
	require.Len(t, wantStreams, 2)

	ctx := context.Background()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}
	spoolCfg := cloudWatchLogsLoggerSpoolTestConfig{
		spoolPath: t.TempDir(),
		target:    target,
		dir:       "cloudwatchlogs",
	}
	entryClock := newMutableCloudWatchLogsClock(entries[0].occurredAt)
	storeClock := fixedCloudWatchLogsClock{t: entries[0].occurredAt.Add(2 * time.Hour)}
	fmCfg := cloudWatchLogsSpoolRunnerFlushManagerConfigWithRoutingGranularity(
		spoolCfg,
		routingGranularity,
	)
	failureHandler, err := BuildFailureHandler(
		ctx,
		storeClock,
		fmCfg,
		nil,
		tracer,
		meter,
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	backendClient := &recordingCloudWatchLogsClient{
		err: &pkgcloudwatchlogs.HTTPStatusError{
			Code:   http.StatusTooManyRequests,
			Status: "429 Too Many Requests",
		},
	}
	logger, err := NewCloudWatchLogsLogger(
		ctx,
		newTestCloudWatchLogsLoggerConfig(100, time.Hour, false),
		backendClient,
		fmCfg,
		newTestCloudWatchLogsResourceProvider(),
		failureHandler,
		tracer,
		meter,
		&LoggerBase{Clock: entryClock},
	)
	require.NoError(t, err)

	for _, entry := range entries {
		entryClock.Set(entry.occurredAt)
		require.NoError(t, handleTestInfo(ctx, logger, entry.message))
	}
	require.NoError(t, logger.Close(ctx))
	require.Len(t, backendClient.calls, 2)
	initialRequestStreams := make([]string, 0, len(backendClient.calls))
	for _, call := range backendClient.calls {
		require.Len(t, call.events, 1)
		initialRequestStreams = append(initialRequestStreams, call.streamName)
	}
	sort.Strings(initialRequestStreams)
	expectedStreams := append([]string(nil), wantStreams...)
	sort.Strings(expectedStreams)
	require.Equal(t, expectedStreams, initialRequestStreams)

	activeRoot := cloudWatchLogsLoggerSpoolStateRoot(spoolCfg, pkgspool.StateActive)
	activeFiles := waitForCloudWatchLogsLoggerSpoolFiles(t, activeRoot, 2)
	require.Len(t, activeFiles, 2)

	storedStreams := make([]string, 0, len(activeFiles))
	for _, spoolFile := range activeFiles {
		require.Lenf(
			t,
			spoolFile.records,
			1,
			"spool file %q must contain records from exactly one PutLogEvents request",
			spoolFile.path,
		)
		doc := unmarshalCloudWatchLogsLoggerSpoolDocument(t, spoolFile.records[0])
		streamName, ok := doc["log_stream"].(string)
		require.Truef(t, ok, "spool file %q log_stream = %#v", spoolFile.path, doc["log_stream"])
		storedStreams = append(storedStreams, streamName)
	}
	sort.Strings(storedStreams)
	assert.Equal(t, initialRequestStreams, storedStreams)

	spoolStore, err := fsstorestore.NewSpoolStore(storeClock, spoolCfg, tracer, meter)
	require.NoError(t, err)
	stateStore, err := fsworker.NewStateStore(spoolCfg)
	require.NoError(t, err)
	resendClient := &recordingCloudWatchLogsSpoolResendClient{}
	spoolSender, err := spoolsendercloudwatchlogs.NewCloudWatchLogsSpoolSender(
		fixedCloudWatchLogsClock{t: storeClock.t.Add(5 * time.Minute)},
		resendClient,
	)
	require.NoError(t, err)
	spoolRunnerWorker, err := spoolworker.New(
		stateStore,
		cloudWatchLogsActiveSpoolWriter{store: spoolStore},
		spoolSender,
	)
	require.NoError(t, err)
	spoolRunnerWorker.TargetBackendService = "cloudwatchlogs"

	lifecycleManager, err := lifecycle.NewManager(cloudWatchLogsSpoolLifecycleStoreStub{}, tracer, meter)
	require.NoError(t, err)
	runner, err := infraspool.NewRunner(
		spoolRunnerWorker,
		lifecycleManager,
		cloudWatchLogsRunnerConfigStub{},
		tracer,
		meter,
	)
	require.NoError(t, err)

	for expectedCalls := 1; expectedCalls <= 2; expectedCalls++ {
		report, runErr := runner.RunOnce(ctx)
		require.NoError(t, runErr)
		assert.Equal(t, spoolworker.ProcessStatusCompleted, report.ProcessReport.Status)
		assert.Equal(t, 1, report.ProcessReport.RecordCount)
		assert.Equal(t, 1, report.ProcessReport.SuccessRecordCount)
		assert.Equal(t, expectedCalls, resendClient.putCalls)
	}

	assert.Empty(t, readCloudWatchLogsLoggerSpoolRecords(t, activeRoot))
	assert.Empty(t, readCloudWatchLogsLoggerSpoolRecords(
		t,
		cloudWatchLogsLoggerSpoolStateRoot(spoolCfg, pkgspool.StateProcessing),
	))
	doneFiles := readCloudWatchLogsLoggerSpoolFiles(
		t,
		cloudWatchLogsLoggerSpoolStateRoot(spoolCfg, pkgspool.StateDone),
	)
	require.Len(t, doneFiles, 2)
	for _, spoolFile := range doneFiles {
		require.Len(t, spoolFile.records, 1)
	}
	require.Len(t, resendClient.streamNames, 2)
	sort.Strings(resendClient.streamNames)
	assert.Equal(t, storedStreams, resendClient.streamNames)
}

// CloudWatch Logs の部分拒否を、受理済み event を含めず retryable/permanent に分け、
// spool と fileArchive の両方へ同じ failure payload として実保存できることを確認する。
func TestCloudWatchLogsBuildFailureHandlerPersistsPartialRejectionsToSpoolAndFileArchive(t *testing.T) {
	ctx := context.Background()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}
	root := t.TempDir()
	spoolCfg := cloudWatchLogsLoggerSpoolTestConfig{
		spoolPath: root,
		target:    "cloudwatchlogs-durable-test",
		dir:       "cloudwatchlogs",
	}
	clock := fixedCloudWatchLogsClock{t: time.Date(2026, 6, 23, 12, 34, 56, 0, time.UTC)}
	fmCfg := cloudWatchLogsSpoolAndFileArchiveFlushManagerConfig(root, spoolCfg)

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
	t.Cleanup(func() {
		require.NoError(t, failureHandler.Drain(context.Background()))
	})
	require.Len(t, failureHandler.Handlers, 2)

	var archivePipeline *fsarchivehandler.FailureArchivePipeline
	spoolHandlerCount := 0
	for _, handler := range failureHandler.Handlers {
		switch typed := handler.(type) {
		case *spoolstorehandler.SpoolStoreHandler:
			spoolHandlerCount++
		case *fsarchivehandler.FailureArchivePipeline:
			archivePipeline = typed
		}
	}
	require.Equal(t, 1, spoolHandlerCount)
	require.NotNil(t, archivePipeline)

	client := &recordingCloudWatchLogsClient{
		output: &pkgcloudwatchlogs.PutLogEventsOutput{
			RejectedLogEventsInfo: &pkgcloudwatchlogs.RejectedLogEventsInfo{
				TooOldLogEventEndIndex:   intPointer(1),
				TooNewLogEventStartIndex: intPointer(2),
			},
		},
	}
	flushManager := newTestCloudWatchLogsFlushManager(client, failureHandler)
	acceptedAt := clock.t
	err = flushManager.Flush(ctx, []*logging.LogEntry{
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "accepted cloudwatch durable", acceptedAt),
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "too new cloudwatch durable", acceptedAt.Add(time.Hour)),
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "too old cloudwatch durable", acceptedAt.Add(-time.Hour)),
	})
	require.NoError(t, err)
	retryableArchivePath, err := archivePipeline.ActiveArchivePath(failurearchive.FailureCategoryRetryable)
	require.NoError(t, err)
	permanentArchivePath, err := archivePipeline.ActiveArchivePath(failurearchive.FailureCategoryPermanent)
	require.NoError(t, err)

	retryableArchives := waitForCloudWatchLogsArchiveRecords(
		t,
		retryableArchivePath,
		1,
	)
	require.Len(t, retryableArchives, 1)
	assertCloudWatchLogsArchiveEvent(
		t,
		retryableArchives[0],
		"retryable",
		string(infracloudwatchlogs.FailureCodeRejectedTooNew),
		"too new cloudwatch durable",
	)

	permanentArchives := waitForCloudWatchLogsArchiveRecords(
		t,
		permanentArchivePath,
		1,
	)
	require.Len(t, permanentArchives, 1)
	assertCloudWatchLogsArchiveEvent(
		t,
		permanentArchives[0],
		"dead",
		string(infracloudwatchlogs.FailureCodeRejectedTooOld),
		"too old cloudwatch durable",
	)

	activeRecords := waitForCloudWatchLogsLoggerSpoolRecords(
		t,
		cloudWatchLogsLoggerSpoolStateRoot(spoolCfg, pkgspool.StateActive),
		1,
	)
	require.Len(t, activeRecords, 1)
	assertCloudWatchLogsSpoolEvent(t, activeRecords[0], "too new cloudwatch durable")

	deadRecords := waitForCloudWatchLogsLoggerSpoolRecords(
		t,
		cloudWatchLogsLoggerSpoolStateRoot(spoolCfg, pkgspool.StateDead),
		1,
	)
	require.Len(t, deadRecords, 1)
	assertCloudWatchLogsSpoolEvent(t, deadRecords[0], "too old cloudwatch durable")
}

func cloudWatchLogsSpoolAndFileArchiveFlushManagerConfig(
	archiveRoot string,
	spoolCfg cloudWatchLogsLoggerSpoolTestConfig,
) CloudWatchLogsFlushManagerConfigProvider {
	return cloudWatchLogsFailureStoreFlushManagerConfig(
		archiveRoot,
		spoolCfg,
		[]pkgdurable.DurableType{failure.DurableTypeSpool, failure.DurableTypeFsstore},
	)
}

func cloudWatchLogsFailureStoreFlushManagerConfig(
	archiveRoot string,
	spoolCfg cloudWatchLogsLoggerSpoolTestConfig,
	durableTypes []pkgdurable.DurableType,
) CloudWatchLogsFlushManagerConfigProvider {
	return NewFlushManagerConfigAdapter(pkglogger.CloudWatchLogsFlushManagerConfig{
		LogGroupName:  "/app/test",
		LogStreamName: "stream-a",
		FailureStoreConfig: pkgdurable.FailureStoreConfig{
			MaxConcurrent: 2,
			DurableType:   append([]pkgdurable.DurableType(nil), durableTypes...),
			DurableStoreConfig: pkgdurable.DurableStoreConfig{
				FileArchiveConfig: pkgdurable.FileArchiveConfig{
					MaxFileSize:                1 * 1024 * 1024,
					FailureFileArchivePath:     archiveRoot,
					Dir:                        "cloudwatchlogs-failure-archive",
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
					Target:                     spoolCfg.target,
					OverloadPolicyConfig: pkgdurable.FileArchiveOverloadPolicyConfig{
						OverloadPolicy:        failure.OverloadPolicyDrop,
						SpillDestination:      failure.SpillDestinationStdout,
						MinAvailableDiskSpace: pkgdurable.MinFileArchiveAvailableDiskSpace,
					},
				},
				SpoolConfig: pkgdurable.SpoolConfig{
					SpoolPath: spoolCfg.spoolPath,
					Target:    spoolCfg.target,
					Dir:       spoolCfg.dir,
					OverloadPolicyConfig: pkgdurable.SpoolOverloadPolicyConfig{
						OverloadPolicy: failure.OverloadPolicyDrop,
					},
				},
			},
		},
	}, pkgcloudwatchlogs.StreamRoutingNone)
}

type cloudWatchLogsArchiveRecord struct {
	Type            json.RawMessage `json:"type"`
	FailureCategory string          `json:"failure_category"`
	Backend         string          `json:"backend"`
	FailureCode     string          `json:"failure_code"`
	Payload         map[string]any  `json:"payload"`
}

func waitForCloudWatchLogsArchiveRecords(
	t *testing.T,
	path string,
	want int,
) []cloudWatchLogsArchiveRecord {
	t.Helper()

	// 書込みの完了を待ちます。時間の上限はhangの検出だけに使います。
	deadline := time.Now().Add(lifecycleHangTimeout)
	for {
		if records, ok := tryReadCloudWatchLogsArchiveRecords(path); ok && len(records) >= want {
			return records
		}
		if time.Now().After(deadline) {
			t.Fatalf("failure archive records at %q did not reach %d", path, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func tryReadCloudWatchLogsArchiveRecords(path string) ([]cloudWatchLogsArchiveRecord, bool) {
	file, err := os.Open(path)
	if err != nil {
		return nil, false
	}
	defer file.Close()

	records := make([]cloudWatchLogsArchiveRecord, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record cloudWatchLogsArchiveRecord
		if json.Unmarshal(scanner.Bytes(), &record) != nil {
			return nil, false
		}
		records = append(records, record)
	}
	if scanner.Err() != nil {
		return nil, false
	}
	return records, true
}

func assertCloudWatchLogsArchiveEvent(
	t *testing.T,
	record cloudWatchLogsArchiveRecord,
	wantFailureCategory string,
	wantFailureCode string,
	wantMessage string,
) {
	t.Helper()

	assert.Empty(t, record.Type, "failure archive JSON must not contain the removed type discriminator")
	assert.Equal(t, wantFailureCategory, record.FailureCategory)
	assert.Equal(t, deliveryBackendCloudWatchLogs, record.Backend)
	assert.Equal(t, wantFailureCode, record.FailureCode)
	assert.Equal(t, wantMessage, record.Payload["message"])
	assert.Contains(t, record.Payload["line"], wantMessage)
	assert.Equal(t, "/app/test", record.Payload["log_group"])
	assert.Equal(t, "stream-a", record.Payload["log_stream"])
	assert.NotEmpty(t, record.Payload["timestamp"])
	assert.NotContains(t, record.Payload["line"], "accepted cloudwatch durable")
}

func assertCloudWatchLogsSpoolEvent(t *testing.T, record pkgspool.SpoolRecord, wantMessage string) {
	t.Helper()

	doc := unmarshalCloudWatchLogsLoggerSpoolDocument(t, record)
	assert.Equal(t, wantMessage, doc["message"])
	assert.Contains(t, doc["line"], wantMessage)
	assert.Equal(t, "/app/test", doc["log_group"])
	assert.Equal(t, "stream-a", doc["log_stream"])
	assert.NotContains(t, doc["line"], "accepted cloudwatch durable")
}

func cloudWatchLogsSpoolRunnerFlushManagerConfig(
	cfg cloudWatchLogsLoggerSpoolTestConfig,
) CloudWatchLogsFlushManagerConfigProvider {
	return cloudWatchLogsSpoolRunnerFlushManagerConfigWithRoutingGranularity(
		cfg,
		pkgcloudwatchlogs.StreamRoutingNone,
	)
}

func cloudWatchLogsSpoolRunnerFlushManagerConfigWithRoutingGranularity(
	cfg cloudWatchLogsLoggerSpoolTestConfig,
	routingGranularity pkgcloudwatchlogs.StreamRouting,
) CloudWatchLogsFlushManagerConfigProvider {
	return NewFlushManagerConfigAdapter(pkglogger.CloudWatchLogsFlushManagerConfig{
		LogGroupName:  "/app/test",
		LogStreamName: "stream-a",
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
	}, routingGranularity)
}

type cloudWatchLogsLoggerSpoolTestConfig struct {
	spoolPath string
	target    string
	dir       string
}

func (c cloudWatchLogsLoggerSpoolTestConfig) SpoolPath() string { return c.spoolPath }
func (c cloudWatchLogsLoggerSpoolTestConfig) Target() string    { return c.target }
func (c cloudWatchLogsLoggerSpoolTestConfig) Dir() string       { return c.dir }
func (c cloudWatchLogsLoggerSpoolTestConfig) FileNameFormat() string {
	return ""
}
func (c cloudWatchLogsLoggerSpoolTestConfig) FileDateFormat() string {
	return "20060102"
}
func (c cloudWatchLogsLoggerSpoolTestConfig) MaxWriteRetries() int {
	return 1
}
func (c cloudWatchLogsLoggerSpoolTestConfig) FileOperationRetryWaitTime() time.Duration {
	return time.Millisecond
}
func (c cloudWatchLogsLoggerSpoolTestConfig) OverloadPolicyConfig() spoolstore.SpoolStoreOverloadPolicyConfigProvider {
	return cloudWatchLogsLoggerSpoolOverloadConfig{}
}

type cloudWatchLogsLoggerSpoolOverloadConfig struct{}

func (cloudWatchLogsLoggerSpoolOverloadConfig) OverloadPolicy() pkgdurable.OverloadPolicy {
	return pkgdurable.OverloadPolicy("drop")
}
func (cloudWatchLogsLoggerSpoolOverloadConfig) SpillDestination() pkgdurable.SpillDestination {
	return pkgdurable.SpillDestination("stdout")
}
func (cloudWatchLogsLoggerSpoolOverloadConfig) MaxCallsSinceScan() uint64 { return 0 }
func (cloudWatchLogsLoggerSpoolOverloadConfig) StatsScanInterval() time.Duration {
	return 0
}
func (cloudWatchLogsLoggerSpoolOverloadConfig) MaxDirSize() uint64                 { return 0 }
func (cloudWatchLogsLoggerSpoolOverloadConfig) ActiveBacklogWarnAfterDays() int    { return 0 }
func (cloudWatchLogsLoggerSpoolOverloadConfig) MinAvailableDiskSpace() uint64      { return 0 }
func (cloudWatchLogsLoggerSpoolOverloadConfig) UseMemoryPressureForOverload() bool { return false }
func (cloudWatchLogsLoggerSpoolOverloadConfig) MemoryCriticalRatio() float64       { return 0 }
func (cloudWatchLogsLoggerSpoolOverloadConfig) MinAvailableMemoryBytes() uint64    { return 0 }
func (cloudWatchLogsLoggerSpoolOverloadConfig) MemoryCriticalConsecutiveHits() int { return 0 }

type cloudWatchLogsActiveSpoolWriter struct {
	store *fsstorestore.SpoolStore
}

func (w cloudWatchLogsActiveSpoolWriter) StoreActive(ctx context.Context, records []pkgspool.SpoolRecord) error {
	return w.store.StoreByState(ctx, pkgspool.StateActive, records)
}

type recordingCloudWatchLogsSpoolResendClient struct {
	putCalls    int
	events      []pkgcloudwatchlogs.Event
	streamNames []string
}

func (c *recordingCloudWatchLogsSpoolResendClient) PutLogEvents(
	ctx context.Context,
	request pkgcloudwatchlogs.PutLogEventsRequest,
) (*pkgcloudwatchlogs.PutLogEventsOutput, error) {
	_ = ctx
	c.putCalls++
	c.events = append([]pkgcloudwatchlogs.Event(nil), request.Events...)
	c.streamNames = append(c.streamNames, request.LogStreamName)
	return &pkgcloudwatchlogs.PutLogEventsOutput{}, nil
}

type cloudWatchLogsRunnerConfigStub struct{}

func (cloudWatchLogsRunnerConfigStub) Interval() time.Duration { return time.Millisecond }
func (cloudWatchLogsRunnerConfigStub) StopOnError() bool       { return true }

type cloudWatchLogsSpoolLifecycleStoreStub struct{}

func (cloudWatchLogsSpoolLifecycleStoreStub) PurgeDoneIfNeed(
	ctx context.Context,
) (spoollifecyclereport.PurgeReport, error) {
	_ = ctx
	return spoollifecyclereport.PurgeReport{}, nil
}

func (cloudWatchLogsSpoolLifecycleStoreStub) PurgeDeadIfNeed(
	ctx context.Context,
) (spoollifecyclereport.PurgeReport, error) {
	_ = ctx
	return spoollifecyclereport.PurgeReport{}, nil
}

func (cloudWatchLogsSpoolLifecycleStoreStub) PurgeQuarantineIfNeed(
	ctx context.Context,
) (spoollifecyclereport.PurgeReport, error) {
	_ = ctx
	return spoollifecyclereport.PurgeReport{}, nil
}

func waitForCloudWatchLogsLoggerSpoolRecords(
	t *testing.T,
	root string,
	want int,
) []pkgspool.SpoolRecord {
	t.Helper()

	// 書込みの完了を待ちます。時間の上限はhangの検出だけに使います。
	deadline := time.Now().Add(lifecycleHangTimeout)
	for {
		records := readCloudWatchLogsLoggerSpoolRecords(t, root)
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

func readCloudWatchLogsLoggerSpoolRecords(t *testing.T, root string) []pkgspool.SpoolRecord {
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

		scanner := newCloudWatchLogsSpoolTestScanner(file)
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

type cloudWatchLogsLoggerSpoolFile struct {
	path    string
	records []pkgspool.SpoolRecord
}

func waitForCloudWatchLogsLoggerSpoolFiles(
	t *testing.T,
	root string,
	want int,
) []cloudWatchLogsLoggerSpoolFile {
	t.Helper()

	// 書込みの完了を待ちます。時間の上限はhangの検出だけに使います。
	deadline := time.Now().Add(lifecycleHangTimeout)
	for {
		files := readCloudWatchLogsLoggerSpoolFiles(t, root)
		if len(files) == want {
			return files
		}
		if time.Now().After(deadline) {
			require.Len(t, files, want)
			return files
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func readCloudWatchLogsLoggerSpoolFiles(
	t *testing.T,
	root string,
) []cloudWatchLogsLoggerSpoolFile {
	t.Helper()

	files := make([]cloudWatchLogsLoggerSpoolFile, 0)
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

		spoolFile := cloudWatchLogsLoggerSpoolFile{path: path}
		scanner := newCloudWatchLogsSpoolTestScanner(file)
		for scanner.Scan() {
			var record pkgspool.SpoolRecord
			if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
				return err
			}
			spoolFile.records = append(spoolFile.records, record)
		}
		if err := scanner.Err(); err != nil {
			return err
		}
		files = append(files, spoolFile)
		return nil
	})
	require.NoError(t, err)
	sort.Slice(files, func(left, right int) bool {
		return files[left].path < files[right].path
	})
	return files
}

// newCloudWatchLogsSpoolTestScannerは、最大eventを含むdurable payloadを読み取れるScannerを
// 返します。payloadには整形済みlineと元messageの両方が入るため、1 eventのAWS上限より
// 大きいJSON lineを許容する必要があります。
func newCloudWatchLogsSpoolTestScanner(file *os.File) *bufio.Scanner {
	scanner := bufio.NewScanner(file)
	scanner.Buffer(nil, 4*pkgcloudwatchlogs.MaxPutLogEventBytes)
	return scanner
}

func unmarshalCloudWatchLogsLoggerSpoolDocument(
	t *testing.T,
	record pkgspool.SpoolRecord,
) map[string]any {
	t.Helper()

	var doc map[string]any
	require.NoError(t, json.Unmarshal(record.Payload, &doc))
	return doc
}

func cloudWatchLogsLoggerSpoolStateRoot(
	cfg cloudWatchLogsLoggerSpoolTestConfig,
	state pkgspool.State,
) string {
	return filepath.Join(cfg.spoolPath, cfg.target, "spool", cfg.dir, string(state))
}

var _ spoolworker.ActiveSpoolWriter = cloudWatchLogsActiveSpoolWriter{}
var _ pkgcloudwatchlogs.Client = (*recordingCloudWatchLogsSpoolResendClient)(nil)
