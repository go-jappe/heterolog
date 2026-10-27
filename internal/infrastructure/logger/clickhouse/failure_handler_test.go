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
	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/fsarchivehandler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/spoolstorehandler"
	loggerhandler "github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildFailureHandlerFileArchivePersistsClickHouseFailuresWithDeduplicationToken(t *testing.T) {
	now := time.Date(2026, 7, 12, 12, 34, 56, 0, time.UTC)
	fmCfg := newClickHouseFailureArchiveFlushManagerConfig(
		t.TempDir(),
		[]pkgdurable.DurableType{failure.DurableTypeFsstore},
	)

	composite, err := BuildFailureHandler(
		context.Background(),
		fixedClickHouseClock{t: now},
		fmCfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, composite.Handlers, 1)

	archivePipeline, ok := composite.Handlers[0].(*fsarchivehandler.FailureArchivePipeline)
	require.True(t, ok, "handler type = %T, want *fsarchivehandler.FailureArchivePipeline", composite.Handlers[0])
	drainClickHouseFailureHandlers(t, composite)
	retryableArchivePath, err := archivePipeline.ActiveArchivePath(failurearchive.FailureCategoryRetryable)
	require.NoError(t, err)
	permanentArchivePath, err := archivePipeline.ActiveArchivePath(failurearchive.FailureCategoryPermanent)
	require.NoError(t, err)

	retryableResult, retryableToken := newClickHouseInsertSendResult(
		t,
		http.StatusTooManyRequests,
		"retryable clickhouse archive",
	)
	require.Len(t, retryableResult.RetryableRecords, 1)
	archivePipeline.HandleFailure(context.Background(), retryableResult)

	permanentResult, permanentToken := newClickHouseInsertSendResult(
		t,
		http.StatusBadRequest,
		"permanent clickhouse archive",
	)
	require.Len(t, permanentResult.DeadRecords, 1)
	archivePipeline.HandleFailure(context.Background(), permanentResult)

	retryableRecords := waitForClickHouseFailureArchiveRecords(
		t,
		retryableArchivePath,
		1,
	)
	require.Len(t, retryableRecords, 1)
	assert.Equal(t, "retryable", retryableRecords[0].FailureCategory)
	assert.Equal(t, "retryable clickhouse archive", retryableRecords[0].Payload["message"])
	assert.Equal(t, retryableToken, retryableRecords[0].Payload[pkgclickhouse.SettingInsertDeduplicationToken])

	permanentRecords := waitForClickHouseFailureArchiveRecords(
		t,
		permanentArchivePath,
		1,
	)
	require.Len(t, permanentRecords, 1)
	assert.Equal(t, "dead", permanentRecords[0].FailureCategory)
	assert.Equal(t, "permanent clickhouse archive", permanentRecords[0].Payload["message"])
	assert.Equal(t, permanentToken, permanentRecords[0].Payload[pkgclickhouse.SettingInsertDeduplicationToken])
}

func TestBuildFailureHandlerWiresArchiveInternalErrorHandler(t *testing.T) {
	fmCfg := newClickHouseFailureArchiveFlushManagerConfig(
		t.TempDir(),
		[]pkgdurable.DurableType{failure.DurableTypeFsstore},
	)
	resolverCalled := false
	callbackCalled := make(chan struct{}, 1)
	resolver := func(
		durableType pkgdurable.DurableType,
		cfg loggerhandler.ConfigProvider,
	) flushfailure.InternalHandlers {
		resolverCalled = true
		assert.Equal(t, pkgdurable.DurableTypeFileArchive, durableType)
		assert.Equal(t, []string{"stdout"}, cfg.OnError())
		return flushfailure.InternalHandlers{
			OnError: func(context.Context, error, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) {
				callbackCalled <- struct{}{}
			},
		}
	}

	composite, err := BuildFailureHandler(
		context.Background(),
		fixedClickHouseClock{t: time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)},
		fmCfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		resolver,
		nil,
		nil,
	)
	require.NoError(t, err)
	require.True(t, resolverCalled)
	require.Len(t, composite.Handlers, 1)

	archivePipeline := composite.Handlers[0].(*fsarchivehandler.FailureArchivePipeline)
	drainClickHouseFailureHandlers(t, composite)
	archivePipeline.HandleFailure(context.Background(), loggerfailure.SendResult{
		RetryableRecords: []loggerfailure.FailedRecord{{Payload: map[string]any{
			"message":     "retry",
			"unencodable": make(chan struct{}),
		}}},
	})
	select {
	case <-callbackCalled:
	case <-time.After(lifecycleHangTimeout):
		t.Fatal("archive write error was not routed to configured OnError handler")
	}
}

func TestBuildFailureHandlerBuildsSpoolAndFileArchiveHandlers(t *testing.T) {
	fmCfg := newClickHouseFailureArchiveFlushManagerConfig(
		t.TempDir(),
		[]pkgdurable.DurableType{failure.DurableTypeSpool, failure.DurableTypeFsstore},
	)

	composite, err := BuildFailureHandler(
		context.Background(),
		fixedClickHouseClock{t: time.Date(2026, 7, 12, 12, 34, 56, 0, time.UTC)},
		fmCfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, composite.Handlers, 2)

	var spoolHandler *spoolstorehandler.SpoolStoreHandler
	var archivePipeline *fsarchivehandler.FailureArchivePipeline
	for _, failureHandler := range composite.Handlers {
		switch typed := failureHandler.(type) {
		case *spoolstorehandler.SpoolStoreHandler:
			spoolHandler = typed
		case *fsarchivehandler.FailureArchivePipeline:
			archivePipeline = typed
		}
	}
	require.NotNil(t, spoolHandler, "spool handler was not built")
	require.NotNil(t, archivePipeline, "file archive pipeline was not built")
	drainClickHouseFailureHandlers(t, composite)
	retryableArchivePath, err := archivePipeline.ActiveArchivePath(failurearchive.FailureCategoryRetryable)
	require.NoError(t, err)

	result, token := newClickHouseInsertSendResult(
		t,
		http.StatusTooManyRequests,
		"clickhouse spool and archive",
	)
	require.Len(t, result.RetryableRecords, 1)
	spoolHandler.HandleFailure(context.Background(), result)
	archivePipeline.HandleFailure(context.Background(), result)

	spoolRecords := waitForClickHouseFailureSpoolRecords(
		t,
		spoolHandler.Store.RootDir.ActiveDir().Path(),
		1,
	)
	require.Len(t, spoolRecords, 1)
	var spoolPayload map[string]any
	require.NoError(t, json.Unmarshal(spoolRecords[0].Payload, &spoolPayload))
	assert.Equal(t, "clickhouse spool and archive", spoolPayload["message"])
	assert.Equal(t, token, spoolPayload[pkgclickhouse.SettingInsertDeduplicationToken])

	archiveRecords := waitForClickHouseFailureArchiveRecords(
		t,
		retryableArchivePath,
		1,
	)
	require.Len(t, archiveRecords, 1)
	assert.Equal(t, "clickhouse spool and archive", archiveRecords[0].Payload["message"])
	assert.Equal(t, token, archiveRecords[0].Payload[pkgclickhouse.SettingInsertDeduplicationToken])
	assert.Equal(
		t,
		spoolPayload[pkgclickhouse.SettingInsertDeduplicationToken],
		archiveRecords[0].Payload[pkgclickhouse.SettingInsertDeduplicationToken],
	)
}

func TestBuildDeliveryFailureHandlerBuildsFileArchiveOnly(t *testing.T) {
	fmCfg := newDeliveryClickHouseFailureArchiveFlushManagerConfig(
		t.TempDir(),
		[]pkgdurable.DurableType{failure.DurableTypeFsstore},
	)

	composite, err := BuildDeliveryFailureHandler(
		context.Background(),
		fixedClickHouseClock{t: time.Date(2026, 7, 12, 12, 34, 56, 0, time.UTC)},
		fmCfg,
		observer.NopTracer{},
		observer.NopMeter{},
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, composite.Handlers, 1)
	_, ok := composite.Handlers[0].(*fsarchivehandler.FailureArchivePipeline)
	require.True(t, ok, "handler type = %T, want *fsarchivehandler.FailureArchivePipeline", composite.Handlers[0])
	drainClickHouseFailureHandlers(t, composite)
}

func TestBuildDeliveryFailureHandlerRejectsSpool(t *testing.T) {
	fmCfg := newDeliveryClickHouseFailureArchiveFlushManagerConfig(
		t.TempDir(),
		[]pkgdurable.DurableType{failure.DurableTypeSpool},
	)

	composite, err := BuildDeliveryFailureHandler(
		context.Background(),
		fixedClickHouseClock{t: time.Date(2026, 7, 12, 12, 34, 56, 0, time.UTC)},
		fmCfg,
		observer.NopTracer{},
		observer.NopMeter{},
		nil,
		nil,
	)
	var durableTypeErr *pkgdurable.DurableTypeError
	require.ErrorAs(t, err, &durableTypeErr)
	require.Equal(t, pkgdurable.DurableTypeError{
		Scope: "delivery failure store", Type: pkgdurable.DurableTypeSpool, Err: pkgdurable.ErrUnsupportedDurableType,
	}, *durableTypeErr)
	require.Empty(t, composite.Handlers)
}

func TestBuildDeliveryFailureHandlerWiresArchiveInternalErrorHandler(t *testing.T) {
	fmCfg := newDeliveryClickHouseFailureArchiveFlushManagerConfig(
		t.TempDir(),
		[]pkgdurable.DurableType{failure.DurableTypeFsstore},
	)
	resolverCalled := false
	callbackCalled := make(chan struct{}, 1)
	resolver := func(
		durableType pkgdurable.DurableType,
		cfg loggerhandler.ConfigProvider,
	) flushfailure.InternalHandlers {
		resolverCalled = true
		assert.Equal(t, pkgdurable.DurableTypeFileArchive, durableType)
		assert.Equal(t, []string{"stdout"}, cfg.OnError())
		return flushfailure.InternalHandlers{
			OnError: func(context.Context, error, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) {
				callbackCalled <- struct{}{}
			},
		}
	}

	composite, err := BuildDeliveryFailureHandler(
		context.Background(),
		fixedClickHouseClock{t: time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)},
		fmCfg,
		observer.NopTracer{},
		observer.NopMeter{},
		resolver,
		nil,
	)
	require.NoError(t, err)
	require.True(t, resolverCalled)
	require.Len(t, composite.Handlers, 1)

	archivePipeline := composite.Handlers[0].(*fsarchivehandler.FailureArchivePipeline)
	drainClickHouseFailureHandlers(t, composite)
	archivePipeline.HandleFailure(context.Background(), loggerfailure.SendResult{
		RetryableRecords: []loggerfailure.FailedRecord{{Payload: map[string]any{
			"message":     "retry",
			"unencodable": make(chan struct{}),
		}}},
	})
	select {
	case <-callbackCalled:
	case <-time.After(lifecycleHangTimeout):
		t.Fatal("delivery archive write error was not routed to configured OnError handler")
	}
}

func newClickHouseFailureArchiveFlushManagerConfig(
	root string,
	durableTypes []pkgdurable.DurableType,
) ClickHouseFlushManagerConfigProvider {
	return NewFlushManagerConfigAdapter(pkglogger.ClickHouseFlushManagerConfig{
		FailureStoreConfig: clickHouseFailureStoreConfig(root, durableTypes),
	})
}

func newDeliveryClickHouseFailureArchiveFlushManagerConfig(
	root string,
	durableTypes []pkgdurable.DurableType,
) DeliveryClickHouseFlushManagerConfigProvider {
	return NewDeliveryFlushManagerConfigAdapter(pkglogger.DeliveryClickHouseFlushManagerConfig{
		FailureStoreConfig: clickHouseDeliveryFailureStoreConfig(root, durableTypes),
	})
}

func clickHouseDeliveryFailureStoreConfig(
	root string,
	durableTypes []pkgdurable.DurableType,
) pkgdurable.DeliveryFailureStoreConfig {
	full := clickHouseFailureStoreConfig(root, durableTypes)
	archive := full.DurableStoreConfig.FileArchiveConfig
	return pkgdurable.DeliveryFailureStoreConfig{
		EnableDegrade: full.EnableDegrade,
		DurableType:   durableTypes,
		SpillType:     full.SpillType,
		FileArchiveConfig: pkgdurable.DeliveryFileArchiveConfig{
			Target:                     archive.Target,
			MaxFileSize:                archive.MaxFileSize,
			FailureFileArchivePath:     archive.FailureFileArchivePath,
			Dir:                        archive.Dir,
			DirCaseRetryable:           archive.DirCaseRetryable,
			DirCasePermanent:           archive.DirCasePermanent,
			FileNameFormat:             archive.FileNameFormat,
			FileDateFormat:             archive.FileDateFormat,
			EnableRotate:               archive.EnableRotate,
			MaxFileBackups:             archive.MaxFileBackups,
			MaxFileAge:                 archive.MaxFileAge,
			MaxDirSize:                 archive.MaxDirSize,
			UsageCacheTTL:              archive.UsageCacheTTL,
			EnableFileCompress:         archive.EnableFileCompress,
			CompressFormat:             archive.CompressFormat,
			RotationType:               archive.RotationType,
			RotationDayOfWeek:          archive.RotationDayOfWeek,
			MaxWriteRetries:            archive.MaxWriteRetries,
			FileOperationRetryWaitTime: archive.FileOperationRetryWaitTime,
			ObjectStorageUploadConfig:  archive.ObjectStorageUploadConfig,
			OverloadPolicyConfig:       archive.OverloadPolicyConfig,
			HandlerConfig:              archive.HandlerConfig,
			IsDebug:                    archive.IsDebug,
			ErrorTraceDepth:            archive.ErrorTraceDepth,
			ErrorTraceDepthDebugMode:   archive.ErrorTraceDepthDebugMode,
		},
	}
}

func clickHouseFailureStoreConfig(
	root string,
	durableTypes []pkgdurable.DurableType,
) pkgdurable.FailureStoreConfig {
	return pkgdurable.FailureStoreConfig{
		MaxConcurrent: 2,
		DurableType:   durableTypes,
		DurableStoreConfig: pkgdurable.DurableStoreConfig{
			FileArchiveConfig: pkgdurable.FileArchiveConfig{
				Target:                     "clickhouse-test",
				MaxFileSize:                infrafsstore.MinAllowedFileSizeBytes,
				FailureFileArchivePath:     root,
				Dir:                        "clickhouse-failure-archive",
				DirCaseRetryable:           "retryable",
				DirCasePermanent:           "permanent",
				FileNameFormat:             pkgfsstore.FailureArchiveFileNameFormat,
				FileDateFormat:             pkgfsstore.FileDateFormat,
				EnableRotate:               false,
				MaxFileBackups:             infrafsstore.MinAllowedMaxFileBackups,
				MaxFileAge:                 infrafsstore.MinAllowedMaxFileAge,
				MaxDirSize:                 infrafsstore.MinAllowedMaxDirSize,
				UsageCacheTTL:              time.Hour,
				EnableFileCompress:         false,
				CompressFormat:             pkgfsstore.CompressGzip,
				RotationType:               pkgfsstore.FileRotationDaily,
				RotationDayOfWeek:          time.Sunday,
				MaxWriteRetries:            0,
				FileOperationRetryWaitTime: time.Millisecond,
				OverloadPolicyConfig: pkgdurable.FileArchiveOverloadPolicyConfig{
					OverloadPolicy:        failure.OverloadPolicyDrop,
					MinAvailableDiskSpace: pkgdurable.MinFileArchiveAvailableDiskSpace,
				},
				HandlerConfig: pkgloggerhandler.HandlerConfig{
					OnErrorConfig: []string{"stdout"},
				},
			},
			SpoolConfig: pkgdurable.SpoolConfig{
				SpoolPath: root,
				Target:    "clickhouse-test",
				Dir:       "clickhouse",
				OverloadPolicyConfig: pkgdurable.SpoolOverloadPolicyConfig{
					OverloadPolicy: failure.OverloadPolicyDrop,
				},
			},
		},
	}
}

func newClickHouseInsertSendResult(
	t *testing.T,
	statusCode int,
	message string,
) (loggerfailure.SendResult, string) {
	t.Helper()

	client := &recordingClickHouseClient{
		err: &pkgclickhouse.HTTPStatusError{
			Code:   statusCode,
			Status: http.StatusText(statusCode),
		},
	}
	flushManager := NewClickHouseFlushManager(
		newTestClickHouseFlushManagerBase(t),
		client,
		newTestClickHouseFlushManagerConfig(),
		nil,
		newTestClickHouseResourceProvider(),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	entry := &logging.LogEntry{
		OccurredAt: time.Date(2026, 7, 12, 12, 30, 0, 0, time.UTC),
		Level:      pkglogger.LogLevelError,
		Message:    message,
	}

	result, err := insertClickHouseLogEntriesAsSingleRequestForTest(
		context.Background(),
		flushManager,
		[]*logging.LogEntry{entry},
	)
	require.NoError(t, err)
	require.Len(t, client.insertOptions, 1)
	token := client.insertOptions[0].InsertDeduplicationToken
	require.NotEmpty(t, token)

	return result, token
}

type clickHouseFailureArchiveRecord struct {
	FailureCategory string         `json:"failure_category"`
	Payload         map[string]any `json:"payload"`
}

func waitForClickHouseFailureArchiveRecords(
	t *testing.T,
	path string,
	want int,
) []clickHouseFailureArchiveRecord {
	t.Helper()

	// 書込みの完了を待ちます。時間の上限はhangの検出だけに使います。
	deadline := time.Now().Add(lifecycleHangTimeout)
	for {
		records, err := tryReadClickHouseFailureArchiveRecords(path)
		if err == nil && len(records) == want {
			return records
		}
		if time.Now().After(deadline) {
			require.NoError(t, err)
			require.Len(t, records, want)
			return records
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func tryReadClickHouseFailureArchiveRecords(
	path string,
) ([]clickHouseFailureArchiveRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var records []clickHouseFailureArchiveRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record clickHouseFailureArchiveRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func waitForClickHouseFailureSpoolRecords(
	t *testing.T,
	root string,
	want int,
) []pkgspool.SpoolRecord {
	t.Helper()

	// 書込みの完了を待ちます。時間の上限はhangの検出だけに使います。
	deadline := time.Now().Add(lifecycleHangTimeout)
	for {
		records, err := tryReadClickHouseFailureSpoolRecords(root)
		if err == nil && len(records) == want {
			return records
		}
		if time.Now().After(deadline) {
			require.NoError(t, err)
			require.Len(t, records, want)
			return records
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func tryReadClickHouseFailureSpoolRecords(root string) ([]pkgspool.SpoolRecord, error) {
	var records []pkgspool.SpoolRecord
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != spooldir.SpoolExt {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var record pkgspool.SpoolRecord
			if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
				_ = file.Close()
				return err
			}
			records = append(records, record)
		}
		scanErr := scanner.Err()
		closeErr := file.Close()
		if scanErr != nil {
			return scanErr
		}
		return closeErr
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

func drainClickHouseFailureHandlers(
	t *testing.T,
	drainer interface{ Drain(context.Context) error },
) {
	t.Helper()
	t.Cleanup(func() {
		require.NoError(t, drainer.Drain(context.Background()))
	})
}
