package loki

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/config/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	lokiretryclassifier "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki/retryclassifier"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/fsarchivehandler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/spoolstorehandler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Loki の送信失敗から生成された backend payload が、retryable/permanent の実 JSONL に残ることを確認する。
func TestLokiFailureHandlerFileArchivePersistsBackendPayload(t *testing.T) {
	t.Parallel()

	archiveRoot := t.TempDir()
	clock := fixedLokiClock{t: time.Date(2026, 7, 12, 12, 34, 56, 0, time.UTC)}
	fmCfg := newLokiFileArchiveTestFlushManagerConfig(archiveRoot, t.TempDir())
	failureHandler, err := BuildFailureHandler(
		context.Background(),
		clock,
		fmCfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, failureHandler.Drain(context.Background()))
	})
	require.Len(t, failureHandler.Handlers, 1)

	archivePipeline, ok := failureHandler.Handlers[0].(*fsarchivehandler.FailureArchivePipeline)
	require.True(t, ok, "fileArchive handler type = %T", failureHandler.Handlers[0])
	retryableArchivePath, err := archivePipeline.ActiveArchivePath(failurearchive.FailureCategoryRetryable)
	require.NoError(t, err)
	permanentArchivePath, err := archivePipeline.ActiveArchivePath(failurearchive.FailureCategoryPermanent)
	require.NoError(t, err)
	require.True(t, filepath.IsAbs(retryableArchivePath))
	require.True(t, filepath.IsAbs(permanentArchivePath))

	client := &recordingFlushManagerLokiClient{
		pushErr: &pkgloki.HTTPStatusError{
			Code:   http.StatusServiceUnavailable,
			Status: "503 Service Unavailable",
		},
	}
	flushManager := newTestLokiFlushManagerWithFailureHandler(client, failureHandler)

	require.NoError(t, flushManager.Push(
		context.Background(),
		newTestLokiLogEntry(pkglogger.LogLevelInfo, "retryable loki archive payload"),
	))

	client.pushErr = &pkgloki.HTTPStatusError{
		Code:   http.StatusUnauthorized,
		Status: "401 Unauthorized",
	}
	require.NoError(t, flushManager.Push(
		context.Background(),
		newTestLokiLogEntry(pkglogger.LogLevelError, "permanent loki archive payload"),
	))

	// 400で拒否されたentryは入力不備として、permanent archiveへquarantineとして残します。
	client.pushErr = &pkgloki.HTTPStatusError{
		Code:   http.StatusBadRequest,
		Status: "400 Bad Request",
	}
	require.NoError(t, flushManager.Push(
		context.Background(),
		newTestLokiLogEntry(pkglogger.LogLevelWarn, "rejected loki archive payload"),
	))

	retryableRecords := waitForLokiFailureArchiveRecords(
		t,
		retryableArchivePath,
		1,
	)
	require.Len(t, retryableRecords, 1)
	assertLokiFailureArchiveRecord(
		t,
		retryableRecords[0],
		"retryable",
		http.StatusServiceUnavailable,
		string(retryable.RetryReasonHTTPStatus),
		"info",
		"retryable loki archive payload",
	)

	permanentRecords := waitForLokiFailureArchiveRecords(
		t,
		permanentArchivePath,
		2,
	)
	require.Len(t, permanentRecords, 2)
	assertLokiFailureArchiveRecord(
		t,
		permanentRecords[0],
		"dead",
		http.StatusUnauthorized,
		string(retryable.RetryReasonHTTPStatus),
		"error",
		"permanent loki archive payload",
	)
	assertLokiFailureArchiveRecord(
		t,
		permanentRecords[1],
		"quarantine",
		http.StatusBadRequest,
		string(lokiretryclassifier.RetryReasonPushRejected),
		"warn",
		"rejected loki archive payload",
	)
}

// Loki の durable 設定に spool と fileArchive を併記した場合、両 handler が同時に構築されることを確認する。
func TestBuildFailureHandlerBuildsSpoolAndFileArchiveHandlers(t *testing.T) {
	t.Parallel()

	fmCfg := newLokiFailureStoreTestFlushManagerConfig(
		t.TempDir(), t.TempDir(),
		[]pkgdurable.DurableType{failure.DurableTypeSpool, failure.DurableTypeFsstore},
	)
	failureHandler, err := BuildFailureHandler(
		context.Background(),
		fixedLokiClock{t: time.Date(2026, 7, 12, 13, 0, 0, 0, time.UTC)},
		fmCfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, failureHandler.Drain(context.Background()))
	})
	require.Len(t, failureHandler.Handlers, 2)

	assert.IsType(t, &spoolstorehandler.SpoolStoreHandler{}, failureHandler.Handlers[0])
	archivePipeline, ok := failureHandler.Handlers[1].(*fsarchivehandler.FailureArchivePipeline)
	require.True(t, ok, "fileArchive handler type = %T", failureHandler.Handlers[1])
	retryableArchivePath, err := archivePipeline.ActiveArchivePath(failurearchive.FailureCategoryRetryable)
	require.NoError(t, err)
	permanentArchivePath, err := archivePipeline.ActiveArchivePath(failurearchive.FailureCategoryPermanent)
	require.NoError(t, err)
	require.True(t, filepath.IsAbs(retryableArchivePath))
	require.True(t, filepath.IsAbs(permanentArchivePath))
}

// Lokiのfile archiveには共通handler名ではなくLoki固有のbackend名が保存される。
func TestLokiFileArchiveUsesLokiBackendName(t *testing.T) {
	t.Parallel()

	fmCfg := newLokiFileArchiveTestFlushManagerConfig(t.TempDir(), t.TempDir())
	failureHandler, err := BuildFailureHandler(
		context.Background(),
		fixedLokiClock{t: time.Date(2026, 7, 12, 13, 30, 0, 0, time.UTC)},
		fmCfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, failureHandler.Drain(context.Background()))
	})
	require.Len(t, failureHandler.Handlers, 1)

	archivePipeline, ok := failureHandler.Handlers[0].(*fsarchivehandler.FailureArchivePipeline)
	require.True(t, ok, "fileArchive handler type = %T", failureHandler.Handlers[0])
	retryableArchivePath, err := archivePipeline.ActiveArchivePath(failurearchive.FailureCategoryRetryable)
	require.NoError(t, err)
	permanentArchivePath, err := archivePipeline.ActiveArchivePath(failurearchive.FailureCategoryPermanent)
	require.NoError(t, err)
	require.True(t, filepath.IsAbs(retryableArchivePath))
	require.True(t, filepath.IsAbs(permanentArchivePath))

	flushManager := newTestLokiFlushManager(&recordingFlushManagerLokiClient{
		pushErr: &pkgloki.HTTPStatusError{
			Code:   http.StatusServiceUnavailable,
			Status: "503 Service Unavailable",
		},
	})
	result, err := flushManager.pushLogEntries(
		context.Background(),
		[]*logging.LogEntry{newTestLokiLogEntry(pkglogger.LogLevelWarn, "loki archive backend")},
	)
	require.NoError(t, err)
	require.True(t, result.HasFailedRecords())

	failureHandler.HandleFailure(context.Background(), result)

	records := waitForLokiFailureArchiveRecords(
		t,
		retryableArchivePath,
		1,
	)
	require.Len(t, records, 1)
	assert.Equal(t, deliveryBackendLoki, records[0].Backend)
}

type lokiFailureArchiveRecord struct {
	Type            json.RawMessage `json:"type"`
	FailureCategory string          `json:"failure_category"`
	Backend         string          `json:"backend"`
	StatusCode      int             `json:"status_code"`
	FailureCode     string          `json:"failure_code"`
	Payload         map[string]any  `json:"payload"`
}

func newLokiFileArchiveTestFlushManagerConfig(
	archiveRoot string,
	spoolRoot string,
) LokiFlushManagerConfigProvider {
	return newLokiFailureStoreTestFlushManagerConfig(
		archiveRoot,
		spoolRoot,
		[]pkgdurable.DurableType{failure.DurableTypeFsstore},
	)
}

func newLokiFailureStoreTestFlushManagerConfig(
	archiveRoot string,
	spoolRoot string,
	durableTypes []pkgdurable.DurableType,
) LokiFlushManagerConfigProvider {
	return NewFlushManagerConfigAdapter(pkglogger.LokiFlushManagerConfig{
		FailureStoreConfig: pkgdurable.FailureStoreConfig{
			MaxConcurrent: 2,
			DurableType:   append([]pkgdurable.DurableType(nil), durableTypes...),
			DurableStoreConfig: pkgdurable.DurableStoreConfig{
				FileArchiveConfig: pkgdurable.FileArchiveConfig{
					Target:                     "loki-test",
					MaxFileSize:                1024 * 1024,
					FailureFileArchivePath:     archiveRoot,
					Dir:                        "loki-failure-archive",
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
					CompressFormat:             pkgfsstore.CompressZstd,
					RotationType:               pkgfsstore.FileRotationDaily,
					RotationDayOfWeek:          time.Sunday,
					MaxWriteAttempts:           1,
					FileOperationRetryWaitTime: time.Millisecond,
					OverloadPolicyConfig: pkgdurable.FileArchiveOverloadPolicyConfig{
						OverloadPolicy:        failure.OverloadPolicyDrop,
						MinAvailableDiskSpace: pkgdurable.MinFileArchiveAvailableDiskSpace,
					},
				},
				SpoolConfig: pkgdurable.SpoolConfig{
					SpoolPath: spoolRoot,
					Target:    "loki-test",
					Dir:       "loki",
					OverloadPolicyConfig: pkgdurable.SpoolOverloadPolicyConfig{
						OverloadPolicy: failure.OverloadPolicyDrop,
					},
				},
			},
		},
	})
}

func waitForLokiFailureArchiveRecords(
	t *testing.T,
	path string,
	want int,
) []lokiFailureArchiveRecord {
	t.Helper()

	// 書込みの完了を待ちます。時間の上限はhangの検出だけに使います。
	deadline := time.Now().Add(lifecycleHangTimeout)
	for {
		if records, ok := tryReadLokiFailureArchiveRecords(path); ok && len(records) >= want {
			return records
		}
		if time.Now().After(deadline) {
			t.Fatalf("failure archive records at %q did not reach %d", path, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func tryReadLokiFailureArchiveRecords(path string) ([]lokiFailureArchiveRecord, bool) {
	file, err := os.Open(path)
	if err != nil {
		return nil, false
	}
	defer file.Close()

	records := make([]lokiFailureArchiveRecord, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record lokiFailureArchiveRecord
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

func assertLokiFailureArchiveRecord(
	t *testing.T,
	record lokiFailureArchiveRecord,
	wantFailureCategory string,
	wantStatus int,
	wantFailureCode string,
	wantLevel string,
	wantMessage string,
) {
	t.Helper()

	assert.Empty(t, record.Type, "failure archive JSON must not contain the removed type discriminator")
	assert.Equal(t, wantFailureCategory, record.FailureCategory)
	assert.Equal(t, deliveryBackendLoki, record.Backend)
	assert.Equal(t, wantStatus, record.StatusCode)
	assert.Equal(t, wantFailureCode, record.FailureCode)

	doc := record.Payload
	assert.Equal(t, strings.ToUpper(wantLevel), doc["level"])
	assert.Equal(t, wantMessage, doc["message"])
	assert.Contains(t, doc["line"], wantMessage)
	assert.Equal(t, "2026-06-23T12:34:56Z", doc["timestamp"])

	labels, ok := doc["labels"].(map[string]any)
	require.True(t, ok, "labels type = %T", doc["labels"])
	assert.Equal(t, "runtime-app", labels["appName"])
	assert.Equal(t, wantLevel, labels["loglevel"])
	assert.Equal(t, "prd", labels["env"])
}
