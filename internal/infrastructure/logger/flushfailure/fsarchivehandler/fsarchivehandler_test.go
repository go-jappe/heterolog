package fsarchivehandler

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	commoncheckpoint "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	loggerhandler "github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	infraobjectstorage "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage"
	objectstoragefailurearchive "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

type archiveFixedClock struct {
	now time.Time
}

func (c archiveFixedClock) Now() time.Time {
	return c.now
}

type testArchiveOverloadConfig struct {
	overloadPolicy        durable.OverloadPolicy
	spillDestination      durable.SpillDestination
	minAvailableDiskSpace uint64
}

func (c testArchiveOverloadConfig) OverloadPolicy() durable.OverloadPolicy {
	return c.overloadPolicy
}

func (c testArchiveOverloadConfig) SpillDestination() durable.SpillDestination {
	return c.spillDestination
}

func (c testArchiveOverloadConfig) MinAvailableDiskSpace() uint64 {
	return c.minAvailableDiskSpace
}

type testArchiveConfig struct {
	target                     string
	lifecycleMode              pkgfsstore.FileLifecycleMode
	archivePath                string
	dir                        string
	dirCaseRetryable           string
	dirCasePermanent           string
	fileNameFormat             string
	fileDateFormat             string
	enableRotate               bool
	maxFileSize                uint64
	maxDirSize                 uint64
	maxFileAge                 int
	maxFileBackups             int
	enableFileCompress         bool
	compressFormat             pkgfsstore.CompressFormat
	compressionCodec           pkgfsstore.CompressionCodec
	rotationType               pkgfsstore.FileRotationType
	rotationDayOfWeek          time.Weekday
	maxWriteAttempts           int
	fileOperationRetryWaitTime time.Duration
	archiveCheckpoint          pkgfsstore.ArchiveCheckpointConfig
	lifecycleRunnerConfig      pkgfsstore.AppendLogRunnerConfig
	overload                   testArchiveOverloadConfig
	isDebug                    bool
	errorTraceDepth            int
	errorTraceDepthDebugMode   int
	objectStorageUpload        objectstorage.UploadTargetConfig
	// spillStdoutは、degrade policyのstdout退避をos.Stdoutの代わりに受け取るwriterです。
	spillStdout io.Writer
	// tracerは構築時に渡すtracerです。nilならobserver.NopTracerを使います。
	tracer observer.Tracer
	// retryableUploader/permanentUploaderは構築時に渡す試験用の宛先付きuploaderです。
	// 本番と同じく構築時にcheckpoint Storeを用意し、宛先を登録します。
	retryableUploader uploader.Uploader
	permanentUploader uploader.Uploader
	// retryableCheckpointStore/permanentCheckpointStoreは、checkpoint設定から本番と同じ手順で
	// 作ったStoreを包む関数です。障害注入や保存内容の確認に使い、保存先・source・宛先の登録と
	// 起動時の世代番号の決定は本番と同じcheckpointを使います。
	retryableCheckpointStore func(commoncheckpoint.Store) commoncheckpoint.Store
	permanentCheckpointStore func(commoncheckpoint.Store) commoncheckpoint.Store
}

// setArchiveUploaderは、failure categoryに対応する構築時のuploaderを設定します。
// 宛先を持たないuploaderは一つの宛先へ送る宛先付きuploaderとして渡します。
func (c *testArchiveConfig) setArchiveUploader(
	failureCategory failurearchive.FailureCategory,
	archiveUploader uploader.Uploader,
) {
	archiveUploader = destinationArchiveUploaderForTest(archiveUploader)
	if failureCategory == failurearchive.FailureCategoryPermanent {
		c.permanentUploader = archiveUploader
		return
	}
	c.retryableUploader = archiveUploader
}

// wrapArchiveCheckpointStoreは、failure categoryの構築時にcheckpoint設定から作ったStoreを包みます。
// uploaderがない系列ではStoreを作らないため、wrapはnilを受け取ります。
func (c *testArchiveConfig) wrapArchiveCheckpointStore(
	failureCategory failurearchive.FailureCategory,
	wrap func(commoncheckpoint.Store) commoncheckpoint.Store,
) {
	if failureCategory == failurearchive.FailureCategoryPermanent {
		c.permanentCheckpointStore = wrap
		return
	}
	c.retryableCheckpointStore = wrap
}

// embedArchiveCheckpointStoreは、checkpoint設定から作ったStoreをinnerへ設定し、wrapperを使わせます。
// innerにはwrapperが埋め込むStoreのfieldを渡します。
func (c *testArchiveConfig) embedArchiveCheckpointStore(
	failureCategory failurearchive.FailureCategory,
	inner *commoncheckpoint.Store,
	wrapper commoncheckpoint.Store,
) {
	c.wrapArchiveCheckpointStore(failureCategory, func(production commoncheckpoint.Store) commoncheckpoint.Store {
		*inner = production
		return wrapper
	})
}

func (c testArchiveConfig) archiveUploaderOverride(
	failureCategory failurearchive.FailureCategory,
) (uploader.Uploader, func(commoncheckpoint.Store) commoncheckpoint.Store, bool) {
	switch failureCategory {
	case failurearchive.FailureCategoryRetryable:
		return c.retryableUploader, c.retryableCheckpointStore,
			c.retryableUploader != nil || c.retryableCheckpointStore != nil
	case failurearchive.FailureCategoryPermanent:
		return c.permanentUploader, c.permanentCheckpointStore,
			c.permanentUploader != nil || c.permanentCheckpointStore != nil
	default:
		return nil, nil, false
	}
}

// testArchiveDestinationIDは、宛先を持たない試験用uploaderへ割り当てる一つの宛先です。
const testArchiveDestinationID objectstorage.DestinationID = "destination"

// singleDestinationArchiveUploaderは、宛先を持たない試験用uploaderを、一つの宛先へ送る
// 本番と同じ宛先付きuploaderとして扱います。各宛先の結果は委譲先の一回のUploadから決めます。
type singleDestinationArchiveUploader struct {
	uploader.Uploader
}

func destinationArchiveUploaderForTest(archiveUploader uploader.Uploader) uploader.Uploader {
	if archiveUploader == nil {
		return nil
	}
	if _, ok := archiveUploader.(uploader.DestinationUploader); ok {
		return archiveUploader
	}
	return singleDestinationArchiveUploader{Uploader: archiveUploader}
}

func (singleDestinationArchiveUploader) DestinationIDs() []objectstorage.DestinationID {
	return []objectstorage.DestinationID{testArchiveDestinationID}
}

func (u singleDestinationArchiveUploader) UploadDestinations(
	ctx context.Context,
	request uploader.UploadRequest,
	destinationIDs []objectstorage.DestinationID,
) uploader.UploadReport {
	report := uploader.UploadReport{Results: make([]uploader.DestinationResult, 0, len(destinationIDs))}
	if len(destinationIDs) == 0 {
		return report
	}
	err := u.Upload(ctx, request)
	for _, destinationID := range destinationIDs {
		report.Results = append(report.Results, uploader.DestinationResult{DestinationID: destinationID, Err: err})
	}
	return report
}

// archiveUploadCheckpointSourceIDForTestは、構築時に作ったcheckpoint Storeのsource IDです。
func archiveUploadCheckpointSourceIDForTest(
	t *testing.T,
	handler *FsArchiveHandler,
	failureCategory failurearchive.FailureCategory,
) string {
	t.Helper()
	fileName, err := handler.archiveFileNamePattern(failureCategory)
	if err != nil {
		t.Fatalf("resolve failure archive file name: %v", err)
	}
	return commoncheckpoint.ArchiveCheckpointSourceID(
		handler.Config.ArchiveCheckpoint().SourcePrefix,
		string(failureCategory),
		fileName,
	)
}

// canonicalArchivePathForTestは、系列の命名規則に従うinactive世代のpathを返します。
// checkpoint付きのuploadは正規の世代名だけを扱います。保持期限はファイル名の日付で
// 判定するため、periodには試験の時刻から保持期限内の日付を指定してください。
func canonicalArchivePathForTest(
	t *testing.T,
	handler *FsArchiveHandler,
	failureCategory failurearchive.FailureCategory,
	period string,
	index uint64,
) string {
	t.Helper()
	fileName, err := handler.archiveFileNamePattern(failureCategory)
	if err != nil {
		t.Fatalf("resolve failure archive file name: %v", err)
	}
	return filepath.Join(fileName.Dir, fmt.Sprintf(
		"%s%s_%s_%d.%s",
		fileName.Prefix,
		fileName.Target,
		period,
		index,
		fileName.Extension,
	))
}

// advanceArchiveUploadCursorForTestは、pathの世代までを構築時の全宛先へ送信済みとして
// 完了cursorへ記録します。未圧縮fileは送信後も通常の保持・purgeへ残るため、
// purge対象の送信済み世代を用意する場合に使います。
func advanceArchiveUploadCursorForTest(
	t *testing.T,
	handler *FsArchiveHandler,
	failureCategory failurearchive.FailureCategory,
	path string,
) {
	t.Helper()
	fileName, err := handler.archiveFileNamePattern(failureCategory)
	if err != nil {
		t.Fatalf("resolve failure archive file name: %v", err)
	}
	generation, canonical := logpattern.ParseCanonicalGenerationFilePath(path, fileName)
	if !canonical {
		t.Fatalf("archive path %q is not a canonical generation", path)
	}
	store := handler.compressedArchiveUploadCheckpointStore(failureCategory)
	destinationUploader, ok := handler.archiveUploader(failureCategory).(uploader.DestinationUploader)
	if store == nil || !ok {
		t.Fatalf("%s failure archive checkpoint store or destination uploader is unavailable", failureCategory)
	}
	key := commoncheckpoint.CheckpointKey{
		Generation: commoncheckpoint.ArchiveCheckpointGenerationKey(generation.Period, generation.Index),
		Compressed: generation.Kind == logpattern.GenerationFileKindCompressed,
	}
	if _, err := store.AdvanceCompletionCursor(
		context.Background(), key, destinationUploader.DestinationIDs(),
	); err != nil {
		t.Fatalf("advance archive upload cursor to %+v: %v", key, err)
	}
}

const testUsageCacheTTL = time.Hour

func (c testArchiveConfig) Target() string { return c.target }
func (c testArchiveConfig) LifecycleMode() pkgfsstore.FileLifecycleMode {
	return c.lifecycleMode
}
func (c testArchiveConfig) RotationType() pkgfsstore.FileRotationType {
	return c.rotationType
}
func (c testArchiveConfig) RotationDayOfWeek() time.Weekday {
	return c.rotationDayOfWeek
}
func (c testArchiveConfig) FileNameFormat() string { return c.fileNameFormat }
func (c testArchiveConfig) FileDateFormat() string { return c.fileDateFormat }
func (c testArchiveConfig) MaxFileSize() uint64    { return c.maxFileSize }
func (c testArchiveConfig) UsageCacheTTL() time.Duration {
	return testUsageCacheTTL
}
func (c testArchiveConfig) MaxDirSize() uint64  { return c.maxDirSize }
func (c testArchiveConfig) MaxFileAge() int     { return c.maxFileAge }
func (c testArchiveConfig) MaxFileBackups() int { return c.maxFileBackups }
func (c testArchiveConfig) EnableFileCompress() bool {
	return c.enableFileCompress
}
func (c testArchiveConfig) CompressFormat() pkgfsstore.CompressFormat {
	return c.compressFormat
}
func (c testArchiveConfig) CompressionCodec() pkgfsstore.CompressionCodec {
	return c.compressionCodec
}
func (c testArchiveConfig) ArchivePath() string { return c.archivePath }
func (c testArchiveConfig) Dir() string         { return c.dir }
func (c testArchiveConfig) DirCaseRetryable() string {
	return c.dirCaseRetryable
}
func (c testArchiveConfig) DirCasePermanent() string {
	return c.dirCasePermanent
}
func (c testArchiveConfig) EnableRotate() bool { return c.enableRotate }
func (c testArchiveConfig) MaxWriteAttempts() int {
	return c.maxWriteAttempts
}
func (c testArchiveConfig) FileOperationRetryWaitTime() time.Duration {
	return c.fileOperationRetryWaitTime
}
func (c testArchiveConfig) ArchiveCheckpoint() pkgfsstore.ArchiveCheckpointConfig {
	return c.archiveCheckpoint
}
func (c testArchiveConfig) LifecycleRunnerConfig() pkgfsstore.AppendLogRunnerConfig {
	return c.lifecycleRunnerConfig
}
func (c testArchiveConfig) OverloadPolicyConfig() loggerdurable.FileArchiveOverloadPolicyConfigProvider {
	return c.overload
}
func (c testArchiveConfig) ObjectStorageUpload() objectstorage.UploadTargetConfig {
	return c.objectStorageUpload
}
func (c testArchiveConfig) Handler() loggerhandler.ConfigProvider { return nil }
func (c testArchiveConfig) IsDebug() bool                         { return c.isDebug }
func (c testArchiveConfig) ErrorTraceDepth() int                  { return c.errorTraceDepth }
func (c testArchiveConfig) ErrorTraceDepthDebugMode() int {
	return c.errorTraceDepthDebugMode
}

// retryable/permanent の失敗種別が、それぞれ専用の archive JSONL に保存されることを確認する。
// request/resource metadata は SendResult.Meta、失敗内容は FailedRecord から保存する。
func TestArchiveFailuresWritesRetryableAndPermanentJSONL(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 8, 12, 34, 56, 789000000, time.UTC)
	handler := newTestArchiveHandler(t, now, nil)

	retryOccurredAt := now.Add(-2 * time.Hour)
	result := loggerfailure.SendResult{
		Meta: loggerfailure.Meta{
			Backend:         "opensearch",
			RequestID:       "request-1",
			TraceID:         "trace-1",
			AppName:         "meta-app",
			HostName:        "meta-host",
			Environment:     "test",
			RuntimeType:     "local",
			ExecutionUnitID: "unit-1",
			Version:         "v1",
			AttemptedAt:     now,
			InputCount:      2,
		},
		RetryableRecords: []loggerfailure.FailedRecord{
			archiveFailedRecord("retry-id", retryOccurredAt, 429, "es_rejected_execution_exception"),
		},
		DeadRecords: []loggerfailure.FailedRecord{
			archiveFailedRecord("dead-id", time.Time{}, 400, "mapper_parsing_exception"),
		},
	}

	if err := handler.HandleFailure(context.Background(), result); err != nil {
		t.Fatalf("HandleFailure() error = %v", err)
	}

	retryableRecords := readArchiveRecords(t, handler.handler.Fsstore.ActiveFilePathRetryable)
	assertArchiveRecordCount(t, retryableRecords, 1, "retryable")
	assertArchiveRecord(t, retryableRecords[0], archiveRecordFailureCategoryRetryable, retryOccurredAt, "request-1", "meta-app", "meta-host", "retry-id", 429)
	if got := retryableRecords[0]; got.Backend != "opensearch" ||
		got.TraceID != "trace-1" || got.Environment != "test" ||
		got.RuntimeType != "local" || got.ExecutionUnitID != "unit-1" ||
		got.Version != "v1" || got.InputCount != 2 || got.Operation != "index" ||
		got.FailureCode != "es_rejected_execution_exception" || got.FailureReason != "test failure" {
		t.Fatalf("retryable archive metadata = %+v, want complete SendResult/FailedRecord metadata", got)
	}

	permanentRecords := readArchiveRecords(t, handler.handler.Fsstore.ActiveFilePathPermanent)
	assertArchiveRecordCount(t, permanentRecords, 1, "permanent")
	assertArchiveRecord(t, permanentRecords[0], archiveRecordFailureCategoryDead, now, "request-1", "meta-app", "meta-host", "dead-id", 400)
}

func TestHandleFailureDelegatesArchiveWriteError(t *testing.T) {
	t.Parallel()

	var delegatedErr error
	var delegatedEntry *logging.LogEntry
	handler := newTestArchiveHandler(
		t,
		time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC),
		nil,
		InternalLogHandlers{
			OnError: func(
				_ context.Context,
				err error,
				entry *logging.LogEntry,
				_ observer.Tracer,
				_ observer.Meter,
			) {
				delegatedErr = err
				delegatedEntry = entry
			},
		},
	)

	closeRetryableArchiveHandleForWriteFailure(t, handler)
	err := handler.handler.handleFailure(context.Background(), loggerfailure.SendResult{
		RetryableRecords: []loggerfailure.FailedRecord{{Payload: map[string]any{"message": "retry me"}}},
	})
	if err == nil {
		t.Fatal("HandleFailure error = nil, want archive write error")
	}
	if delegatedErr == nil {
		t.Fatal("OnError was not called for archive write error")
	}
	if delegatedEntry == nil || delegatedEntry.Level != pkglogger.LogLevelError {
		t.Fatalf("delegated entry = %#v, want error entry", delegatedEntry)
	}
	// WindowsのStatは閉じたhandleでos.ErrClosedではなくERROR_INVALID_HANDLEを返します。
	if !errors.Is(delegatedErr, ErrFailureArchiveWrite) || !testutil.IsClosedFileError(delegatedErr) {
		t.Fatalf("delegated error = %q, want archive write failure caused by the closed handle", delegatedErr)
	}
}

// 空のarchive対象ではactive fileを参照せず、何も書き込みません。
func TestHandleFailureWithNoRecordsDoesNotRequireActiveFile(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	handler := newTestArchiveHandler(t, now, nil)
	activeFile := handler.handler.Fsstore.FileRetryable
	handler.handler.Fsstore.FileRetryable = nil
	defer func() { handler.handler.Fsstore.FileRetryable = activeFile }()

	if err := handler.HandleFailure(context.Background(), loggerfailure.SendResult{}); err != nil {
		t.Fatalf("HandleFailure() error = %v", err)
	}
}

func TestHandleFailureWritesToCurrentActiveFile(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	handler := newTestArchiveHandler(t, now, nil)
	path := handler.handler.Fsstore.ActiveFilePathRetryable
	result := loggerfailure.SendResult{
		Meta: loggerfailure.Meta{
			Backend: "opensearch", RequestID: "request-1", AttemptedAt: now,
		},
		RetryableRecords: []loggerfailure.FailedRecord{
			archiveFailedRecord("doc-id", time.Time{}, 429, "es_rejected_execution_exception"),
		},
	}

	if err := handler.HandleFailure(context.Background(), result); err != nil {
		t.Fatalf("HandleFailure() error = %v", err)
	}

	records := readArchiveRecords(t, path)
	assertArchiveRecordCount(t, records, 1, "custom path")
	assertArchiveRecord(t, records[0], archiveRecordFailureCategoryRetryable, now, "request-1", "", "", "doc-id", 429)
}

func TestNewFailureArchivePipelineBuildsArchiveUploadersFromConfig(t *testing.T) {
	now := time.Date(2026, 7, 12, 9, 30, 0, 0, time.UTC)
	archiveHandler := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.archiveCheckpoint = pkgfsstore.ArchiveCheckpointConfig{
			Directory:    t.TempDir(),
			SourcePrefix: "failure_archive_test",
		}
		cfg.objectStorageUpload = objectstorage.UploadTargetConfig{
			Enabled: true,
			Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
			S3: objectstorage.S3Config{
				Region:          "us-east-1",
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
				Bucket:          "failure-archive",
				Prefix:          "backend",
			},
		}
	})

	if archiveHandler.handler.retryableArchiveUploader == nil {
		t.Fatal("retryable archive uploader is nil")
	}
	if archiveHandler.handler.permanentArchiveUploader == nil {
		t.Fatal("permanent archive uploader is nil")
	}
	for _, state := range []*archiveDirectoryRuntime{
		&archiveHandler.handler.retryableDirectory,
		&archiveHandler.handler.permanentDirectory,
	} {
		if state.uploadCheckpoints == nil {
			t.Fatal("uploader-enabled archive series has no compressed log file checkpoint store")
		}
		if !state.uploadRetry.due(now, archiveHandler.handler.Config.UsageCacheTTL()) {
			t.Fatal("uploader-enabled archive series did not receive restart bootstrap schedule")
		}
	}
}

// HandleFailure の size rotation から background archive upload まで到達することを確認する。
func TestHandleFailureRotationDispatchesArchiveUpload(t *testing.T) {
	now := time.Now()
	recorder := newRecordingFailureArchiveUploader(nil, nil)
	archiveHandler := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.enableRotate = true
		cfg.enableFileCompress = false
		cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, recorder)
	})

	oldPath := archiveHandler.handler.Fsstore.ActiveFilePathRetryable
	if _, err := archiveHandler.handler.Fsstore.FileRetryable.Write(make([]byte, infrafsstore.MinAllowedFileSizeBytes+1)); err != nil {
		t.Fatalf("grow active retryable archive: %v", err)
	}
	if err := archiveHandler.handler.handleFailure(context.Background(), loggerfailure.SendResult{}); err != nil {
		t.Fatalf("handleFailure: %v", err)
	}

	if got := receiveArchiveUploadInput(t, recorder).FilePath(); got != oldPath {
		t.Fatalf("rotated upload path = %q, want %q", got, oldPath)
	}
	if archiveHandler.handler.Fsstore.ActiveFilePathRetryable == oldPath {
		t.Fatalf("active retryable path was not rotated: %q", oldPath)
	}
}

// retryable/permanent は別 uploader に配送し、圧縮無効時は元の JSONL を upload する。
func TestRotatedArchiveUploadsUncompressedFileByFailureCategory(t *testing.T) {
	now := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	retryableUploader := newRecordingFailureArchiveUploader(nil, nil)
	permanentUploader := newRecordingFailureArchiveUploader(nil, nil)
	archiveHandler := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, retryableUploader)
		cfg.setArchiveUploader(failurearchive.FailureCategoryPermanent, permanentUploader)
	})

	retryablePath := filepath.Join(filepath.Dir(archiveHandler.handler.Fsstore.ActiveFilePathRetryable), "test-app_20260711_1.jsonl")
	permanentPath := filepath.Join(filepath.Dir(archiveHandler.handler.Fsstore.ActiveFilePathPermanent), "test-app_20260711_1.jsonl")
	if err := os.WriteFile(retryablePath, []byte("retryable"), 0o600); err != nil {
		t.Fatalf("write retryable archive: %v", err)
	}
	if err := os.WriteFile(permanentPath, []byte("permanent"), 0o600); err != nil {
		t.Fatalf("write permanent archive: %v", err)
	}

	dispatchRotatedArchiveAndWait(t, archiveHandler.handler, archiveHandler.handler.Fsstore.ActiveFilePathRetryable, retryablePath, failurearchive.FailureCategoryRetryable)
	dispatchRotatedArchiveAndWait(t, archiveHandler.handler, archiveHandler.handler.Fsstore.ActiveFilePathPermanent, permanentPath, failurearchive.FailureCategoryPermanent)
	if got := receiveArchiveUploadInput(t, retryableUploader).FilePath(); got != retryablePath {
		t.Fatalf("retryable upload path = %q, want %q", got, retryablePath)
	}
	if got := receiveArchiveUploadInput(t, permanentUploader).FilePath(); got != permanentPath {
		t.Fatalf("permanent upload path = %q, want %q", got, permanentPath)
	}
	if _, err := os.Stat(retryablePath); err != nil {
		t.Fatalf("uncompressed retryable file should remain locally: %v", err)
	}
	if _, err := os.Stat(permanentPath); err != nil {
		t.Fatalf("uncompressed permanent file should remain locally: %v", err)
	}
}

// 圧縮有効時は元 JSONL ではなく、圧縮完了後のファイルを upload する。
func TestRotatedArchiveUploadsCompressedFile(t *testing.T) {
	now := time.Date(2026, 7, 12, 10, 30, 0, 0, time.UTC)
	recorder := newRecordingFailureArchiveUploader(nil, nil)
	archiveHandler := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.enableFileCompress = true
		cfg.compressFormat = pkgfsstore.CompressGzip
		cfg.setArchiveUploader(failurearchive.FailureCategoryPermanent, recorder)
	})

	oldPath := filepath.Join(filepath.Dir(archiveHandler.handler.Fsstore.ActiveFilePathPermanent), "test-app_20260711_1.jsonl")
	if err := os.WriteFile(oldPath, []byte("permanent"), 0o600); err != nil {
		t.Fatalf("write archive: %v", err)
	}
	directoryTracker := archiveHandler.handler.directorySizeTracker(
		failurearchive.FailureCategoryPermanent,
	)
	if !directoryTracker.TryStartObservation(now, archiveHandler.handler.Config, true) {
		t.Fatal("failed to initialize permanent archive directory estimate")
	}
	directoryTracker.FinishObservation(uint64(len("permanent")))
	// 当日のage purgeを済ませ、概算の無効化だけが後続cleanupの観測理由になるようにします。
	archiveHandler.handler.permanentDirectory.lastAgePurgeAt = now
	observations := countArchiveDirectoryObservations(archiveHandler.handler)

	dispatchRotatedArchiveAndWait(t, archiveHandler.handler, archiveHandler.handler.Fsstore.ActiveFilePathPermanent, oldPath, failurearchive.FailureCategoryPermanent)

	compressedPath := oldPath + pkgfsstore.ExtensionCompressGzip
	if got := receiveArchiveUploadInput(t, recorder).FilePath(); got != compressedPath {
		t.Fatalf("compressed upload path = %q, want %q", got, compressedPath)
	}
	if _, err := os.Stat(oldPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("original file stat error = %v, want not exist", err)
	}
	if _, err := os.Stat(compressedPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("uploaded compressed file stat error = %v, want not exist", err)
	}
	// compressionとupload後削除は個別byte差分を追跡せず、rotation taskの完了時に
	// 概算を無効化します。同じtaskの後続cleanupがTTL内でも全体を観測し直します。
	// 観測を一回にまとめることはTestRotationTaskCoalescesInvalidationAndDefersFailedUploadRetryで確認します。
	if got := observations(); got == 0 {
		t.Fatal("cleanup after compressed upload did not observe the directory after invalidating the estimate")
	}
}

// 圧縮失敗時は不完全な archive を upload せず、元 JSONL をローカルに残す。
func TestRotatedArchiveSkipsUploadWhenCompressionFails(t *testing.T) {
	now := time.Date(2026, 7, 12, 11, 0, 0, 0, time.UTC)
	recorder := newRecordingFailureArchiveUploader(nil, nil)
	archiveHandler := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.enableFileCompress = true
		cfg.compressFormat = pkgfsstore.CompressGzip
		cfg.compressionCodec = failingFailureArchiveCompressor{
			format: pkgfsstore.CompressGzip,
			err:    errors.New("compress failed"),
		}
		cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, recorder)
	})

	oldPath := filepath.Join(filepath.Dir(archiveHandler.handler.Fsstore.ActiveFilePathRetryable), "test-app_20260711_1.jsonl")
	if err := os.WriteFile(oldPath, []byte("retryable"), 0o600); err != nil {
		t.Fatalf("write archive: %v", err)
	}

	dispatchRotatedArchiveAndWait(t, archiveHandler.handler, archiveHandler.handler.Fsstore.ActiveFilePathRetryable, oldPath, failurearchive.FailureCategoryRetryable)

	select {
	case input := <-recorder.inputs:
		t.Fatalf("unexpected upload after compression failure: %+v", input)
	default:
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("source file should remain after compression failure: %v", err)
	}
}

// FileLogger と同様、upload 失敗はローカル durable file を削除せず、failure handling 自体も失敗へ変換しない。
// 未圧縮fileはupload成功後も保持するため、失敗後も同じrawを残し、受理済みの再開処理で再送する。
func TestRotatedArchiveUploadFailureKeepsLocalArchive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, 7, 12, 11, 15, 0, 0, time.UTC)
		remote := newDestinationFailureArchiveUploader(testArchiveDestinationID)
		remote.failNext(testArchiveDestinationID, 1)
		archiveHandler := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, remote)
		})

		oldPath := filepath.Join(filepath.Dir(archiveHandler.handler.Fsstore.ActiveFilePathRetryable), "test-app_20260711_1.jsonl")
		if err := os.WriteFile(oldPath, []byte("retryable"), 0o600); err != nil {
			t.Fatalf("write archive: %v", err)
		}

		dispatchRotatedArchiveAndWait(
			t,
			archiveHandler.handler,
			archiveHandler.handler.Fsstore.ActiveFilePathRetryable,
			oldPath,
			failurearchive.FailureCategoryRetryable,
		)
		if _, err := os.Stat(oldPath); err != nil {
			t.Fatalf("local archive should remain after upload failure: %v", err)
		}
		requireFailedArchiveUploadRetriedByRecovery(t, archiveHandler.handler, remote, oldPath)
		if _, err := os.Stat(oldPath); err != nil {
			t.Fatalf("uncompressed archive should remain after retried upload: %v", err)
		}
	})
}

// 圧縮済みarchiveのuploadに失敗した場合はlocal copyを削除せず、受理済みの再開処理で再送し、
// 送信の完了後に削除する。
func TestRotatedArchiveUploadFailureKeepsCompressedArchive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, 7, 12, 11, 20, 0, 0, time.UTC)
		remote := newDestinationFailureArchiveUploader(testArchiveDestinationID)
		remote.failNext(testArchiveDestinationID, 1)
		archiveHandler := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.enableFileCompress = true
			cfg.compressFormat = pkgfsstore.CompressGzip
			cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, remote)
		})

		oldPath := filepath.Join(
			filepath.Dir(archiveHandler.handler.Fsstore.ActiveFilePathRetryable),
			"test-app_20260711_1.jsonl",
		)
		if err := os.WriteFile(oldPath, []byte("retryable"), 0o600); err != nil {
			t.Fatalf("write archive: %v", err)
		}

		dispatchRotatedArchiveAndWait(
			t,
			archiveHandler.handler,
			archiveHandler.handler.Fsstore.ActiveFilePathRetryable,
			oldPath,
			failurearchive.FailureCategoryRetryable,
		)

		compressedPath := oldPath + pkgfsstore.ExtensionCompressGzip
		if _, err := os.Stat(oldPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("compressed source stat error = %v, want not exist", err)
		}
		if _, err := os.Stat(compressedPath); err != nil {
			t.Fatalf("compressed archive should remain after upload failure: %v", err)
		}
		requireFailedArchiveUploadRetriedByRecovery(t, archiveHandler.handler, remote, compressedPath)
		if _, err := os.Stat(compressedPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("retried compressed archive stat error = %v, want not exist", err)
		}
	})
}

// requireFailedArchiveUploadRetriedByRecoveryは、送信に一回失敗したpathをcleanupが直ちに再送せず、
// 受理済みの再開処理が再開間隔の経過時に一回だけ再送することを確認します。synctestのbubble内で
// 失敗の直後に呼んでください。
func requireFailedArchiveUploadRetriedByRecovery(
	t *testing.T,
	handler *FsArchiveHandler,
	remote *destinationFailureArchiveUploader,
	path string,
) {
	t.Helper()
	if got := remote.attemptCount(path, testArchiveDestinationID); got != 1 {
		t.Fatalf("upload attempts after failure = %d, want 1", got)
	}
	// 失敗直後の書き込みに伴うcleanupは、同じfileへ再送を繰り返しません。
	if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
		t.Fatalf("cleanup after upload failure: %v", err)
	}
	time.Sleep(logfile.DefaultLogFileProcessingRecoveryInterval - time.Nanosecond)
	synctest.Wait()
	if got := remote.attemptCount(path, testArchiveDestinationID); got != 1 {
		t.Fatalf("upload attempts before the recovery interval = %d, want 1", got)
	}
	time.Sleep(time.Nanosecond)
	synctest.Wait()
	if got := remote.attemptCount(path, testArchiveDestinationID); got != 2 {
		t.Fatalf("upload attempts after the recovery interval = %d, want 2", got)
	}
}

func TestFailureArchiveObjectPrefixSeparatesRetryableAndPermanent(t *testing.T) {
	retryablePrefix := objectstoragefailurearchive.ObjectStoragePrefix(
		"backend/failures",
		failurearchive.FailureCategoryRetryable,
	)
	permanentPrefix := objectstoragefailurearchive.ObjectStoragePrefix(
		"backend/failures",
		failurearchive.FailureCategoryPermanent,
	)
	if retryablePrefix != "backend/failures/retryable" {
		t.Fatalf("retryable prefix = %q", retryablePrefix)
	}
	if permanentPrefix != "backend/failures/permanent" {
		t.Fatalf("permanent prefix = %q", permanentPrefix)
	}
	if retryablePrefix == permanentPrefix {
		t.Fatal("retryable and permanent prefixes must differ")
	}

	filePath := filepath.Join(t.TempDir(), "test-app_20260712_1.jsonl.gz")
	retryableKey := infraobjectstorage.BuildObjectKey(retryablePrefix, filePath)
	permanentKey := infraobjectstorage.BuildObjectKey(permanentPrefix, filePath)
	if retryableKey != "backend/failures/retryable/test-app_20260712_1.jsonl.gz" {
		t.Fatalf("retryable object key = %q", retryableKey)
	}
	if permanentKey != "backend/failures/permanent/test-app_20260712_1.jsonl.gz" {
		t.Fatalf("permanent object key = %q", permanentKey)
	}
}

func TestShutdownWaitsForAcceptedFailureHandlingBeforeClosingWriter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, time.August, 30, 14, 0, 0, 0, time.UTC)
		handler := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.overload.minAvailableDiskSpace = ^uint64(0)
		})
		entered := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseHandling := func() { releaseOnce.Do(func() { close(release) }) }
		// 試験が途中で失敗しても、受理済みのHandleFailureを終えてからPipelineを閉じます。
		t.Cleanup(releaseHandling)
		handler.handler.OverloadHandler = reportOverloadHandlerFunc(func(
			context.Context,
			loggerfailure.SendResult,
		) error {
			close(entered)
			<-release
			return nil
		})

		handleDone := make(chan error, 1)
		go func() {
			handleDone <- handler.HandleFailure(
				context.Background(),
				failureArchiveResultForFailureCategory(
					failurearchive.FailureCategoryRetryable,
					"accepted-before-shutdown",
				),
			)
		}()
		select {
		case <-entered:
		case err := <-handleDone:
			t.Fatalf("HandleFailure returned before overload handling: %v", err)
		}

		shutdownDone := make(chan error, 1)
		go func() {
			shutdownDone <- handler.Shutdown(context.Background())
		}()
		synctest.Wait()
		select {
		case err := <-shutdownDone:
			t.Fatalf("Shutdown returned before accepted HandleFailure completed: %v", err)
		default:
		}
		if handler.handler.Fsstore.FileRetryable == nil || handler.handler.Fsstore.FilePermanent == nil {
			t.Fatal("Shutdown closed writer files while an accepted HandleFailure was still running")
		}

		releaseHandling()
		if err := <-handleDone; err != nil {
			t.Fatalf("HandleFailure() error = %v", err)
		}
		if err := <-shutdownDone; err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
	})
}

// 受理済みのHandleFailureが保存を始める前にShutdownが始まっても、Shutdownは受理済み処理の
// 完了を待ってからrotation taskの受付を閉じます。そのため受理済み処理のrotation taskは登録され、
// Shutdownはそのtaskの完了も待ちます。
func TestShutdownWaitsForRotationArchiveTaskAcceptedBeforeShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, time.August, 30, 15, 0, 0, 0, time.UTC)
		releaseUpload := make(chan struct{})
		var releaseUploadOnce sync.Once
		releaseUploads := func() { releaseUploadOnce.Do(func() { close(releaseUpload) }) }
		writeGate := newFailureWriteGateTracer()
		recorder := newRecordingFailureArchiveUploader(releaseUpload, nil)
		pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.lifecycleMode = pkgfsstore.FileLifecycleModeInline
			cfg.enableRotate = true
			cfg.enableFileCompress = true
			cfg.compressFormat = pkgfsstore.CompressGzip
			cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, recorder)
			cfg.tracer = writeGate
		})
		// 試験が途中で失敗しても、止めた保存とuploadを再開してからPipelineを閉じます。
		t.Cleanup(releaseUploads)
		t.Cleanup(writeGate.open)

		oldPath := pipeline.handler.Fsstore.ActiveFilePathRetryable
		compressedPath := oldPath + pkgfsstore.ExtensionCompressGzip
		if _, err := pipeline.handler.Fsstore.FileRetryable.Write(
			make([]byte, infrafsstore.MinAllowedFileSizeBytes+1),
		); err != nil {
			t.Fatalf("grow active retryable archive: %v", err)
		}

		handleDone := make(chan error, 1)
		go func() {
			handleDone <- pipeline.HandleFailure(context.Background(), loggerfailure.SendResult{})
		}()
		select {
		case <-writeGate.entered:
		case err := <-handleDone:
			t.Fatalf("HandleFailure returned before the accepted write started: %v", err)
		}

		// 受理済みのHandleFailureを保存・rotationの前で止めたまま、Shutdownを開始します。
		shutdownDone := make(chan error, 1)
		go func() {
			shutdownDone <- pipeline.Shutdown(context.Background())
		}()
		synctest.Wait()
		select {
		case err := <-shutdownDone:
			t.Fatalf("Shutdown returned before accepted HandleFailure completed: %v", err)
		default:
		}

		writeGate.open()
		input := receiveArchiveUploadInput(t, recorder)
		if input.FilePath() != compressedPath {
			t.Fatalf("archive upload path = %q, want %q", input.FilePath(), compressedPath)
		}
		if err := <-handleDone; err != nil {
			t.Fatalf("HandleFailure() error = %v", err)
		}
		synctest.Wait()
		select {
		case err := <-shutdownDone:
			t.Fatalf("Shutdown returned before accepted rotation archive task completed: %v", err)
		default:
		}

		releaseUploads()
		if err := <-shutdownDone; err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
		// Shutdownはtaskを打ち切らず、upload完了後の圧縮fileの削除まで終えてから戻ります。
		if _, err := os.Stat(compressedPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("uploaded archive stat error = %v, want not exist", err)
		}
	})
}

// failureWriteGateTracerは、最初のfailure保存spanの開始をopenまで止めるtracerです。
// 受理済みのHandleFailureを、保存とrotationの開始前で止めるために使います。
type failureWriteGateTracer struct {
	observer.NopTracer
	entered   chan struct{}
	gate      chan struct{}
	enterOnce sync.Once
	openOnce  sync.Once
}

func newFailureWriteGateTracer() *failureWriteGateTracer {
	return &failureWriteGateTracer{
		entered: make(chan struct{}),
		gate:    make(chan struct{}),
	}
}

func (g *failureWriteGateTracer) StartSpan(
	ctx context.Context,
	name string,
	attrs ...observer.Attr,
) (context.Context, observer.Span) {
	if name == SpanFileWrite {
		first := false
		g.enterOnce.Do(func() { first = true })
		if first {
			close(g.entered)
			<-g.gate
		}
	}
	return g.NopTracer.StartSpan(ctx, name, attrs...)
}

func (g *failureWriteGateTracer) open() {
	g.openOnce.Do(func() { close(g.gate) })
}

func TestFailureArchiveRotationErrorNotifiesBeforeTaskCompletion(t *testing.T) {
	codec := &failureArchiveRecoveryCodec{}
	tests := []struct {
		name      string
		configure func(*testArchiveConfig)
		prepare   func(*FailureArchivePipeline)
		repair    func()
	}{
		{
			name: "compression error",
			configure: func(cfg *testArchiveConfig) {
				cfg.lifecycleMode = pkgfsstore.FileLifecycleModeInline
				cfg.enableFileCompress = true
				cfg.compressFormat = pkgfsstore.CompressGzip
				codec.failCompression.Store(true)
				cfg.compressionCodec = codec
			},
			repair: func() { codec.failCompression.Store(false) },
		},
		{
			name: "upload error",
			configure: func(cfg *testArchiveConfig) {
				cfg.lifecycleMode = pkgfsstore.FileLifecycleModeInline
				cfg.setArchiveUploader(
					failurearchive.FailureCategoryRetryable,
					&sequencedFailureArchiveUploader{failuresRemaining: 1},
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pipeline := newTestArchiveHandler(
					t,
					time.Date(2026, 8, 30, 18, 0, 0, 0, time.UTC),
					tt.configure,
				)
				if tt.prepare != nil {
					tt.prepare(pipeline)
				}

				notified := make(chan struct{})
				var notifyOnce sync.Once
				pipeline.handler.OnError = func(
					context.Context,
					error,
					*logging.LogEntry,
					observer.Tracer,
					observer.Meter,
				) {
					notifyOnce.Do(func() {
						pipeline.handler.archiveTaskMu.Lock()
						archiveTaskCount := pipeline.handler.archiveTaskCount
						pipeline.handler.archiveTaskMu.Unlock()
						if archiveTaskCount != 1 {
							t.Errorf("archive task count during notification = %d, want 1", archiveTaskCount)
						}
						ctx, cancel := context.WithTimeout(context.Background(), time.Second)
						defer cancel()
						if err := pipeline.handler.retryableDirectory.guard.RunWithLifecycleLockContext(ctx, func() error { return nil }); err != nil {
							t.Errorf("rotation OnError called before releasing the lifecycle lock: %v", err)
						}
						if tt.repair != nil {
							tt.repair()
						}
						close(notified)
					})
				}

				oldPath := canonicalArchivePathForTest(
					t,
					pipeline.handler,
					failurearchive.FailureCategoryRetryable,
					"20260829",
					1,
				)
				if err := os.WriteFile(oldPath, []byte("retryable"), 0o600); err != nil {
					t.Fatalf("write rotated archive: %v", err)
				}
				pipeline.handler.dispatchArchiveRotatedFile(
					context.Background(),
					pipeline.handler.Fsstore.ActiveFilePathRetryable,
					oldPath,
					failurearchive.FailureCategoryRetryable,
				)

				select {
				case <-notified:
				case <-time.After(10 * time.Second):
					t.Fatal("rotation OnError was not called")
				}
				if err := pipeline.Shutdown(context.Background()); err != nil {
					t.Fatalf("caller Shutdown did not finish accepted work: %v", err)
				}

				pipeline.handler.archiveTaskMu.Lock()
				archiveTaskCount := pipeline.handler.archiveTaskCount
				pipeline.handler.archiveTaskMu.Unlock()
				if archiveTaskCount != 0 {
					t.Fatalf("archive task count after Shutdown = %d, want 0", archiveTaskCount)
				}
			})
		})
	}
}

func TestFailureArchiveRotationTaskPanicKeepsOwnershipUntilForced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const panicValue = "rotation compression panic"
		pipeline := newTestArchiveHandler(
			t,
			time.Date(2026, 8, 30, 18, 30, 0, 0, time.UTC),
			func(cfg *testArchiveConfig) {
				cfg.lifecycleMode = pkgfsstore.FileLifecycleModeInline
				cfg.enableFileCompress = true
				cfg.compressFormat = pkgfsstore.CompressGzip
				cfg.compressionCodec = panicFailureArchiveCompressionCodec{
					format:     pkgfsstore.CompressGzip,
					panicValue: panicValue,
				}
			},
		)
		panicNotification := make(chan error, 2)
		pipeline.handler.OnError = func(
			_ context.Context,
			err error,
			_ *logging.LogEntry,
			_ observer.Tracer,
			_ observer.Meter,
		) {
			var internalErr *FailureArchiveInternalError
			if errors.As(err, &internalErr) && internalErr.Op == OperationRotationArchive {
				select {
				case panicNotification <- err:
				default:
				}
			}
		}

		oldPath := filepath.Join(
			filepath.Dir(pipeline.handler.Fsstore.ActiveFilePathRetryable),
			"rotation-panic.jsonl",
		)
		if err := os.WriteFile(oldPath, []byte("retryable"), 0o600); err != nil {
			t.Fatalf("write rotated archive: %v", err)
		}
		pipeline.handler.dispatchArchiveRotatedFile(
			context.Background(),
			pipeline.handler.Fsstore.ActiveFilePathRetryable,
			oldPath,
			failurearchive.FailureCategoryRetryable,
		)

		select {
		case err := <-panicNotification:
			var panicErr *ArchiveTaskPanicError
			if !errors.As(err, &panicErr) || panicErr.Value != panicValue {
				t.Fatalf("panic notification error = %v, want recovered value %q", err, panicValue)
			}
		case <-time.After(time.Second):
			t.Fatal("rotation task panic was not recovered and reported")
		}
		// panic通知は後片付けより先に届くため、taskの終了を別に待ちます。
		synctest.Wait()

		pipeline.handler.archiveTaskMu.Lock()
		archiveTaskCount := pipeline.handler.archiveTaskCount
		pipeline.handler.archiveTaskMu.Unlock()
		if archiveTaskCount != 0 {
			t.Fatalf("archive task count after panic = %d, want 0", archiveTaskCount)
		}
		if got := pipeline.handler.archiveFileInfosExcludingProtected(
			[]infrafsstore.FileInfo{{Path: oldPath}},
			failurearchive.FailureCategoryRetryable,
		); len(got) != 0 {
			t.Fatalf("panic lost accepted rotated-path protection: %v", got)
		}
		closed := make(chan error, 1)
		go func() { closed <- pipeline.Shutdown(context.Background()) }()
		synctest.Wait()
		select {
		case err := <-closed:
			t.Fatalf("Shutdown() abandoned a pending panic retry: %v", err)
		default:
		}
		if err := pipeline.ForceClose(); !errors.Is(err, shutdown.ErrForcedClose) {
			t.Fatalf("ForceClose() = %v", err)
		}
		if err := <-closed; !errors.Is(err, shutdown.ErrForcedClose) {
			t.Fatalf("Shutdown() after force = %v", err)
		}
		<-pipeline.ShutdownDone()
		if got := pipeline.handler.archiveFileInfosExcludingProtected(
			[]infrafsstore.FileInfo{{Path: oldPath}}, failurearchive.FailureCategoryRetryable,
		); len(got) != 1 {
			t.Fatalf("forced completion retained rotated-path protection: %v", got)
		}
	})
}

// upload中のraw/compressed pathはDirectoryGuardにretainされ、同期cleanupが
// 並行してもpurge候補へ渡されません。
func TestArchiveUploadInFlightIsExcludedFromPurgeCandidates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, 7, 12, 11, 30, 0, 0, time.UTC)
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseUpload := func() { releaseOnce.Do(func() { close(release) }) }
		done := make(chan struct{})
		recorder := newRecordingFailureArchiveUploader(release, done)
		archiveHandler := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, recorder)
		})
		// 試験が途中で失敗しても、止めたuploadを再開してからPipelineを閉じます。
		t.Cleanup(releaseUpload)

		oldPath := canonicalArchivePathForTest(t, archiveHandler.handler, failurearchive.FailureCategoryRetryable, "20260711", 1)
		if err := os.WriteFile(oldPath, []byte("retryable"), 0o600); err != nil {
			t.Fatalf("write archive: %v", err)
		}
		archiveHandler.handler.dispatchArchiveRotatedFile(context.Background(), archiveHandler.handler.Fsstore.ActiveFilePathRetryable, oldPath, failurearchive.FailureCategoryRetryable)
		_ = receiveArchiveUploadInput(t, recorder)

		candidates := []infrafsstore.FileInfo{{Path: oldPath}}
		if got := archiveHandler.handler.archiveFileInfosExcludingProtected(
			candidates,
			failurearchive.FailureCategoryRetryable,
		); len(got) != 0 {
			t.Fatalf("retained purge candidates = %v, want empty", got)
		}

		releaseUpload()
		select {
		case <-done:
		case <-time.After(failureArchiveHangTimeout):
			t.Fatal("timed out waiting for archive upload completion")
		}

		// rotation taskの完了処理が保護を解除した後は、purge候補へ戻ります。
		synctest.Wait()
		if got := archiveHandler.handler.archiveFileInfosExcludingProtected(
			candidates,
			failurearchive.FailureCategoryRetryable,
		); len(got) != 1 {
			t.Fatalf("purge candidates after archive upload completion = %v, want %v", got, candidates)
		}
	})
}

// Shutdownは受理済みのrotation taskのuploadが終わるまで待ち、その間の新しい保存要求を
// closing errorで拒否します。active fileはtaskの完了後に閉じます。
func TestShutdownWaitsForAcceptedArchiveUploadAndRejectsNewRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseUpload := func() { releaseOnce.Do(func() { close(release) }) }
		t.Cleanup(releaseUpload)
		recorder := newRecordingFailureArchiveUploader(release, nil)
		archiveHandler := newTestArchiveHandler(t, time.Date(2026, 7, 12, 11, 40, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, recorder)
		})

		oldPath := canonicalArchivePathForTest(t, archiveHandler.handler, failurearchive.FailureCategoryRetryable, "20260711", 1)
		if err := os.WriteFile(oldPath, []byte("retryable"), 0o600); err != nil {
			t.Fatalf("write archive: %v", err)
		}
		archiveHandler.handler.dispatchArchiveRotatedFile(
			context.Background(),
			archiveHandler.handler.Fsstore.ActiveFilePathRetryable,
			oldPath,
			failurearchive.FailureCategoryRetryable,
		)
		_ = receiveArchiveUploadInput(t, recorder)

		shutdownDone := make(chan error, 1)
		go func() {
			shutdownDone <- archiveHandler.Shutdown(context.Background())
		}()
		// uploadが止まっている間、Shutdownはtaskの完了を待ち続けます。
		synctest.Wait()
		select {
		case err := <-shutdownDone:
			t.Fatalf("Shutdown returned before archive upload completion: %v", err)
		default:
		}
		if err := archiveHandler.HandleFailure(
			context.Background(),
			loggerfailure.SendResult{},
		); !errors.Is(err, ErrFailureArchiveClosing) {
			t.Fatalf("HandleFailure during Shutdown error = %v, want closing error", err)
		}
		report, err := archiveHandler.HandleFailureWithReport(
			context.Background(),
			loggerfailure.SendResult{},
		)
		if !errors.Is(err, ErrFailureArchiveClosing) {
			t.Fatalf("HandleFailureWithReport during Shutdown error = %v, want closing error", err)
		}
		if len(report.Durables) != 1 || report.Durables[0].Status != pkglogger.DurableFailed {
			t.Fatalf("HandleFailureWithReport during Shutdown report = %+v, want one failed durable", report)
		}

		releaseUpload()
		if err := <-shutdownDone; err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
		if archiveHandler.handler.Fsstore.FileRetryable != nil {
			t.Fatal("retryable active file remains open after Shutdown")
		}
		if archiveHandler.handler.Fsstore.FilePermanent != nil {
			t.Fatal("permanent active file remains open after Shutdown")
		}
	})
}

func TestShutdownCanRetryAfterArchiveUploadTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseUpload := func() { releaseOnce.Do(func() { close(release) }) }
		recorder := newRecordingFailureArchiveUploader(release, nil)
		archiveHandler := newTestArchiveHandler(t, time.Date(2026, 7, 12, 11, 50, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, recorder)
		})
		// 試験が途中で失敗しても、止めたuploadを再開してからPipelineを閉じます。
		t.Cleanup(releaseUpload)

		oldPath := canonicalArchivePathForTest(t, archiveHandler.handler, failurearchive.FailureCategoryRetryable, "20260711", 1)
		if err := os.WriteFile(oldPath, []byte("retryable"), 0o600); err != nil {
			t.Fatalf("write archive: %v", err)
		}
		archiveHandler.handler.dispatchArchiveRotatedFile(
			context.Background(),
			archiveHandler.handler.Fsstore.ActiveFilePathRetryable,
			oldPath,
			failurearchive.FailureCategoryRetryable,
		)
		_ = receiveArchiveUploadInput(t, recorder)

		if err := shutdownFailureArchiveWithDeadlineForTest(t, archiveHandler); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("first Shutdown() error = %v, want DeadlineExceeded", err)
		}
		synctest.Wait()
		if archiveHandler.handler.Fsstore.FileRetryable == nil ||
			archiveHandler.handler.Fsstore.FilePermanent == nil {
			t.Fatal("active files were closed before archive upload completed")
		}
		if archiveHandler.archiveRootLease == nil {
			t.Fatal("archive root lease was released while an archive upload was still running")
		}

		releaseUpload()
		if err := archiveHandler.Shutdown(context.Background()); err != nil {
			t.Fatalf("retry Shutdown() error = %v", err)
		}
		if archiveHandler.handler.Fsstore.FileRetryable != nil ||
			archiveHandler.handler.Fsstore.FilePermanent != nil {
			t.Fatal("active files remain open after retry Shutdown")
		}
		if archiveHandler.archiveRootLease != nil {
			t.Fatal("archive root lease remains after successful Shutdown retry")
		}
	})
}

func TestFailureArchiveShutdownIsIdempotent(t *testing.T) {
	handler := newTestArchiveHandler(t, time.Now(), nil)

	if err := shutdownFailureArchiveForTest(handler); err != nil {
		t.Fatalf("first Shutdown() error = %v", err)
	}
	if err := shutdownFailureArchiveForTest(handler); err != nil {
		t.Fatalf("second Shutdown() error = %v", err)
	}
	if handler.shutdownState != shutdownStateCompleted {
		t.Fatalf("shutdownState = %d, want Completed after repeated Shutdown", handler.shutdownState)
	}
	if handler.handler.Fsstore.FileRetryable != nil || handler.handler.Fsstore.FilePermanent != nil {
		t.Fatal("active failure archive file remains after repeated Shutdown")
	}
	if handler.archiveRootLease != nil {
		t.Fatal("archive root lease remains after repeated Shutdown")
	}
}

func TestShutdownReleasesActiveProtectionAfterFileCloseError(t *testing.T) {
	archiveHandler := newTestArchiveHandler(t, time.Now(), nil)
	retryablePath := archiveHandler.handler.Fsstore.ActiveFilePathRetryable
	if err := archiveHandler.handler.Fsstore.FileRetryable.Close(); err != nil {
		t.Fatalf("pre-close retryable archive: %v", err)
	}

	if err := shutdownFailureArchiveForTest(archiveHandler); err == nil {
		t.Fatal("first Shutdown() error = nil, want already-closed file error")
	}
	if archiveHandler.handler.Fsstore.FileRetryable != nil ||
		archiveHandler.handler.Fsstore.FilePermanent != nil {
		t.Fatal("active file pointer remains after Shutdown close attempt")
	}
	if archiveHandler.archiveRootLease != nil {
		t.Fatal("archive root lease remains after all handles became unusable")
	}
	assertArchivePathProtected(
		t,
		archiveHandler.handler.retryableDirectory.guard,
		retryablePath,
		false,
	)

	if err := shutdownFailureArchiveForTest(archiveHandler); err != nil {
		t.Fatalf("retry Shutdown() error = %v, want completed Shutdown", err)
	}
}

// purgeはrotationと同じFileStore lockとDirectoryGuardを使い、active切替中または
// lifecycle処理中の旧fileを削除しません。rotationの切替中に始まったpurgeはlockの解放を待ち、
// 切替の中で保護した旧fileを残し、保護していない期限切れのfileだけを削除します。
func TestInlineCleanupWaitsForRotationAndKeepsProtectedFiles(t *testing.T) {
	now := time.Date(2026, 8, 31, 9, 0, 0, 0, time.UTC)
	archiveHandler := newTestArchiveHandler(t, now, nil)
	directory := filepath.Dir(archiveHandler.handler.Fsstore.ActiveFilePathRetryable)
	oldPath := filepath.Join(directory, "test-app_20260801_1.jsonl")
	expiredPath := filepath.Join(directory, "test-app_20260731_1.jsonl")
	oldTime := now.Add(-72 * time.Hour)
	for _, path := range []string{oldPath, expiredPath} {
		if err := os.WriteFile(path, []byte("retryable"), 0o600); err != nil {
			t.Fatalf("write old archive: %v", err)
		}
		if err := os.Chtimes(path, oldTime, oldTime); err != nil {
			t.Fatalf("age old archive: %v", err)
		}
	}
	if err := os.Chtimes(archiveHandler.handler.Fsstore.ActiveFilePathRetryable, oldTime, oldTime); err != nil {
		t.Fatalf("age active archive: %v", err)
	}

	// writer lockの使用中にpurgeが待機へ入ったことを通知させ、試験が再開させるまで止めます。
	lockWaiting := make(chan struct{})
	resumeLock := make(chan struct{})
	var waitingOnce, resumeOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(resumeLock) }) }
	// 試験が途中で失敗しても、止めたpurgeを再開してから片付けます。
	t.Cleanup(resume)
	archiveHandler.handler.waitArchiveWriterLock = func() {
		waitingOnce.Do(func() { close(lockWaiting) })
		<-resumeLock
	}

	archiveHandler.handler.Fsstore.Mu.Lock()
	locked := true
	defer func() {
		if locked {
			archiveHandler.handler.Fsstore.Mu.Unlock()
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- archiveHandler.handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable)
	}()
	select {
	case <-lockWaiting:
	case err := <-done:
		t.Fatalf("purge completed without waiting for the rotation critical section: %v", err)
	case <-time.After(failureArchiveHangTimeout):
		t.Fatal("purge did not wait for the writer lock held by the rotation critical section")
	}
	select {
	case err := <-done:
		t.Fatalf("purge completed inside rotation critical section: %v", err)
	default:
	}

	protectedPaths := archiveHandler.handler.protectRotatedFileBeforeTransition(
		failurearchive.FailureCategoryRetryable,
		oldPath,
	)
	defer archiveHandler.handler.releaseArchivePathProtection(
		failurearchive.FailureCategoryRetryable,
		protectedPaths,
	)
	archiveHandler.handler.Fsstore.Mu.Unlock()
	locked = false
	resume()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("purge archives: %v", err)
		}
	case <-time.After(failureArchiveHangTimeout):
		t.Fatal("timed out waiting for purge")
	}

	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("retained archive should remain after purge: %v", err)
	}
	if _, err := os.Stat(archiveHandler.handler.Fsstore.ActiveFilePathRetryable); err != nil {
		t.Fatalf("active archive should remain after purge: %v", err)
	}
	if _, err := os.Stat(expiredPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unprotected expired archive stat error = %v, want removed by purge", err)
	}
}

// HandleFailure は archive と cleanup を同期的に完了し、呼び出し直後に保存結果を確認できる。
func TestHandleFailureArchivesAndCleansUpSynchronously(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 21, 9, 0, 0, 0, time.UTC)
	handler := newTestArchiveHandler(t, now, nil)
	// 保持期限を過ぎたrotation済みarchiveは、HandleFailureが戻る前の同期cleanupで削除されます。
	expiredArchives := writeExpiredArchivesForTest(t, handler.handler, now)
	result := loggerfailure.SendResult{
		Meta: loggerfailure.Meta{
			Backend:     "opensearch",
			RequestID:   "request-handle",
			AttemptedAt: now,
			InputCount:  2,
		},
		RetryableRecords: []loggerfailure.FailedRecord{
			archiveFailedRecord("retry-handle", time.Time{}, 429, "es_rejected_execution_exception"),
		},
		DeadRecords: []loggerfailure.FailedRecord{
			archiveFailedRecord("dead-handle", time.Time{}, 400, "mapper_parsing_exception"),
		},
	}

	if err := handler.handler.handleFailure(context.Background(), result); err != nil {
		t.Fatalf("handleFailure returned error: %v", err)
	}

	retryableRecords := readArchiveRecords(t, handler.handler.Fsstore.ActiveFilePathRetryable)
	assertArchiveRecordCount(t, retryableRecords, 1, "retryable")
	assertArchiveRecord(t, retryableRecords[0], archiveRecordFailureCategoryRetryable, now, "request-handle", "", "", "retry-handle", 429)

	permanentRecords := readArchiveRecords(t, handler.handler.Fsstore.ActiveFilePathPermanent)
	assertArchiveRecordCount(t, permanentRecords, 1, "permanent")
	assertArchiveRecord(t, permanentRecords[0], archiveRecordFailureCategoryDead, now, "request-handle", "", "", "dead-handle", 400)

	requireExpiredArchivesRemovedForTest(t, expiredArchives)
}

// writeExpiredArchivesForTestは、各failure categoryのdirectoryへ、nowの時点で最小の保持日数を
// 過ぎたrotation済みarchiveを作り、そのpathを返します。
func writeExpiredArchivesForTest(t *testing.T, handler *FsArchiveHandler, now time.Time) map[failurearchive.FailureCategory]string {
	t.Helper()
	expiredAt := now.AddDate(0, 0, -(infrafsstore.MinAllowedMaxFileAge + 30))
	paths := make(map[failurearchive.FailureCategory]string, 2)
	for _, category := range []failurearchive.FailureCategory{
		failurearchive.FailureCategoryRetryable,
		failurearchive.FailureCategoryPermanent,
	} {
		path := filepath.Join(
			failureArchiveDirectoryForFailureCategory(handler, category),
			"test-app_"+expiredAt.Format("20060102")+"_1.jsonl",
		)
		writeArchiveSizedFile(t, path, 1, expiredAt)
		paths[category] = path
	}
	return paths
}

func requireExpiredArchivesRemovedForTest(t *testing.T, paths map[failurearchive.FailureCategory]string) {
	t.Helper()
	for category, path := range paths {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s expired archive stat error = %v, want removed by the synchronous cleanup", category, err)
		}
	}
}

// fileArchive が空きディスク閾値でoverloadになった場合、設定されたdrop policyを使い、
// active JSONLへ新しいfailure recordを書き込まない。破棄したことはErrLogDroppedで返し、
// 内部エラーとして重ねて通知しない。容量を回復できるよう、同期cleanupは続ける。
func TestHandleFailureDropsArchiveWhenStorageIsOverloaded(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 12, 14, 0, 0, 0, time.UTC)
	var notified []error
	handler := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.overload.overloadPolicy = durable.OverloadPolicy("drop")
		cfg.overload.minAvailableDiskSpace = ^uint64(0)
	}, InternalLogHandlers{OnError: func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
		notified = append(notified, err)
	}})
	expiredArchives := writeExpiredArchivesForTest(t, handler.handler, now)
	result := loggerfailure.SendResult{
		Meta: loggerfailure.Meta{
			Backend:     "opensearch",
			RequestID:   "request-overload-drop",
			AttemptedAt: now,
			InputCount:  1,
		},
		RetryableRecords: []loggerfailure.FailedRecord{
			archiveFailedRecord("retry-overload-drop", time.Time{}, 429, "overloaded"),
		},
	}

	err := handler.handler.handleFailure(context.Background(), result)
	if !errors.Is(err, pkglogger.ErrLogDropped) {
		t.Fatalf("handleFailure error = %v, want ErrLogDropped", err)
	}
	if len(notified) != 0 {
		t.Fatalf("OnError notifications = %v, want none for the configured drop", notified)
	}
	if records := readArchiveRecords(t, handler.handler.Fsstore.ActiveFilePathRetryable); len(records) != 0 {
		t.Fatalf("retryable archive records = %d, want 0 while overloaded", len(records))
	}
	// 記録を破棄した場合も、容量を回復できるよう同期cleanupを行います。
	requireExpiredArchivesRemovedForTest(t, expiredArchives)
}

// HandleFailure は archive 書き込み失敗を caller に返す。
func TestHandleFailureReturnsArchiveFailure(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 21, 9, 30, 0, 0, time.UTC)
	handler := newTestArchiveHandler(t, now, nil)
	closeRetryableArchiveHandleForWriteFailure(t, handler)
	result := loggerfailure.SendResult{
		Meta: loggerfailure.Meta{
			Backend:     "opensearch",
			RequestID:   "request-handle-failed",
			AttemptedAt: now,
			InputCount:  1,
		},
		RetryableRecords: []loggerfailure.FailedRecord{
			archiveFailedRecord("retry-handle-failed", time.Time{}, 429, "es_rejected_execution_exception"),
		},
	}

	if err := handler.handler.handleFailure(context.Background(), result); err == nil {
		t.Fatal("handleFailure error = nil, want archive write error")
	}
}

func closeRetryableArchiveHandleForWriteFailure(t *testing.T, pipeline *FailureArchivePipeline) {
	t.Helper()

	closedFile := pipeline.handler.Fsstore.FileRetryable
	if err := closedFile.Close(); err != nil {
		t.Fatalf("close retryable archive before write: %v", err)
	}
	t.Cleanup(func() {
		if pipeline.handler.Fsstore.FileRetryable != closedFile {
			return
		}
		replacement, err := failurearchive.OpenFile(pipeline.handler.Fsstore.ActiveFilePathRetryable)
		if err != nil {
			t.Errorf("restore retryable archive after write failure: %v", err)
			return
		}
		pipeline.handler.Fsstore.FileRetryable = replacement
	})
}

type recordingFailureArchiveUploader struct {
	inputs   chan uploader.UploadRequest
	release  <-chan struct{}
	done     chan<- struct{}
	doneOnce sync.Once
}

func newRecordingFailureArchiveUploader(release <-chan struct{}, done chan<- struct{}) *recordingFailureArchiveUploader {
	return &recordingFailureArchiveUploader{
		inputs:  make(chan uploader.UploadRequest, 1),
		release: release,
		done:    done,
	}
}

func (u *recordingFailureArchiveUploader) Upload(ctx context.Context, input uploader.UploadRequest) error {
	u.inputs <- input
	if u.release != nil {
		select {
		case <-u.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if u.done != nil {
		// 誤って重複した送信が試験を中断せず、試験の検査で失敗するようにします。
		u.doneOnce.Do(func() { close(u.done) })
	}
	return nil
}

func receiveArchiveUploadInput(t *testing.T, recorder *recordingFailureArchiveUploader) uploader.UploadRequest {
	t.Helper()
	select {
	case input := <-recorder.inputs:
		return input
	case <-time.After(failureArchiveHangTimeout):
		t.Fatal("timed out waiting for archive upload")
		return uploader.UploadRequest{}
	}
}

type failingFailureArchiveCompressor struct {
	format pkgfsstore.CompressFormat
	err    error
}

func (c failingFailureArchiveCompressor) Format() pkgfsstore.CompressFormat {
	return c.format
}

func (c failingFailureArchiveCompressor) Compress(_, _ *os.File) error {
	return c.err
}

func (c failingFailureArchiveCompressor) VerifyCompressedIntegrity(*os.File) error {
	return c.err
}

type failingFailureArchiveUploader struct {
	err error
}

func (u failingFailureArchiveUploader) Upload(context.Context, uploader.UploadRequest) error {
	return u.err
}

// resolvedTempDirForTestは、symlinkを経由する一時directory（macOSの/var→/private/varなど）を
// 実体のpathへ解決して返します。本番は設定したrootの実体pathを使うため、期待値も同じ表記にします。
func resolvedTempDirForTest(t testing.TB) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve temporary directory: %v", err)
	}
	return directory
}

func TestNewFailureArchivePipelineDoesNotDuplicateTargetAlreadyPresentInArchivePath(t *testing.T) {
	t.Parallel()

	archivePath := filepath.Join(resolvedTempDirForTest(t), "test-app")
	if err := os.MkdirAll(archivePath, 0o755); err != nil {
		t.Fatalf("create target-bound archive path: %v", err)
	}
	handler := newTestArchiveHandler(t, time.Date(2026, 7, 12, 15, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
		cfg.archivePath = archivePath
	})

	wantRetryableDir := filepath.Join(archivePath, "backend", "retryable")
	if got := filepath.Dir(handler.handler.Fsstore.ActiveFilePathRetryable); got != wantRetryableDir {
		t.Fatalf("retryable archive dir = %q, want %q", got, wantRetryableDir)
	}
	wantPermanentDir := filepath.Join(archivePath, "backend", "permanent")
	if got := filepath.Dir(handler.handler.Fsstore.ActiveFilePathPermanent); got != wantPermanentDir {
		t.Fatalf("permanent archive dir = %q, want %q", got, wantPermanentDir)
	}
}

func TestNewFailureArchivePipelineRejectsOverlappingArchiveDirectories(t *testing.T) {
	t.Parallel()

	cfg := newValidArchiveHandlerConfig(t)
	cfg.dirCaseRetryable = "failed"
	cfg.dirCasePermanent = "failed/permanent"

	handler, err := NewFailureArchivePipeline(
		context.Background(),
		archiveFixedClock{now: time.Date(2026, 7, 12, 15, 0, 0, 0, time.UTC)},
		cfg,
		observer.NopTracer{},
		observer.NopMeter{},
		InternalLogHandlers{},
	)
	if err == nil {
		if handler != nil {
			if handler.handler != nil && handler.handler.Fsstore != nil && handler.handler.Fsstore.FileRetryable != nil {
				_ = handler.handler.Fsstore.FileRetryable.Close()
			}
			if handler.handler != nil && handler.handler.Fsstore != nil && handler.handler.Fsstore.FilePermanent != nil {
				_ = handler.handler.Fsstore.FilePermanent.Close()
			}
		}
		t.Fatal("NewFailureArchivePipeline returned nil error, want overlapping directory error")
	}
	if !errors.Is(err, failurearchive.ErrArchiveDirectoriesOverlap) {
		t.Fatalf("NewFailureArchivePipeline error = %v, want ErrArchiveDirectoriesOverlap", err)
	}
	if handler != nil {
		t.Fatalf("NewFailureArchivePipeline handler = %#v, want nil", handler)
	}
}

func newTestArchiveHandler(
	t *testing.T,
	now time.Time,
	mutate func(*testArchiveConfig),
	handlerSets ...InternalLogHandlers,
) *FailureArchivePipeline {
	t.Helper()
	if len(handlerSets) > 1 {
		t.Fatalf("internal log handler sets = %d, want at most one", len(handlerSets))
	}
	internalLogHandlers := InternalLogHandlers{}
	if len(handlerSets) == 1 {
		internalLogHandlers = handlerSets[0]
	}

	// 実 FileStore を temp dir 上に作り、命名・open・JSONL 書き込みまで含めて検証する。
	cfg := newValidArchiveHandlerConfig(t)
	if mutate != nil {
		mutate(&cfg)
	}
	if (cfg.retryableUploader != nil || cfg.permanentUploader != nil) &&
		cfg.archiveCheckpoint.Directory == "" && cfg.archiveCheckpoint.SourcePrefix == "" {
		// 本番ではuploaderを使う系列にcheckpointの設定が必須です。試験用uploaderにも
		// 同じ構成を用意し、構築時に作るStoreへ宛先を登録します。
		cfg.archiveCheckpoint = pkgfsstore.ArchiveCheckpointConfig{
			Directory:    t.TempDir(),
			SourcePrefix: "failure_archive_test",
		}
	}
	if cfg.archiveCheckpoint.Directory != "" {
		if err := commoncheckpoint.PrepareDirectory(cfg.archiveCheckpoint.Directory); err != nil {
			t.Fatalf("prepare archive checkpoint directory: %v", err)
		}
	}
	// 共有spoolは呼び出し側で構築し、Pipelineが利用登録を解除した後に閉じます。
	var spoolRuntimes compressedlog.RootRuntimes
	if cfg.lifecycleMode == "" || cfg.lifecycleMode == pkgfsstore.FileLifecycleModeInline || cfg.lifecycleMode == pkgfsstore.FileLifecycleModeRunner {
		uploadCfg := cfg.objectStorageUpload
		configs := compressedlog.NewConfigAdapters(uploadCfg.CompressedLogFileUploadSpool, uploadCfg.CompressedLogFileUploadSpoolServices())
		var err error
		spoolRuntimes, err = compressedlog.NewRootRuntimes(configs...)
		if err != nil {
			t.Fatalf("create compressed log spool runtimes: %v", err)
		}
		t.Cleanup(func() {
			if err := spoolRuntimes.Close(); err != nil {
				t.Errorf("close compressed log spool runtimes: %v", err)
			}
		})
	}

	options := []FailureArchivePipelineOption{WithCompressedLogSpoolRuntimes(spoolRuntimes)}
	if cfg.spillStdout != nil {
		options = append(options, func(options *failureArchivePipelineOptions) {
			options.spillStdout = cfg.spillStdout
		})
	}
	var tracer observer.Tracer = observer.NopTracer{}
	if cfg.tracer != nil {
		tracer = cfg.tracer
	}
	handler, err := NewFailureArchivePipeline(
		context.Background(),
		archiveFixedClock{now: now},
		cfg,
		tracer,
		observer.NopMeter{},
		internalLogHandlers,
		options...,
	)
	if err != nil {
		t.Fatalf("failed to create archive handler: %v", err)
	}
	// 実行環境の空き容量によって記録が過負荷として退避されないよう、固定値を使います。
	useTestArchiveAvailableDiskSpace(handler)

	t.Cleanup(func() {
		// 障害を残す試験も、受付済みの再試行を明示的に止めてから一時directoryを片付けます。
		_ = handler.ForceClose()
		waitFailureArchiveClosedForTest(t, handler)
	})

	return handler
}

// restartTestArchiveHandlerは、以前のPipelineを強制終了してから同じ保存先とcheckpoint設定で
// 構築し直し、再試行の途中でprocessが停止した後の再起動を再現します。受理済みの再開処理は
// 引き継ぎません。構築時のuploaderはmutateで指定し、宛先と既存世代の登録は新しい構築で行います。
func restartTestArchiveHandler(
	t *testing.T,
	previous *FailureArchivePipeline,
	now time.Time,
	mutate func(*testArchiveConfig),
	handlerSets ...InternalLogHandlers,
) *FailureArchivePipeline {
	t.Helper()
	previousCfg, ok := previous.handler.Config.(testArchiveConfig)
	if !ok {
		t.Fatalf("previous archive config type = %T, want testArchiveConfig", previous.handler.Config)
	}
	_ = previous.ForceClose()
	waitFailureArchiveClosedForTest(t, previous)
	return newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		*cfg = previousCfg
		cfg.retryableUploader, cfg.permanentUploader = nil, nil
		cfg.retryableCheckpointStore, cfg.permanentCheckpointStore = nil, nil
		if mutate != nil {
			mutate(cfg)
		}
	}, handlerSets...)
}

func newValidArchiveHandlerConfig(t *testing.T) testArchiveConfig {
	t.Helper()

	return testArchiveConfig{
		target:                     "test-app",
		archivePath:                t.TempDir(),
		dir:                        "backend",
		dirCaseRetryable:           "retryable",
		dirCasePermanent:           "permanent",
		fileNameFormat:             "/%s%s_%s_%d.jsonl",
		fileDateFormat:             pkgfsstore.FileDateFormat,
		enableRotate:               false,
		maxFileSize:                infrafsstore.MinAllowedFileSizeBytes,
		maxDirSize:                 infrafsstore.MinAllowedMaxDirSize,
		maxFileAge:                 infrafsstore.MinAllowedMaxFileAge,
		maxFileBackups:             infrafsstore.MinAllowedMaxFileBackups,
		enableFileCompress:         false,
		compressFormat:             pkgfsstore.CompressGzip,
		rotationType:               pkgfsstore.FileRotationDaily,
		rotationDayOfWeek:          time.Sunday,
		maxWriteAttempts:           1,
		fileOperationRetryWaitTime: time.Millisecond,
		lifecycleRunnerConfig: pkgfsstore.AppendLogRunnerConfig{
			Interval: time.Minute,
		},
		overload: testArchiveOverloadConfig{
			minAvailableDiskSpace: durable.MinFileArchiveAvailableDiskSpace,
		},
		errorTraceDepth:          4,
		errorTraceDepthDebugMode: 8,
	}
}

// testArchiveAvailableDiskSpaceは試験用fixtureが返すarchive directoryの空き容量です。
// 構築時に許可される最小の閾値でも過負荷にならず、閾値を最大値にした試験だけが過負荷になります。
const testArchiveAvailableDiskSpace uint64 = 1 << 40

// useTestArchiveAvailableDiskSpaceは、過負荷判定の空き容量を実行環境に依存しない固定値へ
// 置き換えます。構築直後、failureの受付前に呼んでください。
func useTestArchiveAvailableDiskSpace(pipeline *FailureArchivePipeline) {
	pipeline.handler.availableDiskSpace = func(string) (uint64, error) {
		return testArchiveAvailableDiskSpace, nil
	}
}

func archiveFailedRecord(
	id string,
	failedAt time.Time,
	status int,
	failureCode string,
) loggerfailure.FailedRecord {
	// archive record 生成に必要な最小限の durable payload を作る。
	return loggerfailure.FailedRecord{
		FailedAt:      failedAt,
		Operation:     "index",
		StatusCode:    status,
		FailureCode:   loggerfailure.FailureCode(failureCode),
		FailureReason: "test failure",
		Payload: map[string]any{
			"id":      id,
			"message": id,
		},
	}
}

func readArchiveRecords(t *testing.T, path string) []archiveFailureRecord {
	t.Helper()

	// archive は JSONL なので、1 行ずつ record として復元する。
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open archive file %q: %v", path, err)
	}
	defer file.Close()

	var records []archiveFailureRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record archiveFailureRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("failed to unmarshal archive record: %v", err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("failed to read archive records: %v", err)
	}

	return records
}

func assertArchiveRecordCount(t *testing.T, records []archiveFailureRecord, want int, label string) {
	t.Helper()

	if len(records) != want {
		t.Fatalf("%s record count = %d, want %d", label, len(records), want)
	}
}

func assertArchiveRecord(
	t *testing.T,
	record archiveFailureRecord,
	wantFailureCategory string,
	wantOccurredAt time.Time,
	wantRequestID string,
	wantAppName string,
	wantHostName string,
	wantDocID string,
	wantStatus int,
) {
	t.Helper()

	// 失敗時にどのフィールドが崩れたか分かるよう、主要フィールドを個別に検証する。
	if record.FailureCategory != wantFailureCategory {
		t.Fatalf("FailureCategory = %q, want %q", record.FailureCategory, wantFailureCategory)
	}
	if record.FailedAt != wantOccurredAt.Format(timeLayout) {
		t.Fatalf("FailedAt = %q, want %q", record.FailedAt, wantOccurredAt.Format(timeLayout))
	}
	if record.RequestID != wantRequestID {
		t.Fatalf("RequestID = %q, want %q", record.RequestID, wantRequestID)
	}
	if record.AppName != wantAppName {
		t.Fatalf("AppName = %q, want %q", record.AppName, wantAppName)
	}
	if record.HostName != wantHostName {
		t.Fatalf("HostName = %q, want %q", record.HostName, wantHostName)
	}
	if record.Backend == "" {
		t.Fatal("Backend is empty, want archive routing metadata")
	}
	if got := record.Payload["id"]; got != wantDocID {
		t.Fatalf("Payload[id] = %v, want %q", got, wantDocID)
	}
	if record.StatusCode != wantStatus {
		t.Fatalf("StatusCode = %d, want %d", record.StatusCode, wantStatus)
	}
}
