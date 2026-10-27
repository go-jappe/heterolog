package compressedlogfailure

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	infras3 "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/s3"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgs3 "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestManagedUploaderStoresRetryableFailureInEverySelectedStore(t *testing.T) {
	base := t.TempDir()
	sourcePath, wantPayload := writeCompressedLog(t, base, "app_20260904_1.log.gz")
	remoteErr := errors.New("temporary outage")
	remote := &testDestinationUploader{
		ids:  []objectstorage.DestinationID{"destination-a"},
		errs: map[objectstorage.DestinationID]error{"destination-a": remoteErr},
	}

	var receivedNotification Notification
	managed := newTestManagedUploader(t, ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{
			failurestore.DurableTypeFileArchive,
			failurestore.DurableTypeSpool,
		})),
		SpoolRuntimes: newTestSpoolRuntimes(t, filepath.Join(base, "spool")),
		FileName:      logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
		Notifier: func(_ context.Context, notification Notification) {
			receivedNotification = notification
		},
	}, remote)

	if err := managed.Upload(context.Background(), uploadertest.Request(t, sourcePath)); err != nil {
		t.Fatalf("durably accepted Upload() error = %v", err)
	}
	if got := remote.callCount(); got != 1 {
		t.Fatalf("direct upload calls = %d, want 1", got)
	}
	assertStoredTypes(t, savedKindsForTest(receivedNotification),
		failurestore.DurableTypeFileArchive,
		failurestore.DurableTypeSpool,
	)
	if !errors.Is(receivedNotification.UploadErr, remoteErr) {
		t.Fatalf("receivedNotification upload error = %v, want %v", receivedNotification.UploadErr, remoteErr)
	}
	if receivedNotification.SaveErr != nil {
		t.Fatalf("receivedNotification handoff error = %v", receivedNotification.SaveErr)
	}
	if receivedNotification.FailureCategory != uploader.FailureCategoryRetryable {
		t.Fatalf("archive failure category = %q, want retryable", receivedNotification.FailureCategory)
	}
	if spoolDetailsForTest(receivedNotification).State != pkgspool.StateActive {
		t.Fatalf("spool state = %q, want active", spoolDetailsForTest(receivedNotification).State)
	}
	// directoryを検査する前にreplay coordinatorを止めます。稼働中のworkerは、
	// 延期されたfileをretry期限前にはuploadせずactiveへ戻しますが、その途中で
	// 一時的にprocessingへ移すことがあります。
	if err := managed.Close(); err != nil {
		t.Fatalf("Close() before durable store inspection error = %v", err)
	}

	archiveRoot := newArchiveRoot(t, filepath.Join(base, "archive"))
	wantArchivePath := filepath.Join(archiveRoot.RetryableDir(), filepath.Base(sourcePath))
	if archiveDetailsForTest(receivedNotification).FilePath != wantArchivePath {
		t.Fatalf("archive path = %q, want %q", archiveDetailsForTest(receivedNotification).FilePath, wantArchivePath)
	}
	assertFileBytes(t, wantArchivePath, wantPayload)

	spoolRoot := newSpoolRoot(t, filepath.Join(base, "spool"))
	wantSpoolPath := filepath.Join(spoolRoot.ActiveDir(), filepath.Base(sourcePath))
	if spoolDetailsForTest(receivedNotification).FilePath != wantSpoolPath {
		t.Fatalf("spool path = %q, want %q", spoolDetailsForTest(receivedNotification).FilePath, wantSpoolPath)
	}
	assertFileBytes(t, wantSpoolPath, wantPayload)
	assertOnlyFileInState(t, spoolRoot, pkgspool.StateActive, filepath.Base(sourcePath))
}

func TestManagedUploaderRejectsUnconstructedRequestBeforeUploadOrHandoff(t *testing.T) {
	remote := &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}}
	observed := false
	managed := &ManagedUploader{
		next:     remote,
		fileName: logpattern.FileNamePattern{Dir: t.TempDir(), Target: "app", Extension: ".log"},
		notifier: func(context.Context, Notification) { observed = true },
	}
	if err := managed.Upload(context.Background(), uploader.UploadRequest{}); err == nil {
		t.Fatal("unconstructed upload request was accepted")
	}
	if remote.callCount() != 0 || observed || len(managed.attempts) != 0 {
		t.Fatalf("invalid request reached processing: calls=%d observed=%t attempts=%d",
			remote.callCount(), observed, len(managed.attempts))
	}
}

func TestManagedUploaderUsesRequestAbsolutePathForUploadAndHandoff(t *testing.T) {
	base := t.TempDir()
	source, payload := writeCompressedLog(t, base, "app_20260929_1.log.gz")
	t.Chdir(base)
	request := uploadertest.Request(t, filepath.Base(source))
	remote := &testDestinationUploader{
		ids:  []objectstorage.DestinationID{"destination-a"},
		errs: map[objectstorage.DestinationID]error{"destination-a": errors.New("remote unavailable")},
	}
	var receivedNotification Notification
	managed := newTestManagedUploader(t, ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive})),
		FileName:                            logpattern.FileNamePattern{Dir: ".", Target: "app", Extension: ".log"},
		Notifier:                            func(_ context.Context, notification Notification) { receivedNotification = notification },
	}, remote)
	// 依頼と相対ディレクトリの設定を作った後に作業ディレクトリが変わっても、同じ元ファイルを送信・退避します。
	t.Chdir(t.TempDir())
	if err := managed.Upload(context.Background(), request); err != nil {
		t.Fatalf("upload with handoff: %v", err)
	}
	if remote.lastRequest.FilePath() != source || receivedNotification.SourcePath != source {
		t.Fatalf("source paths: remote=%q handoff=%q, want %q",
			remote.lastRequest.FilePath(), receivedNotification.SourcePath, source)
	}
	assertFileBytes(t, archiveDetailsForTest(receivedNotification).FilePath, payload)
}

func TestManagedUploaderStoresPermanentFailureInPermanentArchiveAndDeadSpool(t *testing.T) {
	base := t.TempDir()
	sourcePath, wantPayload := writeCompressedLog(t, base, "app_20260904_2.log.zst")
	remote := &testDestinationUploader{
		ids: []objectstorage.DestinationID{"destination-a"},
		errs: map[objectstorage.DestinationID]error{
			"destination-a": &pkgs3.HTTPStatusError{Code: http.StatusForbidden},
		},
	}

	var receivedNotification Notification
	managed := newTestManagedUploader(t, ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{
			failurestore.DurableTypeFileArchive,
			failurestore.DurableTypeSpool,
		})),
		SpoolRuntimes: newTestSpoolRuntimes(t, filepath.Join(base, "spool")),
		FileName:      logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
		Notifier: func(_ context.Context, notification Notification) {
			receivedNotification = notification
		},
	}, remote)

	if err := managed.Upload(context.Background(), uploadertest.Request(t, sourcePath)); err != nil {
		t.Fatalf("durably accepted Upload() error = %v", err)
	}
	if receivedNotification.FailureCategory != uploader.FailureCategoryPermanent {
		t.Fatalf("archive failure category = %q, want permanent", receivedNotification.FailureCategory)
	}
	if spoolDetailsForTest(receivedNotification).State != pkgspool.StateDead {
		t.Fatalf("spool state = %q, want dead", spoolDetailsForTest(receivedNotification).State)
	}

	archiveRoot := newArchiveRoot(t, filepath.Join(base, "archive"))
	assertFileBytes(
		t,
		filepath.Join(archiveRoot.PermanentDir(), filepath.Base(sourcePath)),
		wantPayload,
	)
	spoolRoot := newSpoolRoot(t, filepath.Join(base, "spool"))
	assertFileBytes(
		t,
		filepath.Join(spoolRoot.DeadDir(), filepath.Base(sourcePath)),
		wantPayload,
	)
	assertOnlyFileInState(t, spoolRoot, pkgspool.StateDead, filepath.Base(sourcePath))
}

func TestManagedUploaderReportsUploadAndReusedArchiveCategoriesSeparately(t *testing.T) {
	base := t.TempDir()
	source, payload := writeCompressedLog(t, base, "app_20260930_1.log.gz")
	remote := &testDestinationUploader{
		ids: []objectstorage.DestinationID{"destination-a"},
		errs: map[objectstorage.DestinationID]error{
			"destination-a": &pkgs3.HTTPStatusError{Code: http.StatusForbidden},
		},
	}
	var receivedNotification Notification
	managed := newTestManagedUploader(t, ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{
			failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool,
		})),
		SpoolRuntimes: newTestSpoolRuntimes(t, filepath.Join(base, "spool")),
		FileName:      logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
		Notifier:      func(_ context.Context, notification Notification) { receivedNotification = notification },
	}, remote)
	// 前回のarchiveだけ残り、spoolは未保存。送信を試すと今回はpermanentで失敗します。
	archivePath := filepath.Join(archiveRouteForTest(t, managed, "destination-a").store.Root().RetryableDir(), filepath.Base(source))
	if err := os.WriteFile(archivePath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := managed.Upload(context.Background(), uploadertest.Request(t, source)); err != nil {
		t.Fatal(err)
	}
	if receivedNotification.FailureCategory != uploader.FailureCategoryPermanent || remote.callCount() != 1 {
		t.Fatalf("upload classification was overwritten: report=%+v, calls=%d", receivedNotification, remote.callCount())
	}
	archive := archiveDetailsForTest(receivedNotification)
	if archive.FailureCategory != uploader.FailureCategoryRetryable || archive.FilePath != archivePath {
		t.Fatalf("reused archive classification/path lost: %+v", archive)
	}
	if spool := spoolDetailsForTest(receivedNotification); spool.State != pkgspool.StateDead {
		t.Fatalf("spool did not use original upload classification: %+v", spool)
	}
}

func TestManagedUploaderRequiresEverySelectedStoreBeforeAcceptingFailure(t *testing.T) {
	base := t.TempDir()
	sourcePath, _ := writeCompressedLog(t, base, "app_20260904_3.log.gz")
	remoteErr := errors.New("temporary outage")
	remote := &testDestinationUploader{
		ids:  []objectstorage.DestinationID{"destination-a"},
		errs: map[objectstorage.DestinationID]error{"destination-a": remoteErr},
	}
	archivePath := filepath.Join(base, "archive")
	spoolPath := filepath.Join(base, "spool")
	var receivedNotification Notification
	managed := newTestManagedUploader(t, ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(archivePath, []failurestore.DurableType{
			failurestore.DurableTypeFileArchive,
			failurestore.DurableTypeSpool,
		})),
		SpoolRuntimes: newTestSpoolRuntimes(t, spoolPath),
		FileName:      logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
		Notifier: func(_ context.Context, notification Notification) {
			receivedNotification = notification
		},
	}, remote)

	spoolRoot := newSpoolRoot(t, spoolPath)
	stagingPath := filepath.Join(spoolRoot.Path(), ".staging")
	if err := os.RemoveAll(stagingPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stagingPath, []byte("blocks spool publish"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := managed.Upload(context.Background(), uploadertest.Request(t, sourcePath)); err == nil {
		t.Fatal("Upload() error = nil although spool did not accept the compressed log file")
	}
	assertStoredTypes(t, savedKindsForTest(receivedNotification), failurestore.DurableTypeFileArchive)
	if receivedNotification.SaveErr == nil {
		t.Fatal("handoff error = nil")
	}
	if _, err := os.Stat(sourcePath); err != nil {
		t.Fatalf("source was lost after partial durable handoff: %v", err)
	}
	archiveRoot := newArchiveRoot(t, archivePath)
	assertFileBytes(
		t,
		filepath.Join(archiveRoot.RetryableDir(), filepath.Base(sourcePath)),
		[]byte("valuable compressed log"),
	)
}

func TestManagedUploaderDoesNotStoreIneligibleFailure(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		otherDir bool
	}{
		{name: "different directory with same basename", fileName: "app_20260904_1.log.gz", otherDir: true},
		{name: "different series", fileName: "other_20260904_1.log.gz"},
		{name: "raw log", fileName: "app_20260904_1.log"},
		{name: "compression temporary file", fileName: "app_20260904_1.log.gz.tmp"},
		{name: "invalid generation index", fileName: "app_20260904_invalid.log.gz"},
		{name: "zero generation index", fileName: "app_20260904_0.log.gz"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := t.TempDir()
			sourceDirectory := base
			if test.otherDir {
				sourceDirectory = t.TempDir()
			}
			sourcePath, payload := writeCompressedLog(t, sourceDirectory, test.fileName)
			remoteErr := errors.New("temporary outage")
			remote := &testDestinationUploader{
				ids:  []objectstorage.DestinationID{"destination-a"},
				errs: map[objectstorage.DestinationID]error{"destination-a": remoteErr},
			}
			archivePath := filepath.Join(base, "archive")
			spoolPath := filepath.Join(base, "spool")
			observed := false
			managed := newTestManagedUploader(t, ManagedUploaderConfig{
				CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(archivePath, []failurestore.DurableType{
					failurestore.DurableTypeFileArchive,
					failurestore.DurableTypeSpool,
				})),
				SpoolRuntimes: newTestSpoolRuntimes(t, spoolPath),
				FileName:      logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
				Notifier: func(context.Context, Notification) {
					observed = true
				},
			}, remote)

			if err := managed.Upload(context.Background(), uploadertest.Request(t, sourcePath)); !errors.Is(err, remoteErr) {
				t.Fatalf("Upload() error = %v, want remote error %v", err, remoteErr)
			}
			if remote.callCount() != 1 || observed || len(managed.attempts) != 0 {
				t.Fatalf("ineligible request processing: calls=%d observed=%t attempts=%d",
					remote.callCount(), observed, len(managed.attempts))
			}
			assertFileBytes(t, sourcePath, payload)
			archiveRoot := newArchiveRoot(t, archivePath)
			assertDirectoryEmpty(t, archiveRoot.RetryableDir())
			assertDirectoryEmpty(t, archiveRoot.PermanentDir())
			spoolRoot := newSpoolRoot(t, spoolPath)
			assertDirectoryEmpty(t, spoolRoot.ActiveDir())
			assertDirectoryEmpty(t, spoolRoot.DeadDir())
		})
	}
}

func TestNewManagedUploaderRequiresUniqueNonEmptyDestinations(t *testing.T) {
	tests := []struct {
		name string
		ids  []objectstorage.DestinationID
	}{
		{name: "none"},
		{name: "blank", ids: []objectstorage.DestinationID{""}},
		{name: "duplicate", ids: []objectstorage.DestinationID{"destination-a", "destination-a"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := t.TempDir()
			_, err := NewManagedUploader(ManagedUploaderConfig{
				CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive})),
				FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
			}, &testDestinationUploader{ids: test.ids})
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("NewManagedUploader() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestNewManagedUploaderRequiresCompleteFileNamePattern(t *testing.T) {
	tests := []struct {
		name    string
		pattern logpattern.FileNamePattern
	}{
		{name: "missing pattern"},
		{name: "missing directory", pattern: logpattern.FileNamePattern{Target: "app", Extension: ".log"}},
		{name: "blank directory", pattern: logpattern.FileNamePattern{Dir: " \t", Target: "app", Extension: ".log"}},
		{name: "missing target", pattern: logpattern.FileNamePattern{Dir: ".", Extension: ".log"}},
		{name: "blank target", pattern: logpattern.FileNamePattern{Dir: ".", Target: " \t", Extension: ".log"}},
		{name: "missing extension", pattern: logpattern.FileNamePattern{Dir: ".", Target: "app"}},
		{name: "blank extension", pattern: logpattern.FileNamePattern{Dir: ".", Target: "app", Extension: " \t"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			created, err := NewManagedUploader(ManagedUploaderConfig{
				CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(t.TempDir(), "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive})),
				FileName:                            test.pattern,
			}, &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}})
			if managed, ok := created.(*ManagedUploader); ok {
				_ = managed.Close()
			}
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("NewManagedUploader() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestManagedUploaderDurablyPreservesMalformedUploadReport(t *testing.T) {
	tests := []struct {
		name   string
		report uploader.UploadReport
	}{
		{
			name:   "missing requested destination",
			report: uploader.UploadReport{},
		},
		{
			name: "unexpected destination",
			report: uploader.UploadReport{Results: []uploader.DestinationResult{
				{DestinationID: "destination-a"},
				{DestinationID: "unexpected"},
			}},
		},
		{
			name: "duplicate destination",
			report: uploader.UploadReport{Results: []uploader.DestinationResult{
				{DestinationID: "destination-a"},
				{DestinationID: "destination-a"},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := t.TempDir()
			sourcePath, wantPayload := writeCompressedLog(t, base, "app_20260904_4.log.gz")
			remote := &testDestinationUploader{
				ids:         []objectstorage.DestinationID{"destination-a"},
				fixedReport: &test.report,
			}
			var receivedNotification Notification
			managed := newTestManagedUploader(t, ManagedUploaderConfig{
				CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive})),
				FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
				Notifier: func(_ context.Context, notification Notification) {
					receivedNotification = notification
				},
			}, remote)

			// 不正な形式のreportは、remoteへの試行の失敗です。これをacknowledgeできるのは、
			// 選択したdurable storeが先にsourceを受理したためだけです。
			if err := managed.Upload(context.Background(), uploadertest.Request(t, sourcePath)); err != nil {
				t.Fatalf("durably preserved malformed report returned error = %v", err)
			}
			if receivedNotification.UploadErr == nil {
				t.Fatal("malformed upload report was not treated as a remote failure")
			}
			if receivedNotification.FailureCategory != uploader.FailureCategoryRetryable {
				t.Fatalf("failure category = %q, want retryable", receivedNotification.FailureCategory)
			}
			archiveRoot := newArchiveRoot(t, filepath.Join(base, "archive"))
			assertFileBytes(
				t,
				filepath.Join(archiveRoot.RetryableDir(), filepath.Base(sourcePath)),
				wantPayload,
			)
		})
	}
}

func TestManagedUploaderDoesNotHideMalformedReportForIneligibleSource(t *testing.T) {
	base := t.TempDir()
	sourcePath, _ := writeCompressedLog(t, base, "app_20260904_ineligible_contract.log.gz")
	malformed := uploader.UploadReport{}
	remote := &testDestinationUploader{
		ids:         []objectstorage.DestinationID{"destination-a"},
		fixedReport: &malformed,
	}
	managed := newTestManagedUploader(t, ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive})),
		FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
	}, remote)

	if err := managed.Upload(context.Background(), uploadertest.Request(t, sourcePath)); err == nil {
		t.Fatal("Upload() error = nil for an ineligible source and malformed remote report")
	}
	assertDirectoryEmpty(t, newArchiveRoot(t, filepath.Join(base, "archive")).RetryableDir())
}

func TestManagedUploaderNotifierPanicDoesNotUndoDurableSave(t *testing.T) {
	base := t.TempDir()
	sourcePath, wantPayload := writeCompressedLog(t, base, "app_20260904_5.log.gz")
	remote := &testDestinationUploader{
		ids:  []objectstorage.DestinationID{"destination-a"},
		errs: map[objectstorage.DestinationID]error{"destination-a": errors.New("temporary outage")},
	}
	managed := newTestManagedUploader(t, ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive})),
		FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
		Notifier: func(context.Context, Notification) {
			panic("notifier failure")
		},
	}, remote)

	if err := managed.Upload(context.Background(), uploadertest.Request(t, sourcePath)); err != nil {
		t.Fatalf("Upload() error after notifier panic = %v", err)
	}
	archiveRoot := newArchiveRoot(t, filepath.Join(base, "archive"))
	assertFileBytes(
		t,
		filepath.Join(archiveRoot.RetryableDir(), filepath.Base(sourcePath)),
		wantPayload,
	)
}

func TestManagedUploaderSpoolHandoffWaitsForNextRunnerTick(t *testing.T) {
	// bubble内の時刻で、newTestSpoolRuntimesのrunner周期の直前と直後を観測します。
	synctest.Test(t, func(t *testing.T) {
		base := t.TempDir()
		sourcePath, _ := writeCompressedLog(t, base, "app_20260904_6.log.gz")
		remote := &testDestinationUploader{
			ids:  []objectstorage.DestinationID{"destination-a"},
			errs: map[objectstorage.DestinationID]error{"destination-a": errors.New("temporary outage")},
		}
		failureStore := testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeSpool})
		// runnerの初回待機はroute登録時に始まります。登録とUploadの間に時刻は進みません。
		registered := time.Now()
		managed := newTestManagedUploader(t, ManagedUploaderConfig{
			CompressedLogFileUploadFailureStore: NewConfigAdapter(failureStore),
			SpoolRuntimes:                       newTestSpoolRuntimes(t, filepath.Join(base, "spool")),
			FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
		}, remote)

		if err := managed.Upload(context.Background(), uploadertest.Request(t, sourcePath)); err != nil {
			t.Fatalf("durably accepted Upload() error = %v", err)
		}
		if elapsed := time.Since(registered); elapsed != 0 {
			t.Fatalf("route registration and Upload consumed %v of bubble time", elapsed)
		}
		// spoolへの引き渡しは再送を起こさず、次のrunner周期まで待ちます。
		time.Sleep(testSpoolRunnerInterval - time.Nanosecond)
		synctest.Wait()
		if got := remote.callCount(); got != 1 {
			t.Fatalf("upload calls before next runner tick = %d, want 1", got)
		}
		spoolRoot := newSpoolRoot(t, filepath.Join(base, "spool"))
		assertOnlyFileInState(t, spoolRoot, pkgspool.StateActive, filepath.Base(sourcePath))

		// 周期が来ると、spoolへ残したファイルを再送します。
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if got := remote.callCount(); got != 2 {
			t.Fatalf("upload calls after runner tick = %d, want 2", got)
		}

		if err := managed.Close(); err != nil {
			t.Fatalf("first Close() error = %v", err)
		}
		if err := managed.Close(); err != nil {
			t.Fatalf("second Close() error = %v", err)
		}
		// Close後は、次の周期の時刻を過ぎても再送しません。
		time.Sleep(2 * testSpoolRunnerInterval)
		synctest.Wait()
		if got := remote.callCount(); got != 2 {
			t.Fatalf("upload calls after Close() = %d, want 2", got)
		}
	})
}

func TestManagedUploaderFileArchiveCollisionPreservesPreviousFile(t *testing.T) {
	base := t.TempDir()
	source := filepath.Join(base, "same_20260905_1.log.gz")
	if err := os.WriteFile(source, []byte("first valuable log"), 0o600); err != nil {
		t.Fatal(err)
	}
	remote := &testDestinationUploader{
		ids:  []objectstorage.DestinationID{"destination-a"},
		errs: map[objectstorage.DestinationID]error{"destination-a": errors.New("temporary outage")},
	}
	var notifications []Notification
	managed := newTestManagedUploader(t, ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive})),
		FileName:                            logpattern.FileNamePattern{Dir: base, Target: "same", Extension: ".log"},
		Notifier: func(_ context.Context, notification Notification) {
			notifications = append(notifications, notification)
		},
	}, remote)

	if err := managed.Upload(context.Background(), uploadertest.Request(t, source)); err != nil {
		t.Fatalf("first Upload() error = %v", err)
	}
	if err := os.WriteFile(source, []byte("second valuable log"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := managed.Upload(context.Background(), uploadertest.Request(t, source)); err != nil {
		t.Fatalf("second Upload() error = %v, want accepted archive save", err)
	}
	if got := remote.callCount(); got != 2 {
		t.Fatalf("remote upload calls = %d, want 2", got)
	}
	if len(notifications) != 3 {
		t.Fatalf("notifications = %d, want 3", len(notifications))
	}
	if notifications[1].ExistingFileCheckErr != nil || len(notifications[1].ConflictRenames) != 1 {
		t.Fatalf("archive preflight = %+v, want one preserved conflict", notifications[1])
	}
	if notifications[2].SaveErr != nil || len(notifications[2].ConflictRenames) != 0 {
		t.Fatalf("second handoff = %+v, want accepted archive save", notifications[2])
	}
	archiveRoot := newArchiveRoot(t, filepath.Join(base, "archive"))
	conflict := notifications[1].ConflictRenames[0]
	if conflict.OriginalPath != filepath.Join(archiveRoot.RetryableDir(), filepath.Base(source)) {
		t.Fatalf("renamed conflict source = %q", conflict.OriginalPath)
	}
	assertFileBytes(
		t,
		conflict.RetainedPath,
		[]byte("first valuable log"),
	)
	assertFileBytes(t, conflict.OriginalPath, []byte("second valuable log"))
	assertFileBytes(t, source, []byte("second valuable log"))
}

func TestManagedUploaderReplacementWaitsForSameRootHandoffBeforeStartingNewLifecycle(
	t *testing.T,
) {
	testManagedUploaderReplacementWaitsForRetirement(t, func(path string) string { return path })
}

func testManagedUploaderReplacementWaitsForRetirement(t *testing.T, aliasRoot func(string) string) {
	t.Helper()
	// bubble内で全goroutineが待機状態になってから、置き換えがまだ作成されていないことを確かめます。
	synctest.Test(t, func(t *testing.T) {
		base := t.TempDir()
		archivePath := filepath.Join(base, "archive")
		failureStoreConfig := testFailureStoreConfig(archivePath, []failurestore.DurableType{failurestore.DurableTypeFileArchive})
		cfg := ManagedUploaderConfig{
			CompressedLogFileUploadFailureStore: NewConfigAdapter(failureStoreConfig),
			FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
		}
		remote := &testDestinationUploader{
			ids: []objectstorage.DestinationID{"destination-a"},
			errs: map[objectstorage.DestinationID]error{
				"destination-a": errors.New("temporary outage"),
			},
		}
		first := newTestManagedUploader(t, cfg, remote)
		lockedPurge, err := archiveRouteForTest(t, first, "destination-a").owner.coordinator.lockPurgeDuringSave(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		unlockPurge := sync.OnceFunc(lockedPurge)
		// 途中で失敗しても保存中の保護を解除し、古いuploaderのCloseを終了させます。
		t.Cleanup(unlockPurge)

		closeDone := make(chan error, 1)
		go func() { closeDone <- first.Close() }()
		waitForArchiveShutdownStarted(t, archiveRouteForTest(t, first, "destination-a").owner.coordinator)

		type createResult struct {
			uploader uploader.Uploader
			err      error
		}
		created := make(chan createResult, 1)
		replacementConfig := cfg
		failureStoreConfig.DurableStoreConfig.FileArchiveConfig.RootPath = aliasRoot(archivePath)
		replacementConfig.CompressedLogFileUploadFailureStore = NewConfigAdapter(failureStoreConfig)
		go func() {
			managed, createErr := NewManagedUploader(replacementConfig, remote)
			created <- createResult{uploader: managed, err: createErr}
		}()
		replacementReceived := false
		// 途中で失敗しても、保存中の保護を解除した後に置き換えを閉じ、bubbleを終了させます。
		t.Cleanup(func() {
			if !replacementReceived {
				if managed, ok := (<-created).uploader.(*ManagedUploader); ok {
					_ = managed.Close()
				}
			}
		})
		t.Cleanup(unlockPurge)
		// 置き換えが古い管理処理の終了待ちに入るまで進めます。時刻は進めません。
		synctest.Wait()
		select {
		case result := <-created:
			replacementReceived = true
			if managed, ok := result.uploader.(*ManagedUploader); ok {
				_ = managed.Close()
			}
			t.Fatalf("replacement uploader was created before old handoff drained: %v", result.err)
		default:
		}

		unlockPurge()
		select {
		case closeErr := <-closeDone:
			if closeErr != nil {
				t.Fatal(closeErr)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("old uploader Close did not drain its handoff")
		}

		var replacement createResult
		select {
		case replacement = <-created:
			replacementReceived = true
		case <-time.After(2 * time.Second):
			t.Fatal("replacement uploader was not created after old lifecycle retired")
		}
		if replacement.err != nil {
			t.Fatal(replacement.err)
		}
		second, ok := replacement.uploader.(*ManagedUploader)
		if !ok {
			t.Fatalf("replacement uploader = %T, want *ManagedUploader", replacement.uploader)
		}
		t.Cleanup(func() {
			if err := second.Close(); err != nil {
				t.Errorf("replacement Close() error = %v", err)
			}
		})
		if archiveRouteForTest(t, second, "destination-a").owner.coordinator == archiveRouteForTest(t, first, "destination-a").owner.coordinator {
			t.Fatal("replacement uploader reused the stopped archive lifecycle")
		}

		sourcePath, payload := writeCompressedLog(t, base, "app_20260906_1.log.gz")
		if err := second.Upload(context.Background(), uploadertest.Request(t, sourcePath)); err != nil {
			t.Fatalf("replacement Upload() error = %v", err)
		}
		archiveRoot := newArchiveRoot(t, archivePath)
		assertFileBytes(
			t,
			filepath.Join(archiveRoot.RetryableDir(), filepath.Base(sourcePath)),
			payload,
		)
	})
}

func TestNewManagedUploaderNoneReturnsRawUploader(t *testing.T) {
	remote := &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}}
	created, err := NewManagedUploader(ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(objectstorage.CompressedLogFileUploadFailureStoreConfig{
			DurableType: []failurestore.DurableType{failurestore.DurableTypeNone},
		}),
	}, remote)
	if err != nil {
		t.Fatal(err)
	}
	if created != remote {
		t.Fatalf("created uploader = %T, want raw uploader", created)
	}
}

func newTestManagedUploader(
	t *testing.T,
	cfg ManagedUploaderConfig,
	remote uploader.Uploader,
) *ManagedUploader {
	t.Helper()
	created, err := NewManagedUploader(cfg, remote)
	if err != nil {
		t.Fatal(err)
	}
	managed, ok := created.(*ManagedUploader)
	if !ok {
		t.Fatalf("created uploader = %T, want *ManagedUploader", created)
	}
	t.Cleanup(func() {
		if err := managed.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return managed
}

func testFailureStoreConfig(
	archiveRoot string,
	types []failurestore.DurableType,
) objectstorage.CompressedLogFileUploadFailureStoreConfig {
	return objectstorage.CompressedLogFileUploadFailureStoreConfig{
		DurableType: types,
		DurableStoreConfig: objectstorage.CompressedLogFileUploadFailureDurableStoreConfig{
			FileArchiveConfig: objectstorage.CompressedLogFileUploadFailureArchiveConfig{
				RootPath:          archiveRoot,
				LoggerContract:    pkglogger.LoggerContractSync.String(),
				LifecycleInterval: time.Hour,
			},
		},
	}
}

func newArchiveRoot(t *testing.T, base string) compressedlogarchive.Root {
	t.Helper()
	root, err := compressedlogarchive.NewRoot(
		base,
		objectstorage.UploadTargetS3,
		pkglogger.LoggerContractSync,
	)
	if err != nil {
		t.Fatal(err)
	}
	destinationRoot, err := root.ForDestination("destination-a")
	if err != nil {
		t.Fatal(err)
	}
	return destinationRoot
}

func newSpoolRoot(t *testing.T, base string) objectstoragespool.Root {
	t.Helper()
	root, err := objectstoragespool.NewRoot(
		base,
		"heterolog",
		objectstorage.UploadTargetS3,
		pkglogger.LoggerContractSync,
	)
	if err != nil {
		t.Fatal(err)
	}
	root, err = root.ForDestination("destination-a")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func writeCompressedLog(t *testing.T, base string, name string) (string, []byte) {
	t.Helper()
	payload := []byte("valuable compressed log")
	path := filepath.Join(base, name)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	return path, payload
}

func assertStoredTypes(t *testing.T, got []failurestore.DurableType, want ...failurestore.DurableType) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("stored types = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("stored types = %v, want %v", got, want)
		}
	}
}

func savedKindsForTest(notification Notification) []failurestore.DurableType {
	kinds := make([]failurestore.DurableType, len(notification.SavedTargets))
	for index, target := range notification.SavedTargets {
		kinds[index] = target.Kind()
	}
	return kinds
}

func archiveDetailsForTest(notification Notification) SavedArchiveDetails {
	for _, target := range notification.SavedTargets {
		if saved, ok := target.(SavedArchiveDetails); ok {
			return saved
		}
	}
	return SavedArchiveDetails{}
}

func spoolDetailsForTest(notification Notification) SavedSpoolDetails {
	for _, target := range notification.SavedTargets {
		if saved, ok := target.(SavedSpoolDetails); ok {
			return saved
		}
	}
	return SavedSpoolDetails{}
}

func archiveRouteForTest(t *testing.T, managed *ManagedUploader, destination objectstorage.DestinationID) archiveRoute {
	t.Helper()
	for _, target := range managed.failureTargets[destination] {
		if archive, ok := target.(archiveFailureTarget); ok {
			return archive.route
		}
	}
	t.Fatalf("no archive target for destination %q", destination)
	return archiveRoute{}
}

func spoolRouteForTest(t *testing.T, managed *ManagedUploader, destination objectstorage.DestinationID) *compressedlog.ManagedRoute {
	t.Helper()
	for _, target := range managed.failureTargets[destination] {
		if spool, ok := target.(spoolFailureTarget); ok {
			return spool.route
		}
	}
	t.Fatalf("no spool target for destination %q", destination)
	return nil
}

func assertOnlyFileInState(
	t *testing.T,
	root objectstoragespool.Root,
	state pkgspool.State,
	wantName string,
) {
	t.Helper()
	store, err := objectstoragespool.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	files, err := store.ListFiles(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].FileName != wantName {
		t.Fatalf("%s spool files = %#v, want only %q", state, files, wantName)
	}
}

func assertDirectoryEmpty(t *testing.T, path string) {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("directory %q entries = %v, want empty", path, entries)
	}
}

func assertFileBytes(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("payload = %q, want %q", got, want)
	}
}

type testDestinationUploader struct {
	mu          sync.Mutex
	ids         []objectstorage.DestinationID
	services    map[objectstorage.DestinationID]objectstorage.UploadTarget
	errs        map[objectstorage.DestinationID]error
	fixedReport *uploader.UploadReport
	afterUpload func()
	calls       int
	lastRequest uploader.UploadRequest
}

func (u *testDestinationUploader) DestinationIDs() []objectstorage.DestinationID {
	return append([]objectstorage.DestinationID(nil), u.ids...)
}

func (u *testDestinationUploader) DestinationService(id objectstorage.DestinationID) (objectstorage.UploadTarget, bool) {
	if u.services != nil {
		service, exists := u.services[id]
		return service, exists
	}
	// 既存の試験fixtureはS3を模します。backendを指定する試験だけservicesを渡します。
	for _, registered := range u.ids {
		if registered == id {
			return objectstorage.UploadTargetS3, true
		}
	}
	return "", false
}

func (u *testDestinationUploader) Upload(
	ctx context.Context,
	input uploader.UploadRequest,
) error {
	return u.UploadDestinations(ctx, input, u.DestinationIDs()).Err()
}

func (u *testDestinationUploader) UploadDestinations(
	ctx context.Context,
	request uploader.UploadRequest,
	destinationIDs []objectstorage.DestinationID,
) uploader.UploadReport {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.calls++
	u.lastRequest = request
	if u.afterUpload != nil {
		u.afterUpload()
	}
	if u.fixedReport != nil {
		return cloneUploadReport(*u.fixedReport)
	}
	report := uploader.UploadReport{Results: make([]uploader.DestinationResult, len(destinationIDs))}
	for index, destinationID := range destinationIDs {
		report.Results[index] = uploader.DestinationResult{
			DestinationID: destinationID,
			Err:           u.errs[destinationID],
		}
		if report.Results[index].Err != nil {
			// このfixtureはS3を模し、実backendと同じく分類済みの結果を返します。
			report.Results[index].FailureCategory = infras3.ClassifyDurableUploadFailure(ctx, report.Results[index].Err).FailureCategory
		}
	}
	return report
}

func (u *testDestinationUploader) callCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.calls
}

func cloneUploadReport(report uploader.UploadReport) uploader.UploadReport {
	return uploader.UploadReport{
		Results: append([]uploader.DestinationResult(nil), report.Results...),
	}
}

// testSpoolRunnerIntervalはnewTestSpoolRuntimesの再送周期です。通常の試験中には
// 周期処理が始まらない長さにし、周期を観測する試験はsynctestで時刻を進めます。
const testSpoolRunnerInterval = time.Hour

func newTestSpoolRuntimes(t *testing.T, spoolPath string) compressedlog.RootRuntimes {
	t.Helper()
	root, err := compressedlog.NewRootRuntime(compressedlog.NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(objectstorage.CompressedLogFileUploadSpoolConfig{
		SpoolPath: spoolPath, Target: "heterolog",
		LoggerContract: pkglogger.LoggerContractSync.String(),
		RunnerInterval: testSpoolRunnerInterval, LifecycleInterval: time.Hour, CircuitBreakerOpenCooldown: 5 * time.Minute,
	}), objectstorage.UploadTargetS3))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Errorf("root Close: %v", err)
		}
	})
	return compressedlog.RootRuntimes{objectstorage.UploadTargetS3: root}
}
