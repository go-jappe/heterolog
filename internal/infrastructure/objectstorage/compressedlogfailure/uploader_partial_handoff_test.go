package compressedlogfailure

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestManagedUploaderRenamesConflictingCopyAndCompletesHandoff(t *testing.T) {
	for _, failedStore := range []failurestore.DurableType{
		failurestore.DurableTypeFileArchive,
		failurestore.DurableTypeSpool,
	} {
		t.Run(string(failedStore), func(t *testing.T) {
			base := t.TempDir()
			source, payload := writeCompressedLog(t, base, "app_20260915_1.log.gz")
			remoteErr := errors.New("temporary remote outage")
			remote := &testDestinationUploader{
				ids:  []objectstorage.DestinationID{"destination-a"},
				errs: map[objectstorage.DestinationID]error{"destination-a": remoteErr},
			}
			archiveBase, spoolBase := filepath.Join(base, "archive"), filepath.Join(base, "spool")
			var notifications []Notification
			managed := newTestManagedUploader(t, ManagedUploaderConfig{
				CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(archiveBase, []failurestore.DurableType{failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool})),
				SpoolRuntimes:                       newTestSpoolRuntimes(t, spoolBase),
				FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
				Notifier: func(_ context.Context, notification Notification) {
					notifications = append(notifications, notification)
				},
			}, remote)
			if err := archiveRouteForTest(t, managed, "destination-a").store.EnsureDirectories(context.Background()); err != nil {
				t.Fatal(err)
			}
			archiveRoot, spoolRoot := newArchiveRoot(t, archiveBase), newSpoolRoot(t, spoolBase)
			archivePath := filepath.Join(archiveRoot.RetryableDir(), filepath.Base(source))
			spoolPath := filepath.Join(spoolRoot.ActiveDir(), filepath.Base(source))
			failedPath, successfulPath := archivePath, spoolPath
			if failedStore == failurestore.DurableTypeSpool {
				failedPath, successfulPath = spoolPath, archivePath
			}
			// 一方だけ同名・別内容にし、退避先間の衝突保全と通知を確認します。
			conflictingPayload := []byte("previous different compressed log")
			if err := os.WriteFile(failedPath, conflictingPayload, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := managed.Upload(context.Background(), uploadertest.Request(t, source)); err != nil {
				t.Fatalf("Upload error = %v, want accepted durable save", err)
			}
			if len(notifications) == 0 || len(notifications) > 2 {
				t.Fatalf("notifications = %+v, want a save result and optional pre-upload conflict notice", notifications)
			}
			// 衝突が送信前の確認・保存のどちらで見つかっても、全通知を通して保全を確認します。
			var retainedPath string
			conflictCount := 0
			for index, notification := range notifications {
				if notification.SaveErr != nil || notification.ExistingFileCheckErr != nil {
					t.Fatalf("notification = %+v, want successful inspection and saving", notification)
				}
				if index < len(notifications)-1 && (notification.UploadErr != nil || len(savedKindsForTest(notification)) != 0) {
					t.Fatalf("pre-upload inspection notification claimed a save result: %+v", notification)
				}
				for _, conflict := range notification.ConflictRenames {
					conflictCount++
					if conflict.OriginalPath != failedPath {
						t.Fatalf("conflict original path = %q, want %q", conflict.OriginalPath, failedPath)
					}
					retainedPath = conflict.RetainedPath
				}
			}
			if conflictCount != 1 {
				t.Fatalf("notifications = %+v, want one conflict rename across all notifications", notifications)
			}
			savedNotification := notifications[len(notifications)-1]
			if !errors.Is(savedNotification.UploadErr, remoteErr) {
				t.Fatalf("notifications = %+v, want accepted failure and remote error", notifications)
			}
			if remote.callCount() != 1 {
				t.Fatalf("remote upload calls=%d, want one", remote.callCount())
			}
			assertStoredTypes(t, savedKindsForTest(savedNotification), failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool)
			assertFileBytes(t, source, payload)
			assertFileBytes(t, retainedPath, conflictingPayload)
			assertFileBytes(t, failedPath, payload)
			assertFileBytes(t, successfulPath, payload)
			beforeRetry, err := os.Stat(successfulPath)
			if err != nil {
				t.Fatal(err)
			}

			// checkpointを設定していないため、次回は現存する両退避先を再確認します。
			// remote送信や新たな衝突保全・通知は繰り返しません。
			notificationsBeforeRetry := len(notifications)
			if err := managed.Upload(context.Background(), uploadertest.Request(t, source)); err != nil {
				t.Fatalf("retry Upload error = %v", err)
			}
			if len(notifications) != notificationsBeforeRetry {
				t.Fatalf("notifications after retry = %+v, want no new notification for clean reuse", notifications)
			}
			assertFileBytes(t, archivePath, payload)
			assertFileBytes(t, spoolPath, payload)
			assertFileBytes(t, source, payload)
			assertFileBytes(t, retainedPath, conflictingPayload)
			afterRetry, err := os.Stat(successfulPath)
			if err != nil {
				t.Fatal(err)
			}
			if !os.SameFile(beforeRetry, afterRetry) {
				t.Fatal("retry replaced the copy already saved successfully")
			}
			if remote.callCount() != 1 {
				t.Fatalf("retry repeated remote upload: calls=%d", remote.callCount())
			}
		})
	}
}

func TestManagedUploaderReportsPreservedConflictWhenAnotherFailureStoreFails(t *testing.T) {
	base := t.TempDir()
	source, payload := writeCompressedLog(t, base, "app_20260915_1.log.gz")
	archiveBase, spoolBase := filepath.Join(base, "archive"), filepath.Join(base, "spool")
	archivePath := filepath.Join(newArchiveRoot(t, archiveBase).RetryableDir(), filepath.Base(source))
	remote := &testDestinationUploader{
		ids:  []objectstorage.DestinationID{"destination-a"},
		errs: map[objectstorage.DestinationID]error{"destination-a": errors.New("remote unavailable")},
	}
	var injectionErr error
	remote.afterUpload = func() {
		// 送信前の既存ファイル確認後、保存処理の前に競合を発生させます。
		injectionErr = os.WriteFile(archivePath, []byte("old archive content"), 0o600)
	}
	var notifications []Notification
	managed := newTestManagedUploader(t, ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(archiveBase, []failurestore.DurableType{
			failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool,
		})),
		SpoolRuntimes: newTestSpoolRuntimes(t, spoolBase),
		FileName:      logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
		Notifier: func(_ context.Context, notification Notification) {
			notifications = append(notifications, notification)
		},
	}, remote)
	if err := archiveRouteForTest(t, managed, "destination-a").store.EnsureDirectories(context.Background()); err != nil {
		t.Fatal(err)
	}
	stagingPath := filepath.Join(newSpoolRoot(t, spoolBase).Path(), ".staging")
	if err := os.Remove(stagingPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stagingPath, []byte("blocks spool saves"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := managed.Upload(context.Background(), uploadertest.Request(t, source)); err == nil {
		t.Fatal("Upload() succeeded despite the spool save failure")
	}
	if injectionErr != nil {
		t.Fatal(injectionErr)
	}
	if len(notifications) != 1 || notifications[0].SaveErr == nil || len(notifications[0].ConflictRenames) != 1 {
		t.Fatalf("notifications = %+v, want failure plus one conflict rename", notifications)
	}
	assertFileBytes(t, notifications[0].ConflictRenames[0].RetainedPath, []byte("old archive content"))
	assertFileBytes(t, archivePath, payload)
	assertFileBytes(t, source, payload)
}
