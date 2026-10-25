//go:build !windows

package compressedlogfailure

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestManagedUploaderReportsUnconfirmedDurabilityWithoutAcceptingSource(t *testing.T) {
	for _, contract := range []pkglogger.LoggerContractKind{pkglogger.LoggerContractSync, pkglogger.LoggerContractDelivery} {
		t.Run(contract.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				base := t.TempDir()
				source, payload := writeCompressedLog(t, base, "app_20260911_1.log.gz")
				remoteErr := errors.New("temporary remote outage")
				remote := &testDestinationUploader{
					ids:  []objectstorage.DestinationID{"destination-a"},
					errs: map[objectstorage.DestinationID]error{"destination-a": remoteErr},
				}
				cfg := testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive})
				cfg.DurableStoreConfig.FileArchiveConfig.LoggerContract = contract.String()
				// 初回の保守完了後は、この短い試験中に次の周期処理が始まらない設定にします。
				cfg.DurableStoreConfig.FileArchiveConfig.LifecycleInterval = time.Hour
				var receivedNotification Notification
				observationCount := 0
				managed := newTestManagedUploader(t, ManagedUploaderConfig{
					CompressedLogFileUploadFailureStore: NewConfigAdapter(cfg),
					FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
					Notifier: func(_ context.Context, notification Notification) {
						receivedNotification = notification
						observationCount++
					},
				}, remote)
				// 起動直後の保守処理がfixtureのdirectoryを先に作り直さないよう、
				// 次の周期を待つ状態になるまで進めます。時刻は進めません。
				synctest.Wait()
				root := archiveRouteForTest(t, managed, "destination-a").store.Root()
				// 初期化済みStoreはlayoutを再作成しないため、初回初期化の親sync失敗を
				// このfixture専用の未初期化Storeで起こします。共有保守の初回完了後に
				// 差し替え、Publish自体は通常のhandoff排他と通知経路を通します。
				uninitializedStore, storeErr := compressedlogarchive.NewStore(root)
				if storeErr != nil {
					t.Fatal(storeErr)
				}
				archive := archiveRouteForTest(t, managed, "destination-a")
				archive.store = uninitializedStore
				managed.failureTargets["destination-a"] = []failureTarget{archiveFailureTarget{route: archive}}
				// 空の保存先を作り直し、親をmkdir可能・同期用open不可にします。
				// 製品の関数を差し替えず、実際の権限エラーで初回の保存失敗を起こします。
				if err := os.Remove(root.RetryableDir()); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(root.Path(), 0o300); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := os.Chmod(root.Path(), 0o750); err != nil {
						t.Error(err)
					}
					if err := archiveRouteForTest(t, managed, "destination-a").store.EnsureDirectories(context.Background()); err != nil {
						t.Error(err)
					}
				})
				if err := infrafsstore.SyncDirectory(root.Path()); err == nil {
					t.Skip("current user or filesystem bypasses Unix directory read permissions")
				} else if !errors.Is(err, os.ErrPermission) {
					t.Fatal(err)
				}
				err := managed.Upload(context.Background(), uploadertest.Request(t, source))
				if !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) || !errors.Is(err, remoteErr) {
					t.Fatalf("initial upload error = %v, want remote and durability errors", err)
				}
				if !errors.Is(receivedNotification.SaveErr, pkgfsstore.ErrDurabilityUnconfirmed) || !errors.Is(receivedNotification.SaveErr, os.ErrPermission) {
					t.Fatalf("handoff error = %v, want identifiable durability and cause errors", receivedNotification.SaveErr)
				}
				if observationCount != 1 || len(savedKindsForTest(receivedNotification)) != 0 || archiveDetailsForTest(receivedNotification).FilePath != "" {
					t.Fatalf("unconfirmed save accepted as durable or not reported once: count=%d report=%+v", observationCount, receivedNotification)
				}
				assertFileBytes(t, source, payload)
				assertDirectoryEmpty(t, root.RetryableDir())

				// 親の権限もremoteの失敗も直さず再試行します。前回作成したdirectoryの親を
				// 同期できない間は、保存先が作成済みでも完了を返さず、元fileを保持させます。
				report := managed.UploadDestinations(context.Background(), uploadertest.Request(t, source), remote.DestinationIDs())
				if err := report.Err(); !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) || !errors.Is(err, os.ErrPermission) {
					t.Fatalf("retry while parent is unsyncable error = %v, want unconfirmed durability", err)
				}
				if len(report.Results) != 1 || report.Results[0].DestinationID != "destination-a" || report.Results[0].Succeeded() {
					t.Fatalf("retry while parent is unsyncable reported a durable destination result: %+v", report)
				}
				var syncErr *infrafsstore.DirectorySyncError
				if observationCount != 2 || !errors.As(receivedNotification.SaveErr, &syncErr) || syncErr.Directory != root.Path() ||
					!errors.Is(receivedNotification.UploadErr, remoteErr) || remote.callCount() != 1 {
					t.Fatalf("retry handoff = %+v, observations=%d remote attempts=%d, want unconfirmed parent %q",
						receivedNotification, observationCount, remote.callCount(), root.Path())
				}
				if len(savedKindsForTest(receivedNotification)) != 0 || archiveDetailsForTest(receivedNotification).FilePath != "" {
					t.Fatalf("unconfirmed retry accepted as durable: %+v", receivedNotification)
				}
				assertFileBytes(t, source, payload)
				assertDirectoryEmpty(t, root.RetryableDir())
				if err := infrafsstore.SyncDirectory(root.Path()); !errors.Is(err, os.ErrPermission) {
					t.Fatalf("retry silently changed parent permissions: %v", err)
				}

				// 親を同期できるようになれば、記録した同期を先に再試行し、
				// 今回の圧縮ファイルの保存・同期が成功してからproducerへ完了を返します。
				if err := os.Chmod(root.Path(), 0o750); err != nil {
					t.Fatal(err)
				}
				report = managed.UploadDestinations(context.Background(), uploadertest.Request(t, source), remote.DestinationIDs())
				if err := report.Err(); err != nil {
					t.Fatalf("retry after parent sync recovery: %v", err)
				}
				if len(report.Results) != 1 || report.Results[0].DestinationID != "destination-a" || !report.Results[0].Succeeded() {
					t.Fatalf("producer did not receive completed destination result: %+v", report)
				}
				if observationCount != 3 || receivedNotification.SaveErr != nil || !errors.Is(receivedNotification.UploadErr, remoteErr) || remote.callCount() != 1 {
					t.Fatalf("retry handoff = %+v, observations=%d remote attempts=%d", receivedNotification, observationCount, remote.callCount())
				}
				assertStoredTypes(t, savedKindsForTest(receivedNotification), failurestore.DurableTypeFileArchive)
				if receivedNotification.FailureCategory != uploader.FailureCategoryRetryable || spoolDetailsForTest(receivedNotification).FilePath != "" {
					t.Fatalf("expected file archive acceptance, not spool replay: %+v", receivedNotification)
				}
				assertFileBytes(t, archiveDetailsForTest(receivedNotification).FilePath, payload)
				// fileArchiveへの保存完了でありremote送信成功ではありません。元fileの削除はproducerが担当します。
				assertFileBytes(t, source, payload)
			})
		})
	}
}
