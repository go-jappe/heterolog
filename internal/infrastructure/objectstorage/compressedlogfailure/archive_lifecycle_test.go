package compressedlogfailure

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestArchiveLifecyclePurgesFreshFileAfterSaveLockReleased(t *testing.T) {
	root, err := compressedlogarchive.NewRoot(
		t.TempDir(), objectstorage.UploadTargetS3, pkglogger.LoggerContractSync,
	)
	if err != nil {
		t.Fatal(err)
	}
	root, err = root.ForDestination("destination-a")
	if err != nil {
		t.Fatal(err)
	}
	store, err := compressedlogarchive.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	type purgeResult struct {
		report compressedlogarchive.PurgeReport
		err    error
	}
	results := make(chan purgeResult, 3)
	owner, coordinator, err := acquireArchiveLifecycle(store, NewFileArchiveConfigAdapter(
		objectstorage.CompressedLogFileUploadFailureArchiveConfig{
			LifecycleInterval: time.Hour,
			Retryable: objectstorage.CompressedLogFileUploadFailureArchivePurgeConfig{
				Enabled: true, MaxFileAgeDays: 365, MaxDirSize: 1,
			},
		},
	), clock.NewUTCClock(), func(_ context.Context, report compressedlogarchive.PurgeReport, err error) {
		results <- purgeResult{report: report, err: err}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.close(); err != nil {
			t.Error(err)
		}
	})
	// 起動直後のpurgeを完了させ、次の周期を待たずに保存中の排他を検証します。
	select {
	case result := <-results:
		if result.err != nil {
			t.Fatalf("startup purge: %v", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("startup purge did not complete")
	}
	unlock, err := coordinator.lockPurgeDuringSave(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if unlock != nil {
			unlock()
		}
	}()
	source, _ := writeCompressedLog(t, t.TempDir(), "app_"+time.Now().UTC().Format("20060102")+"_1.log.gz")
	archived, err := store.Save(context.Background(), compressedlogarchive.SaveInput{
		SourcePath: source, FailureCategory: uploader.FailureCategoryRetryable,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	coordinator.runOnce(ctx)
	if result := <-results; !errors.Is(result.err, context.DeadlineExceeded) {
		t.Fatalf("purge during save = %v, want deadline while waiting for save lock", result.err)
	}
	if _, err := os.Lstat(archived.Path); err != nil {
		t.Fatalf("archive was removed while save lock was held: %v", err)
	}
	unlock()
	unlock = nil

	// 保存終了後は、設定された周期と同じ長さの保持猶予を設けず容量で削除します。
	coordinator.runOnce(context.Background())
	result := <-results
	if result.err != nil {
		t.Fatal(result.err)
	}
	retryable := result.report.ByFailureCategory[uploader.FailureCategoryRetryable]
	if retryable.DeletedFileCount != 1 || retryable.CapacityLimitExceeded {
		t.Fatalf("purge after save = %#v", retryable)
	}
	if _, err := os.Lstat(archived.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fresh over-capacity archive remains: %v", err)
	}
}

func TestArchiveLifecycleIsSharedByRootUntilLastOwnerCloses(t *testing.T) {
	root, err := compressedlogarchive.NewRoot(
		t.TempDir(),
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
	firstStore, err := compressedlogarchive.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	secondStore, err := compressedlogarchive.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	cfg := objectstorage.CompressedLogFileUploadFailureArchiveConfig{
		LifecycleInterval: time.Hour,
	}
	firstOwner, firstCoordinator, err := acquireArchiveLifecycle(firstStore, NewFileArchiveConfigAdapter(cfg), clock.NewUTCClock(), nil)
	if err != nil {
		t.Fatal(err)
	}
	secondOwner, secondCoordinator, err := acquireArchiveLifecycle(secondStore, NewFileArchiveConfigAdapter(cfg), clock.NewUTCClock(), nil)
	if err != nil {
		_ = firstOwner.close()
		t.Fatal(err)
	}
	if firstCoordinator != secondCoordinator {
		t.Fatal("same archive root acquired two lifecycle coordinators")
	}
	if firstCoordinator.store != secondCoordinator.store {
		t.Fatal("same archive root did not share the Store lock with lifecycle")
	}

	if err := firstOwner.close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstCoordinator.purgeLoopStopped:
		t.Fatal("archive lifecycle stopped while another owner remained")
	default:
	}
	archiveLifecycleRegistry.Lock()
	_, existsAfterFirstClose := archiveLifecycleRegistry.byRoot[pathidentity.ComparisonKey(root.Path())]
	archiveLifecycleRegistry.Unlock()
	if !existsAfterFirstClose {
		t.Fatal("archive lifecycle stopped while another owner remained")
	}
	if err := secondOwner.close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstCoordinator.purgeLoopStopped:
	default:
		t.Fatal("archive lifecycle remains running after its last owner closed")
	}
	select {
	case <-firstCoordinator.shutdownComplete:
	default:
		t.Fatal("archive shutdown was not completed after its last owner closed")
	}
	archiveLifecycleRegistry.Lock()
	_, existsAfterLastClose := archiveLifecycleRegistry.byRoot[pathidentity.ComparisonKey(root.Path())]
	archiveLifecycleRegistry.Unlock()
	if existsAfterLastClose {
		t.Fatal("archive lifecycle remains registered after its last owner closed")
	}
}

func TestArchiveLifecycleReplacementWaitsForHandoffAndStoppedCoordinatorRetirement(
	t *testing.T,
) {
	// bubble内で全goroutineが待機状態になってから、置き換えがまだ取得できないことを確かめます。
	synctest.Test(t, func(t *testing.T) {
		root, err := compressedlogarchive.NewRoot(
			t.TempDir(),
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
		firstStore, err := compressedlogarchive.NewStore(root)
		if err != nil {
			t.Fatal(err)
		}
		cfg := objectstorage.CompressedLogFileUploadFailureArchiveConfig{
			LifecycleInterval: time.Hour,
		}
		firstOwner, firstCoordinator, err := acquireArchiveLifecycle(
			firstStore,
			NewFileArchiveConfigAdapter(cfg),
			clock.NewUTCClock(),
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		lockedPurge, err := firstCoordinator.lockPurgeDuringSave(context.Background())
		if err != nil {
			_ = firstOwner.close()
			t.Fatal(err)
		}
		unlockPurge := sync.OnceFunc(lockedPurge)
		// 途中で失敗しても保存中の保護を解除し、古い管理処理を終了させます。
		t.Cleanup(unlockPurge)

		closeDone := make(chan error, 1)
		go func() { closeDone <- firstOwner.close() }()
		waitForArchiveShutdownStarted(t, firstCoordinator)

		// 受理済みpurgeが保存の終了を待っている場合も、その処理は中止しません。
		// 周期処理の停止状態にかかわらず、保存中は管理処理全体の終了を通知しません。
		select {
		case <-firstCoordinator.shutdownComplete:
			t.Fatal("archive shutdown completed while a save remained protected")
		default:
		}

		type acquireResult struct {
			owner       *archiveLifecycleOwner
			coordinator *sharedArchiveLifecycle
			err         error
		}
		acquired := make(chan acquireResult, 1)
		go func() {
			secondStore, createErr := compressedlogarchive.NewStore(root)
			if createErr != nil {
				acquired <- acquireResult{err: createErr}
				return
			}
			owner, coordinator, acquireErr := acquireArchiveLifecycle(secondStore, NewFileArchiveConfigAdapter(cfg), clock.NewUTCClock(), nil)
			acquired <- acquireResult{
				owner:       owner,
				coordinator: coordinator,
				err:         acquireErr,
			}
		}()
		replacementReceived := false
		// 途中で失敗しても、保存中の保護を解除した後に置き換えのownerを閉じ、bubbleを終了させます。
		t.Cleanup(func() {
			if !replacementReceived {
				if result := <-acquired; result.owner != nil {
					_ = result.owner.close()
				}
			}
		})
		t.Cleanup(unlockPurge)

		// 置き換えが古い管理処理の終了待ちに入るまで進めます。時刻は進めません。
		synctest.Wait()
		select {
		case result := <-acquired:
			replacementReceived = true
			if result.owner != nil {
				_ = result.owner.close()
			}
			t.Fatalf("replacement acquired before old handoff drained: %v", result.err)
		default:
		}
		archiveLifecycleRegistry.Lock()
		registeredWhileClosing := archiveLifecycleRegistry.byRoot[pathidentity.ComparisonKey(root.Path())]
		archiveLifecycleRegistry.Unlock()
		if registeredWhileClosing != firstCoordinator {
			t.Fatal("retiring coordinator was removed from registry before handoff drain")
		}

		unlockPurge()
		select {
		case err := <-closeDone:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("last owner Close did not finish after handoff drain")
		}
		var replacement acquireResult
		select {
		case replacement = <-acquired:
			replacementReceived = true
		case <-time.After(2 * time.Second):
			t.Fatal("replacement did not acquire after old coordinator retired")
		}
		if replacement.err != nil {
			t.Fatal(replacement.err)
		}
		if replacement.coordinator == firstCoordinator {
			_ = replacement.owner.close()
			t.Fatal("replacement reused a stopped coordinator")
		}
		if err := replacement.owner.close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestArchiveLifecycleRejectsInvalidPurgeConfigWhenRootIsShared(t *testing.T) {
	root, err := compressedlogarchive.NewRoot(
		t.TempDir(), objectstorage.UploadTargetS3, pkglogger.LoggerContractSync,
	)
	if err != nil {
		t.Fatal(err)
	}
	root, err = root.ForDestination("destination-a")
	if err != nil {
		t.Fatal(err)
	}
	store, err := compressedlogarchive.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	config := objectstorage.CompressedLogFileUploadFailureArchiveConfig{LifecycleInterval: time.Hour}
	owner, coordinator, err := acquireArchiveLifecycle(store, NewFileArchiveConfigAdapter(config), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.close(); err != nil {
			t.Error(err)
		}
	})

	for _, category := range []uploader.FailureCategory{
		uploader.FailureCategoryRetryable, uploader.FailureCategoryPermanent,
	} {
		t.Run(string(category), func(t *testing.T) {
			invalid := config
			if category == uploader.FailureCategoryRetryable {
				invalid.Retryable.Enabled = true
			} else {
				invalid.Permanent.Enabled = true
			}
			invalidOwner, shared, err := acquireArchiveLifecycle(store, NewFileArchiveConfigAdapter(invalid), nil, nil)
			if invalidOwner != nil {
				_ = invalidOwner.close()
			}
			if _, found := errortree.Find(err, func(got *compressedlogarchive.FailureCategoryPurgeConfigError) bool {
				return got.FailureCategory == category && errors.Is(got, compressedlogarchive.ErrInvalidPurgeMaxFileAgeDays)
			}); !found {
				t.Fatalf("shared root accepted invalid purge config: %v", err)
			}
			if invalidOwner != nil || shared != nil {
				t.Fatal("invalid config registered an archive lifecycle owner")
			}
			coordinator.mu.Lock()
			ownerCount := len(coordinator.purgeObservers)
			shutdownStarted := coordinator.shutdownStarted
			coordinator.mu.Unlock()
			if ownerCount != 1 || shutdownStarted {
				t.Fatalf("invalid config changed existing lifecycle: owners=%d, shutdown=%t", ownerCount, shutdownStarted)
			}
		})
	}
}

func waitForArchiveShutdownStarted(t *testing.T, coordinator *sharedArchiveLifecycle) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		coordinator.mu.Lock()
		shutdownStarted := coordinator.shutdownStarted
		coordinator.mu.Unlock()
		if shutdownStarted {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("archive lifecycle did not begin shutdown")
		}
		time.Sleep(time.Millisecond)
	}
}
