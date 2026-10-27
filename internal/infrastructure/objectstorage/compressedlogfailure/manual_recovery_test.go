package compressedlogfailure

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestManagedUploaderManualRecoverySavesWithoutMaintainingExistingFiles(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base := t.TempDir()
		archivePath, spoolPath := filepath.Join(base, "archive"), filepath.Join(base, "spool")
		archiveRoot, spoolRoot := newArchiveRoot(t, archivePath), newSpoolRoot(t, spoolPath)
		var existing []string
		for _, dir := range []string{
			archiveRoot.RetryableDir(), archiveRoot.PermanentDir(),
			spoolRoot.ActiveDir(), spoolRoot.ProcessingDir(), spoolRoot.DoneDir(),
			spoolRoot.DeadDir(), spoolRoot.QuarantineDir(), filepath.Join(spoolRoot.Path(), ".staging"),
		} {
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			path, _ := writeCompressedLog(t, dir, "app_20000101_1.log.gz")
			old := time.Now().Add(-365 * 24 * time.Hour)
			if err := os.Chtimes(path, old, old); err != nil {
				t.Fatal(err)
			}
			existing = append(existing, path)
		}
		failureCfg := testFailureStoreConfig(archivePath, []failurestore.DurableType{
			failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool,
		})
		archiveCfg := &failureCfg.DurableStoreConfig.FileArchiveConfig
		archiveCfg.LifecycleInterval = time.Second
		archiveCfg.Retryable = objectstorage.CompressedLogFileUploadFailureArchivePurgeConfig{Enabled: true, MaxFileAgeDays: 1, MaxDirSize: 1}
		archiveCfg.Permanent = archiveCfg.Retryable
		purge := objectstorage.CompressedLogFileUploadSpoolPurgeConfig{Enabled: true, MaxFileAgeDays: 1, MaxDirSize: 1}
		runtimes, err := compressedlog.NewRootRuntimes(compressedlog.NewConfigAdapters(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(objectstorage.CompressedLogFileUploadSpoolConfig{
			SpoolPath: spoolPath, Target: "heterolog", LoggerContract: pkglogger.LoggerContractSync.String(),
			RunnerInterval: time.Second, LifecycleInterval: time.Second, CircuitBreakerOpenCooldown: time.Second,
			Done: purge, Dead: purge, Quarantine: purge,
		}), []objectstorage.UploadTarget{objectstorage.UploadTargetS3})...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := runtimes.Close(); err != nil {
				t.Error(err)
			}
		})
		remote := &testDestinationUploader{
			ids:  []objectstorage.DestinationID{"destination-a"},
			errs: map[objectstorage.DestinationID]error{"destination-a": errors.New("remote unavailable")},
		}
		var receivedNotification Notification
		managed := newTestManagedUploader(t, ManagedUploaderConfig{
			DisableMaintenance:                  true,
			CompressedLogFileUploadFailureStore: NewConfigAdapter(failureCfg),
			SpoolRuntimes:                       runtimes,
			FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
			Notifier:                            func(_ context.Context, notification Notification) { receivedNotification = notification },
		}, remote)
		source, payload := writeCompressedLog(t, base, "app_20261007_2.log.gz")
		if err := managed.Upload(context.Background(), uploadertest.Request(t, source)); err != nil {
			t.Fatal(err)
		}
		// replayとpurgeのintervalを超えて時間を進めます。起動時のcleanupも禁止されています。
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if got := remote.callCount(); got != 1 {
			t.Fatalf("remote calls = %d, want only the explicit upload", got)
		}
		for _, path := range existing {
			assertFileBytes(t, path, payload)
		}
		assertStoredTypes(t, savedKindsForTest(receivedNotification), failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool)
		assertFileBytes(t, archiveDetailsForTest(receivedNotification).FilePath, payload)
		assertFileBytes(t, spoolDetailsForTest(receivedNotification).FilePath, payload)
	})
}

func TestManagedUploaderRejectsMixedArchiveMaintenanceModes(t *testing.T) {
	for _, firstDisabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal_first", true: "manual_first"}[firstDisabled], func(t *testing.T) {
			cfg := ManagedUploaderConfig{
				DisableMaintenance:                  firstDisabled,
				CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(t.TempDir(), []failurestore.DurableType{failurestore.DurableTypeFileArchive})),
				FileName:                            logpattern.FileNamePattern{Dir: t.TempDir(), Target: "app", Extension: ".log"},
			}
			remote := &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}}
			first := newTestManagedUploader(t, cfg, remote)
			cfg.DisableMaintenance = !firstDisabled
			if _, err := NewManagedUploader(cfg, remote); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("mixed-mode construction = %v, want invalid config", err)
			}
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}
			newTestManagedUploader(t, cfg, remote)
		})
	}
}
