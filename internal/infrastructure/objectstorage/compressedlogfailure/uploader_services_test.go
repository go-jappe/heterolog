package compressedlogfailure

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	spooluploader "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/uploader"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestManagedUploaderBuildsArchiveAndObserverForEachDestinationService(t *testing.T) {
	base := t.TempDir()
	source, payload := writeCompressedLog(t, base, "app_20261001_1.log.gz")
	// IDの表記からbackendを推測しないことも確認します。
	services := map[objectstorage.DestinationID]objectstorage.UploadTarget{
		"opaque-one":    objectstorage.UploadTargetS3,
		"s3-looking-id": "another-backend",
	}
	remote := &testDestinationUploader{
		ids:      []objectstorage.DestinationID{"opaque-one", "s3-looking-id"},
		services: services,
		errs: map[objectstorage.DestinationID]error{
			"opaque-one": errors.New("unavailable"), "s3-looking-id": errors.New("unavailable"),
		},
	}
	var observerRoots []string
	managed := newTestManagedUploader(t, ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive})),
		FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
		ArchivePurgeObserverForRoot: func(root compressedlogarchive.Root) ArchivePurgeObserver {
			observerRoots = append(observerRoots, root.Path())
			return nil
		},
	}, remote)
	if err := managed.Upload(context.Background(), uploadertest.Request(t, source)); err != nil {
		t.Fatal(err)
	}
	for index, destination := range remote.ids {
		root, err := compressedlogarchive.NewRoot(filepath.Join(base, "archive"), services[destination], pkglogger.LoggerContractSync)
		if err != nil {
			t.Fatal(err)
		}
		root, err = root.ForDestination(destination)
		if err != nil {
			t.Fatal(err)
		}
		if got := archiveRouteForTest(t, managed, destination).store.Root().Path(); got != root.Path() {
			t.Fatalf("destination %q archive root = %q, want %q", destination, got, root.Path())
		}
		if observerRoots[index] != root.Path() {
			t.Fatalf("destination %q observer root = %q, want %q", destination, observerRoots[index], root.Path())
		}
		assertFileBytes(t, filepath.Join(root.RetryableDir(), filepath.Base(source)), payload)
	}
}

func TestManagedUploaderRejectsMissingDestinationService(t *testing.T) {
	remote := &testDestinationUploader{
		ids:      []objectstorage.DestinationID{"destination-a"},
		services: map[objectstorage.DestinationID]objectstorage.UploadTarget{},
	}
	_, err := NewManagedUploader(ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(t.TempDir(), []failurestore.DurableType{failurestore.DurableTypeFileArchive})),
		FileName:                            logpattern.FileNamePattern{Dir: t.TempDir(), Target: "app", Extension: ".log"},
	}, remote)
	if !errors.Is(err, ErrInvalidConfig) || !errors.Is(err, ErrNoDestinationService) {
		t.Fatalf("missing service error = %v", err)
	}
}

func TestManagedUploaderSelectsSpoolRuntimeAndAdapterByDestinationService(t *testing.T) {
	for _, test := range []struct {
		name            string
		service         objectstorage.UploadTarget
		runtimeService  objectstorage.UploadTarget
		wantMismatch    bool
		wantUnsupported bool
	}{
		{name: "matching s3", service: objectstorage.UploadTargetS3, runtimeService: objectstorage.UploadTargetS3},
		{name: "wrong runtime", service: "another-backend", runtimeService: objectstorage.UploadTargetS3, wantMismatch: true},
		{name: "unsupported adapter", service: "another-backend", runtimeService: "another-backend", wantUnsupported: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := objectstorage.CompressedLogFileUploadSpoolConfig{
				SpoolPath: t.TempDir(), Target: "heterolog", LoggerContract: pkglogger.LoggerContractSync.String(),
				RunnerInterval: time.Hour, LifecycleInterval: time.Hour, CircuitBreakerOpenCooldown: time.Minute,
			}
			runtimes, err := compressedlog.NewRootRuntimes(compressedlog.NewConfigAdapters(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), []objectstorage.UploadTarget{test.runtimeService})...)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := runtimes.Close(); err != nil {
					t.Error(err)
				}
			})
			runtime := runtimes[test.runtimeService]
			remote := &testDestinationUploader{
				ids:      []objectstorage.DestinationID{"destination-a"},
				services: map[objectstorage.DestinationID]objectstorage.UploadTarget{"destination-a": test.service},
			}
			var observedRuntime *compressedlog.RootRuntime
			managed, err := NewManagedUploader(ManagedUploaderConfig{
				CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(t.TempDir(), []failurestore.DurableType{failurestore.DurableTypeSpool})),
				SpoolRuntimes:                       compressedlog.RootRuntimes{test.service: runtime},
				FileName:                            logpattern.FileNamePattern{Dir: t.TempDir(), Target: "app", Extension: ".log"},
				SpoolRunObserverForRoot: func(root *compressedlog.RootRuntime) compressedlog.ManagedRunObserver {
					observedRuntime = root
					return nil
				},
			}, remote)
			if test.wantMismatch || test.wantUnsupported {
				var mismatch *SpoolRuntimeMismatchError
				switch {
				case !errors.Is(err, ErrInvalidConfig):
					t.Fatalf("construction error = %v, want ErrInvalidConfig", err)
				case test.wantMismatch:
					if !errors.As(err, &mismatch) || mismatch.Destination != "destination-a" || mismatch.Service != test.service {
						t.Fatalf("construction error = %v, want no spool runtime for service %q", err, test.service)
					}
				default:
					var unsupported *spooluploader.UnsupportedUploadServiceError
					if errors.As(err, &mismatch) || !errors.Is(err, spooluploader.ErrUnsupportedUploadService) ||
						!errors.As(err, &unsupported) || unsupported.Service != test.service {
						t.Fatalf("construction error = %v, want unsupported spool upload service %q", err, test.service)
					}
				}
				if remote.callCount() != 0 {
					t.Fatal("invalid construction attempted upload")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			owner := managed.(*ManagedUploader)
			t.Cleanup(func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			})
			if observedRuntime != runtime {
				t.Fatal("spool observer received another backend's runtime")
			}
			if got := spoolRouteForTest(t, owner, "destination-a").Root().Service(); got != test.service {
				t.Fatalf("spool service = %q, want %q", got, test.service)
			}
		})
	}
}

var _ uploader.DestinationServiceProvider = (*testDestinationUploader)(nil)
