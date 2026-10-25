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

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/retryafter"
	infras3 "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/s3"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgs3 "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
)

// scriptedRetryAfterUploaderは、呼び出しごとに指定したerrorを返し、以降の呼び出しは成功させます。
// 直接uploadとspoolからの再送の両方を受け取り、時刻と送信したfile名を記録します。
type scriptedRetryAfterUploader struct {
	mu    sync.Mutex
	errs  []error
	calls []scriptedRetryAfterUpload
}

type scriptedRetryAfterUpload struct {
	at   time.Time
	file string
}

func (u *scriptedRetryAfterUploader) DestinationIDs() []objectstorage.DestinationID {
	return []objectstorage.DestinationID{"destination-a"}
}

func (u *scriptedRetryAfterUploader) DestinationService(id objectstorage.DestinationID) (objectstorage.UploadTarget, bool) {
	return objectstorage.UploadTargetS3, id == "destination-a"
}

func (u *scriptedRetryAfterUploader) Upload(ctx context.Context, request uploader.UploadRequest) error {
	return u.UploadDestinations(ctx, request, u.DestinationIDs()).Err()
}

func (u *scriptedRetryAfterUploader) UploadDestinations(
	ctx context.Context,
	request uploader.UploadRequest,
	destinationIDs []objectstorage.DestinationID,
) uploader.UploadReport {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.calls = append(u.calls, scriptedRetryAfterUpload{at: time.Now(), file: filepath.Base(request.FilePath())})
	var err error
	if len(u.errs) > 0 {
		err, u.errs = u.errs[0], u.errs[1:]
	}
	report := uploader.UploadReport{Results: make([]uploader.DestinationResult, len(destinationIDs))}
	for index, id := range destinationIDs {
		report.Results[index] = uploader.DestinationResult{DestinationID: id, Err: err}
		if err != nil {
			report.Results[index].FailureCategory = infras3.ClassifyDurableUploadFailure(ctx, err).FailureCategory
		}
	}
	return report
}

func (u *scriptedRetryAfterUploader) recorded() []scriptedRetryAfterUpload {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]scriptedRetryAfterUpload(nil), u.calls...)
}

// retryAfterTestMaxRetryAfterは、試験の再送周期より長いRetry-Afterを短縮せずに扱う待機の上限です。
const retryAfterTestMaxRetryAfter = 3 * time.Hour

// newRetryAfterManagedUploaderは、宛先別spoolだけへ退避するuploaderと、その再送runtimeを作ります。
// 再送周期はnewTestSpoolRuntimesと同じで、待機の上限だけを延ばします。
func newRetryAfterManagedUploader(t *testing.T, base string, remote uploader.Uploader) (*ManagedUploader, *compressedlog.RootRuntime) {
	t.Helper()
	root, err := compressedlog.NewRootRuntime(compressedlog.NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(objectstorage.CompressedLogFileUploadSpoolConfig{
		SpoolPath: filepath.Join(base, "spool"), Target: "heterolog",
		LoggerContract: pkglogger.LoggerContractSync.String(),
		RunnerInterval: testSpoolRunnerInterval, LifecycleInterval: time.Hour, CircuitBreakerOpenCooldown: 5 * time.Minute,
		MaxRetryAfter: retryAfterTestMaxRetryAfter,
	}), objectstorage.UploadTargetS3))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Errorf("root Close: %v", err)
		}
	})
	runtimes := compressedlog.RootRuntimes{objectstorage.UploadTargetS3: root}
	managed := newTestManagedUploader(t, ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(
			filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeSpool},
		)),
		SpoolRuntimes: runtimes,
		FileName:      logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
	}, remote)
	return managed, runtimes[objectstorage.UploadTargetS3]
}

// 直接uploadが一時的な失敗とともにRetry-Afterを返した場合は、ファイルをspoolへ保存するときに
// 宛先の待機期限を記録・保存する。稼働中の再送workerも再起動後のworkerも、期限を持たない古い
// ファイルを含め、その宛先のファイルを期限まで送らない。
func TestManagedUploaderDirectUploadRetryAfterDefersSpoolReplay(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "running spool runtime"
		if restart {
			name = "after restart"
		}
		t.Run(name, func(t *testing.T) {
			start := time.Now()
			synctest.Test(t, func(t *testing.T) {
				// 実際のfilesystemが付ける更新時刻と、bubble内の時刻の前後関係を揃えます。
				time.Sleep(time.Until(start))
				base := t.TempDir()
				olderPath, _ := writeCompressedLog(t, base, "app_20260904_1.log.gz")
				throttledPath, _ := writeCompressedLog(t, base, "app_20260904_2.log.gz")
				remote := &scriptedRetryAfterUploader{errs: []error{
					errors.New("temporary outage without Retry-After"),
					&pkgs3.HTTPStatusError{Code: http.StatusTooManyRequests, RetryAfterDuration: 90 * time.Minute},
				}}
				managed, runtime := newRetryAfterManagedUploader(t, base, remote)
				if err := managed.Upload(context.Background(), uploadertest.Request(t, olderPath)); err != nil {
					t.Fatalf("Upload(older) error = %v, want the failure durably saved", err)
				}
				time.Sleep(time.Second)
				if err := managed.Upload(context.Background(), uploadertest.Request(t, throttledPath)); err != nil {
					t.Fatalf("Upload(throttled) error = %v, want the failure durably saved", err)
				}
				notBefore := time.Now().Add(90 * time.Minute)
				marker := filepath.Join(newSpoolRoot(t, filepath.Join(base, "spool")).Path(), retryafter.DirectoryName, "destination-a.json")
				if _, err := os.Stat(marker); err != nil {
					t.Fatalf("direct upload Retry-After marker: %v", err)
				}
				if restart {
					if err := errors.Join(managed.Close(), runtime.Close()); err != nil {
						t.Fatal(err)
					}
					newRetryAfterManagedUploader(t, base, remote)
				}

				// 最初の再送周期は期限より前に来ます。期限を持たない古いファイルも送りません。
				time.Sleep(testSpoolRunnerInterval)
				synctest.Wait()
				if calls := remote.recorded(); len(calls) != 2 {
					t.Fatalf("uploads before the direct-upload deadline = %+v, want only the two direct uploads", calls)
				}

				time.Sleep(2 * testSpoolRunnerInterval)
				synctest.Wait()
				calls := remote.recorded()
				if len(calls) != 4 || calls[2].file != filepath.Base(olderPath) || calls[3].file != filepath.Base(throttledPath) {
					t.Fatalf("uploads after the deadline = %+v, want the older file then the throttled file replayed", calls)
				}
				for _, call := range calls[2:] {
					if call.at.Before(notBefore) {
						t.Fatalf("replayed %s at %s before the deadline %s", call.file, call.at, notBefore)
					}
				}
			})
		})
	}
}
