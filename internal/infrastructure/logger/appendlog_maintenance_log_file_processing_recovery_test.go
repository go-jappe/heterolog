package logger

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	logfilerunner "github.com/go-jappe/heterolog/internal/infrastructure/logfile/runner"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// maintenanceRecoveryCheckpointStoreは初回更新を失敗させ、再試行を障害解消まで待たせます。
type maintenanceRecoveryCheckpointStore struct {
	checkpointstore.Store
	recordCalls              atomic.Int32
	started                  chan struct{}
	stopped                  chan struct{}
	abort                    chan struct{}
	normalDone               chan struct{}
	closeCalls               atomic.Int32
	closedBeforeAcceptedWork atomic.Bool
}

type maintenanceNotificationCheckpointStore struct {
	checkpointstore.Store
	closed            chan struct{}
	notificationsDone chan struct{}
}

func (s *maintenanceNotificationCheckpointStore) Close() error {
	close(s.closed)
	return nil
}

func (s *maintenanceNotificationCheckpointStore) NotificationsDone() <-chan struct{} {
	return s.notificationsDone
}

type maintenanceExternalRunReporter struct{}

func (maintenanceExternalRunReporter) ReportRun(context.Context, logfilelifecycle.RunReport, error) {}

func TestAppendLogMaintenanceRuntimeWaitsForCheckpointNotificationsRegardlessOfReporter(t *testing.T) {
	for _, test := range []struct {
		name     string
		reporter logfilelifecycle.RunReporter
	}{
		{"internal", &appendLogMaintenanceResultNotifier{}},
		{"external", maintenanceExternalRunReporter{}},
		{"absent", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				directory := t.TempDir()
				checkpoint := &maintenanceNotificationCheckpointStore{
					closed: make(chan struct{}), notificationsDone: make(chan struct{}),
				}
				finishNotifications := sync.OnceFunc(func() { close(checkpoint.notificationsDone) })
				defer finishNotifications()
				manager := logfilelifecycle.NewFileSeriesManager(logfilelifecycle.FileSeriesManagerOptions{
					FileName: logpattern.FileNamePattern{Dir: directory, Target: "app", Extension: "log"},
					Config: appendlog.NewConfigAdapter(pkgfsstore.Config{
						Layout: pkgfsstore.FsstoreConfig{Target: "app", LogPath: directory, LogDir: "logs",
							FileNameFormat: pkgfsstore.FileNameFormat, FileDateFormat: pkgfsstore.FileDateFormat},
						Lifecycle: pkgfsstore.FileLifecycleConfig{EnableRotate: true, RotationType: pkgfsstore.FileRotationDaily},
					}),
					Uploader: &maintenanceRecoveryUploader{}, CheckpointStore: checkpoint,
				})
				lifecycle, err := logfilelifecycle.NewDirectoryLifecycle(manager)
				require.NoError(t, err)
				runtime := &appendLogMaintenanceRuntime{entries: []appendLogMaintenanceRuntimeEntry{{
					owner: "checkpoint notifications", runner: &logfilerunner.Runner{
						DirectoryLifecycle: lifecycle, Reporter: test.reporter,
					},
				}}}
				closed := make(chan error, 1)
				go func() { closed <- runtime.Close() }()
				<-checkpoint.closed
				synctest.Wait()
				select {
				case <-closed:
					t.Fatal("runtime returned before its accepted checkpoint notifications drained")
				default:
				}
				finishNotifications()
				require.NoError(t, <-closed)
			})
		})
	}
}

func (s *maintenanceRecoveryCheckpointStore) Close() error {
	s.closeCalls.Add(1)
	select {
	case <-s.stopped:
	default:
		s.closedBeforeAcceptedWork.Store(true)
	}
	select {
	case <-s.normalDone:
	default:
		s.closedBeforeAcceptedWork.Store(true)
	}
	return s.Store.Close()
}

// errInitialCheckpointPersistenceは、maintenanceRecoveryCheckpointStoreが最初の
// checkpoint保存で返す失敗です。
var errInitialCheckpointPersistence = errors.New("initial checkpoint persistence failed")

func (s *maintenanceRecoveryCheckpointStore) AdvanceCompletionCursor(
	ctx context.Context,
	key checkpointstore.CheckpointKey,
	destinations []objectstorage.DestinationID,
) (checkpointstore.Checkpoint, error) {
	if s.recordCalls.Add(1) == 1 {
		return checkpointstore.Checkpoint{}, errInitialCheckpointPersistence
	}
	close(s.started)
	defer close(s.stopped)
	select {
	case <-ctx.Done():
		return checkpointstore.Checkpoint{}, ctx.Err()
	case <-s.abort:
		return s.Store.AdvanceCompletionCursor(ctx, key, destinations)
	}
}

type maintenanceRecoveryUploader struct {
	uploadCalls atomic.Int32
}

func (*maintenanceRecoveryUploader) DestinationIDs() []objectstorage.DestinationID {
	return []objectstorage.DestinationID{"archive"}
}

func (u *maintenanceRecoveryUploader) Upload(ctx context.Context, input uploader.UploadRequest) error {
	return u.UploadDestinations(ctx, input, u.DestinationIDs()).Err()
}

func (u *maintenanceRecoveryUploader) UploadDestinations(
	_ context.Context,
	_ uploader.UploadRequest,
	destinations []objectstorage.DestinationID,
) uploader.UploadReport {
	u.uploadCalls.Add(1)
	report := uploader.UploadReport{}
	for _, destination := range destinations {
		report.Results = append(report.Results, uploader.DestinationResult{DestinationID: destination})
	}
	return report
}

// 復旧の再試行は固定間隔のtimerで始まるため、synctestの仮想時刻で待ちます。
// 「まだ戻らない」ことはsynctest.Waitで全goroutineの停止を待ってから確認し、
// time.Afterは仮想時刻上のhang検出としてだけ使います。
func TestAppendLogMaintenanceRuntimeCloseWaitsForAcceptedWorkAndRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		const target = "maintenance-recovery"
		directory := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(directory, 0o750))
		period := time.Now().UTC().Format(pkgfsstore.FileDateFormat)
		compressedPath := filepath.Join(directory, target+"_"+period+"_2.log.gz")
		var compressed bytes.Buffer
		writer := gzip.NewWriter(&compressed)
		_, err := writer.Write([]byte("upload must not repeat during shutdown"))
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		require.NoError(t, os.WriteFile(compressedPath, compressed.Bytes(), 0o600))
		require.NoError(t, os.WriteFile(filepath.Join(directory, target+"_"+period+"_3.log"), []byte("active"), 0o600))

		checkpointDirectory := t.TempDir()
		require.NoError(t, checkpointstore.PrepareDirectory(checkpointDirectory))
		remote := &maintenanceRecoveryUploader{}
		compressedStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, "maintenance-recovery")
		require.NoError(t, err)
		// 宛先はこのファイルの作成前から有効だった状態にします。
		require.NoError(t, compressedStore.RegisterDestinations(ctx, remote.DestinationIDs(), nil))
		checkpoint := &maintenanceRecoveryCheckpointStore{
			Store: compressedStore, started: make(chan struct{}), stopped: make(chan struct{}), abort: make(chan struct{}),
			normalDone: make(chan struct{}),
		}
		manager := logfilelifecycle.NewFileSeriesManager(logfilelifecycle.FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
			Config: appendlog.NewConfigAdapter(pkgfsstore.Config{
				Layout: pkgfsstore.FsstoreConfig{
					Target: target, LogPath: root, LogDir: "logs",
					FileNameFormat: pkgfsstore.FileNameFormat, FileDateFormat: pkgfsstore.FileDateFormat,
				},
				Lifecycle: pkgfsstore.FileLifecycleConfig{
					EnableRotate: true, RotationType: pkgfsstore.FileRotationDaily,
					MaxFileBackups: 10, MaxFileSize: 1024, CompressFormat: pkgfsstore.CompressGzip,
				},
			}),
			Uploader: remote, CheckpointStore: checkpoint,
		})
		lifecycle, err := logfilelifecycle.NewDirectoryLifecycle(manager)
		require.NoError(t, err)
		runtime := &appendLogMaintenanceRuntime{entries: []appendLogMaintenanceRuntimeEntry{{
			owner: "log file processing recovery test", runner: &logfilerunner.Runner{DirectoryLifecycle: lifecycle},
		}}}
		finishNormal := make(chan struct{})
		releaseNormal := sync.OnceFunc(func() { close(finishNormal) })
		resumeRecovery := sync.OnceFunc(func() { close(checkpoint.abort) })
		t.Cleanup(func() {
			releaseNormal()
			resumeRecovery()
			require.NoError(t, runtime.Close())
		})
		_, err = lifecycle.RunOnce(ctx)
		require.ErrorIs(t, err, errInitialCheckpointPersistence)
		select {
		case <-checkpoint.started:
		case <-time.After(8 * time.Second):
			t.Fatal("independent file operation retry did not start")
		}

		// 通常処理が終了してから復旧をdrainします。どちらかが残る間はStoreを閉じません。
		runtime.wg.Add(1)
		go func() {
			<-finishNormal
			close(checkpoint.normalDone)
			runtime.wg.Done()
		}()
		closed := make(chan error, 1)
		go func() { closed <- runtime.Close() }()
		synctest.Wait()
		select {
		case <-checkpoint.stopped:
			t.Fatal("runtime Close canceled accepted recovery")
		case <-closed:
			t.Fatal("runtime Close returned before accepted work finished")
		default:
		}
		require.Zero(t, checkpoint.closeCalls.Load(), "accepted normal work still owns the checkpoint Store")
		releaseNormal()
		<-checkpoint.normalDone
		synctest.Wait()
		select {
		case <-closed:
			t.Fatal("runtime Close returned before recovery succeeded")
		case <-checkpoint.stopped:
			t.Fatal("runtime Close canceled recovery instead of draining it")
		default:
		}
		resumeRecovery()
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("runtime Close did not complete after successful recovery")
		}
		select {
		case <-checkpoint.stopped:
		default:
			t.Fatal("runtime Close returned while log file processing recovery was still using its dependencies")
		}
		require.EqualValues(t, 1, remote.uploadCalls.Load())
		require.EqualValues(t, 1, checkpoint.closeCalls.Load())
		require.False(t, checkpoint.closedBeforeAcceptedWork.Load())
		require.NoFileExists(t, compressedPath)
		appendlog.DirectoryGuardForPath(directory).WithProtectedPaths(func(paths map[string]struct{}) {
			require.NotContains(t, paths, compressedPath, "recovery protection must be released before runtime close finishes")
		})
	})
}
