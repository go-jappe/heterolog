package compressedlogfailure

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

const thirdTestDurableType failurestore.DurableType = "third-test-store"

type testSavedThirdTarget struct{ target string }

func (t testSavedThirdTarget) Kind() failurestore.DurableType       { return thirdTestDurableType }
func (t testSavedThirdTarget) Root() string                         { return t.target }
func (t testSavedThirdTarget) Validate() error                      { return nil }
func (t testSavedThirdTarget) Clone() checkpoint.SavedFailureTarget { return t }
func (t testSavedThirdTarget) Equal(other checkpoint.SavedFailureTarget) bool {
	saved, ok := other.(testSavedThirdTarget)
	return ok && t == saved
}

type testThirdTargetReport struct{ target string }

func (testThirdTargetReport) Kind() failurestore.DurableType { return thirdTestDurableType }
func (r testThirdTargetReport) String() string               { return string(r.Kind()) + " " + r.target }

// testThirdFailureTargetはcopyを永続化し、設定したtargetが再利用と保存の両方に
// 参加するかをtestで観測できるようにします。
type testThirdFailureTarget struct {
	rootPath     string
	panicOnSave  bool
	panicOnReuse bool
	locks        atomic.Int32
	releases     atomic.Int32
}

func (t *testThirdFailureTarget) kind() failurestore.DurableType { return thirdTestDurableType }
func (t *testThirdFailureTarget) root() string                   { return t.rootPath }
func (t *testThirdFailureTarget) name() string                   { return "third test store" }
func (t *testThirdFailureTarget) close() error                   { return nil }
func (t *testThirdFailureTarget) requestForceClose()             {}
func (t *testThirdFailureTarget) preserve(ctx context.Context, sourcePath string) (string, error) {
	return infrafsstore.PreserveRegularFile(ctx, t.root(), sourcePath)
}
func (t *testThirdFailureTarget) lock(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.locks.Add(1)
	return func() { t.releases.Add(1) }, nil
}

func (t *testThirdFailureTarget) filePath(sourcePath string) string {
	return filepath.Join(t.rootPath, filepath.Base(sourcePath))
}

func (t *testThirdFailureTarget) file(sourcePath string) failureTargetFile {
	return failureTargetFile{
		saved: testSavedThirdTarget{target: t.root()},
		path:  t.filePath(sourcePath),
	}
}

func (t *testThirdFailureTarget) save(ctx context.Context, sourcePath string, _ uploader.FailureCategory) (failureTargetFile, error) {
	if err := ctx.Err(); err != nil {
		return failureTargetFile{}, err
	}
	if t.panicOnSave {
		panic("third test store save panic")
	}
	content, err := os.ReadFile(sourcePath)
	if err != nil {
		return failureTargetFile{}, err
	}
	if err := os.WriteFile(t.filePath(sourcePath), content, 0o600); err != nil {
		return failureTargetFile{}, err
	}
	return t.file(sourcePath), nil
}

func (t *testThirdFailureTarget) reuse(ctx context.Context, sourcePath string) (failureTargetFile, bool, error) {
	if err := ctx.Err(); err != nil {
		return failureTargetFile{}, false, err
	}
	if t.panicOnReuse {
		panic("third test store reuse panic")
	}
	content, err := os.ReadFile(t.filePath(sourcePath))
	if errors.Is(err, os.ErrNotExist) {
		return failureTargetFile{}, false, nil
	}
	if err != nil {
		return failureTargetFile{}, false, err
	}
	sourceContent, err := os.ReadFile(sourcePath)
	if err != nil {
		return failureTargetFile{}, false, err
	}
	if !bytes.Equal(content, sourceContent) {
		return failureTargetFile{}, false, nil
	}
	return t.file(sourcePath), true, nil
}

func (t *testThirdFailureTarget) savedFileDetails(_ failureTargetFile) (SavedTargetDetails, error) {
	return testThirdTargetReport{target: t.root()}, nil
}

func TestFailureTargetReportRejectsWrongSavedType(t *testing.T) {
	wrong := failureTargetFile{saved: testSavedThirdTarget{target: "/failure/other"}}
	spool, err := (spoolFailureTarget{}).savedFileDetails(wrong)
	var typeErr *SavedTargetTypeError
	require.ErrorAs(t, err, &typeErr)
	require.Equal(t, failurestore.DurableTypeSpool, typeErr.Kind)
	require.Equal(t, wrong.saved, typeErr.Saved)
	require.Nil(t, spool)
	archive, err := (archiveFailureTarget{}).savedFileDetails(wrong)
	require.ErrorAs(t, err, &typeErr)
	require.Equal(t, failurestore.DurableTypeFileArchive, typeErr.Kind)
	require.Equal(t, wrong.saved, typeErr.Saved)
	require.Nil(t, archive)
}

func newUploaderWithThirdFailureTarget(t *testing.T, remote *testDestinationUploader) (*ManagedUploader, *testThirdFailureTarget, string, []byte) {
	t.Helper()
	base := t.TempDir()
	source, payload := writeCompressedLog(t, base, "app_20260930_1.log.gz")
	managed := newTestManagedUploader(t, ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{
			failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool,
		})),
		SpoolRuntimes: newTestSpoolRuntimes(t, filepath.Join(base, "spool")),
		FileName:      logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
	}, remote)
	third := &testThirdFailureTarget{rootPath: t.TempDir()}
	for _, destination := range remote.ids {
		managed.failureTargets[destination] = append(managed.failureTargets[destination], third)
	}
	return managed, third, source, payload
}

func TestManagedUploaderRequiresThirdFailureTargetForReuse(t *testing.T) {
	destination := objectstorage.DestinationID("destination-a")
	remote := &testDestinationUploader{ids: []objectstorage.DestinationID{destination}}
	managed, third, source, payload := newUploaderWithThirdFailureTarget(t, remote)
	archivePath := filepath.Join(archiveRouteForTest(t, managed, destination).store.Root().PermanentDir(), filepath.Base(source))
	spoolPath := filepath.Join(spoolRouteForTest(t, managed, destination).Root().DeadDir(), filepath.Base(source))
	require.NoError(t, os.WriteFile(archivePath, payload, 0o600))
	require.NoError(t, os.WriteFile(spoolPath, payload, 0o600))
	var notifications []Notification
	managed.notifier = func(_ context.Context, notification Notification) {
		notifications = append(notifications, notification)
	}

	// 一致するcopyが2つだけでは、3つのtargetを設定したhandoffは完了しません。
	require.NoError(t, managed.Upload(context.Background(), uploadertest.Request(t, source)))
	require.Equal(t, 1, remote.callCount())
	require.Empty(t, notifications)

	require.NoError(t, os.WriteFile(third.filePath(source), payload, 0o600))
	require.NoError(t, managed.Upload(context.Background(), uploadertest.Request(t, source)))
	require.Equal(t, 1, remote.callCount(), "all three copies should be reused without another upload")
	require.Empty(t, notifications, "reuse without a conflict does not need an notifier notification")
}

func TestManagedUploaderSavesToThirdFailureTargetAfterUploadFailure(t *testing.T) {
	destination := objectstorage.DestinationID("destination-a")
	remoteErr := errors.New("remote unavailable")
	remote := &testDestinationUploader{
		ids:  []objectstorage.DestinationID{destination},
		errs: map[objectstorage.DestinationID]error{destination: remoteErr},
	}
	managed, third, source, payload := newUploaderWithThirdFailureTarget(t, remote)
	var notifications []Notification
	managed.notifier = func(_ context.Context, notification Notification) {
		notifications = append(notifications, notification)
	}

	require.NoError(t, managed.Upload(context.Background(), uploadertest.Request(t, source)))
	require.NoError(t, managed.Close())
	require.Equal(t, 1, remote.callCount())
	require.Len(t, notifications, 1)
	require.ErrorIs(t, notifications[0].UploadErr, remoteErr)
	require.NoError(t, notifications[0].SaveErr)
	require.ElementsMatch(t, []failurestore.DurableType{
		failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool, thirdTestDurableType,
	}, savedKindsForTest(notifications[0]))
	assertFileBytes(t, archiveDetailsForTest(notifications[0]).FilePath, payload)
	assertFileBytes(t, spoolDetailsForTest(notifications[0]).FilePath, payload)
	assertFileBytes(t, third.filePath(source), payload)
}

func TestManagedUploaderReleasesThirdTargetLockAfterSavePanic(t *testing.T) {
	destination := objectstorage.DestinationID("destination-a")
	remoteErr := errors.New("remote unavailable")
	remote := &testDestinationUploader{
		ids:  []objectstorage.DestinationID{destination},
		errs: map[objectstorage.DestinationID]error{destination: remoteErr},
	}
	managed, third, source, _ := newUploaderWithThirdFailureTarget(t, remote)
	third.panicOnSave = true
	var notifications []Notification
	managed.notifier = func(_ context.Context, notification Notification) {
		notifications = append(notifications, notification)
	}

	err := managed.Upload(context.Background(), uploadertest.Request(t, source))
	requireRecoveredPanic(t, err, ErrDurableStoreSavePanic, "third test store save panic")
	require.Equal(t, 1, remote.callCount())
	require.Len(t, notifications, 1)
	require.ErrorIs(t, notifications[0].UploadErr, remoteErr)
	requireRecoveredPanic(t, notifications[0].SaveErr, ErrDurableStoreSavePanic, "third test store save panic")
	require.Positive(t, third.locks.Load(), "save acquired the target lock")
	require.Equal(t, third.locks.Load(), third.releases.Load(), "all target locks must be released before Upload returns")
	require.FileExists(t, source)
}

func TestManagedUploaderReleasesThirdTargetLockAfterReusePanic(t *testing.T) {
	destination := objectstorage.DestinationID("destination-a")
	remote := &testDestinationUploader{ids: []objectstorage.DestinationID{destination}}
	managed, third, source, payload := newUploaderWithThirdFailureTarget(t, remote)
	// ほかの退避先で確認を打ち切らないよう、確認順によらず一致するファイルを用意します。
	archivePath := filepath.Join(archiveRouteForTest(t, managed, destination).store.Root().PermanentDir(), filepath.Base(source))
	spoolPath := filepath.Join(spoolRouteForTest(t, managed, destination).Root().DeadDir(), filepath.Base(source))
	require.NoError(t, os.WriteFile(archivePath, payload, 0o600))
	require.NoError(t, os.WriteFile(spoolPath, payload, 0o600))
	third.panicOnReuse = true

	err := managed.Upload(context.Background(), uploadertest.Request(t, source))
	requireRecoveredPanic(t, err, ErrDestinationUploadPanic, "third test store reuse panic")
	require.Zero(t, remote.callCount())
	require.Equal(t, int32(1), third.locks.Load())
	require.Equal(t, third.locks.Load(), third.releases.Load(), "target lock must be released before Upload returns")
	require.FileExists(t, source)
}

// requireRecoveredPanicは、errがkindのPanicErrorとしてvalueのpanicを回復したことを確認します。
func requireRecoveredPanic(t *testing.T, err error, kind error, value any) {
	t.Helper()
	var panicErr *PanicError
	require.ErrorAs(t, err, &panicErr)
	require.ErrorIs(t, panicErr, kind)
	require.Equal(t, value, panicErr.Value)
}
