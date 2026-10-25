package runner

import (
	"compress/gzip"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	infraobjectstorage "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// 周期制御の試験では、lifecycleの公開constructorへ設定とuploaderを渡します。
func testFileSeriesManagerConfig(root, target string, maxBackups, maxAge int, maxDirSize uint64, compress bool) appendlog.ConfigProvider {
	return appendlog.NewConfigAdapter(pkgfsstore.Config{
		Layout: pkgfsstore.FsstoreConfig{
			Target: target, LogPath: root, LogDir: "logs",
			FileNameFormat: pkgfsstore.FileNameFormat, FileDateFormat: pkgfsstore.FileDateFormat,
		},
		Lifecycle: pkgfsstore.FileLifecycleConfig{
			EnableRotate: true, RotationType: pkgfsstore.FileRotationDaily,
			MaxFileBackups: maxBackups, MaxFileAge: maxAge, MaxFileSize: 1024, MaxDirSize: maxDirSize,
			EnableFileCompress: compress, CompressFormat: pkgfsstore.CompressGzip,
		},
	})
}

func testFileSeriesUploadDependencies(t *testing.T, remote uploader.Uploader) (uploader.DestinationUploader, checkpointstore.Store) {
	t.Helper()
	destinations, ok := remote.(uploader.DestinationUploader)
	if !ok {
		multi, err := infraobjectstorage.NewMultiUploader(uploader.Destination{ID: "archive", Uploader: remote})
		require.NoError(t, err)
		destinations = multi
	}
	directory := resolvedTempDirForTest(t)
	require.NoError(t, checkpointstore.PrepareDirectory(directory))
	store, err := checkpointstore.NewFilesystemStore(directory, "runner-log-series")
	require.NoError(t, err)
	// 既存ファイルは新宛先の初期cursorで完了扱いにせず、送信対象に残します。
	require.NoError(t, store.RegisterDestinations(context.Background(), destinations.DestinationIDs(), nil))
	return destinations, store
}

type registrationFailingRunnerCheckpointStore struct {
	checkpointstore.Store
	err error
}

func (s *registrationFailingRunnerCheckpointStore) RegisterDestinations(context.Context, []objectstorage.DestinationID, []checkpointstore.CheckpointKey) error {
	return s.err
}

// currentLogDateは、現在時刻が属する期間をログfile名の日付で返します。lifecycleの既定の時計
// （clock.NewUTCClock）と同じUTCで求め、試験を実行するprocessのlocal time zoneに依存させません。
func currentLogDate() string { return time.Now().UTC().Format(pkgfsstore.FileDateFormat) }

func writeFileSeriesCheckpointLog(t *testing.T, directory, target, period string, index uint64) string {
	t.Helper()
	path := filepath.Join(directory, target+"_"+period+"_"+strconv.FormatUint(index, 10)+".log")
	require.NoError(t, os.WriteFile(path, []byte(filepath.Base(path)), 0o600))
	return path
}

func writeGzipFile(t *testing.T, path string, content []byte) {
	t.Helper()
	file, err := os.Create(path)
	require.NoError(t, err)
	writer := gzip.NewWriter(file)
	_, err = writer.Write(content)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, file.Close())
}

var errTestArchiveUpload = errors.New("test archive upload failed")

type failingUploader struct{ err error }

func (u failingUploader) Upload(context.Context, uploader.UploadRequest) error { return u.err }
