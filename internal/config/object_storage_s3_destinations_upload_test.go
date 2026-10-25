package config

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/uploader/compose"
	s3builder "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/uploader/s3"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// 外部S3は使わず、envから作った二つの接続先・認証情報・prefixが実際のPUTまで届くことを確認します。
func TestConfiguredS3DestinationsUploadIndependently(t *testing.T) {
	// Windowsの一時directoryは環境変数から決まるため、環境を消去する前に作成します。
	directory := t.TempDir()
	restoreProcessEnvAfterTest(t)
	clearProcessEnvForTest(t)
	setTestS3CommonEnvironment(t)
	t.Setenv("HETEROLOG_ENABLE_LOG_FILE_OBJECT_STORAGE_UPLOAD", "true")
	t.Setenv("HETEROLOG_LOG_FILE_OBJECT_STORAGE_UPLOAD_TYPE", "s3")
	const payload = "compressed file bytes"
	var primaryCalls, backupCalls atomic.Int32
	var backupAvailable atomic.Bool
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryCalls.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != payload || r.Method != http.MethodPut ||
			r.URL.Path != "/primary-bucket/logs/app_20260929_1.log.gz" {
			t.Error("primary received an unexpected upload")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		assert.Contains(t, r.Header.Get("Authorization"), "Credential=test-key/")
		w.WriteHeader(http.StatusOK)
	}))
	defer primary.Close()
	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backupCalls.Add(1)
		assert.Equal(t, "/backup-bucket/copy/app_20260929_1.log.gz", r.URL.Path)
		assert.Contains(t, r.Header.Get("Authorization"), "Credential=backup-key/")
		assert.Contains(t, r.Header.Get("Authorization"), "/us-east-1/s3/aws4_request")
		if !backupAvailable.Load() {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`<Error><Code>ServiceUnavailable</Code></Error>`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer backup.Close()
	t.Setenv("HETEROLOG_LOG_FILE_OBJECT_STORAGE_S3_DESTINATIONS", fmt.Sprintf(`[
		{"bucket":"primary-bucket","prefix":"logs","endpoint":%q,"force_path_style":true},
		{"bucket":"backup-bucket","prefix":"copy","endpoint":%q,"force_path_style":true,
		 "region":"us-east-1","access_key_id":"backup-key","secret_access_key":"backup-secret"}
	]`, primary.URL, backup.URL))
	cfg, err := loadLogFileObjectStorageUploadConfig(processEnvironmentForTest())
	require.NoError(t, err)
	targetConfig := objectstorage.NewUploadTargetConfigAdapter(cfg.Default)
	remote, err := compose.FromTargets(targetConfig, s3builder.Builders())
	require.NoError(t, err)
	ids, err := compose.DestinationIDsFromTargets(targetConfig, s3builder.DestinationIDResolvers())
	require.NoError(t, err)
	require.Len(t, ids, 2)
	file := filepath.Join(directory, "app_20260929_1.log.gz")
	require.NoError(t, os.WriteFile(file, []byte(payload), 0o600))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	destinationUploader := remote.(uploader.DestinationUploader)
	report := destinationUploader.UploadDestinations(ctx, uploadertest.Request(t, file), ids)
	require.Len(t, report.Results, 2)
	require.Equal(t, ids[0], report.Results[0].DestinationID)
	require.NoError(t, report.Results[0].Err)
	require.Equal(t, ids[1], report.Results[1].DestinationID)
	require.Error(t, report.Results[1].Err)
	backupAvailable.Store(true)
	report = destinationUploader.UploadDestinations(ctx, uploadertest.Request(t, file), []objectstorage.DestinationID{ids[1]})
	require.NoError(t, report.Err())
	require.EqualValues(t, 1, primaryCalls.Load())
	require.EqualValues(t, 2, backupCalls.Load())
}
