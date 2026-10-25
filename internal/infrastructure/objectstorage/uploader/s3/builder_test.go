package s3

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/uploader/compose"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	s3uploader "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
)

func TestBuildersRegistersS3Target(t *testing.T) {
	t.Parallel()

	builders := Builders()
	if builders[objectstorage.UploadTargetS3] == nil {
		t.Fatalf("Builders()[UploadTargetS3] = nil, want builder")
	}
}

func TestNewBuildsIdentifiedS3Destination(t *testing.T) {
	t.Parallel()

	cfg := objectstorage.UploadTargetConfig{
		S3: objectstorage.S3Config{
			Region:          "ap-northeast-1",
			Endpoint:        "https://s3.example.com",
			AccessKeyID:     "access-key",
			SecretAccessKey: "secret-key",
			Bucket:          "archive-bucket",
			Prefix:          "logs/default",
		},
	}
	wantID, err := s3uploader.ResolveDestinationID(cfg.S3)
	if err != nil {
		t.Fatalf("ResolveDestinationID() error = %v", err)
	}

	created, err := New(objectstorage.NewUploadTargetConfigAdapter(cfg))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	require.Len(t, created, 1)
	if created[0].ID != wantID {
		t.Fatalf("destination id = %q, want %q", created[0].ID, wantID)
	}
	if created[0].Service != objectstorage.UploadTargetS3 {
		t.Fatalf("destination service = %q, want s3", created[0].Service)
	}
	if created[0].Uploader == nil {
		t.Fatal("destination uploader = nil")
	}
}

func TestDestinationIDResolversMatchUploaderIdentityAndValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*objectstorage.S3Config)
		valid  bool
	}{
		{name: "default endpoint", valid: true},
		// regionの大文字小文字と前後の空白は受理します。宛先IDは表記差を同じ保存先とし、
		// clientは前後の空白だけを除いた入力値で署名します。
		{name: "normalized endpoint, prefix and region", valid: true, change: func(cfg *objectstorage.S3Config) {
			cfg.Endpoint, cfg.Prefix, cfg.Region = "HTTPS://S3.example.com/archive/", "/logs/error/", " AP-NORTHEAST-1 "
		}},
		{name: "missing region", change: func(cfg *objectstorage.S3Config) { cfg.Region = "" }},
		{name: "missing bucket", change: func(cfg *objectstorage.S3Config) { cfg.Bucket = "" }},
		{name: "missing access key", change: func(cfg *objectstorage.S3Config) { cfg.AccessKeyID = "" }},
		{name: "missing secret", change: func(cfg *objectstorage.S3Config) { cfg.SecretAccessKey = "" }},
		{name: "invalid endpoint", change: func(cfg *objectstorage.S3Config) { cfg.Endpoint = "not-a-url" }},
		// clientはendpointのqueryを全PUTへ付与するため、queryとfragmentは設定errorです。
		{name: "endpoint query", change: func(cfg *objectstorage.S3Config) { cfg.Endpoint = "https://s3.example.com/?b=2&a=1" }},
		{name: "endpoint empty query", change: func(cfg *objectstorage.S3Config) { cfg.Endpoint = "https://s3.example.com/?" }},
		{name: "endpoint fragment", change: func(cfg *objectstorage.S3Config) { cfg.Endpoint = "https://s3.example.com/#archive" }},
		{name: "invalid timeout", change: func(cfg *objectstorage.S3Config) { cfg.Timeout = -time.Second }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := objectstorage.UploadTargetConfig{
				Enabled: true, Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
				S3: objectstorage.S3Config{Region: "ap-northeast-1", Bucket: "archive", AccessKeyID: "test", SecretAccessKey: "test"},
			}
			if tc.change != nil {
				tc.change(&cfg.S3)
			}
			provider := objectstorage.NewUploadTargetConfigAdapter(cfg)
			ids, resolveErr := compose.DestinationIDsFromTargets(provider, DestinationIDResolvers())
			remote, buildErr := compose.FromTargets(provider, Builders())
			if !tc.valid {
				require.Error(t, resolveErr)
				require.Error(t, buildErr)
				require.Nil(t, ids)
				return
			}
			require.NoError(t, resolveErr)
			require.NoError(t, buildErr)
			require.Equal(t, remote.(uploader.DestinationUploader).DestinationIDs(), ids)
		})
	}
}

func TestNewBuildsUploaderThatAttemptsOnePut(t *testing.T) {
	for _, test := range []struct {
		name     string
		status   int
		code     string
		category uploader.FailureCategory
	}{
		{"service unavailable", http.StatusServiceUnavailable, "ServiceUnavailable", uploader.FailureCategoryRetryable},
		{"access denied", http.StatusForbidden, "AccessDenied", uploader.FailureCategoryPermanent},
		{"request timeout", http.StatusBadRequest, "RequestTimeout", uploader.FailureCategoryRetryable},
	} {
		t.Run(test.name, func(t *testing.T) {
			var requestCount atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requestCount.Add(1)
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(`<Error><Code>` + test.code + `</Code></Error>`))
			}))
			defer server.Close()

			filePath := filepath.Join(t.TempDir(), "app_20260906_1.log.gz")
			require.NoError(t, os.WriteFile(filePath, []byte("compressed log"), 0o600))
			created, err := compose.FromTargets(objectstorage.NewUploadTargetConfigAdapter(objectstorage.UploadTargetConfig{
				Enabled: true, Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
				S3: objectstorage.S3Config{
					Region: "ap-northeast-1", Endpoint: server.URL,
					AccessKeyID: "access-key", SecretAccessKey: "secret-key",
					Bucket: "archive-bucket", ForcePathStyle: true,
				},
			}), Builders())
			require.NoError(t, err)
			remote := created.(uploader.DestinationUploader)
			report := remote.UploadDestinations(context.Background(), uploadertest.Request(t, filePath), remote.DestinationIDs())
			require.Len(t, report.Results, 1)
			require.Error(t, report.Results[0].Err)
			require.Equal(t, test.category, report.Results[0].FailureCategory)
			require.EqualValues(t, 1, requestCount.Load(), "S3 PUT must be attempted exactly once")
		})
	}
}
