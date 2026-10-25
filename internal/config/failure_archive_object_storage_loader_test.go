package config

import (
	"testing"

	pkgobjectstorage "github.com/go-jappe/heterolog/pkg/objectstorage"
)

// TestFailureArchiveObjectStorageLoadersReadExplicitContractKeys verifies that each
// zero-argument loader owns complete env keys for its backend and LoggerContractKind.
// Every key receives a distinct value so a cross-contract reference cannot pass by accident.
func TestFailureArchiveObjectStorageLoadersReadExplicitContractKeys(t *testing.T) {
	t.Setenv("HETEROLOG_ENABLE_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD", "true")
	t.Setenv("HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE", "s3")
	t.Setenv("HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET", "opensearch-sync-bucket")
	t.Setenv("HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX", "opensearch/sync")
	t.Setenv("HETEROLOG_ENABLE_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD", "true")
	t.Setenv("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE", "s3")
	t.Setenv("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET", "opensearch-async-bucket")
	t.Setenv("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX", "opensearch/async")
	t.Setenv("HETEROLOG_ENABLE_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD", "true")
	t.Setenv("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE", "s3")
	t.Setenv("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET", "opensearch-delivery-bucket")
	t.Setenv("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX", "opensearch/delivery")

	t.Setenv("HETEROLOG_ENABLE_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD", "true")
	t.Setenv("HETEROLOG_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE", "s3")
	t.Setenv("HETEROLOG_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET", "loki-sync-bucket")
	t.Setenv("HETEROLOG_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX", "loki/sync")
	t.Setenv("HETEROLOG_ENABLE_ASYNC_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD", "true")
	t.Setenv("HETEROLOG_ASYNC_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE", "s3")
	t.Setenv("HETEROLOG_ASYNC_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET", "loki-async-bucket")
	t.Setenv("HETEROLOG_ASYNC_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX", "loki/async")
	t.Setenv("HETEROLOG_ENABLE_DELIVERY_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD", "true")
	t.Setenv("HETEROLOG_DELIVERY_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE", "s3")
	t.Setenv("HETEROLOG_DELIVERY_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET", "loki-delivery-bucket")
	t.Setenv("HETEROLOG_DELIVERY_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX", "loki/delivery")

	t.Setenv("HETEROLOG_ENABLE_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD", "true")
	t.Setenv("HETEROLOG_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE", "s3")
	t.Setenv("HETEROLOG_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET", "cloudwatch-sync-bucket")
	t.Setenv("HETEROLOG_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX", "cloudwatch/sync")
	t.Setenv("HETEROLOG_ENABLE_ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD", "true")
	t.Setenv("HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE", "s3")
	t.Setenv("HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET", "cloudwatch-async-bucket")
	t.Setenv("HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX", "cloudwatch/async")
	t.Setenv("HETEROLOG_ENABLE_DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD", "true")
	t.Setenv("HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE", "s3")
	t.Setenv("HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET", "cloudwatch-delivery-bucket")
	t.Setenv("HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX", "cloudwatch/delivery")

	tests := []struct {
		name       string
		load       func() (pkgobjectstorage.UploadTargetConfig, error)
		wantBucket string
		wantPrefix string
	}{
		{name: "opensearch sync", load: fromProcessEnvironment(loadOpenSearchFailureArchiveObjectStorageUploadConfig), wantBucket: "opensearch-sync-bucket", wantPrefix: "opensearch/sync"},
		{name: "opensearch async", load: fromProcessEnvironment(loadAsyncOpenSearchFailureArchiveObjectStorageUploadConfig), wantBucket: "opensearch-async-bucket", wantPrefix: "opensearch/async"},
		{name: "opensearch delivery", load: fromProcessEnvironment(loadDeliveryOpenSearchFailureArchiveObjectStorageUploadConfig), wantBucket: "opensearch-delivery-bucket", wantPrefix: "opensearch/delivery"},
		{name: "loki sync", load: fromProcessEnvironment(loadLokiFailureArchiveObjectStorageUploadConfig), wantBucket: "loki-sync-bucket", wantPrefix: "loki/sync"},
		{name: "loki async", load: fromProcessEnvironment(loadAsyncLokiFailureArchiveObjectStorageUploadConfig), wantBucket: "loki-async-bucket", wantPrefix: "loki/async"},
		{name: "loki delivery", load: fromProcessEnvironment(loadDeliveryLokiFailureArchiveObjectStorageUploadConfig), wantBucket: "loki-delivery-bucket", wantPrefix: "loki/delivery"},
		{name: "cloudwatch sync", load: fromProcessEnvironment(loadCloudWatchLogsFailureArchiveObjectStorageUploadConfig), wantBucket: "cloudwatch-sync-bucket", wantPrefix: "cloudwatch/sync"},
		{name: "cloudwatch async", load: fromProcessEnvironment(loadAsyncCloudWatchLogsFailureArchiveObjectStorageUploadConfig), wantBucket: "cloudwatch-async-bucket", wantPrefix: "cloudwatch/async"},
		{name: "cloudwatch delivery", load: fromProcessEnvironment(loadDeliveryCloudWatchLogsFailureArchiveObjectStorageUploadConfig), wantBucket: "cloudwatch-delivery-bucket", wantPrefix: "cloudwatch/delivery"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.load()
			if err != nil {
				t.Fatalf("load failure archive object storage config: %v", err)
			}
			if !got.Enabled {
				t.Fatal("Enabled = false, want true")
			}
			if len(got.Targets) != 1 || got.Targets[0] != pkgobjectstorage.UploadTargetS3 {
				t.Fatalf("Targets = %v, want [%q]", got.Targets, pkgobjectstorage.UploadTargetS3)
			}
			if got.S3.Bucket != tt.wantBucket {
				t.Fatalf("S3.Bucket = %q, want %q", got.S3.Bucket, tt.wantBucket)
			}
			if got.S3.Prefix != tt.wantPrefix {
				t.Fatalf("S3.Prefix = %q, want %q", got.S3.Prefix, tt.wantPrefix)
			}
		})
	}
}
