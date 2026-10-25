package config

import (
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/internal/config/failure"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	s3uploader "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
	objectstoragevalidation "github.com/go-jappe/heterolog/pkg/objectstorage/validation"
	"github.com/go-jappe/heterolog/pkg/opensearch"
)

func TestMapLoggerConfigRejectsNonCanonicalOrDuplicateTypedUploadTargets(t *testing.T) {
	validS3 := objectstorage.S3Config{
		Region:          "ap-northeast-1",
		AccessKeyID:     "access-key",
		SecretAccessKey: "secret-key",
		Bucket:          "archive-bucket",
	}
	tests := []struct {
		name          string
		targets       []objectstorage.UploadTarget
		wantErr       error
		wantIndex     int
		wantCanonical objectstorage.UploadTarget
	}{
		{
			name:          "surrounding whitespace",
			targets:       []objectstorage.UploadTarget{" s3 "},
			wantErr:       objectstoragevalidation.ErrNonCanonicalUploadTarget,
			wantCanonical: objectstorage.UploadTargetS3,
		},
		{
			name:          "uppercase",
			targets:       []objectstorage.UploadTarget{"S3"},
			wantErr:       objectstoragevalidation.ErrNonCanonicalUploadTarget,
			wantCanonical: objectstorage.UploadTargetS3,
		},
		{
			name:      "duplicate",
			targets:   []objectstorage.UploadTarget{objectstorage.UploadTargetS3, objectstorage.UploadTargetS3},
			wantErr:   objectstoragevalidation.ErrDuplicateUploadTarget,
			wantIndex: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mapperValidationTestConfig()
			cfg.Logger.HandlerTargetDestinations =
				pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
			cfg.Logger.FileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig =
				objectstorage.LogFileUploadConfig{
					Enabled: true,
					Default: objectstorage.UploadTargetConfig{
						Enabled: true,
						Targets: tt.targets,
						S3:      validS3,
					},
				}

			_, err := MapLoggerConfig(cfg)
			var targetErr *objectstoragevalidation.UploadTargetError
			if !errors.As(err, &targetErr) || !errors.Is(targetErr, tt.wantErr) ||
				targetErr.Index != tt.wantIndex || targetErr.Target != tt.targets[tt.wantIndex] ||
				targetErr.Canonical != tt.wantCanonical {
				t.Fatalf(
					"MapLoggerConfig() error = %v, want %v for target %q at index %d (canonical %q)",
					err,
					tt.wantErr,
					tt.targets[tt.wantIndex],
					tt.wantIndex,
					tt.wantCanonical,
				)
			}
		})
	}
}

func TestMapLoggerConfigValidatesSelectedFileUploadS3Config(t *testing.T) {
	cfg := mapperValidationTestConfig()
	cfg.Logger.HandlerTargetDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
	cfg.Logger.FileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig =
		objectstorage.LogFileUploadConfig{
			Enabled: true,
			Default: objectstorage.UploadTargetConfig{
				Enabled: true,
				Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
				S3: objectstorage.S3Config{
					Endpoint:        "https://s3.example.com",
					AccessKeyID:     "access-key",
					SecretAccessKey: "secret-key",
					Bucket:          "logs",
				},
			},
		}

	_, err := MapLoggerConfig(cfg)
	if !errors.Is(err, objectstoragevalidation.ErrInvalidS3Config) || !errors.Is(err, s3uploader.ErrRegionRequired) {
		t.Fatalf("MapLoggerConfig() error = %v, want %v caused by %v", err, objectstoragevalidation.ErrInvalidS3Config, s3uploader.ErrRegionRequired)
	}
}

func TestMapLoggerConfigIgnoresUnmappedFileUploadConfig(t *testing.T) {
	cfg := mapperValidationTestConfig()
	// fileを選択しない構成でも、出力先のない設定にはしません。
	cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
	cfg.Logger.FileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig =
		objectstorage.LogFileUploadConfig{
			Enabled: true,
			Default: objectstorage.UploadTargetConfig{
				Enabled: true,
				Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
			},
		}

	if _, err := MapLoggerConfig(cfg); err != nil {
		t.Fatalf("MapLoggerConfig() error = %v, want nil for unmapped file config", err)
	}
}

func TestMapLoggerConfigValidatesSelectedFailureArchiveUpload(t *testing.T) {
	cfg := mapperValidationTestConfig()
	cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{
		pkglogger.LogOutputOpenSearch,
	}
	cfg.OpenSearchClient.Endpoint = "http://localhost:9200"
	cfg.OpenSearchClient.Auth.Type = opensearch.AuthTypeNone

	failureCfg := &cfg.Logger.OpenSearchConfig.
		OpenSearchFlushManagerConfig.
		FailureHandlerConfig
	failureCfg.DurableType = []durable.DurableType{durable.DurableTypeFileArchive}
	failureCfg.Fsstore.FileArchiveConfig.LifecycleMode = failure.DefaultFileArchiveLifecycleMode
	failureCfg.Fsstore.FileArchiveConfig.ObjectStorageUploadConfig =
		objectstorage.UploadTargetConfig{
			Enabled: true,
			Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
			S3: objectstorage.S3Config{
				Region:          "ap-northeast-1",
				Endpoint:        "https://s3.example.com",
				AccessKeyID:     "access-key",
				SecretAccessKey: "secret-key",
				Bucket:          "invalid/bucket",
			},
		}

	_, err := MapLoggerConfig(cfg)
	if !errors.Is(err, s3uploader.ErrBucketContainsPathSeparator) {
		t.Fatalf("MapLoggerConfig() error = %v, want %v", err, s3uploader.ErrBucketContainsPathSeparator)
	}
}
