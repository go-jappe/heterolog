package failurearchive

import (
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	fsfailurearchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/uploader/compose"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkguploader "github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	s3uploader "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
	objectstoragevalidation "github.com/go-jappe/heterolog/pkg/objectstorage/validation"
)

func TestNewScopesEveryS3DestinationWithoutMutatingOriginalList(t *testing.T) {
	cfg := validUploadTargetConfig()
	second := cfg.S3
	second.Bucket, second.Prefix = "backup-bucket", "backup"
	cfg.S3Destinations = []objectstorage.S3Config{cfg.S3, second}
	original := slices.Clone(cfg.S3Destinations)
	var previous []objectstorage.DestinationID
	for _, category := range []fsfailurearchive.FailureCategory{fsfailurearchive.FailureCategoryRetryable, fsfailurearchive.FailureCategoryPermanent} {
		remote, err := New(cfg, category, nil, nil)
		require.NoError(t, err)
		configs := slices.Clone(original)
		for index := range configs {
			configs[index].Prefix = ObjectStoragePrefix(configs[index].Prefix, category)
		}
		ids, err := s3uploader.ResolveDestinationIDs(configs)
		require.NoError(t, err)
		assertDestinationIDs(t, remote, ids)
		require.Equal(t, original, cfg.S3Destinations)
		for _, id := range ids {
			require.NotContains(t, previous, id)
		}
		previous = ids
	}
}

func TestNewBuildsFailureCategorySpecificDestinationWithoutChangingSourceConfig(t *testing.T) {
	t.Parallel()

	cfg := validUploadTargetConfig()
	retryable, err := New(cfg, fsfailurearchive.FailureCategoryRetryable, nil, nil)
	if err != nil {
		t.Fatalf("New(retryable) error = %v", err)
	}
	permanent, err := New(cfg, fsfailurearchive.FailureCategoryPermanent, nil, nil)
	if err != nil {
		t.Fatalf("New(permanent) error = %v", err)
	}

	retryableCfg := uploadTargetConfigForFailureCategory(cfg, fsfailurearchive.FailureCategoryRetryable)
	permanentCfg := uploadTargetConfigForFailureCategory(cfg, fsfailurearchive.FailureCategoryPermanent)
	wantRetryableID, err := s3uploader.ResolveDestinationID(retryableCfg.S3)
	if err != nil {
		t.Fatalf("ResolveDestinationID(retryable) error = %v", err)
	}
	wantPermanentID, err := s3uploader.ResolveDestinationID(permanentCfg.S3)
	if err != nil {
		t.Fatalf("ResolveDestinationID(permanent) error = %v", err)
	}

	assertDestinationIDs(t, retryable, []objectstorage.DestinationID{wantRetryableID})
	assertDestinationIDs(t, permanent, []objectstorage.DestinationID{wantPermanentID})
	if wantRetryableID == wantPermanentID {
		t.Fatalf("retryable and permanent destination IDs are both %q", wantRetryableID)
	}
	if cfg.S3.Prefix != "backend/failures" {
		t.Fatalf("source prefix = %q, want unchanged %q", cfg.S3.Prefix, "backend/failures")
	}
	if len(cfg.Targets) != 1 || cfg.Targets[0] != objectstorage.UploadTargetS3 {
		t.Fatalf("source targets = %v, want unchanged [s3]", cfg.Targets)
	}
	if retryableCfg.S3.Prefix != "backend/failures/retryable" {
		t.Fatalf("retryable prefix = %q", retryableCfg.S3.Prefix)
	}
	if permanentCfg.S3.Prefix != "backend/failures/permanent" {
		t.Fatalf("permanent prefix = %q", permanentCfg.S3.Prefix)
	}
}

func TestNewPreservesTargetValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		targets []objectstorage.UploadTarget
		wantErr error
		// wantUnsupportedは、対応するbuilderのないtargetです。
		wantUnsupported objectstorage.UploadTarget
	}{
		{
			name:    "empty targets",
			wantErr: compose.ErrUploadTargetsRequired,
		},
		{
			name:            "unsupported target",
			targets:         []objectstorage.UploadTarget{"unsupported"},
			wantErr:         objectstoragevalidation.ErrUnsupportedUploadTarget,
			wantUnsupported: "unsupported",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := validUploadTargetConfig()
			cfg.Targets = tt.targets

			created, err := New(cfg, fsfailurearchive.FailureCategoryRetryable, nil, nil)
			if created != nil {
				t.Fatalf("New() = %T, want nil", created)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("New() error = %v, want %v", err, tt.wantErr)
			}
			var targetErr *objectstoragevalidation.UnsupportedUploadTargetError
			if tt.wantUnsupported != "" && (!errors.As(err, &targetErr) || targetErr.Target != tt.wantUnsupported) {
				t.Fatalf("New() error = %v, want unsupported target %q", err, tt.wantUnsupported)
			}
		})
	}
}

func TestNewReturnsNilWhenUploadIsDisabled(t *testing.T) {
	t.Parallel()

	created, err := New(
		objectstorage.UploadTargetConfig{},
		fsfailurearchive.FailureCategoryRetryable,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if created != nil {
		t.Fatalf("New() = %T, want nil", created)
	}
}

func assertDestinationIDs(
	t *testing.T,
	created pkguploader.Uploader,
	want []objectstorage.DestinationID,
) {
	t.Helper()
	provider, ok := created.(pkguploader.DestinationUploader)
	if !ok {
		t.Fatalf("uploader type %T does not implement DestinationUploader", created)
	}
	got := provider.DestinationIDs()
	if len(got) != len(want) {
		t.Fatalf("destination IDs = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("destination ID[%d] = %q, want %q", index, got[index], want[index])
		}
	}
}

func validUploadTargetConfig() objectstorage.UploadTargetConfig {
	return objectstorage.UploadTargetConfig{
		Enabled: true,
		Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
		S3: objectstorage.S3Config{
			Region:          "ap-northeast-1",
			Endpoint:        "https://s3.example.test",
			AccessKeyID:     "access-key",
			SecretAccessKey: "secret-key",
			Bucket:          "failure-archive",
			Prefix:          "backend/failures",
			ForcePathStyle:  true,
		},
	}
}
