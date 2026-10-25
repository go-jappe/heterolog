package file

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	objectstoragevalidation "github.com/go-jappe/heterolog/pkg/objectstorage/validation"

	"github.com/stretchr/testify/require"
)

// requireUnsupportedUploadTargetは、errが未対応のupload target "unsupported"による拒否であることを確認します。
func requireUnsupportedUploadTarget(t *testing.T, err error) {
	t.Helper()
	require.ErrorIs(t, err, objectstoragevalidation.ErrUnsupportedUploadTarget)
	var targetErr *objectstoragevalidation.UnsupportedUploadTargetError
	require.ErrorAs(t, err, &targetErr)
	require.Equal(t, objectstorage.UploadTarget("unsupported"), targetErr.Target)
}

func TestFileLoggerConstructsArchiveUploaderOnlyForInlineLifecycle(t *testing.T) {
	tests := []struct {
		name      string
		mode      pkgfsstore.FileLifecycleMode
		wantError bool
	}{
		{
			name:      "inline mode owns uploader construction",
			mode:      pkgfsstore.FileLifecycleModeInline,
			wantError: true,
		},
		{
			name: "runner mode leaves uploader construction to lifecycle runner",
			mode: pkgfsstore.FileLifecycleModeRunner,
		},
		{
			name: "disabled lifecycle does not construct uploader",
			mode: pkgfsstore.FileLifecycleModeDisable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "lifecycle-uploader")
			cfg.LifecycleMode = tt.mode
			cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{
				Enabled: true,
				Default: objectstorage.UploadTargetConfig{
					Enabled: true,
					Targets: []objectstorage.UploadTarget{"unsupported"},
				},
			}

			logger, err := newStandaloneFileLogger(
				context.Background(),
				NewConfigAdapter(cfg),
				nil,
				nil,
			)
			if tt.wantError {
				requireUnsupportedUploadTarget(t, err)
				require.Nil(t, logger)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, logger)
			require.Nil(t, logger.ArchiveUploader)
			require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "logger Close", logger))
		})
	}
}

func TestNewLoggerSetRejectsUnknownLifecycleMode(t *testing.T) {
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "unknown-lifecycle-mode")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleMode("unknown")

	_, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	if !errors.Is(err, pkgfsstore.ErrUnsupportedFileLifecycleMode) {
		t.Fatalf("NewLoggerSet() error = %v, want %v", err, pkgfsstore.ErrUnsupportedFileLifecycleMode)
	}
}

func TestLoggerSetConstructorsAllowUnknownRotationForDailyFallback(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "unknown-normal-rotation")
		cfg.FsstoreConfig.Lifecycle.RotationType = pkgfsstore.FileRotationType("hourly")

		loggerSet, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
		require.NoError(t, err)
		require.NotNil(t, loggerSet)
		require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "loggerSet Close", loggerSet))
	})

	t.Run("delivery", func(t *testing.T) {
		cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "unknown-delivery-rotation")
		cfg.FsstoreConfig.Rotation.RotationType = pkgfsstore.FileRotationType("hourly")

		loggerSet, err := NewDeliveryLoggerSet(NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
		require.NoError(t, err)
		require.NotNil(t, loggerSet)
		require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "loggerSet Close", loggerSet))
	})
}

func TestNewLoggerSetValidatesInlineArchiveUploadBeforeFirstStream(t *testing.T) {
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "invalid-inline-upload")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{
		Enabled: true,
		Default: objectstorage.UploadTargetConfig{
			Enabled: true,
			Targets: []objectstorage.UploadTarget{"unsupported"},
		},
	}

	loggerSet, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	requireUnsupportedUploadTarget(t, err)
	require.Nil(t, loggerSet)
}

func TestNewLoggerSetIgnoresUploadForUnsplitPhysicalRole(t *testing.T) {
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "unused-split-upload")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
	cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{
		Enabled: true,
		Error: objectstorage.UploadTargetConfig{
			Enabled: true,
			Targets: []objectstorage.UploadTarget{"unsupported"},
		},
	}

	loggerSet, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	require.NotNil(t, loggerSet)
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "loggerSet Close", loggerSet))
}
