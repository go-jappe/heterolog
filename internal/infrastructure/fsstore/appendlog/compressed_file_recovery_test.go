package appendlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestVerifyExistingCompressedFileUsesItsFormatAfterConfigurationChange(t *testing.T) {
	for _, format := range []pkgfsstore.CompressFormat{
		pkgfsstore.CompressGzip, pkgfsstore.CompressZstd, pkgfsstore.CompressLz4, pkgfsstore.CompressXz,
	} {
		t.Run(string(format), func(t *testing.T) {
			source := filepath.Join(t.TempDir(), "app_20260914_1.log")
			require.NoError(t, os.WriteFile(source, []byte("valuable original log\n"), 0o600))
			original := FileStore{Config: NewConfigAdapter(pkgfsstore.Config{
				Lifecycle: pkgfsstore.FileLifecycleConfig{CompressFormat: format},
			})}
			returnedPath, err := original.CompressOldFileAndRemoveSource(context.Background(), source, nil)
			require.NoError(t, err)
			before, err := os.ReadFile(returnedPath)
			require.NoError(t, err)
			changed := FileStore{Config: NewConfigAdapter(pkgfsstore.Config{
				Lifecycle: pkgfsstore.FileLifecycleConfig{CompressFormat: pkgfsstore.CompressZstd},
			})}
			require.NoError(t, changed.VerifyExistingCompressedFileIntegrity(returnedPath))
			after, err := os.ReadFile(returnedPath)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestVerifyExistingCompressedFileDoesNotTreatUnknownCodecAsSuccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app_20260914_1.log.custom")
	payload := []byte(userDefinedAppendLogCompressionHeader + "valuable original log\n")
	require.NoError(t, os.WriteFile(path, payload, 0o600))
	err := VerifyExistingCompressedFileIntegrity(path, nil)
	require.ErrorIs(t, err, fsstore.ErrUnsupportedCompressionFormat)
	require.NotErrorIs(t, err, pkgfsstore.ErrCorruptCompressedFile)
	require.NoError(t, VerifyExistingCompressedFileIntegrity(path, userDefinedAppendLogCompressionCodec{format: "custom"}))
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestVerifyExistingCompressedFileDistinguishesCorruptionAndIOFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app_20260914_1.log.gz")
	err := VerifyExistingCompressedFileIntegrity(path, nil)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NotErrorIs(t, err, pkgfsstore.ErrCorruptCompressedFile)
	require.NoError(t, os.WriteFile(path, []byte("invalid gzip"), 0o600))
	require.ErrorIs(t, VerifyExistingCompressedFileIntegrity(path, nil), pkgfsstore.ErrCorruptCompressedFile)
}

func TestRecoverPublishedCompressionAfterSyncFailureAndFormatChange(t *testing.T) {
	source := filepath.Join(t.TempDir(), "app_20260914_1.log")
	payload := []byte("original log retained until compression commit\n")
	require.NoError(t, os.WriteFile(source, payload, 0o600))
	old := FileStore{Config: NewConfigAdapter(pkgfsstore.Config{
		Lifecycle: pkgfsstore.FileLifecycleConfig{CompressFormat: pkgfsstore.CompressGzip},
	})}
	syncErr := errors.New("directory sync failed after final publication")
	returnedPath, err := old.compressOldFile(context.Background(), source, nil,
		os.Remove, (*os.File).Sync, func(string) error { return syncErr })
	require.ErrorIs(t, err, syncErr)
	require.Equal(t, source+".gz", returnedPath)
	require.FileExists(t, source)
	before, err := os.ReadFile(returnedPath)
	require.NoError(t, err)

	// 新しいStoreには前回のメモリ状態がなく、設定もzstdへ変更されています。
	restarted := FileStore{Config: NewConfigAdapter(pkgfsstore.Config{
		Lifecycle: pkgfsstore.FileLifecycleConfig{CompressFormat: pkgfsstore.CompressZstd},
	})}
	actual, exists := fsstore.FindExistingCompressedFilePath(source, []string{source, returnedPath})
	require.True(t, exists)
	require.Equal(t, returnedPath, actual)
	require.NoError(t, restarted.VerifyExistingCompressedFileIntegrity(actual))
	require.NoError(t, RemoveSourceAfterCompression(source, actual))
	require.NoFileExists(t, source)
	require.NoFileExists(t, source+".zst")
	after, err := os.ReadFile(actual)
	require.NoError(t, err)
	require.Equal(t, before, after)
}
