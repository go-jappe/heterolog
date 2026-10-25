package fsarchivehandler

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestFailureArchivePipelineUsesCallerPreparedCheckpointDirectory(t *testing.T) {
	cfg := newValidArchiveHandlerConfig(t)
	cfg.lifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.archiveCheckpoint = pkgfsstore.ArchiveCheckpointConfig{
		Directory: filepath.Join(t.TempDir(), "checkpoints"), SourcePrefix: "archive-startup",
	}
	pipeline, err := createArchiveHandler(cfg)
	require.NoError(t, err, "upload disabled must not require a checkpoint directory")
	require.Nil(t, pipeline.handler.retryableDirectory.uploadCheckpoints)
	require.Nil(t, pipeline.handler.permanentDirectory.uploadCheckpoints)
	require.NoError(t, shutdownFailureArchiveForTest(pipeline))
	require.NoDirExists(t, cfg.archiveCheckpoint.Directory, "pipeline construction must not prepare checkpoint directories")

	// upload無効時は既存のcheckpointを開かず、再有効化したときに読みます。
	require.NoError(t, checkpointstore.PrepareDirectory(cfg.archiveCheckpoint.Directory))
	for _, category := range []failurearchive.FailureCategory{failurearchive.FailureCategoryRetryable, failurearchive.FailureCategoryPermanent} {
		pattern := pipeline.handler.Fsstore.FileNameRetryable
		if category == failurearchive.FailureCategoryPermanent {
			pattern = pipeline.handler.Fsstore.FileNamePermanent
		}
		unused, err := newCompressedArchiveUploadCheckpointStore(cfg.archiveCheckpoint, category, pattern, nil, nil)
		require.NoError(t, err)
		require.Nil(t, unused)
		existing, err := newCompressedArchiveUploadCheckpointStore(cfg.archiveCheckpoint, category, pattern, &recordingFailureArchiveUploader{}, nil)
		require.NoError(t, err)
		require.NoError(t, existing.Close())
	}
	pipeline, err = createArchiveHandler(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, shutdownFailureArchiveForTest(pipeline)) })
	require.Nil(t, pipeline.handler.retryableDirectory.uploadCheckpoints)
	require.Nil(t, pipeline.handler.permanentDirectory.uploadCheckpoints)
}

func TestFailureArchiveIgnoresCorruptCheckpointWhileUploadDisabled(t *testing.T) {
	cfg := newValidArchiveHandlerConfig(t)
	cfg.lifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.archiveCheckpoint = pkgfsstore.ArchiveCheckpointConfig{
		Directory: filepath.Join(t.TempDir(), "checkpoints"), SourcePrefix: "archive-disabled-corrupt",
	}
	initial, err := createArchiveHandler(cfg)
	require.NoError(t, err)
	pattern := initial.handler.Fsstore.FileNameRetryable
	require.NoError(t, shutdownFailureArchiveForTest(initial))

	require.NoError(t, checkpointstore.PrepareDirectory(cfg.archiveCheckpoint.Directory))
	store, err := newCompressedArchiveUploadCheckpointStore(
		cfg.archiveCheckpoint, failurearchive.FailureCategoryRetryable, pattern,
		&recordingFailureArchiveUploader{}, nil,
	)
	require.NoError(t, err)
	require.NoError(t, store.Close())
	source := checkpointstore.ArchiveCheckpointSourceID(
		cfg.archiveCheckpoint.SourcePrefix, string(failurearchive.FailureCategoryRetryable), pattern,
	)
	path := filepath.Join(cfg.archiveCheckpoint.Directory, source+".log-file-uploads.json")
	corrupt := []byte("invalid json")
	require.NoError(t, os.WriteFile(path, corrupt, 0o600))

	disabled, err := createArchiveHandler(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, shutdownFailureArchiveForTest(disabled)) })
	require.Nil(t, disabled.handler.retryableDirectory.uploadCheckpoints)
	require.Nil(t, disabled.handler.permanentDirectory.uploadCheckpoints)
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, corrupt, contents)

	reenabled, err := newCompressedArchiveUploadCheckpointStore(
		cfg.archiveCheckpoint, failurearchive.FailureCategoryRetryable, pattern,
		&recordingFailureArchiveUploader{}, nil,
	)
	require.ErrorIs(t, err, checkpointstore.ErrCorruptCheckpoint)
	require.Nil(t, reenabled)
}

func TestFailureArchiveRejectsInvalidCheckpointSourceWhileUploadDisabled(t *testing.T) {
	cfg := newValidArchiveHandlerConfig(t)
	cfg.lifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.archiveCheckpoint = pkgfsstore.ArchiveCheckpointConfig{
		Directory: filepath.Join(t.TempDir(), "checkpoints"), SourcePrefix: "Bad/Prefix",
	}
	pipeline, err := createArchiveHandler(cfg)
	require.ErrorIs(t, err, checkpointstore.ErrInvalidSourceID)
	require.Nil(t, pipeline)
	require.NoDirExists(t, cfg.archiveCheckpoint.Directory)
}

func TestFailureArchiveCheckpointRequiresPreparedDirectoryWhenUploading(t *testing.T) {
	cfg := pkgfsstore.ArchiveCheckpointConfig{Directory: filepath.Join(t.TempDir(), "checkpoints"), SourcePrefix: "archive"}
	pattern := logpattern.FileNamePattern{Dir: t.TempDir(), Target: "backend", Extension: "jsonl"}
	store, err := newCompressedArchiveUploadCheckpointStore(cfg, failurearchive.FailureCategoryRetryable, pattern, &recordingFailureArchiveUploader{}, nil)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Nil(t, store)
	require.NoDirExists(t, cfg.Directory)
	require.NoError(t, os.WriteFile(cfg.Directory, []byte("not a directory"), 0o600))
	store, err = newCompressedArchiveUploadCheckpointStore(cfg, failurearchive.FailureCategoryRetryable, pattern, nil, nil)
	requireCheckpointDirectoryObstructionForTest(t, err, cfg.Directory, "upload disabled must not hide invalid destinations")
	require.Nil(t, store)
	alias := filepath.Join(t.TempDir(), "checkpoint-alias")
	testutil.Symlink(t, t.TempDir(), alias)
	cfg.Directory = alias
	store, err = newCompressedArchiveUploadCheckpointStore(cfg, failurearchive.FailureCategoryRetryable, pattern, nil, nil)
	requireCheckpointDirectoryObstructionForTest(t, err, alias, "upload disabled must still reject a linked checkpoint directory")
	require.Nil(t, store)
}

// requireCheckpointDirectoryObstructionForTestは、errがcheckpoint directoryの位置を塞ぐ
// directory以外の項目（通常fileやlink）を、その項目自身のpathで示すことを確認します。
// 親directoryは実体pathへ解決されるため、symlinkを経由する一時directoryでも同じ項目かを比較します。
func requireCheckpointDirectoryObstructionForTest(t *testing.T, err error, directory string, message string) {
	t.Helper()
	var obstruction *pathidentity.ManagedDirectoryObstructionError
	require.ErrorAs(t, err, &obstruction, message)
	want, statErr := os.Lstat(directory)
	require.NoError(t, statErr)
	got, statErr := os.Lstat(obstruction.Path)
	require.NoError(t, statErr)
	require.True(t, os.SameFile(want, got), "obstruction path = %q, want the checkpoint directory entry %q", obstruction.Path, directory)
}

func TestFailureArchiveStartupUsesMissingCheckpointRecoverySetting(t *testing.T) {
	for _, mode := range []pkgfsstore.FileLifecycleMode{pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner} {
		for _, category := range []failurearchive.FailureCategory{failurearchive.FailureCategoryRetryable, failurearchive.FailureCategoryPermanent} {
			for _, recoverMissing := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/recover=%t", mode, category, recoverMissing), func(t *testing.T) {
					cfg := newValidArchiveHandlerConfig(t)
					cfg.lifecycleMode = mode
					cfg.archiveCheckpoint = pkgfsstore.ArchiveCheckpointConfig{
						Directory: t.TempDir(), SourcePrefix: "failure-startup", AutoRecoverMissingCheckpoint: recoverMissing,
					}
					cfg.objectStorageUpload = objectstorage.UploadTargetConfig{
						Enabled: true, Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
						S3: objectstorage.S3Config{Region: "us-east-1", Bucket: "failure-archive", AccessKeyID: "test", SecretAccessKey: "test"},
					}
					initial, err := createArchiveHandler(cfg)
					require.NoError(t, err)
					pattern := initial.handler.Fsstore.FileNameRetryable
					if category == failurearchive.FailureCategoryPermanent {
						pattern = initial.handler.Fsstore.FileNamePermanent
					}
					source := checkpointstore.ArchiveCheckpointSourceID(cfg.archiveCheckpoint.SourcePrefix, string(category), pattern)
					path := filepath.Join(cfg.archiveCheckpoint.Directory, source+".log-file-uploads.json")
					require.NoError(t, shutdownFailureArchiveForTest(initial))
					require.NoError(t, os.Remove(path))
					restarted, err := createArchiveHandler(cfg)
					if !recoverMissing {
						require.ErrorIs(t, err, checkpointstore.ErrRegisteredCheckpointMissing)
						require.Nil(t, restarted)
						require.NoFileExists(t, path)
						return
					}
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, shutdownFailureArchiveForTest(restarted)) })
					require.FileExists(t, path)
				})
			}
		}
	}
}
