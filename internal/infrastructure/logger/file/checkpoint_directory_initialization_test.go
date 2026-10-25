package file

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	commoncheckpoint "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestLoggerSetUsesCallerPreparedCheckpointDirectory(t *testing.T) {
	root := t.TempDir()
	cfg := newTestFileLoggerConfig(root, "logs", "checkpoint-startup")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.ArchiveCheckpointConfig = pkgfsstore.ArchiveCheckpointConfig{
		Directory: filepath.Join(root, "checkpoints"), SourcePrefix: "source",
	}
	set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	initialSet := set
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "initialSet Close", func() closerForTest { return initialSet }, true)
	require.NoDirExists(t, cfg.ArchiveCheckpointConfig.Directory, "LoggerSet must not prepare checkpoint directories")
	require.Empty(t, set.loggers)
	logger, err := set.NewFileLogger(context.Background())
	require.NoError(t, err, "upload disabled must not require a checkpoint directory")
	require.Nil(t, logger.archiveUploadCheckpoints)
	require.NoDirExists(t, cfg.ArchiveCheckpointConfig.Directory, "lazy logger creation must not prepare checkpoint directories")
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "set Close", set))

	// 既存の系列記録は、uploadを無効にしている間は開きません。
	require.NoError(t, commoncheckpoint.PrepareDirectory(cfg.ArchiveCheckpointConfig.Directory))
	unused, err := newArchiveCompressedLogCheckpointStore(cfg.ArchiveCheckpointConfig, DefaultLogger, logger.Fsstore.FileName, false, nil, nil)
	require.NoError(t, err)
	require.Nil(t, unused)
	entries, err := os.ReadDir(cfg.ArchiveCheckpointConfig.Directory)
	require.NoError(t, err)
	require.Empty(t, entries)
	existing, err := newArchiveCompressedLogCheckpointStore(cfg.ArchiveCheckpointConfig, DefaultLogger, logger.Fsstore.FileName, true, nil, nil)
	require.NoError(t, err)
	require.NoError(t, existing.Close())
	set, err = NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
	require.DirExists(t, cfg.ArchiveCheckpointConfig.Directory)
	require.Empty(t, set.loggers, "LoggerSet must keep lazy loggers unconstructed")
	logger, err = set.NewFileLogger(context.Background())
	require.NoError(t, err)
	require.Nil(t, logger.archiveUploadCheckpoints)
	reenabled, err := newArchiveCompressedLogCheckpointStore(cfg.ArchiveCheckpointConfig, DefaultLogger, logger.Fsstore.FileName, true, nil, nil)
	require.NoError(t, err, "re-enabled upload must still read the retained checkpoint")
	require.NoError(t, reenabled.Close())
}

func TestInlineLoggerIgnoresCorruptCheckpointWhileUploadDisabled(t *testing.T) {
	root := t.TempDir()
	cfg := newTestFileLoggerConfig(root, "logs", "checkpoint-disabled-corrupt")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.ArchiveCheckpointConfig = pkgfsstore.ArchiveCheckpointConfig{
		Directory: filepath.Join(root, "checkpoints"), SourcePrefix: "source",
	}
	set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	logger, err := set.NewFileLogger(context.Background())
	require.NoError(t, err)
	pattern := logger.Fsstore.FileName
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "set Close", set))

	require.NoError(t, commoncheckpoint.PrepareDirectory(cfg.ArchiveCheckpointConfig.Directory))
	store, err := newArchiveCompressedLogCheckpointStore(cfg.ArchiveCheckpointConfig, DefaultLogger, pattern, true, nil, nil)
	require.NoError(t, err)
	require.NoError(t, store.Close())
	sourceID := commoncheckpoint.ArchiveCheckpointSourceID(cfg.ArchiveCheckpointConfig.SourcePrefix, string(DefaultLogger), pattern)
	documentPath := filepath.Join(cfg.ArchiveCheckpointConfig.Directory, sourceID+".log-file-uploads.json")
	corrupt := []byte("invalid json")
	require.NoError(t, os.WriteFile(documentPath, corrupt, 0o600))

	disabled, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "disabled Close", func() closerForTest { return disabled }, true)
	logger, err = disabled.NewFileLogger(context.Background())
	require.NoError(t, err)
	require.Nil(t, logger.archiveUploadCheckpoints)
	contents, err := os.ReadFile(documentPath)
	require.NoError(t, err)
	require.Equal(t, corrupt, contents, "disabled upload must leave the checkpoint unchanged")

	reenabled, err := newArchiveCompressedLogCheckpointStore(cfg.ArchiveCheckpointConfig, DefaultLogger, pattern, true, nil, nil)
	require.ErrorIs(t, err, commoncheckpoint.ErrCorruptCheckpoint)
	require.Nil(t, reenabled)
}

func TestInlineLoggerRejectsInvalidCheckpointSourceWhileUploadDisabled(t *testing.T) {
	root := t.TempDir()
	cfg := newTestFileLoggerConfig(root, "logs", "checkpoint-disabled-invalid-source")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.ArchiveCheckpointConfig = pkgfsstore.ArchiveCheckpointConfig{
		Directory: filepath.Join(root, "checkpoints"), SourcePrefix: "Bad/Prefix",
	}
	set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)

	logger, err := set.NewFileLogger(context.Background())
	require.ErrorIs(t, err, commoncheckpoint.ErrInvalidSourceID)
	require.Nil(t, logger)
	require.NoDirExists(t, cfg.ArchiveCheckpointConfig.Directory)
}

func TestInlineCheckpointRequiresPreparedDirectoryWhenUploading(t *testing.T) {
	cfg := pkgfsstore.ArchiveCheckpointConfig{Directory: filepath.Join(t.TempDir(), "checkpoints"), SourcePrefix: "logger"}
	pattern := logpattern.FileNamePattern{Dir: t.TempDir(), Target: "app", Extension: "log"}
	store, err := newArchiveCompressedLogCheckpointStore(cfg, DefaultLogger, pattern, true, nil, nil)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Nil(t, store)
	require.NoDirExists(t, cfg.Directory)
	require.NoError(t, os.WriteFile(cfg.Directory, []byte("not a directory"), 0o600))
	store, err = newArchiveCompressedLogCheckpointStore(cfg, DefaultLogger, pattern, false, nil, nil)
	requireManagedDirectoryObstruction(t, err, cfg.Directory, "upload disabled must not hide invalid destinations")
	require.Nil(t, store)
	alias := filepath.Join(t.TempDir(), "checkpoint-alias")
	testutil.Symlink(t, t.TempDir(), alias)
	cfg.Directory = alias
	store, err = newArchiveCompressedLogCheckpointStore(cfg, DefaultLogger, pattern, false, nil, nil)
	requireManagedDirectoryObstruction(t, err, alias, "upload disabled must still reject a linked checkpoint directory")
	require.Nil(t, store)
}

// requireManagedDirectoryObstructionは、errが、checkpoint保存先になるべきpathが通常fileやlinkで
// 塞がれていることを表し、そのpathがobstructedであることを確認します。errorのpathは実体の
// pathで表されるため、表記ではなく同じentryかで比べます。
func requireManagedDirectoryObstruction(t *testing.T, err error, obstructed string, msg string) {
	t.Helper()
	var obstruction *pathidentity.ManagedDirectoryObstructionError
	require.ErrorAs(t, err, &obstruction, msg)
	reported, statErr := os.Lstat(obstruction.Path)
	require.NoError(t, statErr)
	expected, statErr := os.Lstat(obstructed)
	require.NoError(t, statErr)
	require.True(t, os.SameFile(expected, reported), "obstruction path = %q, want the entry at %q", obstruction.Path, obstructed)
}
