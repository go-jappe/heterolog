package lifecycle

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestFileSeriesManagerEmptyZstdPeerDoesNotRetireNonemptySource(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "empty-compressed-peer"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	source := filepath.Join(logDir, target+"_"+currentLogDate()+"_1.log")
	peer := source + pkgfsstore.ExtensionCompressZstd
	latest := filepath.Join(logDir, target+"_"+currentLogDate()+"_2.log")
	payload := []byte("first preserved record\nsecond preserved record\n")
	require.NoError(t, os.WriteFile(source, payload, 0o600))
	require.NoError(t, os.WriteFile(peer, nil, 0o600))
	require.NoError(t, os.WriteFile(latest, []byte("active"), 0o600))
	options := FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config: testFileSeriesManagerConfigWithCompressionFormat(
			root, target, pkgfsstore.FileRotationDaily, 0, 0, 0, true, pkgfsstore.CompressZstd,
		),
	}
	manager := NewFileSeriesManager(options)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), boundedwait.NoError)
	require.True(t, manager.shouldProtectFromLifecyclePurge(source), "the empty peer is not proof that the raw log is dispensable")
	report, err := manager.RunOnce(context.Background())
	require.ErrorIs(t, err, pkgfsstore.ErrCorruptCompressedFile)
	require.Empty(t, report.Compressed)
	got, err := os.ReadFile(source)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	require.NoFileExists(t, peer)

	// A new manager has no in-memory history of the rejected peer. It must still
	// recover the original records on its next scan, with no loss or duplication.
	restarted := NewFileSeriesManager(options)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "restarted.Close()", restarted.Close, boundedwait.ForceCloserOf(restarted), boundedwait.NoError)
	report, err = restarted.RunOnce(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Compressed, 1)
	require.NoFileExists(t, source)
	encoded, err := os.ReadFile(peer)
	require.NoError(t, err)
	decoder, err := zstd.NewReader(bytes.NewReader(encoded))
	require.NoError(t, err)
	defer decoder.Close()
	decoded, err := io.ReadAll(decoder)
	require.NoError(t, err)
	require.Equal(t, payload, decoded)
	got, err = os.ReadFile(latest)
	require.NoError(t, err)
	require.Equal(t, []byte("active"), got)
}

func TestFileSeriesManagerCachedEmptyPeerCannotRetireSourceThatGainedRecords(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "cached-empty-peer"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	source := filepath.Join(logDir, target+"_"+currentLogDate()+"_1.log")
	peer := source + pkgfsstore.ExtensionCompressZstd
	require.NoError(t, os.WriteFile(source, nil, 0o600))
	require.NoError(t, os.WriteFile(peer, nil, 0o600))
	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config: testFileSeriesManagerConfigWithCompressionFormat(
			root, target, pkgfsstore.FileRotationDaily, 0, 0, 0, true, pkgfsstore.CompressZstd,
		),
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), boundedwait.NoError)
	valid, err := manager.existingCompressedFileIsValid(source, peer)
	require.NoError(t, err)
	require.True(t, valid, "an empty raw log may legitimately have an empty Zstd representation")

	// The compressed file's fingerprint has not changed, but its source is no longer empty.
	payload := []byte("record appended after the first inspection\n")
	require.NoError(t, os.WriteFile(source, payload, 0o600))
	valid, err = manager.existingCompressedFileIsValid(source, peer)
	require.ErrorIs(t, err, pkgfsstore.ErrCorruptCompressedFile)
	require.False(t, valid)
	require.True(t, manager.shouldProtectFromLifecyclePurge(source))
	got, err := os.ReadFile(source)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	require.FileExists(t, peer, "validation itself must not remove either file")
}
