package processing

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
	"github.com/ulikunitz/xz"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestProcessCompressionEmptyPeerPreservesNonemptySource(t *testing.T) {
	for _, format := range []pkgfsstore.CompressFormat{
		pkgfsstore.CompressGzip, pkgfsstore.CompressZstd, pkgfsstore.CompressLz4, pkgfsstore.CompressXz,
	} {
		for _, removeCorrupt := range []bool{false, true} {
			name := string(format) + "/preserve-peer"
			if removeCorrupt {
				name = string(format) + "/remove-peer"
			}
			t.Run(name, func(t *testing.T) {
				source := filepath.Join(resolvedTempDirForTest(t), "app_20261021_1.log")
				peer := source + format.Extension()
				payload := []byte("first log record\nsecond log record\n")
				require.NoError(t, os.WriteFile(source, payload, 0o600))
				require.NoError(t, os.WriteFile(peer, nil, 0o600))
				operations, compressCalls := compressionFileOperations(format)
				result, err := ProcessCompression(context.Background(), CompressionRequest{
					SourcePath: source, CompressedPath: peer, ExistingFileObserved: true, RemoveCorruptPeer: removeCorrupt,
				}, operations)
				require.ErrorIs(t, err, pkgfsstore.ErrCorruptCompressedFile)
				require.False(t, result.ReadyForUpload)
				require.False(t, result.ExistingFileVerified)
				require.Equal(t, CompressionFailureExistingFile, result.FailureStage)
				require.Zero(t, *compressCalls, "a rejected peer must not retire or immediately recompress its source")
				got, err := os.ReadFile(source)
				require.NoError(t, err)
				require.Equal(t, payload, got)
				if !removeCorrupt {
					got, err = os.ReadFile(peer)
					require.NoError(t, err)
					require.Empty(t, got, "inspection must preserve the peer when cleanup was not requested")
					return
				}
				require.NoFileExists(t, peer)
				// The next pass compresses the retained records; success must preserve their exact contents.
				result, err = ProcessCompression(context.Background(), CompressionRequest{
					SourcePath: source, CompressedPath: peer,
				}, operations)
				require.NoError(t, err)
				require.True(t, result.ReadyForUpload)
				require.Equal(t, 1, *compressCalls)
				require.NoFileExists(t, source)
				require.Equal(t, payload, decodeCompressionResult(t, peer, format))
			})
		}
	}
}

func TestProcessCompressionEmptySourceCanBeCompressedAndRecovered(t *testing.T) {
	for _, format := range []pkgfsstore.CompressFormat{
		pkgfsstore.CompressGzip, pkgfsstore.CompressZstd, pkgfsstore.CompressLz4, pkgfsstore.CompressXz,
	} {
		t.Run(string(format), func(t *testing.T) {
			source := filepath.Join(resolvedTempDirForTest(t), "app_20261021_1.log")
			require.NoError(t, os.WriteFile(source, nil, 0o600))
			operations, calls := compressionFileOperations(format)
			result, err := ProcessCompression(context.Background(), CompressionRequest{SourcePath: source}, operations)
			require.NoError(t, err)
			require.True(t, result.ReadyForUpload)
			require.Equal(t, 1, *calls)
			require.NoFileExists(t, source)
			require.Empty(t, decodeCompressionResult(t, result.CompressedPath, format))
			before, err := os.ReadFile(result.CompressedPath)
			require.NoError(t, err)

			// Recreate the state left by a process stopping after publication but before source retirement.
			// A fresh store must accept the valid compressed empty log, regardless of its encoded length.
			require.NoError(t, os.WriteFile(source, nil, 0o600))
			restarted, restartedCalls := compressionFileOperations(format)
			recovered, err := ProcessCompression(context.Background(), CompressionRequest{
				SourcePath: source, CompressedPath: result.CompressedPath, ExistingFileObserved: true,
			}, restarted)
			require.NoError(t, err)
			require.True(t, recovered.ReadyForUpload)
			require.True(t, recovered.ExistingFileVerified)
			require.Zero(t, *restartedCalls)
			require.NoFileExists(t, source)
			after, err := os.ReadFile(recovered.CompressedPath)
			require.NoError(t, err)
			require.Equal(t, before, after)
			// Source retirement must also leave the peer verifiable for later recovery steps.
			exists, err := restarted.VerifyExisting(source, recovered.CompressedPath)
			require.NoError(t, err)
			require.True(t, exists)
		})
	}
}

func compressionFileOperations(format pkgfsstore.CompressFormat) (CompressionOperations, *int) {
	store := &appendlog.FileStore{Config: appendlog.NewConfigAdapter(pkgfsstore.Config{
		Lifecycle: pkgfsstore.FileLifecycleConfig{CompressFormat: format},
	})}
	calls := new(int)
	return CompressionOperations{
		VerifyExisting: func(source, peer string) (bool, error) {
			return VerifyCompressionPeer(source, peer, func(source, peer string) error {
				return fsstore.VerifyExistingCompressedFileMatchesSource(source, peer, nil)
			})
		},
		Compress: func(ctx context.Context, source string) (string, error) {
			*calls++
			return store.CompressOldFileAndRemoveSource(ctx, source, nil)
		},
	}, calls
}

func decodeCompressionResult(t *testing.T, path string, format pkgfsstore.CompressFormat) []byte {
	t.Helper()
	encoded, err := os.ReadFile(path)
	require.NoError(t, err)
	input := bytes.NewReader(encoded)
	var decoder io.Reader
	switch format {
	case pkgfsstore.CompressGzip:
		reader, err := gzip.NewReader(input)
		require.NoError(t, err)
		defer reader.Close()
		decoder = reader
	case pkgfsstore.CompressZstd:
		reader, err := zstd.NewReader(input)
		require.NoError(t, err)
		defer reader.Close()
		decoder = reader
	case pkgfsstore.CompressLz4:
		decoder = lz4.NewReader(input)
	case pkgfsstore.CompressXz:
		decoder, err = xz.NewReader(input)
		require.NoError(t, err)
	default:
		t.Fatalf("unexpected format %q", format)
	}
	decoded, err := io.ReadAll(decoder)
	require.NoError(t, err)
	return decoded
}
