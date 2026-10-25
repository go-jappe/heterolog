package processing

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

// writeCompressedPeerForTestは、contentを圧縮した完全なpeerをpeerPathへ作ります。
// 一時的な元ログから圧縮し、元ログとpeerが共存する停止直後の状態を再現します。
func writeCompressedPeerForTest(t *testing.T, format pkgfsstore.CompressFormat, peerPath string, content []byte) {
	t.Helper()
	input := filepath.Join(resolvedTempDirForTest(t), "input.log")
	require.NoError(t, os.WriteFile(input, content, 0o600))
	codec, err := fsstore.ResolveCompressionCodec(format, nil)
	require.NoError(t, err)
	in, out, err := fsstore.OpenFilesForCompression(input, peerPath)
	require.NoError(t, err)
	compressErr := codec.Compress(in, out)
	require.NoError(t, errors.Join(compressErr, in.Close(), out.Close()))
}

func TestProcessCompressionRetiresSourceOnlyWhenPeerContentMatches(t *testing.T) {
	for _, format := range []pkgfsstore.CompressFormat{
		pkgfsstore.CompressGzip, pkgfsstore.CompressZstd, pkgfsstore.CompressLz4, pkgfsstore.CompressXz,
	} {
		t.Run(string(format), func(t *testing.T) {
			source := filepath.Join(resolvedTempDirForTest(t), "app_20261022_1.log")
			peer := source + format.Extension()
			payload := []byte("first log record\nsecond log record\n")
			require.NoError(t, os.WriteFile(source, payload, 0o600))
			writeCompressedPeerForTest(t, format, peer, payload)
			operations, compressCalls := compressionFileOperations(format)

			result, err := ProcessCompression(context.Background(), CompressionRequest{
				SourcePath: source, CompressedPath: peer, ExistingFileObserved: true,
				RemoveCorruptPeer: true, PreserveMismatchedSource: true,
			}, operations)
			require.NoError(t, err)
			require.True(t, result.ExistingFileVerified)
			require.True(t, result.ReadyForUpload)
			require.Empty(t, result.PreservedSourcePath)
			require.Zero(t, *compressCalls, "a matching peer must not be recompressed")
			require.NoFileExists(t, source)
			require.Equal(t, payload, decodeCompressionResult(t, peer, format))
		})
	}
}

func TestProcessCompressionMismatchedPeerDoesNotDeleteSource(t *testing.T) {
	payload := []byte("first log record\nsecond log record\nrecord appended after compression\n")
	for _, format := range []pkgfsstore.CompressFormat{
		pkgfsstore.CompressGzip, pkgfsstore.CompressZstd, pkgfsstore.CompressLz4, pkgfsstore.CompressXz,
	} {
		for _, preserve := range []bool{false, true} {
			name := string(format) + "/keep-both"
			if preserve {
				name = string(format) + "/preserve-source"
			}
			t.Run(name, func(t *testing.T) {
				dir := resolvedTempDirForTest(t)
				source := filepath.Join(dir, "app_20261022_1.log")
				peer := source + format.Extension()
				require.NoError(t, os.WriteFile(source, payload, 0o600))
				// 完全なpeerですが、元ログの末尾に追記されたrecordを含みません。
				peerContent := payload[:bytes.LastIndex(payload[:len(payload)-1], []byte("\n"))+1]
				writeCompressedPeerForTest(t, format, peer, peerContent)
				peerBefore, err := os.ReadFile(peer)
				require.NoError(t, err)
				operations, compressCalls := compressionFileOperations(format)

				result, processErr := ProcessCompression(context.Background(), CompressionRequest{
					SourcePath: source, CompressedPath: peer, ExistingFileObserved: true,
					RemoveCorruptPeer: true, PreserveMismatchedSource: preserve,
				}, operations)
				require.ErrorIs(t, processErr, fsstore.ErrCompressedContentMismatch)
				require.NotErrorIs(t, processErr, pkgfsstore.ErrCorruptCompressedFile)
				require.Equal(t, CompressionFailureExistingFile, result.FailureStage)
				require.False(t, result.ExistingFileVerified)
				require.False(t, result.ReadyForUpload)
				require.Zero(t, *compressCalls, "a mismatched peer must not be overwritten")
				peerAfter, err := os.ReadFile(peer)
				require.NoError(t, err)
				require.Equal(t, peerBefore, peerAfter, "an intact peer must not be removed as corrupt")
				require.NoFileExists(t, logpattern.CompressionSourceDeletePendingPath(source))

				preservedContent := func(path string) []byte {
					t.Helper()
					got, err := os.ReadFile(path)
					require.NoError(t, err)
					return got
				}
				if !preserve {
					require.Empty(t, result.PreservedSourcePath)
					require.False(t, result.FilesMayHaveChanged)
					require.Equal(t, payload, preservedContent(source))
					return
				}
				// 元ログは削除せず.preservedへ退避し、退避先をerrorで報告します。
				require.NoFileExists(t, source)
				require.NotEmpty(t, result.PreservedSourcePath)
				require.True(t, result.FilesMayHaveChanged)
				require.Equal(t, filepath.Join(dir, fsstore.PreservedDirectoryName), filepath.Dir(result.PreservedSourcePath))
				var preserved *CompressionSourcePreservedError
				require.ErrorAs(t, processErr, &preserved)
				require.Equal(t, source, preserved.SourcePath)
				require.Equal(t, peer, preserved.CompressedPath)
				require.Equal(t, result.PreservedSourcePath, preserved.PreservedPath)
				require.Equal(t, payload, preservedContent(result.PreservedSourcePath))
				count, err := fsstore.CountPreservedFiles(context.Background(), dir)
				require.NoError(t, err)
				require.Equal(t, 1, count)

				// 退避後は圧縮ファイルだけが残り、その世代の完全な圧縮済みファイルとして検証できます。
				exists, err := operations.VerifyExisting(source, peer)
				require.NoError(t, err)
				require.True(t, exists)
			})
		}
	}
}

func TestProcessCompressionCorruptPeerIsRemovedEvenWhenDecodedContentDiffers(t *testing.T) {
	dir := resolvedTempDirForTest(t)
	source := filepath.Join(dir, "app_20261022_1.log")
	peer := source + pkgfsstore.ExtensionCompressGzip
	payload := []byte("first log record\nsecond log record\n")
	require.NoError(t, os.WriteFile(source, payload, 0o600))
	writeCompressedPeerForTest(t, pkgfsstore.CompressGzip, peer, []byte("other records\n"))
	encoded, err := os.ReadFile(peer)
	require.NoError(t, err)
	// trailerのCRCを壊し、内容も元ログと異なる破損peerにします。
	encoded[len(encoded)-8] ^= 0xff
	require.NoError(t, os.WriteFile(peer, encoded, 0o600))
	operations, compressCalls := compressionFileOperations(pkgfsstore.CompressGzip)

	result, err := ProcessCompression(context.Background(), CompressionRequest{
		SourcePath: source, CompressedPath: peer, ExistingFileObserved: true,
		RemoveCorruptPeer: true, PreserveMismatchedSource: true,
	}, operations)
	require.ErrorIs(t, err, pkgfsstore.ErrCorruptCompressedFile)
	require.NotErrorIs(t, err, fsstore.ErrCompressedContentMismatch)
	require.Empty(t, result.PreservedSourcePath)
	require.True(t, result.FilesMayHaveChanged)
	require.NoFileExists(t, peer)
	got, err := os.ReadFile(source)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	require.NoDirExists(t, filepath.Join(dir, fsstore.PreservedDirectoryName))

	// 破損peerは従来どおり削除し、次の処理で残した元ログから再圧縮します。
	result, err = ProcessCompression(context.Background(), CompressionRequest{
		SourcePath: source, CompressedPath: peer,
	}, operations)
	require.NoError(t, err)
	require.True(t, result.ReadyForUpload)
	require.Equal(t, 1, *compressCalls)
	require.NoFileExists(t, source)
	require.Equal(t, payload, decodeCompressionResult(t, peer, pkgfsstore.CompressGzip))
}

func TestProcessCompressionReportsMismatchWhenSourcePreservationFails(t *testing.T) {
	preservationFailure := errors.New("cannot reserve preservation path")
	mismatch := errors.Join(fsstore.ErrCompressedContentMismatch, errors.New("decoded content differs"))
	removed := false
	result, err := ProcessCompression(context.Background(), CompressionRequest{
		SourcePath: "app_20261022_1.log", CompressedPath: "app_20261022_1.log.gz", ExistingFileObserved: true,
		RemoveCorruptPeer: true, PreserveMismatchedSource: true,
	}, CompressionOperations{
		VerifyExisting: func(string, string) (bool, error) { return false, mismatch },
		Compress: func(context.Context, string) (string, error) {
			t.Fatal("a mismatched peer must not be overwritten")
			return "", nil
		},
		RemoveSource: func(string, string) error {
			removed = true
			return nil
		},
		RemoveInvalid: func(string, string) error {
			removed = true
			return nil
		},
		PreserveSource: func(string) (string, error) { return "", preservationFailure },
	})
	require.ErrorIs(t, err, fsstore.ErrCompressedContentMismatch)
	require.ErrorIs(t, err, preservationFailure)
	require.False(t, removed)
	require.Empty(t, result.PreservedSourcePath)
	require.False(t, result.FilesMayHaveChanged)
	require.False(t, result.ReadyForUpload)
	require.Equal(t, CompressionFailureExistingFile, result.FailureStage)
}
