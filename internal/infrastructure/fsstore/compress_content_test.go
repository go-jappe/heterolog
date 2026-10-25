package fsstore

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

var builtInCompressionFormatsForContentTest = []pkgfsstore.CompressFormat{
	pkgfsstore.CompressGzip,
	pkgfsstore.CompressZstd,
	pkgfsstore.CompressLz4,
	pkgfsstore.CompressXz,
}

// compressionSourceContentForTestは、比較用bufferを複数回使う大きさの、位置ごとに異なる内容です。
func compressionSourceContentForTest() []byte {
	var content bytes.Buffer
	for line := 0; content.Len() < 3*compressedContentComparisonBufferSize+17; line++ {
		fmt.Fprintf(&content, "record %06d keeps its own bytes\n", line)
	}
	return content.Bytes()
}

// writeCompressedForContentTestは、contentを組み込みcodecで圧縮したfileをpathへ作ります。
func writeCompressedForContentTest(t *testing.T, format pkgfsstore.CompressFormat, path string, content []byte) {
	t.Helper()
	input := filepath.Join(t.TempDir(), "input.log")
	require.NoError(t, os.WriteFile(input, content, 0o600))
	codec, err := ResolveCompressionCodec(format, nil)
	require.NoError(t, err)
	in, out, err := OpenFilesForCompression(input, path)
	require.NoError(t, err)
	compressErr := codec.Compress(in, out)
	require.NoError(t, errors.Join(compressErr, in.Close(), out.Close()))
}

func TestVerifyCompressedFileMatchesSourceAcceptsSameContentForEverySupportedFormat(t *testing.T) {
	content := compressionSourceContentForTest()
	for _, format := range builtInCompressionFormatsForContentTest {
		t.Run(string(format), func(t *testing.T) {
			dir := t.TempDir()
			sourcePath := filepath.Join(dir, "app_20261022_1.log")
			compressedPath := GenerateCompressedPath(sourcePath, format)
			require.NoError(t, os.WriteFile(sourcePath, content, 0o600))
			writeCompressedForContentTest(t, format, compressedPath, content)
			codec, err := ResolveCompressionCodec(format, nil)
			require.NoError(t, err)

			require.NoError(t, VerifyCompressedFileMatchesSource(compressedPath, codec, sourcePath))
			// 既存fileの検証では、設定上の形式ではなくfile自身のextensionからcodecを選びます。
			require.NoError(t, VerifyExistingCompressedFileMatchesSource(sourcePath, compressedPath, nil))
		})
	}
}

func TestVerifyCompressedFileMatchesSourceRejectsDifferentIntactContent(t *testing.T) {
	content := compressionSourceContentForTest()
	changedOffset := 2*compressedContentComparisonBufferSize + 5
	changed := bytes.Clone(content)
	changed[changedOffset] ^= 0x20
	testCases := []struct {
		name       string
		compressed []byte
		wantKind   CompressedContentMismatchKind
		wantOffset int64
	}{
		{
			name:       "same length",
			compressed: changed,
			wantKind:   CompressedContentDiffers,
			wantOffset: int64(changedOffset),
		},
		{
			// 圧縮後に元ログへ追記された状態です。
			name:       "source has more records",
			compressed: content[:len(content)-100],
			wantKind:   CompressedContentDecodedEndsFirst,
			wantOffset: int64(len(content) - 100),
		},
		{
			name:       "source is shorter",
			compressed: append(bytes.Clone(content), []byte("record that only the peer has\n")...),
			wantKind:   CompressedContentSourceEndsFirst,
			wantOffset: int64(len(content)),
		},
		{
			name:       "empty peer stream",
			compressed: []byte{},
			wantKind:   CompressedContentDecodedEndsFirst,
			wantOffset: 0,
		},
	}
	for _, format := range builtInCompressionFormatsForContentTest {
		for _, testCase := range testCases {
			t.Run(string(format)+"/"+testCase.name, func(t *testing.T) {
				dir := t.TempDir()
				sourcePath := filepath.Join(dir, "app_20261022_1.log")
				compressedPath := GenerateCompressedPath(sourcePath, format)
				require.NoError(t, os.WriteFile(sourcePath, content, 0o600))
				writeCompressedForContentTest(t, format, compressedPath, testCase.compressed)
				codec, err := ResolveCompressionCodec(format, nil)
				require.NoError(t, err)

				// 完全性だけの検証では、別の内容を持つ完全なfileを見分けられません。
				require.NoError(t, VerifyCompressedFileIntegrity(compressedPath, codec))
				err = VerifyCompressedFileMatchesSource(compressedPath, codec, sourcePath)
				require.ErrorIs(t, err, ErrCompressedContentMismatch)
				require.NotErrorIs(t, err, pkgfsstore.ErrCorruptCompressedFile, "an intact peer must not be removed as corrupt")
				require.NotErrorIs(t, err, ErrInvalidCompressedFile)
				// 最初の不一致の食い違い方と位置を、文言ではなく型で示します。
				var mismatch *CompressedContentMismatchError
				require.ErrorAs(t, err, &mismatch)
				require.Equal(t, testCase.wantKind, mismatch.Kind)
				require.Equal(t, testCase.wantOffset, mismatch.Offset)
				require.Equal(t, compressedPath, mismatch.CompressedPath)
				require.Equal(t, sourcePath, mismatch.SourcePath)
				require.ErrorIs(t, VerifyExistingCompressedFileMatchesSource(sourcePath, compressedPath, nil), ErrCompressedContentMismatch)

				// 検証は元ログも圧縮済みfileも変更しません。
				got, err := os.ReadFile(sourcePath)
				require.NoError(t, err)
				require.Equal(t, content, got)
				require.FileExists(t, compressedPath)
			})
		}
	}
}

func TestVerifyCompressedFileMatchesSourceClassifiesCorruptionBeforeMismatch(t *testing.T) {
	content := compressionSourceContentForTest()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "app_20261022_1.log")
	require.NoError(t, os.WriteFile(sourcePath, content, 0o600))
	codec, err := ResolveCompressionCodec(pkgfsstore.CompressGzip, nil)
	require.NoError(t, err)

	other := filepath.Join(dir, "other.log.gz")
	writeCompressedForContentTest(t, pkgfsstore.CompressGzip, other, []byte("different records\n"))
	otherData, err := os.ReadFile(other)
	require.NoError(t, err)
	// trailerのCRCだけを壊したfileは、展開した内容が元ログと異なっても破損として扱います。
	brokenChecksum := bytes.Clone(otherData)
	brokenChecksum[len(brokenChecksum)-8] ^= 0xff
	matching := filepath.Join(dir, "matching.log.gz")
	writeCompressedForContentTest(t, pkgfsstore.CompressGzip, matching, content)
	matchingData, err := os.ReadFile(matching)
	require.NoError(t, err)

	for name, data := range map[string][]byte{
		"different content with broken checksum": brokenChecksum,
		"truncated stream of the same content":   matchingData[:len(matchingData)/2],
		"not compressed":                         content[:64],
	} {
		t.Run(name, func(t *testing.T) {
			compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
			require.NoError(t, os.WriteFile(compressedPath, data, 0o600))

			err := VerifyCompressedFileMatchesSource(compressedPath, codec, sourcePath)
			require.ErrorIs(t, err, ErrInvalidCompressedFile)
			require.ErrorIs(t, err, pkgfsstore.ErrCorruptCompressedFile)
			require.NotErrorIs(t, err, ErrCompressedContentMismatch)
		})
	}
}

func TestVerifyCompressedFileMatchesSourceChecksIntegrityWhenSourceIsAbsent(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "app_20261022_1.log")
	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	writeCompressedForContentTest(t, pkgfsstore.CompressGzip, compressedPath, []byte("retired source records\n"))
	codec, err := ResolveCompressionCodec(pkgfsstore.CompressGzip, nil)
	require.NoError(t, err)

	// 元ログの退避後は比較対象がないため、圧縮済みfile自体の完全性だけを確認します。
	require.NoError(t, VerifyCompressedFileMatchesSource(compressedPath, codec, sourcePath))
	require.NoError(t, os.WriteFile(compressedPath, []byte("not a gzip stream"), 0o600))
	require.ErrorIs(t, VerifyCompressedFileMatchesSource(compressedPath, codec, sourcePath), pkgfsstore.ErrCorruptCompressedFile)
}

func TestVerifyCompressedFileMatchesSourceDoesNotClassifyUnreadableSource(t *testing.T) {
	for _, kind := range []string{"directory", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			sourcePath := filepath.Join(dir, "app_20261022_1.log")
			compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
			content := []byte("records\n")
			writeCompressedForContentTest(t, pkgfsstore.CompressGzip, compressedPath, content)
			switch kind {
			case "directory":
				require.NoError(t, os.Mkdir(sourcePath, 0o700))
			case "symlink":
				target := filepath.Join(dir, "target.log")
				require.NoError(t, os.WriteFile(target, content, 0o600))
				testutil.Symlink(t, target, sourcePath)
			}
			codec, err := ResolveCompressionCodec(pkgfsstore.CompressGzip, nil)
			require.NoError(t, err)

			err = VerifyCompressedFileMatchesSource(compressedPath, codec, sourcePath)
			require.Error(t, err)
			require.NotErrorIs(t, err, ErrCompressedContentMismatch)
			require.NotErrorIs(t, err, pkgfsstore.ErrCorruptCompressedFile)
		})
	}
}

func TestVerifyCompressedFileMatchesSourceKeepsIntegrityContractForCustomCodec(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "app_20261022_1.log")
	require.NoError(t, os.WriteFile(sourcePath, []byte("source records\n"), 0o600))
	custom := userDefinedCompressionCodec{}
	compressedPath := GenerateCompressedPath(sourcePath, custom.Format())
	require.NoError(t, os.WriteFile(compressedPath, []byte(userDefinedCompressionHeader+"other records\n"), 0o600))

	// 公開APIのcodecは展開結果を返さないため、内容を比較できず従来の完全性検証だけを行います。
	require.NoError(t, VerifyCompressedFileMatchesSource(compressedPath, custom, sourcePath))
	require.NoError(t, VerifyExistingCompressedFileMatchesSource(sourcePath, compressedPath, custom))
	require.NoError(t, os.WriteFile(compressedPath, []byte("missing header"), 0o600))
	require.ErrorIs(t, VerifyCompressedFileMatchesSource(compressedPath, custom, sourcePath), pkgfsstore.ErrCorruptCompressedFile)
}

// decodingCustomCompressionCodecは、組み込みgzipへ処理を委ね、展開に使えそうなmethodも持つ独自codecです。
type decodingCustomCompressionCodec struct {
	pkgfsstore.CompressionCodec
}

func (c decodingCustomCompressionCodec) DecodeCompressed(in *os.File, out io.Writer) error {
	reader, err := gzip.NewReader(in)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, reader)
	return errors.Join(copyErr, reader.Close())
}

func TestVerifyCompressedFileMatchesSourceComparesOnlyBuiltInCodecs(t *testing.T) {
	builtIn, err := ResolveCompressionCodec(pkgfsstore.CompressGzip, nil)
	require.NoError(t, err)
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "app_20261022_1.log")
	require.NoError(t, os.WriteFile(sourcePath, []byte("source records\n"), 0o600))
	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	writeCompressedForContentTest(t, pkgfsstore.CompressGzip, compressedPath, []byte("other records\n"))

	require.ErrorIs(t, VerifyCompressedFileMatchesSource(compressedPath, builtIn, sourcePath), ErrCompressedContentMismatch)
	// 独自codecは展開に使えるmethodを持っていても、README・公開APIの契約どおり完全性だけを検証します。
	custom := decodingCustomCompressionCodec{CompressionCodec: builtIn}
	require.NoError(t, VerifyCompressedFileMatchesSource(compressedPath, custom, sourcePath))
	require.NoError(t, VerifyExistingCompressedFileMatchesSource(sourcePath, compressedPath, custom))
}

func TestResolveExistingCompressionCodecUsesFileExtension(t *testing.T) {
	dir := t.TempDir()
	custom := userDefinedCompressionCodec{}

	codec, err := ResolveExistingCompressionCodec(filepath.Join(dir, "app_20261022_1.log.zst"), custom)
	require.NoError(t, err)
	require.Equal(t, pkgfsstore.CompressZstd, codec.Format())
	codec, err = ResolveExistingCompressionCodec(filepath.Join(dir, "app_20261022_1.log.br"), custom)
	require.NoError(t, err)
	require.Equal(t, custom, codec)
	_, err = ResolveExistingCompressionCodec(filepath.Join(dir, "app_20261022_1.log.br"), nil)
	require.ErrorIs(t, err, ErrUnsupportedCompressionFormat)
}

func TestPreserveCompressionSourceWithMismatchedPeerMovesOnlySource(t *testing.T) {
	// 退避先の作成はリンクを拒否するため、macOSの/varなど一時ディレクトリの上位のリンクを解決します。
	dir := resolvedTempDirForTest(t)
	sourcePath := filepath.Join(dir, "app_20261022_1.log")
	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	writeCompressedForContentTest(t, pkgfsstore.CompressGzip, compressedPath, []byte("peer records\n"))
	peerBefore, err := os.ReadFile(compressedPath)
	require.NoError(t, err)

	var preservedPaths []string
	for _, content := range []string{"first source records\n", "second source records\n"} {
		require.NoError(t, os.WriteFile(sourcePath, []byte(content), 0o600))
		preservedPath, err := PreserveCompressionSourceWithMismatchedPeer(sourcePath)
		require.NoError(t, err)

		require.NoFileExists(t, sourcePath)
		require.Equal(t, filepath.Join(dir, PreservedDirectoryName), filepath.Dir(preservedPath))
		require.True(t, strings.HasPrefix(filepath.Base(preservedPath), filepath.Base(sourcePath)+".compressed-content-mismatch-"))
		require.True(t, strings.HasSuffix(preservedPath, ".preserved"))
		got, err := os.ReadFile(preservedPath)
		require.NoError(t, err)
		require.Equal(t, content, string(got))
		preservedPaths = append(preservedPaths, preservedPath)
	}
	// 同じ元ログ名を再び退避しても、以前の保全fileを上書きしません。
	require.NotEqual(t, preservedPaths[0], preservedPaths[1])
	first, err := os.ReadFile(preservedPaths[0])
	require.NoError(t, err)
	require.Equal(t, "first source records\n", string(first))
	peerAfter, err := os.ReadFile(compressedPath)
	require.NoError(t, err)
	require.Equal(t, peerBefore, peerAfter)
	count, err := CountPreservedFiles(t.Context(), dir)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	_, err = PreserveCompressionSourceWithMismatchedPeer(sourcePath)
	require.ErrorIs(t, err, os.ErrNotExist)
}
