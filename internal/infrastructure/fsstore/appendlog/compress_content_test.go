package appendlog

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

// replaceTemporaryOutputWithGzipは、組み込みcodecが書き終えたtmpを、payloadを圧縮した
// 完全なgzip streamへ置き換えてから同期します。tmp同期のseamから呼び、元ログを表さない
// 圧縮結果を生成したcodecの故障を再現します。
func replaceTemporaryOutputWithGzip(payload []byte) func(*os.File) error {
	return func(tmp *os.File) error {
		if err := tmp.Truncate(0); err != nil {
			return err
		}
		if _, err := tmp.Seek(0, io.SeekStart); err != nil {
			return err
		}
		writer := gzip.NewWriter(tmp)
		_, writeErr := writer.Write(payload)
		if err := errors.Join(writeErr, writer.Close()); err != nil {
			return err
		}
		return tmp.Sync()
	}
}

func TestCompressOldFileRejectsBuiltInOutputThatDoesNotDecodeToSource(t *testing.T) {
	content := []byte("first record\nsecond record\n")
	for name, payload := range map[string][]byte{
		"different records": []byte("records that are not in the source\n"),
		"missing tail":      content[:len(content)-7],
	} {
		t.Run(name, func(t *testing.T) {
			sourcePath := filepath.Join(t.TempDir(), "app_20261022_1.log")
			require.NoError(t, os.WriteFile(sourcePath, content, 0o640))
			store := FileStore{Config: newCompressionTestConfig()}

			returnedPath, err := store.compressOldFile(
				context.Background(),
				sourcePath,
				nil,
				os.Remove,
				replaceTemporaryOutputWithGzip(payload),
				infrafsstore.SyncDirectory,
			)

			// 完全でも元ログを表さないtmpは公開せず、元ログも退避・削除しません。
			require.ErrorIs(t, err, infrafsstore.ErrCompressedContentMismatch)
			require.NotErrorIs(t, err, ErrCompressionSourceCleanup)
			require.Empty(t, returnedPath)
			got, readErr := os.ReadFile(sourcePath)
			require.NoError(t, readErr)
			require.Equal(t, content, got)
			compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
			require.NoFileExists(t, compressedPath)
			require.NoFileExists(t, compressedPath+".tmp")
			require.NoFileExists(t, logpattern.CompressionSourceDeletePendingPath(sourcePath))
		})
	}
}
