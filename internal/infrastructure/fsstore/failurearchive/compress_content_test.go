package failurearchive

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

// replaceTemporaryArchiveOutputWithGzipは、組み込みcodecが書き終えたtmpを、payloadを
// 圧縮した完全なgzip streamへ置き換えてから同期します。tmp同期のseamから呼び、
// 元fileを表さない圧縮結果を生成したcodecの故障を再現します。
func replaceTemporaryArchiveOutputWithGzip(payload []byte) func(*os.File) error {
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

func TestCompressOldFileRejectsBuiltInArchiveOutputThatDoesNotDecodeToSource(t *testing.T) {
	content := []byte("{\"record\":1}\n{\"record\":2}\n")
	for name, payload := range map[string][]byte{
		"different records": []byte("{\"record\":\"not in the source\"}\n"),
		"missing tail":      content[:len(content)-5],
	} {
		t.Run(name, func(t *testing.T) {
			sourcePath := filepath.Join(t.TempDir(), "test-app_20261022_1.jsonl")
			require.NoError(t, os.WriteFile(sourcePath, content, 0o640))
			cfg := newValidTestFileArchiveConfig(t)
			cfg.enableFileCompress = true
			store := FileStore{Config: cfg}

			returnedPath, err := store.compressOldFileWithResultAndDurability(
				context.Background(),
				sourcePath,
				replaceTemporaryArchiveOutputWithGzip(payload),
				infrafsstore.SyncDirectory,
			)

			// 完全でも元fileを表さないtmpは公開せず、元fileも退避・削除しません。
			require.ErrorIs(t, err, infrafsstore.ErrCompressedContentMismatch)
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
