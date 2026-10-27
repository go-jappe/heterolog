package appendlog

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

// 不具合のあるcompressorは、何も出力せずに成功することがあります。Zstdのdecoderは
// 空のstreamを受け付けるため、形式の検証だけでは欠落したrecordを検出できません。
type emptyOutputCompressionCodec struct{}

func (emptyOutputCompressionCodec) Format() pkgfsstore.CompressFormat { return pkgfsstore.CompressZstd }
func (emptyOutputCompressionCodec) Compress(*os.File, *os.File) error { return nil }
func (emptyOutputCompressionCodec) VerifyCompressedIntegrity(in *os.File) error {
	codec, err := fsstore.ResolveCompressionCodec(pkgfsstore.CompressZstd, nil)
	if err != nil {
		return err
	}
	return codec.VerifyCompressedIntegrity(in)
}

func TestCompressOldFileRejectsEmptyOutputForNonemptySourceBeforePublication(t *testing.T) {
	source := filepath.Join(t.TempDir(), "app.log")
	payload := []byte("record that the faulty compressor discarded\n")
	require.NoError(t, os.WriteFile(source, payload, 0o600))
	store := FileStore{Config: newCompressionTestConfigWithCodec(emptyOutputCompressionCodec{})}
	path, err := store.CompressOldFileAndRemoveSource(context.Background(), source, nil)
	require.ErrorIs(t, err, pkgfsstore.ErrCorruptCompressedFile)
	require.Empty(t, path)
	got, err := os.ReadFile(source)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	entries, err := os.ReadDir(filepath.Dir(source))
	require.NoError(t, err)
	require.Len(t, entries, 1, "neither a final peer nor temporary/pending-delete files may remain")
	require.Equal(t, filepath.Base(source), entries[0].Name())
}
