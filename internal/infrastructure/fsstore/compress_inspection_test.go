package fsstore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestVerifyCompressedFileIntegrityDoesNotClassifyInspectionFailureAsCorruption(t *testing.T) {
	codec, err := ResolveCompressionCodec(pkgfsstore.CompressGzip, nil)
	require.NoError(t, err)

	for _, kind := range []string{"missing", "directory", "symlink", "dangling symlink", "invalid path"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "compressed.log.gz")
			target := filepath.Join(dir, "target.log.gz")
			switch kind {
			case "directory":
				require.NoError(t, os.Mkdir(path, 0o700))
			case "symlink":
				require.NoError(t, os.WriteFile(target, []byte("not compressed content"), 0o600))
				testutil.Symlink(t, target, path)
			case "dangling symlink":
				testutil.Symlink(t, target, path)
			case "invalid path":
				path += "\x00"
			}

			err := VerifyCompressedFileIntegrity(path, codec)
			require.ErrorIs(t, err, ErrInvalidCompressedFile)
			require.NotErrorIs(t, err, pkgfsstore.ErrCorruptCompressedFile)
			if kind == "missing" {
				require.ErrorIs(t, err, os.ErrNotExist)
			} else {
				require.NotErrorIs(t, err, os.ErrNotExist)
			}
		})
	}
}
