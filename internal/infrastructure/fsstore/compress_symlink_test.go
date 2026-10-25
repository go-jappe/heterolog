package fsstore

import (
	"compress/gzip"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestVerifyCompressedFileIntegrityRejectsSymlinkToValidCompressedFile(t *testing.T) {
	dir := t.TempDir()
	targetPath := filepath.Join(dir, "valid-target.log.gz")
	symlinkPath := filepath.Join(dir, "compressed-peer.log.gz")
	target, err := os.Create(targetPath)
	if err != nil {
		t.Fatalf("create compressed target: %v", err)
	}
	writer := gzip.NewWriter(target)
	_, writeErr := writer.Write([]byte("valid compressed content"))
	closeErr := errors.Join(writer.Close(), target.Close())
	if err := errors.Join(writeErr, closeErr); err != nil {
		t.Fatalf("write compressed target: %v", err)
	}
	testutil.Symlink(t, filepath.Base(targetPath), symlinkPath)
	codec, err := ResolveCompressionCodec(pkgfsstore.CompressGzip, nil)
	if err != nil {
		t.Fatalf("ResolveCompressionCodec() error = %v", err)
	}

	err = VerifyCompressedFileIntegrity(symlinkPath, codec)
	if !errors.Is(err, ErrInvalidCompressedFile) {
		t.Fatalf("VerifyCompressedFileIntegrity() error = %v, want ErrInvalidCompressedFile", err)
	}
	info, lstatErr := os.Lstat(symlinkPath)
	if lstatErr != nil {
		t.Fatalf("lstat compressed peer symlink: %v", lstatErr)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("compressed peer mode = %s, want symlink", info.Mode())
	}
}
