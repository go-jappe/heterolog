package heterolog_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/ulikunitz/xz"
)

func TestPublicCompressionKeepsLogReadPermissions(t *testing.T) {
	for _, format := range []pkgfsstore.CompressFormat{
		pkgfsstore.CompressGzip, pkgfsstore.CompressZstd, pkgfsstore.CompressLz4, pkgfsstore.CompressXz,
	} {
		for _, mode := range []string{"native permissions", "owner only", "uncompressed control"} {
			t.Run(string(format)+"/"+mode, func(t *testing.T) {
				root, target, logDir := t.TempDir(), "compression-permissions", "logs"
				directory := filepath.Join(root, target, logDir)
				err := os.MkdirAll(directory, 0o755)
				if err != nil {
					t.Fatal(err)
				}
				cfg := xzRecoveryConfig(root, target, logDir)
				cfg.File.FsstoreConfig.Lifecycle.EnableRotate = true
				cfg.File.FsstoreConfig.Lifecycle.MaxFileSize = 1 << 20
				cfg.File.FsstoreConfig.Lifecycle.EnableFileCompress = mode != "uncompressed control"
				cfg.File.FsstoreConfig.Lifecycle.CompressFormat = format
				cfg.MaxLogEntryBytes = 2 << 20
				logger, err := heterolog.New(context.Background(), cfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = logger.Close() })
				// Create the source through the actual public writer, then rotate it
				// on the next entry. No archived input or compressor is substituted.
				if err := logger.Info(context.Background(), strings.Repeat("private application record", 45000)); err != nil {
					t.Fatal(err)
				}
				paths, err := filepath.Glob(filepath.Join(directory, "*.log"))
				if err != nil || len(paths) != 1 {
					t.Fatalf("active files = %v, error = %v", paths, err)
				}
				path := paths[0]
				if mode == "owner only" && runtime.GOOS != "windows" {
					if err := os.Chmod(path, 0o600); err != nil {
						t.Fatal(err)
					}
				}
				before, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				original, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := logger.Info(context.Background(), "trigger size rotation"); err != nil {
					t.Fatal(err)
				}
				if err := logger.Close(); err != nil {
					t.Fatal(err)
				}
				if mode != "uncompressed control" {
					path += format.Extension()
				}
				after, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				if runtime.GOOS == "windows" {
					if mode != "uncompressed control" {
						// Windows mode bits do not describe the read ACL. Check the
						// existing private-file policy rather than asserting Unix modes.
						testutil.RequirePrivatePermissions(t, path, 0)
					}
				} else if extra := after.Mode().Perm() &^ before.Mode().Perm(); extra != 0 {
					t.Fatalf("compression broadened permissions: source=%#o output=%#o added=%#o", before.Mode().Perm(), after.Mode().Perm(), extra)
				}
				var actual []byte
				if mode == "uncompressed control" {
					actual, err = os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
				} else {
					actual = decodePublicPermissionArchive(t, path, format)
				}
				if !bytes.Equal(actual, original) {
					t.Fatal("rotation changed the original log contents")
				}
			})
		}
	}
}

func decodePublicPermissionArchive(t *testing.T, path string, format pkgfsstore.CompressFormat) []byte {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var reader io.Reader
	switch format {
	case pkgfsstore.CompressGzip:
		decoder, err := gzip.NewReader(file)
		if err != nil {
			t.Fatal(err)
		}
		defer decoder.Close()
		reader = decoder
	case pkgfsstore.CompressZstd:
		decoder, err := zstd.NewReader(file)
		if err != nil {
			t.Fatal(err)
		}
		defer decoder.Close()
		reader = decoder
	case pkgfsstore.CompressLz4:
		reader = lz4.NewReader(file)
	case pkgfsstore.CompressXz:
		reader, err = xz.NewReader(file)
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unsupported test format %q", format)
	}
	contents, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return contents
}
