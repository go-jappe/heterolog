package failurearchive

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestCompressOldFileRejectsEmptySourceBeforeFileOperations(t *testing.T) {
	t.Parallel()

	store := FileStore{Config: newValidTestFileArchiveConfig(t)}
	returnedPath, err := store.CompressOldFileWithResult(context.Background(), "")
	assertErrorIs(t, err, ErrCompressionSourcePathEmpty)
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		t.Fatalf("empty source reached filesystem: %v", err)
	}
	if returnedPath != "" {
		t.Fatalf("compressed path = %q, want empty", returnedPath)
	}
	assertErrorIs(t, store.CompressOldFile(context.Background(), ""), ErrCompressionSourcePathEmpty)
}

func TestCompressOldFileInvalidFormatKeepsSourceWithoutArtifacts(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "failed-docs.jsonl")
	wantContent := []byte("keep source when compressed path cannot be generated\n")
	if err := os.WriteFile(sourcePath, wantContent, 0o640); err != nil {
		t.Fatal(err)
	}
	// 設定検証を通さず、生成境界が空のcompressed pathを拒否する契約を確認する。
	cfg := newValidTestFileArchiveConfig(t)
	cfg.compressFormat = pkgfsstore.CompressFormat("invalid/format")
	store := FileStore{Config: cfg}
	returnedPath, err := store.CompressOldFileWithResult(context.Background(), sourcePath)
	var formatErr *fsstore.CompressionFormatExtensionError
	if !errors.As(err, &formatErr) || formatErr.Format != cfg.compressFormat {
		t.Fatalf("error = %v, want missing extension for %q", err, cfg.compressFormat)
	}
	if returnedPath != "" {
		t.Fatalf("compressed path = %q, want empty", returnedPath)
	}
	content, err := os.ReadFile(sourcePath)
	if err != nil || string(content) != string(wantContent) {
		t.Fatalf("source content = %q, err = %v, want %q", content, err, wantContent)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(sourcePath) {
		t.Fatalf("directory entries = %v, want only source (no compressed/tmp/pending)", entries)
	}
}
