package appendlog

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

var errInvalidUserDefinedAppendLogCompressedFile = errors.New(
	"invalid user-defined appendlog compressed file",
)

const userDefinedAppendLogCompressionHeader = "appendlog-test-codec-v1\n"

// failingAppendLogCompressionCodecはfinal path公開前の圧縮失敗を再現します。
type failingAppendLogCompressionCodec struct {
	err error
}

func (c failingAppendLogCompressionCodec) Format() pkgfsstore.CompressFormat {
	return pkgfsstore.CompressGzip
}

func (c failingAppendLogCompressionCodec) Compress(_, _ *os.File) error {
	return c.err
}

func (failingAppendLogCompressionCodec) VerifyCompressedIntegrity(*os.File) error {
	return nil
}

// invalidOutputAppendLogCompressionCodecは圧縮自体は成功を返す一方、生成物の検証だけが
// 失敗するcodecです。rename前の完全性検証を再現するために使用します。
type invalidOutputAppendLogCompressionCodec struct {
	verificationErr error
	verifyCalls     int
}

func (*invalidOutputAppendLogCompressionCodec) Format() pkgfsstore.CompressFormat {
	return pkgfsstore.CompressFormat("br")
}

func (*invalidOutputAppendLogCompressionCodec) Compress(_, out *os.File) error {
	_, err := io.WriteString(out, "invalid compressed output")
	return err
}

func (c *invalidOutputAppendLogCompressionCodec) VerifyCompressedIntegrity(*os.File) error {
	c.verifyCalls++
	return c.verificationErr
}

type userDefinedAppendLogCompressionCodec struct {
	format pkgfsstore.CompressFormat
}

func (c userDefinedAppendLogCompressionCodec) Format() pkgfsstore.CompressFormat {
	return c.format
}

func (c userDefinedAppendLogCompressionCodec) Compress(in, out *os.File) error {
	if _, err := io.WriteString(out, userDefinedAppendLogCompressionHeader); err != nil {
		return err
	}
	_, err := io.Copy(out, in)
	return err
}

func (c userDefinedAppendLogCompressionCodec) VerifyCompressedIntegrity(in *os.File) error {
	content, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	if !bytes.HasPrefix(content, []byte(userDefinedAppendLogCompressionHeader)) {
		return fmt.Errorf(
			"%w: %w: format=%q header is missing",
			pkgfsstore.ErrCorruptCompressedFile,
			errInvalidUserDefinedAppendLogCompressedFile,
			c.format,
		)
	}
	return nil
}

func TestCompressOldFileAndRemoveSourceUsesUserDefinedFormatExtension(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "custom-compression.log")
	wantContent := []byte("custom compression payload\n")
	if err := os.WriteFile(sourcePath, wantContent, 0o640); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	codec := userDefinedAppendLogCompressionCodec{format: "br"}
	store := FileStore{Config: newCompressionTestConfigWithCodec(codec)}
	returnedPath, err := store.CompressOldFileAndRemoveSource(context.Background(), sourcePath, nil)
	if err != nil {
		t.Fatalf("CompressOldFileAndRemoveSource() error = %v", err)
	}

	wantPath := sourcePath + ".br"
	if returnedPath != wantPath {
		t.Fatalf("compressed path = %q, want %q", returnedPath, wantPath)
	}
	if _, err := os.Stat(sourcePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source os.Stat() error = %v, want os.ErrNotExist", err)
	}
	gotContent, err := os.ReadFile(wantPath)
	if err != nil {
		t.Fatalf("read custom compressed file: %v", err)
	}
	wantCompressedContent := append([]byte(userDefinedAppendLogCompressionHeader), wantContent...)
	if string(gotContent) != string(wantCompressedContent) {
		t.Fatalf("custom compressed content = %q, want %q", gotContent, wantCompressedContent)
	}
	if err := store.VerifyCompressedFileIntegrity(wantPath); err != nil {
		t.Fatalf("VerifyCompressedFileIntegrity() error = %v", err)
	}
}

func TestFileStoreVerifyCompressedFileIntegrityRejectsCorruptUserDefinedFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.br")
	if err := os.WriteFile(path, []byte("missing appendlog codec header"), 0o600); err != nil {
		t.Fatalf("write corrupt compressed file: %v", err)
	}
	codec := userDefinedAppendLogCompressionCodec{format: "br"}
	store := FileStore{Config: newCompressionTestConfigWithCodec(codec)}

	err := store.VerifyCompressedFileIntegrity(path)
	if !errors.Is(err, infrafsstore.ErrInvalidCompressedFile) {
		t.Fatalf("VerifyCompressedFileIntegrity() error = %v, want ErrInvalidCompressedFile", err)
	}
	if !errors.Is(err, errInvalidUserDefinedAppendLogCompressedFile) {
		t.Fatalf("VerifyCompressedFileIntegrity() error = %v, want codec verification cause", err)
	}
}

func TestVerifyExistingCompressedFileIntegrityUsesFileFormatAfterConfigChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app_20260906_1.log.gz")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := gzip.NewWriter(file)
	if _, err := writer.Write([]byte("old gzip payload")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	// 稼働設定はzstdへ変更済みでも、旧.gzはgzipとして検証します。
	store := FileStore{Config: NewConfigAdapter(pkgfsstore.Config{
		Lifecycle: pkgfsstore.FileLifecycleConfig{
			EnableFileCompress: true,
			CompressFormat:     pkgfsstore.CompressZstd,
		},
	})}
	if err := store.VerifyExistingCompressedFileIntegrity(path); err != nil {
		t.Fatalf("VerifyExistingCompressedFileIntegrity() error = %v", err)
	}
}

func TestVerifyExistingCompressedFileIntegrityRejectsCorruptKnownOldFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app_20260906_2.log.gz")
	if err := os.WriteFile(path, []byte("not gzip"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := FileStore{Config: NewConfigAdapter(pkgfsstore.Config{
		Lifecycle: pkgfsstore.FileLifecycleConfig{CompressFormat: pkgfsstore.CompressZstd},
	})}
	err := store.VerifyExistingCompressedFileIntegrity(path)
	if !errors.Is(err, infrafsstore.ErrInvalidCompressedFile) {
		t.Fatalf("VerifyExistingCompressedFileIntegrity() error = %v, want ErrInvalidCompressedFile", err)
	}
	if !errors.Is(err, pkgfsstore.ErrCorruptCompressedFile) {
		t.Fatalf("VerifyExistingCompressedFileIntegrity() error = %v, want ErrCorruptCompressedFile", err)
	}
}

func TestVerifyExistingCompressedFileIntegrityRejectsUnavailableOldCustomFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app_20260906_3.log.retired_codec")
	if err := os.WriteFile(path, []byte("opaque old custom bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := FileStore{Config: newCompressionTestConfig()}
	err := store.VerifyExistingCompressedFileIntegrity(path)
	if !errors.Is(err, infrafsstore.ErrUnsupportedCompressionFormat) {
		t.Fatalf("unavailable old custom codec must prevent source retirement: %v", err)
	}
	if errors.Is(err, pkgfsstore.ErrCorruptCompressedFile) {
		t.Fatalf("unavailable old custom codec must not be treated as corruption: %v", err)
	}
}

func TestVerifyExistingCompressedFileIntegrityUsesCurrentCustomCodec(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app_20260906_4.log.br")
	if err := os.WriteFile(path, []byte("invalid custom bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	codec := userDefinedAppendLogCompressionCodec{format: "br"}
	store := FileStore{Config: newCompressionTestConfigWithCodec(codec)}
	err := store.VerifyExistingCompressedFileIntegrity(path)
	if !errors.Is(err, errInvalidUserDefinedAppendLogCompressedFile) {
		t.Fatalf("VerifyExistingCompressedFileIntegrity() error = %v, want custom verification cause", err)
	}
	if !errors.Is(err, pkgfsstore.ErrCorruptCompressedFile) {
		t.Fatalf("VerifyExistingCompressedFileIntegrity() error = %v, want ErrCorruptCompressedFile", err)
	}
}

func TestVerifyExistingCompressedFileIntegrityDoesNotInferCustomCodecCorruption(t *testing.T) {
	for _, verificationErr := range []error{
		errors.New("custom codec verification was inconclusive"),
		context.Canceled,
		context.DeadlineExceeded,
	} {
		path := filepath.Join(t.TempDir(), "app_20260906_5.log.br")
		if err := os.WriteFile(path, []byte("opaque custom bytes"), 0o600); err != nil {
			t.Fatal(err)
		}
		codec := &invalidOutputAppendLogCompressionCodec{verificationErr: verificationErr}
		store := FileStore{Config: newCompressionTestConfigWithCodec(codec)}

		err := store.VerifyExistingCompressedFileIntegrity(path)
		if !errors.Is(err, infrafsstore.ErrInvalidCompressedFile) ||
			!errors.Is(err, verificationErr) {
			t.Fatalf("VerifyExistingCompressedFileIntegrity() error = %v, want invalid/%v", err, verificationErr)
		}
		if errors.Is(err, pkgfsstore.ErrCorruptCompressedFile) {
			t.Fatalf("custom codec error was inferred as corruption: %v", err)
		}
	}
}

func TestCompressOldFileAndRemoveSourceRejectsEmptySourceAtEntry(t *testing.T) {
	store := FileStore{Config: newCompressionTestConfig()}
	returnedPath, err := store.CompressOldFileAndRemoveSource(context.Background(), "", nil)
	if !errors.Is(err, ErrCompressionSourcePathEmpty) {
		t.Fatalf("CompressOldFileAndRemoveSource(empty source) error = %v, want %v", err, ErrCompressionSourcePathEmpty)
	}
	if returnedPath != "" {
		t.Fatalf("compressed path = %q, want empty", returnedPath)
	}
}

func TestCompressOldFileAndRemoveSourceRejectsUnsafeUserDefinedFormatBeforeMutation(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "unsafe-custom-compression.log")
	wantContent := []byte("source must remain\n")
	if err := os.WriteFile(sourcePath, wantContent, 0o640); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	codec := userDefinedAppendLogCompressionCodec{format: "../br"}
	store := FileStore{Config: newCompressionTestConfigWithCodec(codec)}
	returnedPath, err := store.CompressOldFileAndRemoveSource(context.Background(), sourcePath, nil)
	if !errors.Is(err, infrafsstore.ErrCompressionFormatWithoutExtension) {
		t.Fatalf("CompressOldFileAndRemoveSource() error = %v, want generated path validation error", err)
	}
	if returnedPath != "" {
		t.Fatalf("compressed path = %q, want empty", returnedPath)
	}
	gotContent, readErr := os.ReadFile(sourcePath)
	if readErr != nil {
		t.Fatalf("read retained source: %v", readErr)
	}
	if string(gotContent) != string(wantContent) {
		t.Fatalf("retained source content = %q, want %q", gotContent, wantContent)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read source directory: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(sourcePath) {
		t.Fatalf("directory entries = %v, want only the unchanged source and no tmp/pending file", entries)
	}
}

func TestCompressOldFileAndRemoveSourceReturnsOuterErrorBeforeCompressedFileIsPublished(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "before-rename.log")
	wantContent := []byte("source must remain when compression fails before rename\n")
	if err := os.WriteFile(sourcePath, wantContent, 0o640); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	compressErr := errors.New("injected compression failure")
	codec := failingAppendLogCompressionCodec{err: compressErr}
	store := FileStore{Config: newCompressionTestConfigWithCodec(codec)}

	returnedPath, err := store.CompressOldFileAndRemoveSource(context.Background(), sourcePath, nil)
	if !errors.Is(err, compressErr) {
		t.Fatalf("CompressOldFileAndRemoveSource() error = %v, want %v", err, compressErr)
	}
	if returnedPath != "" {
		t.Fatalf("compressed path = %q, want empty before rename", returnedPath)
	}

	gotContent, readErr := os.ReadFile(sourcePath)
	if readErr != nil {
		t.Fatalf("read retained source file: %v", readErr)
	}
	if string(gotContent) != string(wantContent) {
		t.Fatalf("retained source content = %q, want %q", gotContent, wantContent)
	}

	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	assertFileDoesNotExist(t, compressedPath)
	assertFileDoesNotExist(t, compressedPath+".tmp")
}

func TestCompressOldFileAndRemoveSourceRejectsInvalidOutputBeforeCommit(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "invalid-output.log")
	wantContent := []byte("source must remain when generated output is invalid\n")
	if err := os.WriteFile(sourcePath, wantContent, 0o640); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	verificationErr := errors.New("generated compressed output integrity verification failed")
	codec := &invalidOutputAppendLogCompressionCodec{verificationErr: verificationErr}
	store := FileStore{Config: newCompressionTestConfigWithCodec(codec)}
	cleanupCalled := false
	returnedPath, err := store.compressOldFile(
		context.Background(),
		sourcePath,
		nil,
		func(string) error {
			cleanupCalled = true
			return nil
		},
		(*os.File).Sync,
		infrafsstore.SyncDirectory,
	)
	if !errors.Is(err, infrafsstore.ErrInvalidCompressedFile) {
		t.Fatalf("compressOldFile() error = %v, want ErrInvalidCompressedFile", err)
	}
	if !errors.Is(err, verificationErr) {
		t.Fatalf("compressOldFile() error = %v, want %v", err, verificationErr)
	}
	if returnedPath != "" {
		t.Fatalf("compressed path = %q, want empty", returnedPath)
	}
	if cleanupCalled {
		t.Fatal("source cleanup was called before compressed output verification succeeded")
	}
	if codec.verifyCalls != 1 {
		t.Fatalf("VerifyCompressedIntegrity() calls = %d, want 1", codec.verifyCalls)
	}

	gotContent, readErr := os.ReadFile(sourcePath)
	if readErr != nil {
		t.Fatalf("read retained source file: %v", readErr)
	}
	if string(gotContent) != string(wantContent) {
		t.Fatalf("retained source content = %q, want %q", gotContent, wantContent)
	}
	compressedPath := sourcePath + ".br"
	assertFileDoesNotExist(t, compressedPath)
	assertFileDoesNotExist(t, compressedPath+".tmp")
}

func TestCompressOldFileAndRemoveSourceSeparatesSourceCleanupFailureAfterRename(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source-cleanup-failure.log")
	wantContent := []byte("published compressed content remains usable when source cleanup fails\n")
	if err := os.WriteFile(sourcePath, wantContent, 0o640); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	config := newCompressionTestConfig()
	store := FileStore{Config: config}
	removeErr := errors.New("injected source removal failure")
	pendingSourcePath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	returnedPath, err := store.compressOldFile(
		context.Background(),
		sourcePath,
		nil,
		func(path string) error {
			if path != pendingSourcePath {
				t.Fatalf("removePendingSource() path = %q, want %q", path, pendingSourcePath)
			}
			if _, statErr := os.Stat(sourcePath); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("canonical source during cleanup os.Stat() error = %v, want os.ErrNotExist", statErr)
			}
			gotPendingContent, readErr := os.ReadFile(pendingSourcePath)
			if readErr != nil {
				t.Fatalf("read pending source during cleanup: %v", readErr)
			}
			if string(gotPendingContent) != string(wantContent) {
				t.Fatalf("pending source content during cleanup = %q, want %q", gotPendingContent, wantContent)
			}
			return removeErr
		},
		(*os.File).Sync,
		infrafsstore.SyncDirectory,
	)
	if !errors.Is(err, removeErr) {
		t.Fatalf("compressOldFile() error = %v, want %v", err, removeErr)
	}

	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	if returnedPath != compressedPath {
		t.Fatalf("compressed path = %q, want %q", returnedPath, compressedPath)
	}
	if !errors.Is(err, ErrCompressionSourceCleanup) {
		t.Fatalf("compressOldFile() error = %v, want ErrCompressionSourceCleanup", err)
	}
	requireCompressionSourceRetirementPaths(t, err, sourcePath, pendingSourcePath, compressedPath)

	if _, statErr := os.Stat(sourcePath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("canonical source os.Stat() error = %v, want os.ErrNotExist", statErr)
	}
	gotSourceContent, readErr := os.ReadFile(pendingSourcePath)
	if readErr != nil {
		t.Fatalf("read pending source after cleanup failure: %v", readErr)
	}
	if string(gotSourceContent) != string(wantContent) {
		t.Fatalf("pending source content after cleanup failure = %q, want %q", gotSourceContent, wantContent)
	}
	if gotCompressedContent := readGzipContent(t, compressedPath); string(gotCompressedContent) != string(wantContent) {
		t.Fatalf("compressed content = %q, want %q", gotCompressedContent, wantContent)
	}
	assertFileDoesNotExist(t, compressedPath+".tmp")
}

func TestRemoveSourceAfterCompressionDoesNotRestoreCanonicalSourceWhenCompressedFileIsLaterRemoved(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "manual-compressed-removal_20260830_1.log")
	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	wantContent := []byte("source already had one successful compression\n")
	if err := os.WriteFile(sourcePath, wantContent, 0o640); err != nil {
		t.Fatalf("write source file: %v", err)
	}
	if err := os.WriteFile(compressedPath, []byte("published compressed file"), 0o640); err != nil {
		t.Fatalf("write compressed file: %v", err)
	}

	removeErr := errors.New("injected pending source removal failure")
	err := removeSourceAfterCompression(
		sourcePath,
		compressedPath,
		os.Rename,
		func(string) error { return removeErr },
		infrafsstore.SyncDirectory,
	)
	if !errors.Is(err, removeErr) {
		t.Fatalf("removeSourceAfterCompression() error = %v, want %v", err, removeErr)
	}
	pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	if _, statErr := os.Stat(sourcePath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("canonical source os.Stat() error = %v, want os.ErrNotExist", statErr)
	}
	if got, readErr := os.ReadFile(pendingPath); readErr != nil || string(got) != string(wantContent) {
		t.Fatalf("pending source = %q, %v; want %q", got, readErr, wantContent)
	}

	// 圧縮正常完了後の外部操作を再現します。圧縮fileが失われても、sourceはmanagedな
	// `.log`名へ戻らないため、次回走査で再圧縮されません。
	if err := os.Remove(compressedPath); err != nil {
		t.Fatalf("remove compressed file manually: %v", err)
	}
	pattern := logpattern.FileNamePattern{
		Dir:       dir,
		Target:    "manual-compressed-removal",
		Extension: "log",
	}
	managed, err := logpattern.GlobFileNames(pattern, logpattern.GlobByTarget)
	if err != nil {
		t.Fatalf("GlobFileNames() error = %v", err)
	}
	if len(managed) != 0 {
		t.Fatalf("managed files after compressed removal = %v, want none", managed)
	}

	pending, err := logpattern.FindCompressionSourceDeletePendingPaths(pattern, logpattern.GlobByTarget)
	if err != nil {
		t.Fatalf("FindCompressionSourceDeletePendingPaths() error = %v", err)
	}
	if len(pending) != 1 || pending[0] != pendingPath {
		t.Fatalf("pending source paths = %v, want [%s]", pending, pendingPath)
	}
	if _, err := removeCompressionSourceDeletePendingPath(pendingPath, os.Remove, infrafsstore.SyncDirectory); err != nil {
		t.Fatalf("removeCompressionSourceDeletePendingPath() error = %v", err)
	}
	assertFileDoesNotExist(t, pendingPath)
}

func TestRemoveSourceAfterCompressionUsesExistingPendingSourceAfterRestart(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "restart-retirement.log")
	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	if err := os.WriteFile(compressedPath, []byte("published compressed file"), 0o600); err != nil {
		t.Fatalf("write compressed file: %v", err)
	}
	if err := os.WriteFile(pendingPath, []byte("retired source"), 0o600); err != nil {
		t.Fatalf("write pending source: %v", err)
	}

	if err := RemoveSourceAfterCompression(sourcePath, compressedPath); err != nil {
		t.Fatalf("RemoveSourceAfterCompression() error = %v", err)
	}
	assertFileDoesNotExist(t, sourcePath)
	assertFileDoesNotExist(t, pendingPath)
}

func TestRemoveSourceAfterCompressionRenameFailureLeavesCanonicalSourceRetryable(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "retirement-rename-failure.log")
	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	wantSource := []byte("source must remain retryable")
	if err := os.WriteFile(sourcePath, wantSource, 0o600); err != nil {
		t.Fatalf("write source file: %v", err)
	}
	if err := os.WriteFile(compressedPath, []byte("published compressed file"), 0o600); err != nil {
		t.Fatalf("write compressed file: %v", err)
	}

	renameErr := errors.New("injected source retirement rename failure")
	removeCalled := false
	err := removeSourceAfterCompression(
		sourcePath,
		compressedPath,
		func(oldPath, newPath string) error {
			if oldPath != sourcePath || newPath != logpattern.CompressionSourceDeletePendingPath(sourcePath) {
				t.Fatalf("rename paths = (%q, %q), want (%q, %q)", oldPath, newPath, sourcePath, logpattern.CompressionSourceDeletePendingPath(sourcePath))
			}
			return renameErr
		},
		func(string) error {
			removeCalled = true
			return nil
		},
		infrafsstore.SyncDirectory,
	)
	if !errors.Is(err, renameErr) {
		t.Fatalf("removeSourceAfterCompression() error = %v, want %v", err, renameErr)
	}
	if errors.Is(err, ErrCompressionSourceCleanup) {
		t.Fatalf("removeSourceAfterCompression() error = %v, must not match cleanup sentinel", err)
	}
	if removeCalled {
		t.Fatal("remove function called after source retirement rename failure")
	}
	gotSource, readErr := os.ReadFile(sourcePath)
	if readErr != nil || string(gotSource) != string(wantSource) {
		t.Fatalf("canonical source after rename failure = %q, %v; want %q", gotSource, readErr, wantSource)
	}
	assertFileDoesNotExist(t, logpattern.CompressionSourceDeletePendingPath(sourcePath))
}

func TestRemoveSourceAfterCompressionRejectsCanonicalAndPendingConflict(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "retirement-conflict.log")
	pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	if err := os.WriteFile(sourcePath, []byte("canonical"), 0o600); err != nil {
		t.Fatalf("write canonical source: %v", err)
	}
	if err := os.WriteFile(pendingPath, []byte("previous retired source"), 0o600); err != nil {
		t.Fatalf("write pending source: %v", err)
	}

	err := RemoveSourceAfterCompression(sourcePath, compressedPath)
	if !errors.Is(err, ErrCompressionSourceDeletePendingConflict) {
		t.Fatalf("RemoveSourceAfterCompression() error = %v, want conflict", err)
	}
	canonical, readErr := os.ReadFile(sourcePath)
	if readErr != nil || string(canonical) != "canonical" {
		t.Fatalf("canonical source = %q, %v; want unchanged", canonical, readErr)
	}
	pending, readErr := os.ReadFile(pendingPath)
	if readErr != nil || string(pending) != "previous retired source" {
		t.Fatalf("pending source = %q, %v; want unchanged", pending, readErr)
	}
}

func TestFindCompressionSourceDeletePendingPathsSeparatesTargetAndDirectoryModes(t *testing.T) {
	dir := t.TempDir()
	paths := []string{
		logpattern.CompressionSourceDeletePendingPath(filepath.Join(dir, "app_20260830_1.log")),
		logpattern.CompressionSourceDeletePendingPath(filepath.Join(dir, "other_20260830_1.log")),
	}
	for _, path := range paths {
		if err := os.WriteFile(path, []byte("pending"), 0o600); err != nil {
			t.Fatalf("write pending source %q: %v", path, err)
		}
	}
	pattern := logpattern.FileNamePattern{Dir: dir, Target: "app", Extension: "log"}

	target, err := logpattern.FindCompressionSourceDeletePendingPaths(pattern, logpattern.GlobByTarget)
	if err != nil {
		t.Fatalf("target pending source scan: %v", err)
	}
	if len(target) != 1 || target[0] != paths[0] {
		t.Fatalf("target pending paths = %v, want [%s]", target, paths[0])
	}
	all, err := logpattern.FindCompressionSourceDeletePendingPaths(pattern, logpattern.GlobAllFiles)
	if err != nil {
		t.Fatalf("directory pending source scan: %v", err)
	}
	if len(all) != len(paths) {
		t.Fatalf("all pending paths = %v, want %v", all, paths)
	}
}

func newCompressionTestConfig() ConfigProvider {
	return newCompressionTestConfigWithCodec(nil)
}

func newCompressionTestConfigWithCodec(codec pkgfsstore.CompressionCodec) ConfigProvider {
	return NewConfigAdapter(pkgfsstore.Config{
		CompressionCodec: codec,
		Lifecycle: pkgfsstore.FileLifecycleConfig{
			EnableFileCompress: true,
			CompressFormat:     pkgfsstore.CompressGzip,
		},
	})
}

func readGzipContent(t *testing.T, path string) []byte {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open gzip file %q: %v", path, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			t.Errorf("close gzip file %q: %v", path, closeErr)
		}
	}()

	reader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("create gzip reader for %q: %v", path, err)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			t.Errorf("close gzip reader for %q: %v", path, closeErr)
		}
	}()

	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read gzip file %q: %v", path, err)
	}
	return content
}

func assertFileDoesNotExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("os.Stat(%q) error = %v, want os.ErrNotExist", path, err)
	}
}
