package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

type processingTestConfig struct {
	spoolPath string
}

func (c processingTestConfig) SpoolPath() string { return c.spoolPath }
func (c processingTestConfig) Target() string    { return "test-app" }
func (c processingTestConfig) Dir() string       { return "opensearch" }

func TestMoveProcessingToQuarantinePreservesReasonInManagedFileName(t *testing.T) {
	t.Parallel()

	store, err := NewStateStore(processingTestConfig{spoolPath: t.TempDir()})
	if err != nil {
		t.Fatalf("NewStateStore returned error: %v", err)
	}
	rel := filepath.Join("2026", "08", "20", "12", "34", "56789_random"+spooldir.SpoolExt)
	processingPath := filepath.Join(store.RootDir.ProcessingDir().Path(), rel)
	if err := os.MkdirAll(filepath.Dir(processingPath), 0o755); err != nil {
		t.Fatalf("create processing directory: %v", err)
	}
	if err := os.WriteFile(processingPath, []byte("broken spool body"), 0o644); err != nil {
		t.Fatalf("write processing file: %v", err)
	}

	err = store.MoveProcessingToQuarantine(
		context.Background(),
		processingPath,
		pkgspool.QuarantineReasonInvalidSpoolFile,
	)
	if err != nil {
		t.Fatalf("MoveProcessingToQuarantine returned error: %v", err)
	}

	quarantineDir := filepath.Join(
		store.RootDir.QuarantineDir().Path(),
		filepath.Dir(rel),
	)
	entries, err := os.ReadDir(quarantineDir)
	if err != nil {
		t.Fatalf("read quarantine directory: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("quarantine file count = %d, want 1", len(entries))
	}
	name := entries[0].Name()
	if !strings.Contains(name, "-quarantine-invalid-spool-file") {
		t.Fatalf("quarantine file name %q does not contain the reason", name)
	}
	if _, err := os.Stat(processingPath); !os.IsNotExist(err) {
		t.Fatalf("processing file stat error = %v, want not exist", err)
	}
}

func TestProcessingFileExistsChecksPathItself(t *testing.T) {
	t.Parallel()

	store, err := NewStateStore(processingTestConfig{spoolPath: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	dir := store.RootDir.ProcessingDir().Path()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	existing := filepath.Join(dir, "existing"+spooldir.SpoolExt)
	missing := filepath.Join(dir, "missing"+spooldir.SpoolExt)
	dangling := filepath.Join(dir, "dangling"+spooldir.SpoolExt)
	if err := os.WriteFile(existing, []byte("spool body"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		path string
		want bool
	}{
		{name: "existing", path: existing, want: true},
		{name: "missing", path: missing},
		{name: "dangling_symlink", path: dangling, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.path == dangling {
				testutil.Symlink(t, missing, dangling)
			}
			exists, err := store.ProcessingFileExists(context.Background(), tc.path)
			if err != nil || exists != tc.want {
				t.Fatalf("ProcessingFileExists = %v, %v, want %v, nil", exists, err, tc.want)
			}
		})
	}
}

func TestProcessingFileExistsPreservesContextAndPathValidation(t *testing.T) {
	t.Parallel()

	store, err := NewStateStore(processingTestConfig{spoolPath: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	outside := filepath.Join(store.RootDir.ActiveDir().Path(), "outside"+spooldir.SpoolExt)
	// contextを先に検査し、processing外のpathは存在確認前に既存の分類で拒否する。
	for _, tc := range []struct {
		name string
		ctx  context.Context
		path string
		err  error
	}{
		{name: "canceled", ctx: ctx, path: filepath.Join(store.RootDir.ProcessingDir().Path(), "missing"+spooldir.SpoolExt), err: context.Canceled},
		{name: "canceled_before_validation", ctx: ctx, path: outside, err: context.Canceled},
		{name: "outside_processing", ctx: context.Background(), path: outside, err: pkgspool.ErrValidateProcessingSpoolFilePath},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exists, err := store.ProcessingFileExists(tc.ctx, tc.path)
			if exists || !errors.Is(err, tc.err) {
				t.Fatalf("ProcessingFileExists = %v, %v, want false, %v", exists, err, tc.err)
			}
		})
	}
}

func TestProcessingFileExistsPreservesFilesystemErrorWrapping(t *testing.T) {
	t.Parallel()

	store, err := NewStateStore(processingTestConfig{spoolPath: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	dir := store.RootDir.ProcessingDir().Path()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// 不存在以外の理由でLstatが失敗するpathを、OSごとのfixtureで作ります。
	path := processingPathWithLstatFailure(t, dir)
	_, lstatErr := os.Lstat(path)
	var want *os.PathError
	if !errors.As(lstatErr, &want) || errors.Is(lstatErr, os.ErrNotExist) {
		t.Fatalf("fixture Lstat error = %v, want a PathError other than not-exist", lstatErr)
	}
	exists, err := store.ProcessingFileExists(context.Background(), path)
	var pathErr *os.PathError
	if exists || !errors.As(err, &pathErr) {
		t.Fatalf("ProcessingFileExists = %v, %v, want false and wrapped PathError", exists, err)
	}
	// 呼び出し箇所の文脈を付けてwrapし、Lstatが返したPathErrorをそのまま保持します。
	if pathErr.Op != want.Op || pathErr.Path != path || !errors.Is(pathErr.Err, want.Err) || err == error(pathErr) {
		t.Fatalf("filesystem error wrapping changed: %v, want wrapped %v", err, lstatErr)
	}
}
