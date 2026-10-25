package failurearchive

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

type testFileArchiveConfig struct {
	target                     string
	lifecycleMode              pkgfsstore.FileLifecycleMode
	rotationType               pkgfsstore.FileRotationType
	rotationDayOfWeek          time.Weekday
	fileNameFormat             string
	fileDateFormat             string
	maxFileSize                uint64
	maxDirSize                 uint64
	maxFileAge                 int
	maxFileBackups             int
	enableFileCompress         bool
	compressFormat             pkgfsstore.CompressFormat
	compressionCodec           pkgfsstore.CompressionCodec
	archivePath                string
	dir                        string
	dirCaseRetryable           string
	dirCasePermanent           string
	enableRotate               bool
	maxWriteAttempts           int
	fileOperationRetryWaitTime time.Duration
}

type usageCacheTTLTestFileArchiveConfig struct {
	testFileArchiveConfig
	usageCacheTTL time.Duration
}

func (c usageCacheTTLTestFileArchiveConfig) UsageCacheTTL() time.Duration {
	return c.usageCacheTTL
}

func (c testFileArchiveConfig) Target() string { return c.target }
func (c testFileArchiveConfig) LifecycleMode() pkgfsstore.FileLifecycleMode {
	return c.lifecycleMode
}
func (c testFileArchiveConfig) RotationType() pkgfsstore.FileRotationType {
	return c.rotationType
}
func (c testFileArchiveConfig) RotationDayOfWeek() time.Weekday {
	return c.rotationDayOfWeek
}
func (c testFileArchiveConfig) FileNameFormat() string { return c.fileNameFormat }
func (c testFileArchiveConfig) FileDateFormat() string { return c.fileDateFormat }
func (c testFileArchiveConfig) MaxFileSize() uint64    { return c.maxFileSize }
func (c testFileArchiveConfig) UsageCacheTTL() time.Duration {
	return time.Hour
}
func (c testFileArchiveConfig) MaxDirSize() uint64  { return c.maxDirSize }
func (c testFileArchiveConfig) MaxFileAge() int     { return c.maxFileAge }
func (c testFileArchiveConfig) MaxFileBackups() int { return c.maxFileBackups }
func (c testFileArchiveConfig) EnableFileCompress() bool {
	return c.enableFileCompress
}
func (c testFileArchiveConfig) CompressFormat() pkgfsstore.CompressFormat {
	return c.compressFormat
}
func (c testFileArchiveConfig) CompressionCodec() pkgfsstore.CompressionCodec {
	return c.compressionCodec
}
func (c testFileArchiveConfig) ArchivePath() string { return c.archivePath }
func (c testFileArchiveConfig) Dir() string         { return c.dir }
func (c testFileArchiveConfig) DirCaseRetryable() string {
	return c.dirCaseRetryable
}
func (c testFileArchiveConfig) DirCasePermanent() string {
	return c.dirCasePermanent
}
func (c testFileArchiveConfig) EnableRotate() bool { return c.enableRotate }
func (c testFileArchiveConfig) MaxWriteAttempts() int {
	return c.maxWriteAttempts
}
func (c testFileArchiveConfig) FileOperationRetryWaitTime() time.Duration {
	return c.fileOperationRetryWaitTime
}

type failingCompressor struct {
	format pkgfsstore.CompressFormat
	err    error
}

func (c failingCompressor) Format() pkgfsstore.CompressFormat {
	return c.format
}

func (c failingCompressor) Compress(_, _ *os.File) error {
	return c.err
}

func (c failingCompressor) VerifyCompressedIntegrity(*os.File) error {
	return c.err
}

type corruptOutputCompressor struct {
	format          pkgfsstore.CompressFormat
	verificationErr error
}

func (c corruptOutputCompressor) Format() pkgfsstore.CompressFormat {
	return c.format
}

func (c corruptOutputCompressor) Compress(_, out *os.File) error {
	_, err := out.Write([]byte("invalid compressed output"))
	return err
}

func (c corruptOutputCompressor) VerifyCompressedIntegrity(*os.File) error {
	return c.verificationErr
}

func TestValidateFileArchiveConfigProvider(t *testing.T) {
	t.Parallel()

	t.Run("valid", func(t *testing.T) {
		t.Parallel()

		cfg := newValidTestFileArchiveConfig(t)

		if err := ValidateFileArchiveConfigProvider(cfg); err != nil {
			t.Fatalf("ValidateFileArchiveConfigProvider returned error: %v", err)
		}
	})

	t.Run("reports joined errors", func(t *testing.T) {
		t.Parallel()

		cfg := newValidTestFileArchiveConfig(t)
		cfg.target = ""
		cfg.archivePath = ""
		cfg.dirCaseRetryable = ""
		cfg.dirCasePermanent = ""
		cfg.fileNameFormat = "/wrong_%s.log"
		cfg.fileDateFormat = "2006-01-02"
		cfg.maxFileSize = 1
		cfg.maxDirSize = 2
		// 保持上限の0は上限なしとして受け付けるため、0以外で最小値未満の値を使います。
		cfg.maxFileAge = -1
		cfg.maxFileBackups = 1
		cfg.maxWriteAttempts = 0
		cfg.fileOperationRetryWaitTime = 0

		err := ValidateFileArchiveConfigProvider(cfg)
		if err == nil {
			t.Fatal("ValidateFileArchiveConfigProvider returned nil, want error")
		}

		assertErrorIs(t, err,
			ErrInvalidTarget,
			ErrInvalidArchivePath,
			ErrInvalidDirCaseRetryable,
			ErrInvalidDirCasePermanent,
			ErrInvalidFileNameFormat,
			ErrFileNameFormatInvalidPrefix,
			ErrInvalidFileDateFormat,
			fsstore.ErrInvalidMaxFileSize,
			fsstore.ErrInvalidMaxFileBackups,
			fsstore.ErrInvalidMaxFileAge,
			fsstore.ErrInvalidMaxDirSize,
			fsstore.ErrInvalidMaxWriteAttempts,
			fsstore.ErrInvalidFileOperationRetryWaitTime,
		)
	})

	t.Run("rejects overlapping resolved archive directories", func(t *testing.T) {
		t.Parallel()

		cfg := newValidTestFileArchiveConfig(t)
		cfg.dirCaseRetryable = "failed"
		cfg.dirCasePermanent = "failed/permanent"

		err := ValidateFileArchiveConfigProvider(cfg)
		if err == nil {
			t.Fatal("ValidateFileArchiveConfigProvider returned nil, want error")
		}
		assertErrorIs(t, err, ErrArchiveDirectoriesOverlap)
	})

	t.Run("rejects archive directories outside the leased root", func(t *testing.T) {
		t.Parallel()

		cfg := newValidTestFileArchiveConfig(t)
		cfg.dirCaseRetryable = "../retryable"
		cfg.dirCasePermanent = "../permanent"

		err := ValidateFileArchiveConfigProvider(cfg)
		if err == nil {
			t.Fatal("ValidateFileArchiveConfigProvider returned nil, want root containment error")
		}
		assertErrorIs(t, err, ErrArchiveDirectoryOutsideRoot)
	})

	t.Run("rejects archive root itself as a terminal directory", func(t *testing.T) {
		t.Parallel()

		cfg := newValidTestFileArchiveConfig(t)
		cfg.dirCaseRetryable = "."

		err := ValidateFileArchiveConfigProvider(cfg)
		if err == nil {
			t.Fatal("ValidateFileArchiveConfigProvider returned nil, want strict descendant error")
		}
		assertErrorIs(t, err, ErrArchiveDirectoryOutsideRoot)
	})
}

func TestValidateFileArchiveLifecycleConfigProvider(t *testing.T) {
	t.Parallel()

	t.Run("valid", func(t *testing.T) {
		t.Parallel()

		cfg := newValidTestFileArchiveConfig(t)
		if err := ValidateFileArchiveLifecycleConfigProvider(cfg); err != nil {
			t.Fatalf("ValidateFileArchiveLifecycleConfigProvider returned error: %v", err)
		}
	})

	t.Run("rejects non-zero values below the shared fsstore limits", func(t *testing.T) {
		t.Parallel()

		cfg := newValidTestFileArchiveConfig(t)
		cfg.maxFileSize = 1
		cfg.maxDirSize = 2
		cfg.maxFileAge = -1
		cfg.maxFileBackups = 1

		err := ValidateFileArchiveLifecycleConfigProvider(cfg)
		if err == nil {
			t.Fatal("ValidateFileArchiveLifecycleConfigProvider returned nil, want error")
		}
		assertErrorIs(t, err,
			fsstore.ErrInvalidMaxFileSize,
			fsstore.ErrInvalidMaxFileBackups,
			fsstore.ErrInvalidMaxFileAge,
			fsstore.ErrInvalidMaxDirSize,
		)
	})

	// 保持世代数・保持日数・ディレクトリ最大サイズの0は、上限なし（その条件では削除しない）です。
	// 各項目を単独で0にしても、すべてを0にしても受け付けます。
	for _, test := range []struct {
		name   string
		mutate func(*testFileArchiveConfig)
	}{
		{name: "zero backups", mutate: func(cfg *testFileArchiveConfig) { cfg.maxFileBackups = 0 }},
		{name: "zero age", mutate: func(cfg *testFileArchiveConfig) { cfg.maxFileAge = 0 }},
		{name: "zero dir size", mutate: func(cfg *testFileArchiveConfig) { cfg.maxDirSize = 0 }},
		{name: "all retention limits zero", mutate: func(cfg *testFileArchiveConfig) {
			cfg.maxFileBackups = 0
			cfg.maxFileAge = 0
			cfg.maxDirSize = 0
		}},
	} {
		t.Run("accepts "+test.name+" as unlimited", func(t *testing.T) {
			t.Parallel()

			cfg := newValidTestFileArchiveConfig(t)
			test.mutate(&cfg)
			if err := ValidateFileArchiveLifecycleConfigProvider(cfg); err != nil {
				t.Fatalf("ValidateFileArchiveLifecycleConfigProvider() error = %v, want nil", err)
			}
			if err := ValidateFileArchiveConfigProvider(cfg); err != nil {
				t.Fatalf("ValidateFileArchiveConfigProvider() error = %v, want nil", err)
			}
		})
	}

	t.Run("still rejects zero max file size", func(t *testing.T) {
		t.Parallel()

		cfg := newValidTestFileArchiveConfig(t)
		cfg.maxFileSize = 0
		cfg.maxFileBackups = 0
		cfg.maxFileAge = 0
		cfg.maxDirSize = 0

		err := ValidateFileArchiveLifecycleConfigProvider(cfg)
		if err == nil {
			t.Fatal("ValidateFileArchiveLifecycleConfigProvider returned nil, want max file size error")
		}
		assertErrorIs(t, err, fsstore.ErrInvalidMaxFileSize)
	})

	t.Run("rejects unknown lifecycle mode", func(t *testing.T) {
		t.Parallel()

		cfg := newValidTestFileArchiveConfig(t)
		cfg.lifecycleMode = pkgfsstore.FileLifecycleMode("unknown")

		err := ValidateFileArchiveLifecycleConfigProvider(cfg)
		if err == nil {
			t.Fatal("ValidateFileArchiveLifecycleConfigProvider returned nil, want error")
		}
		assertErrorIs(t, err, pkgfsstore.ErrUnsupportedFileLifecycleMode)
	})

	t.Run("allows unknown rotation for daily fallback", func(t *testing.T) {
		t.Parallel()

		cfg := newValidTestFileArchiveConfig(t)
		cfg.rotationType = pkgfsstore.FileRotationType("hourly")

		if err := ValidateFileArchiveLifecycleConfigProvider(cfg); err != nil {
			t.Fatalf("ValidateFileArchiveLifecycleConfigProvider() error = %v, want nil", err)
		}
	})

	t.Run("rejects invalid weekly rotation day", func(t *testing.T) {
		t.Parallel()

		cfg := newValidTestFileArchiveConfig(t)
		cfg.rotationType = pkgfsstore.FileRotationWeekly
		cfg.rotationDayOfWeek = time.Weekday(7)

		err := ValidateFileArchiveLifecycleConfigProvider(cfg)
		if err == nil {
			t.Fatal("ValidateFileArchiveLifecycleConfigProvider returned nil, want weekday error")
		}
		assertErrorIs(t, err, pkgfsstore.ErrInvalidFileRotationWeekday)
	})

	for _, test := range []struct {
		name    string
		mode    pkgfsstore.FileLifecycleMode
		wantErr bool
	}{
		{name: "default inline rejects invalid usage cache TTL", wantErr: true},
		{name: "inline rejects invalid usage cache TTL", mode: pkgfsstore.FileLifecycleModeInline, wantErr: true},
		{name: "runner rejects invalid usage cache TTL", mode: pkgfsstore.FileLifecycleModeRunner, wantErr: true},
		{name: "disable ignores unused usage cache TTL", mode: pkgfsstore.FileLifecycleModeDisable},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			cfg := newValidTestFileArchiveConfig(t)
			cfg.lifecycleMode = test.mode
			err := ValidateFileArchiveLifecycleConfigProvider(usageCacheTTLTestFileArchiveConfig{
				testFileArchiveConfig: cfg,
				usageCacheTTL:         -time.Second,
			})
			if (err != nil) != test.wantErr {
				t.Fatalf("ValidateFileArchiveLifecycleConfigProvider() error = %v, wantErr %t", err, test.wantErr)
			}
			if err != nil {
				assertErrorIs(t, err, pkgfsstore.ErrInvalidUsageCacheTTL)
			}
		})
	}
}

func TestValidateFileArchiveDirectorySeparationConfigProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		dirCaseRetryable string
		dirCasePermanent string
		wantOverlapError bool
	}{
		{
			name:             "separate sibling directories",
			dirCaseRetryable: "failed/retryable",
			dirCasePermanent: "failed/permanent",
		},
		{
			name:             "similar prefixes are separate directories",
			dirCaseRetryable: "failure",
			dirCasePermanent: "failures",
		},
		{
			name:             "identical directories",
			dirCaseRetryable: "failed",
			dirCasePermanent: "failed",
			wantOverlapError: true,
		},
		{
			name:             "normalized directories are identical",
			dirCaseRetryable: "failed",
			dirCasePermanent: "temporary/../failed",
			wantOverlapError: true,
		},
		{
			name:             "permanent is nested below retryable",
			dirCaseRetryable: "failed",
			dirCasePermanent: "failed/permanent",
			wantOverlapError: true,
		},
		{
			name:             "retryable is nested below permanent",
			dirCaseRetryable: "failed/retryable",
			dirCasePermanent: "failed",
			wantOverlapError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := newValidTestFileArchiveConfig(t)
			cfg.dirCaseRetryable = tt.dirCaseRetryable
			cfg.dirCasePermanent = tt.dirCasePermanent

			err := ValidateFileArchiveDirectorySeparationConfigProvider(cfg)
			if !tt.wantOverlapError {
				if err != nil {
					t.Fatalf("ValidateFileArchiveDirectorySeparationConfigProvider returned error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("ValidateFileArchiveDirectorySeparationConfigProvider returned nil, want error")
			}
			assertErrorIs(t, err, ErrArchiveDirectoriesOverlap)
		})
	}
}

func TestValidateLayoutConfigProviderRejectsInvalidFileNameFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		format  string
		wantErr error
	}{
		{
			name:    "missing extension",
			format:  "/%s%s_%s_%d",
			wantErr: ErrFileNameFormatNoExtension,
		},
		{
			name:    "invalid prefix",
			format:  "%s%s_%s_%d.jsonl",
			wantErr: ErrFileNameFormatInvalidPrefix,
		},
		{
			name:    "extension has whitespace",
			format:  "/%s%s_%s_%d.jsonl ",
			wantErr: ErrFileNameFormatExtWhitespace,
		},
		{
			name:    "extension has separator",
			format:  "/%s%s_%s_%d.jsonl/bak",
			wantErr: ErrFileNameFormatExtSeparator,
		},
		{
			name:    "extension has control character",
			format:  "/%s%s_%s_%d.jsonl\x00",
			wantErr: ErrFileNameFormatExtControl,
		},
		{
			name:    "structurally valid log format violates JSONL contract",
			format:  pkgfsstore.FileNameFormat,
			wantErr: ErrFileNameFormatMismatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := newValidTestFileArchiveConfig(t)
			cfg.fileNameFormat = tt.format

			err := ValidateLayoutConfigProvider(cfg)
			if err == nil {
				t.Fatal("ValidateLayoutConfigProvider returned nil, want error")
			}
			assertErrorIs(t, err, ErrInvalidFileNameFormat, tt.wantErr)
		})
	}
}

func TestGetNewFilePathsCreateCaseDirectories(t *testing.T) {
	t.Parallel()

	cfg := newValidTestFileArchiveConfig(t)
	retryablePattern := newTestFileNamePattern(cfg)
	permanentPattern := newTestFileNamePattern(cfg)
	resolvedAt := time.Date(2042, time.May, 6, 8, 30, 0, 0, time.UTC)

	retryablePath, err := GetNewRetryableFilePath(&retryablePattern, cfg, resolvedAt)
	if err != nil {
		t.Fatalf("GetNewRetryableFilePath returned error: %v", err)
	}
	permanentPath, err := GetNewPermanentFilePath(&permanentPattern, cfg, resolvedAt)
	if err != nil {
		t.Fatalf("GetNewPermanentFilePath returned error: %v", err)
	}

	wantRetryableDir := filepath.Join(cfg.archivePath, cfg.target, cfg.dir, cfg.dirCaseRetryable)
	wantPermanentDir := filepath.Join(cfg.archivePath, cfg.target, cfg.dir, cfg.dirCasePermanent)
	assertDirExists(t, wantRetryableDir)
	assertDirExists(t, wantPermanentDir)

	if filepath.Dir(retryablePath) != wantRetryableDir {
		t.Fatalf("retryable dir = %q, want %q", filepath.Dir(retryablePath), wantRetryableDir)
	}
	if filepath.Dir(permanentPath) != wantPermanentDir {
		t.Fatalf("permanent dir = %q, want %q", filepath.Dir(permanentPath), wantPermanentDir)
	}
	if !strings.HasSuffix(retryablePath, ".jsonl") {
		t.Fatalf("retryable path = %q, want .jsonl suffix", retryablePath)
	}
	if !strings.HasSuffix(permanentPath, ".jsonl") {
		t.Fatalf("permanent path = %q, want .jsonl suffix", permanentPath)
	}
}

func TestUpdateFilePathAndGetOldpath(t *testing.T) {
	t.Parallel()

	cfg := newValidTestFileArchiveConfig(t)
	store := FileStore{
		Config:                  cfg,
		ActiveFilePathRetryable: "/old/retryable.jsonl",
		ActiveFilePathPermanent: "/old/permanent.jsonl",
		FileNameRetryable:       newTestFileNamePattern(cfg),
		FileNamePermanent:       newTestFileNamePattern(cfg),
	}

	resolvedAt := time.Date(2042, time.May, 6, 8, 30, 0, 0, time.UTC)
	oldRetryablePath, err := store.UpdateFileRetryableFilePathAndGetOldpath(resolvedAt)
	if err != nil {
		t.Fatalf("UpdateFileRetryableFilePathAndGetOldpath returned error: %v", err)
	}
	oldPermanentPath, err := store.UpdateFilePermanentFilePathAndGetOldpath(resolvedAt)
	if err != nil {
		t.Fatalf("UpdateFilePermanentFilePathAndGetOldpath returned error: %v", err)
	}

	if oldRetryablePath != "/old/retryable.jsonl" {
		t.Fatalf("old retryable path = %q, want %q", oldRetryablePath, "/old/retryable.jsonl")
	}
	if oldPermanentPath != "/old/permanent.jsonl" {
		t.Fatalf("old permanent path = %q, want %q", oldPermanentPath, "/old/permanent.jsonl")
	}
	if filepath.Dir(store.ActiveFilePathRetryable) != filepath.Join(cfg.archivePath, cfg.target, cfg.dir, cfg.dirCaseRetryable) {
		t.Fatalf("ActiveFilePathRetryable = %q, want retryable dir", store.ActiveFilePathRetryable)
	}
	if filepath.Dir(store.ActiveFilePathPermanent) != filepath.Join(cfg.archivePath, cfg.target, cfg.dir, cfg.dirCasePermanent) {
		t.Fatalf("ActiveFilePathPermanent = %q, want permanent dir", store.ActiveFilePathPermanent)
	}
}

func TestShouldCompressFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		active  string
		old     string
		enabled bool
		want    bool
	}{
		{
			name:    "old path is empty",
			active:  "/tmp/current.jsonl",
			old:     "",
			enabled: true,
			want:    false,
		},
		{
			name:    "old path is still active",
			active:  "/tmp/current.jsonl",
			old:     "/tmp/current.jsonl",
			enabled: true,
			want:    false,
		},
		{
			name:    "compression disabled",
			active:  "/tmp/current.jsonl",
			old:     "/tmp/old.jsonl",
			enabled: false,
			want:    false,
		},
		{
			name:    "old inactive file with compression enabled",
			active:  "/tmp/current.jsonl",
			old:     "/tmp/old.jsonl",
			enabled: true,
			want:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := newValidTestFileArchiveConfig(t)
			cfg.enableFileCompress = tt.enabled
			store := FileStore{Config: cfg}

			if got := store.ShouldCompressFile(tt.active, tt.old); got != tt.want {
				t.Fatalf("ShouldCompressFile = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompressOldFileWithGzipReplacesSourceWithCompressedFile(t *testing.T) {
	t.Parallel()

	source := filepath.Join(t.TempDir(), "failed-docs.jsonl")
	wantContent := []byte("first failed doc\nsecond failed doc\n")
	if err := os.WriteFile(source, wantContent, 0640); err != nil {
		t.Fatalf("failed to write source file: %v", err)
	}

	cfg := newValidTestFileArchiveConfig(t)
	cfg.enableFileCompress = true
	cfg.compressFormat = pkgfsstore.CompressGzip
	store := FileStore{Config: cfg}

	if err := store.CompressOldFile(context.Background(), source); err != nil {
		t.Fatalf("CompressOldFile returned error: %v", err)
	}

	if _, err := os.Stat(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source stat error = %v, want os.ErrNotExist", err)
	}
	compressedPath := source + pkgfsstore.ExtensionCompressGzip
	if _, err := os.Stat(compressedPath + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tmp stat error = %v, want os.ErrNotExist", err)
	}

	gotContent := readGzipFile(t, compressedPath)
	if string(gotContent) != string(wantContent) {
		t.Fatalf("compressed content = %q, want %q", gotContent, wantContent)
	}
}

func TestCompressOldFileCleansTempAndKeepsSourceOnCompressorError(t *testing.T) {
	t.Parallel()

	source := filepath.Join(t.TempDir(), "failed-docs.jsonl")
	wantContent := []byte("keep this file when compression fails\n")
	if err := os.WriteFile(source, wantContent, 0640); err != nil {
		t.Fatalf("failed to write source file: %v", err)
	}

	compressErr := errors.New("compress failed")
	cfg := newValidTestFileArchiveConfig(t)
	cfg.enableFileCompress = true
	cfg.compressionCodec = failingCompressor{format: pkgfsstore.CompressGzip, err: compressErr}
	store := FileStore{Config: cfg}

	err := store.CompressOldFile(context.Background(), source)
	if !errors.Is(err, compressErr) {
		t.Fatalf("CompressOldFile error = %v, want %v", err, compressErr)
	}

	gotContent, err := os.ReadFile(source)
	if err != nil {
		t.Fatalf("failed to read source file: %v", err)
	}
	if string(gotContent) != string(wantContent) {
		t.Fatalf("source content = %q, want %q", gotContent, wantContent)
	}
	if _, err := os.Stat(source + pkgfsstore.ExtensionCompressGzip); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("compressed stat error = %v, want os.ErrNotExist", err)
	}
	if _, err := os.Stat(source + pkgfsstore.ExtensionCompressGzip + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tmp stat error = %v, want os.ErrNotExist", err)
	}
}

func TestCompressOldFileDoesNotCommitOutputThatFailsCodecVerification(t *testing.T) {
	t.Parallel()

	source := filepath.Join(t.TempDir(), "failed-docs.jsonl")
	wantContent := []byte("keep source when codec output is invalid\n")
	if err := os.WriteFile(source, wantContent, 0o640); err != nil {
		t.Fatalf("failed to write source file: %v", err)
	}

	verificationErr := errors.New("compressed output integrity verification failed")
	cfg := newValidTestFileArchiveConfig(t)
	cfg.enableFileCompress = true
	cfg.compressionCodec = corruptOutputCompressor{
		format:          pkgfsstore.CompressGzip,
		verificationErr: verificationErr,
	}
	store := FileStore{Config: cfg}

	err := store.CompressOldFile(context.Background(), source)
	if !errors.Is(err, verificationErr) {
		t.Fatalf("CompressOldFile error = %v, want %v", err, verificationErr)
	}
	gotContent, err := os.ReadFile(source)
	if err != nil {
		t.Fatalf("failed to read source file: %v", err)
	}
	if string(gotContent) != string(wantContent) {
		t.Fatalf("source content = %q, want %q", gotContent, wantContent)
	}
	compressedPath := source + pkgfsstore.ExtensionCompressGzip
	if _, err := os.Stat(compressedPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("compressed stat error = %v, want os.ErrNotExist", err)
	}
	if _, err := os.Stat(compressedPath + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tmp stat error = %v, want os.ErrNotExist", err)
	}
}

func newValidTestFileArchiveConfig(t *testing.T) testFileArchiveConfig {
	t.Helper()

	// 保存先は解決済みのパスを返すため、macOSの/varなど一時ディレクトリの上位のリンクを
	// 解決して比べます。
	archivePath := resolvedTempDirForTest(t)
	return testFileArchiveConfig{
		target:                     "test-app",
		rotationType:               pkgfsstore.FileRotationDaily,
		rotationDayOfWeek:          time.Sunday,
		fileNameFormat:             FileNameFormat,
		fileDateFormat:             pkgfsstore.FileDateFormat,
		maxFileSize:                2 * 1024 * 1024,
		maxDirSize:                 20 * 1024 * 1024,
		maxFileAge:                 3,
		maxFileBackups:             2,
		enableFileCompress:         false,
		compressFormat:             pkgfsstore.CompressGzip,
		archivePath:                archivePath,
		dir:                        "backend",
		dirCaseRetryable:           "retryable",
		dirCasePermanent:           "permanent",
		enableRotate:               true,
		maxWriteAttempts:           1,
		fileOperationRetryWaitTime: time.Millisecond,
	}
}

func newTestFileNamePattern(cfg testFileArchiveConfig) logpattern.FileNamePattern {
	return logpattern.FileNamePattern{
		Prefix:    "",
		Target:    cfg.target,
		Date:      "",
		Extension: "jsonl",
		Index:     1,
	}
}

func readGzipFile(t *testing.T, path string) []byte {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open gzip file: %v", err)
	}
	defer file.Close()

	reader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("failed to create gzip reader: %v", err)
	}
	defer reader.Close()

	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read gzip file: %v", err)
	}
	return content
}

// resolvedTempDirForTestは、一時ディレクトリの上位のリンクを解決したパスを返します。
func resolvedTempDirForTest(t *testing.T) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return directory
}

// cleanedTempDirForTestは、保存先のパスを正規化して扱う処理と比べられるよう、TMPDIRの
// 「//」などを残すt.TempDirを正規化したパスを返します。リンクは解決しません。
func cleanedTempDirForTest(t *testing.T) string {
	t.Helper()
	return filepath.Clean(t.TempDir())
}

func assertErrorIs(t *testing.T, err error, targets ...error) {
	t.Helper()

	for _, target := range targets {
		if !errors.Is(err, target) {
			t.Fatalf("error %v does not wrap %v", err, target)
		}
	}
}

func assertDirExists(t *testing.T, path string) {
	t.Helper()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("failed to stat dir %q: %v", path, err)
	}
	if !info.IsDir() {
		t.Fatalf("%q is not a directory", path)
	}
}
