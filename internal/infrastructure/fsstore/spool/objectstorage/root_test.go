package objectstoragespool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestNewRootBuildsLoggerContractDirectoriesBelowObjectStorage(t *testing.T) {
	t.Parallel()

	// NewRootは既存祖先のlinkを実体pathへ解決するため、固定の"/var/lib"ではなく、macOSの/varなど
	// 上位のlinkを解決済みの一時directoryを基点にし、OSに依存しない期待値にします。
	spoolPath := resolvedTempDirForTest(t)
	tests := []struct {
		name     string
		contract pkglogger.LoggerContractKind
		want     string
	}{
		{name: "logger", contract: pkglogger.LoggerContractSync, want: filepath.Join(spoolPath, "heterolog", "spool", "objectstorage", "s3", "logger")},
		{name: "async logger", contract: pkglogger.LoggerContractAsyncBestEffort, want: filepath.Join(spoolPath, "heterolog", "spool", "objectstorage", "s3", "async_logger")},
		{name: "delivery logger", contract: pkglogger.LoggerContractDelivery, want: filepath.Join(spoolPath, "heterolog", "spool", "objectstorage", "s3", "delivery_logger")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root, err := NewRoot(
				spoolPath,
				"heterolog",
				objectstorage.UploadTargetS3,
				tt.contract,
			)
			if err != nil {
				t.Fatalf("NewRoot() error = %v", err)
			}
			if got := root.Path(); got != tt.want {
				t.Fatalf("Root.Path() = %q, want %q", got, tt.want)
			}
			if got := root.Service(); got != objectstorage.UploadTargetS3 {
				t.Fatalf("Root.Service() = %q, want %q", got, objectstorage.UploadTargetS3)
			}
			if got := root.ActiveDir(); got != filepath.Join(tt.want, "active") {
				t.Fatalf("Root.ActiveDir() = %q", got)
			}
			if got, err := root.StateDir(pkgspool.StateQuarantine); err != nil || got != filepath.Join(tt.want, "quarantine") {
				t.Fatalf("Root.StateDir(quarantine) = %q, %v", got, err)
			}
		})
	}
}

func TestNewRootRejectsTargetTraversalAndUnknownContract(t *testing.T) {
	workingDirectory := t.TempDir()
	t.Chdir(workingDirectory)
	spoolPath := filepath.Join(workingDirectory, "spool")

	tests := []struct {
		name      string
		spoolPath string
		target    string
		service   objectstorage.UploadTarget
		contract  pkglogger.LoggerContractKind
	}{
		{name: "empty spool path", target: "heterolog", service: objectstorage.UploadTargetS3, contract: pkglogger.LoggerContractSync},
		{name: "ASCII whitespace spool path", spoolPath: " \t\r\n", target: "heterolog", service: objectstorage.UploadTargetS3, contract: pkglogger.LoggerContractSync},
		{name: "Unicode whitespace spool path", spoolPath: "\u3000", target: "heterolog", service: objectstorage.UploadTargetS3, contract: pkglogger.LoggerContractSync},
		{name: "target traversal", spoolPath: spoolPath, target: "../heterolog", service: objectstorage.UploadTargetS3, contract: pkglogger.LoggerContractSync},
		{name: "target separator", spoolPath: spoolPath, target: "foo/bar", service: objectstorage.UploadTargetS3, contract: pkglogger.LoggerContractSync},
		{name: "empty service", spoolPath: spoolPath, target: "heterolog", contract: pkglogger.LoggerContractSync},
		{name: "service traversal", spoolPath: spoolPath, target: "heterolog", service: "../s3", contract: pkglogger.LoggerContractSync},
		{name: "service separator", spoolPath: spoolPath, target: "heterolog", service: "s3/archive", contract: pkglogger.LoggerContractSync},
		{name: "non-canonical service", spoolPath: spoolPath, target: "heterolog", service: "S3", contract: pkglogger.LoggerContractSync},
		{name: "unknown contract", spoolPath: spoolPath, target: "heterolog", service: objectstorage.UploadTargetS3, contract: "unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root, err := NewRoot(tt.spoolPath, tt.target, tt.service, tt.contract)
			if !errors.Is(err, ErrInvalidRoot) {
				t.Fatalf("NewRoot() error = %v, want ErrInvalidRoot", err)
			}
			if root != (Root{}) {
				t.Fatalf("NewRoot() returned a nonzero root for invalid input: %+v", root)
			}
			entries, err := os.ReadDir(workingDirectory)
			if err != nil || len(entries) != 0 {
				t.Fatalf("working directory after rejected input: entries = %v, error = %v", entries, err)
			}
		})
	}
}

func TestNewRootSeparatesObjectStorageServices(t *testing.T) {
	t.Parallel()

	s3, err := NewRoot("/var/lib", "heterolog", objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewRoot("/var/lib", "heterolog", "future-storage", pkglogger.LoggerContractSync)
	if err != nil {
		t.Fatal(err)
	}
	if s3.Path() == other.Path() {
		t.Fatalf("different services share root %q", s3.Path())
	}
}

func TestNewRootCanonicalizesRelativeAndAbsoluteSpoolPaths(t *testing.T) {
	t.Parallel()

	relativeBase := filepath.Join("state", "..", "spool-base")
	absoluteBase, err := filepath.Abs(relativeBase)
	if err != nil {
		t.Fatalf("filepath.Abs() error = %v", err)
	}
	relativeRoot, err := NewRoot(
		relativeBase,
		"heterolog",
		objectstorage.UploadTargetS3,
		pkglogger.LoggerContractSync,
	)
	if err != nil {
		t.Fatalf("NewRoot(relative) error = %v", err)
	}
	absoluteRoot, err := NewRoot(
		absoluteBase,
		"heterolog",
		objectstorage.UploadTargetS3,
		pkglogger.LoggerContractSync,
	)
	if err != nil {
		t.Fatalf("NewRoot(absolute) error = %v", err)
	}
	if !filepath.IsAbs(relativeRoot.Path()) {
		t.Fatalf("NewRoot(relative).Path() = %q, want absolute path", relativeRoot.Path())
	}
	if relativeRoot.Path() != absoluteRoot.Path() {
		t.Fatalf(
			"relative and absolute paths resolved different roots: relative=%q absolute=%q",
			relativeRoot.Path(),
			absoluteRoot.Path(),
		)
	}
}

func TestValidateCompressedLogFileNameAcceptsOnlyCompressedLogFiles(t *testing.T) {
	t.Parallel()

	for _, fileName := range []string{
		"app_20260906_1.log.gz",
		"error_app_20260906_2.log.zst",
		"debug_app_20260906_3.log.custom_format-v2",
		"backend_20260906_1.jsonl.zst",
		"backend_20260906_2.jsonl.custom_format-v2",
	} {
		for _, nameCase := range []pathidentity.NameCase{pathidentity.NameCaseSensitive, pathidentity.NameCaseInsensitive} {
			if err := validateCompressedLogFileNameForNameCase(fileName, nameCase); err != nil {
				t.Errorf("validateCompressedLogFileNameForNameCase(%q, %d) error = %v", fileName, nameCase, err)
			}
		}
	}
	for _, fileName := range []string{
		"app_20260906_1.log",
		"backend_20260906_1.jsonl",
		"backend_20260906_1.jsonl.zst.tmp",
		".jsonl.zst",
		"app_20260906_1.log.gz.tmp",
		"../app_20260906_1.log.gz",
	} {
		for _, nameCase := range []pathidentity.NameCase{pathidentity.NameCaseSensitive, pathidentity.NameCaseInsensitive} {
			if err := validateCompressedLogFileNameForNameCase(fileName, nameCase); err == nil {
				t.Errorf("validateCompressedLogFileNameForNameCase(%q, %d) error = nil", fileName, nameCase)
			}
		}
	}
}

// base extensionの大小文字は、OS名ではなく保存先directoryの規則で比較します。
// 規則を指定して両方をどのOSでも確認し、directoryを渡す検証がその判定を使うことも確認します。
func TestValidateCompressedLogFileNameFollowsDirectoryNameCase(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	for _, fileName := range []string{"App_20260906_1.LOG.GZ", "Backend_20260906_1.JSONL.ZST", "app_20260906_1.Log.gz"} {
		for _, test := range []struct {
			nameCase pathidentity.NameCase
			valid    bool
		}{
			{nameCase: pathidentity.NameCaseSensitive, valid: false},
			{nameCase: pathidentity.NameCaseInsensitive, valid: true},
		} {
			err := validateCompressedLogFileNameForNameCase(fileName, test.nameCase)
			if (err == nil) != test.valid {
				t.Errorf("validateCompressedLogFileNameForNameCase(%q, %d) error = %v, want valid=%t", fileName, test.nameCase, err, test.valid)
			}
			if err != nil && !errors.Is(err, ErrInvalidCompressedLogFileName) {
				t.Errorf("validateCompressedLogFileNameForNameCase(%q, %d) error = %v, want ErrInvalidCompressedLogFileName", fileName, test.nameCase, err)
			}
		}
		want := validateCompressedLogFileNameForNameCase(fileName, pathidentity.DirectoryNameCase(directory))
		if got := validateCompressedLogFileName(directory, fileName); (got == nil) != (want == nil) {
			t.Errorf("validateCompressedLogFileName(%q) error = %v, want directory rule result %v", fileName, got, want)
		}
	}
}

// Storeは保存先spool rootの規則で保存する名前を検証します。
func TestStoreValidatesSavedNameWithRootNameCase(t *testing.T) {
	store := newTestStore(t)
	sourcePath := writeTestSource(t, t.TempDir(), "app_20260906_1.LOG.GZ", "compressed-log")
	_, err := store.SaveRetryableFailure(context.Background(), sourcePath)
	if pathidentity.DirectoryNameCase(store.Root().Path()) == pathidentity.NameCaseInsensitive {
		if err != nil {
			t.Fatalf("SaveRetryableFailure() error = %v, want accepted in a case-insensitive root", err)
		}
		return
	}
	if !errors.Is(err, ErrInvalidCompressedLogFileName) {
		t.Fatalf("SaveRetryableFailure() error = %v, want ErrInvalidCompressedLogFileName in a case-sensitive root", err)
	}
}
