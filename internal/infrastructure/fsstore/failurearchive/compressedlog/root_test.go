package compressedlogarchive

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestNewRootBuildsFailureCategoryDirectoriesBelowResolvedRoot(t *testing.T) {
	t.Parallel()

	resolvedRoot := filepath.Join("var", "lib", "heterolog", "compressed_log_file_upload_failures")
	root, err := NewRoot(
		resolvedRoot,
		objectstorage.UploadTargetS3,
		pkglogger.LoggerContractAsyncBestEffort,
	)
	if err != nil {
		t.Fatalf("NewRoot() error = %v", err)
	}
	root, err = root.ForDestination("primary_s3")
	if err != nil {
		t.Fatalf("ForDestination() error = %v", err)
	}
	absoluteResolvedRoot, err := filepath.Abs(resolvedRoot)
	if err != nil {
		t.Fatalf("filepath.Abs() error = %v", err)
	}
	wantRoot := filepath.Join(
		absoluteResolvedRoot,
		string(objectstorage.UploadTargetS3),
		pkglogger.LoggerContractAsyncBestEffort.String(),
		"primary_s3",
	)
	if root.Path() != wantRoot {
		t.Fatalf("Root.Path() = %q, want %q", root.Path(), wantRoot)
	}
	if root.Service() != objectstorage.UploadTargetS3 {
		t.Fatalf("Root.Service() = %q, want %q", root.Service(), objectstorage.UploadTargetS3)
	}
	if root.RetryableDir() != filepath.Join(wantRoot, "retryable") {
		t.Fatalf("RetryableDir() = %q", root.RetryableDir())
	}
	if root.PermanentDir() != filepath.Join(wantRoot, "permanent") {
		t.Fatalf("PermanentDir() = %q", root.PermanentDir())
	}
	if got, err := root.FailureCategoryDir(uploader.FailureCategoryRetryable); err != nil || got != root.RetryableDir() {
		t.Fatalf("FailureCategoryDir(retryable) = %q, %v", got, err)
	}
}

func TestRootRejectsUnsafeDestinationIDs(t *testing.T) {
	root, err := NewRoot(t.TempDir(), objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewStore(root); !errors.Is(err, ErrInvalidRoot) {
		t.Errorf("NewStore(unbound root) error = %v, want ErrInvalidRoot", err)
	}
	for _, id := range []objectstorage.DestinationID{"", "../other", "UPPER", "has space", "a/b", `a\b`} {
		if _, err := root.ForDestination(id); !errors.Is(err, ErrInvalidRoot) {
			t.Errorf("ForDestination(%q) error = %v, want ErrInvalidRoot", id, err)
		}
	}
	if _, err := root.ForDestination("destination-a"); err != nil {
		t.Fatal(err)
	}
}

func TestNewRootCanonicalizesRelativeAndAbsoluteArchivePaths(t *testing.T) {
	t.Parallel()

	relativeBase := filepath.Join("state", "..", "compressed-log-file-upload-failures")
	absoluteBase, err := filepath.Abs(relativeBase)
	if err != nil {
		t.Fatalf("filepath.Abs() error = %v", err)
	}
	relativeRoot, err := NewRoot(
		relativeBase,
		objectstorage.UploadTargetS3,
		pkglogger.LoggerContractDelivery,
	)
	if err != nil {
		t.Fatalf("NewRoot(relative) error = %v", err)
	}
	absoluteRoot, err := NewRoot(
		absoluteBase,
		objectstorage.UploadTargetS3,
		pkglogger.LoggerContractDelivery,
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

func TestNewRootRejectsInvalidInputs(t *testing.T) {
	workingDirectory := t.TempDir()
	t.Chdir(workingDirectory)
	rootPath := filepath.Join(workingDirectory, "archive")

	tests := []struct {
		name     string
		rootPath string
		service  objectstorage.UploadTarget
		contract pkglogger.LoggerContractKind
	}{
		{name: "empty root", service: objectstorage.UploadTargetS3, contract: pkglogger.LoggerContractSync},
		{name: "ASCII whitespace root", rootPath: " \t\r\n", service: objectstorage.UploadTargetS3, contract: pkglogger.LoggerContractSync},
		{name: "Unicode whitespace root", rootPath: "\u3000", service: objectstorage.UploadTargetS3, contract: pkglogger.LoggerContractSync},
		{name: "whitespace root", rootPath: " " + rootPath + " ", service: objectstorage.UploadTargetS3, contract: pkglogger.LoggerContractSync},
		{name: "empty service", rootPath: rootPath, contract: pkglogger.LoggerContractSync},
		{name: "service traversal", rootPath: rootPath, service: "../s3", contract: pkglogger.LoggerContractSync},
		{name: "service separator", rootPath: rootPath, service: "s3/archive", contract: pkglogger.LoggerContractSync},
		{name: "non-canonical service", rootPath: rootPath, service: "S3", contract: pkglogger.LoggerContractSync},
		{name: "unknown contract", rootPath: rootPath, service: objectstorage.UploadTargetS3, contract: "unknown"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root, err := NewRoot(test.rootPath, test.service, test.contract)
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

func TestRootRejectsUnknownFailureCategoryAndCompressedLogTraversal(t *testing.T) {
	t.Parallel()

	root, err := NewRoot(t.TempDir(), objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
	if err != nil {
		t.Fatalf("NewRoot() error = %v", err)
	}
	if _, err := root.FailureCategoryDir("unknown"); !errors.Is(err, ErrInvalidFailureCategory) {
		t.Fatalf("FailureCategoryDir(unknown) error = %v", err)
	}
	for _, sourceFileName := range []string{"", ".", "..", "../escape.log.gz", "nested/app.log.gz", `nested\app.log.gz`, " app.log.gz "} {
		if _, err := root.archivedFilePath(
			uploader.FailureCategoryRetryable,
			sourceFileName,
		); !errors.Is(err, ErrInvalidCompressedLogPath) {
			t.Errorf("archivedFilePath(%q) error = %v, want ErrInvalidCompressedLogPath", sourceFileName, err)
		}
	}
}
