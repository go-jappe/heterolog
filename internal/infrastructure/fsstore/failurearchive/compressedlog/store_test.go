package compressedlogarchive

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestStorePublishStoresDirectCompressedFileByteForByte(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fileName        string
		failureCategory uploader.FailureCategory
	}{
		{fileName: "app_20260904_1.log.gz", failureCategory: uploader.FailureCategoryRetryable},
		{fileName: "app_20260904_2.log.zst", failureCategory: uploader.FailureCategoryPermanent},
		{fileName: "app_20260904_3.log.lz4", failureCategory: uploader.FailureCategoryRetryable},
		{fileName: "app_20260904_4.log.xz", failureCategory: uploader.FailureCategoryPermanent},
		{fileName: "app_20260904_5.log.br", failureCategory: uploader.FailureCategoryRetryable},
		{fileName: "app_20260904_6.jsonl.gz", failureCategory: uploader.FailureCategoryRetryable},
		{fileName: "app_20260904_7.jsonl.zst", failureCategory: uploader.FailureCategoryPermanent},
		{fileName: "app_20260904_8.jsonl.custom_format-v2", failureCategory: uploader.FailureCategoryRetryable},
	}
	for index, test := range tests {
		t.Run(test.fileName, func(t *testing.T) {
			t.Parallel()
			store := newTestStore(t, pkglogger.LoggerContractSync)
			body := []byte{0x00, 0x1f, 0x8b, byte(index), 0xff, 0x10, 0x00, 0x7f}
			sourcePath := writeTestCompressedLog(t, test.fileName, body)
			input := testSaveInput(sourcePath, test.failureCategory)

			archived, err := store.Save(context.Background(), input)
			if err != nil {
				t.Fatalf("Save() error = %v", err)
			}
			if archived.FailureCategory != test.failureCategory {
				t.Fatalf("FailureCategory = %q, want %q", archived.FailureCategory, test.failureCategory)
			}
			if filepath.Base(archived.Path) != test.fileName {
				t.Fatalf("archived file name = %q", filepath.Base(archived.Path))
			}
			info, err := os.Lstat(archived.Path)
			if err != nil {
				t.Fatal(err)
			}
			if !info.Mode().IsRegular() {
				t.Fatalf("archived mode = %v", info.Mode())
			}
			testutil.RequirePrivatePermissions(t, archived.Path, archivedFileMode)
			// 新規directoryはdirectoryModeとprocess umaskで作成します。umaskが追加で外したbitは
			// 許容し、directoryModeを超える権限が付かないことを確認します。
			createdMode := createdDirectoryModeForTest(t, directoryMode)
			for _, directory := range []string{store.Root().RetryableDir(), store.Root().PermanentDir()} {
				testutil.RequirePrivatePermissions(t, directory, createdMode)
			}
			assertFileBytes(t, archived.Path, body)
			assertFileBytes(t, sourcePath, body)
			entries, err := os.ReadDir(filepath.Dir(archived.Path))
			if err != nil || len(entries) != 1 || entries[0].IsDir() {
				t.Fatalf("failure category entries = %v, error = %v", entries, err)
			}
			assertNoArchiveTemporaryFiles(t, store)
			rootEntries, err := os.ReadDir(store.Root().Path())
			if err != nil || len(rootEntries) != 2 {
				t.Fatalf("archive root entries = %v, error = %v, want only retryable and permanent", rootEntries, err)
			}
		})
	}
}

func TestStorePublishIsIdempotentAcrossChangedFailureCategory(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, pkglogger.LoggerContractSync)
	firstSource := writeTestCompressedLogIn(t, t.TempDir(), "idempotent.log.gz", []byte("irreplaceable"))
	input := testSaveInput(firstSource, uploader.FailureCategoryRetryable)
	first, err := store.Save(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	secondSource := writeTestCompressedLogIn(t, t.TempDir(), "idempotent.log.gz", []byte("irreplaceable"))
	input.SourcePath = secondSource
	input.FailureCategory = uploader.FailureCategoryPermanent
	second, err := store.Save(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(second, first) {
		t.Fatalf("second archived file = %#v, want %#v", second, first)
	}
	assertDirectoryEmpty(t, store.Root().PermanentDir())
}

func TestStoreReusesCompressedBackendArchiveWithoutChangingFailureCategory(t *testing.T) {
	t.Parallel()
	for _, failureCategory := range archiveFailureCategories {
		t.Run(string(failureCategory), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := newTestStore(t, pkglogger.LoggerContractSync)
			body := []byte("compressed backend failure records")
			source := writeTestCompressedLog(t, "app_20261008_1.jsonl.custom_format-v2", body)
			saved, err := store.Save(ctx, testSaveInput(source, failureCategory))
			if err != nil {
				t.Fatal(err)
			}
			reused, found, err := store.ReuseExistingFile(ctx, source)
			if err != nil || !found || !reflect.DeepEqual(reused, saved) {
				t.Fatalf("ReuseExistingFile() = %#v, %t, %v, want %#v", reused, found, err, saved)
			}
			assertFileBytes(t, reused.Path, body)
			assertFileBytes(t, source, body)
			entries, err := os.ReadDir(filepath.Dir(saved.Path))
			if err != nil || len(entries) != 1 {
				t.Fatalf("archive entries after reuse = %v, error = %v", entries, err)
			}
		})
	}
}

func TestStoreSavePreservesDifferentBytesWithoutOverwriting(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, pkglogger.LoggerContractSync)
	firstSource := writeTestCompressedLogIn(t, t.TempDir(), "same.log.gz", []byte("first-content"))
	input := testSaveInput(firstSource, uploader.FailureCategoryRetryable)
	first, err := store.Save(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	secondSource := writeTestCompressedLogIn(t, t.TempDir(), "same.log.gz", []byte("second-content"))
	input.SourcePath = secondSource
	input.FailureCategory = uploader.FailureCategoryPermanent
	saved, err := store.Save(context.Background(), input)
	if err != nil {
		t.Fatalf("Save(different bytes) error = %v", err)
	}
	if len(saved.ConflictRenames) != 1 || saved.ConflictRenames[0].OriginalPath != first.Path {
		t.Fatalf("Save(different bytes) conflicts = %#v", saved.ConflictRenames)
	}
	assertFileBytes(t, saved.ConflictRenames[0].RetainedPath, []byte("first-content"))
	if _, err := os.Lstat(first.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("conflicting canonical path still exists: %v", err)
	}
	assertFileBytes(t, saved.Path, []byte("second-content"))
	assertFileBytes(t, secondSource, []byte("second-content"))
}

func TestStoreConcurrentSameCompressedLogPublishProducesOneFile(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, pkglogger.LoggerContractSync)
	sourcePath := writeTestCompressedLog(t, "concurrent.log.gz", []byte("one durable payload"))
	input := testSaveInput(sourcePath, uploader.FailureCategoryRetryable)
	const goroutines = 8
	results := make(chan ArchivedFile, goroutines)
	errs := make(chan error, goroutines)
	var wait sync.WaitGroup
	for range goroutines {
		wait.Add(1)
		go func() {
			defer wait.Done()
			archived, err := store.Save(context.Background(), input)
			results <- archived
			errs <- err
		}()
	}
	wait.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Save() error = %v", err)
		}
	}
	var want ArchivedFile
	for result := range results {
		if want.Path == "" {
			want = result
			continue
		}
		if !reflect.DeepEqual(result, want) {
			t.Fatalf("concurrent result = %#v, want %#v", result, want)
		}
	}
	entries, err := os.ReadDir(store.Root().RetryableDir())
	if err != nil || len(entries) != 1 || entries[0].IsDir() {
		t.Fatalf("retryable entries = %v, error = %v", entries, err)
	}
	assertFileBytes(t, want.Path, []byte("one durable payload"))
	assertNoArchiveTemporaryFiles(t, store)
}

func TestStoreConcurrentFailureCategoryPublishesKeepOnlyFirstFailureCategory(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, pkglogger.LoggerContractSync)
	sourcePath := writeTestCompressedLog(t, "classification-race.log.gz", []byte("one logical failure"))
	start := make(chan struct{})
	results := make(chan ArchivedFile, 2)
	errs := make(chan error, 2)
	var wait sync.WaitGroup
	for _, failureCategory := range []uploader.FailureCategory{
		uploader.FailureCategoryRetryable,
		uploader.FailureCategoryPermanent,
	} {
		wait.Add(1)
		go func(failureCategory uploader.FailureCategory) {
			defer wait.Done()
			<-start
			archived, err := store.Save(
				context.Background(),
				testSaveInput(sourcePath, failureCategory),
			)
			results <- archived
			errs <- err
		}(failureCategory)
	}
	close(start)
	wait.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent failure category Save() error = %v", err)
		}
	}
	var first ArchivedFile
	for result := range results {
		if first.Path == "" {
			first = result
			continue
		}
		if !reflect.DeepEqual(result, first) {
			t.Fatalf("failure category results differ: %#v and %#v", first, result)
		}
	}
	retryable, err := os.ReadDir(store.Root().RetryableDir())
	if err != nil {
		t.Fatal(err)
	}
	permanent, err := os.ReadDir(store.Root().PermanentDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(retryable)+len(permanent) != 1 {
		t.Fatalf("published entries: retryable=%v permanent=%v", retryable, permanent)
	}
}

func TestStoreArchivePreservesLongSourceBasename(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, pkglogger.LoggerContractSync)
	longName := strings.Repeat("a", 200) + ".log.gz"
	sourcePath := writeTestCompressedLog(t, longName, []byte("long-name"))
	archived, err := store.Save(
		context.Background(),
		testSaveInput(sourcePath, uploader.FailureCategoryRetryable),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := filepath.Base(archived.Path), longName; got != want {
		t.Fatalf("archived name = %q, want %q", got, want)
	}
}

func TestStoreRejectsUnsupportedPathsFailureCategoriesAndSymlinks(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, pkglogger.LoggerContractSync)
	for _, name := range []string{
		"raw.log",
		"compressed.gz",
		"records.jsonl",
		"backend_20260904_1.jsonl.zst.tmp",
		".jsonl.zst",
		".log.gz",
	} {
		sourcePath := writeTestCompressedLog(t, name, []byte("body"))
		if _, err := store.Save(
			context.Background(),
			testSaveInput(sourcePath, uploader.FailureCategoryRetryable),
		); !errors.Is(err, ErrUnsupportedCompressedLog) {
			t.Errorf("Save(%q) error = %v, want ErrUnsupportedCompressedLog", name, err)
		}
	}
	sourcePath := writeTestCompressedLog(t, "safe.log.gz", []byte("safe"))
	invalidFailureCategory := testSaveInput(sourcePath, "unknown")
	if _, err := store.Save(context.Background(), invalidFailureCategory); !errors.Is(err, ErrInvalidFailureCategory) {
		t.Fatalf("Save(invalid failure category) error = %v", err)
	}
	t.Run("symlink source", func(t *testing.T) {
		symlinkPath := filepath.Join(t.TempDir(), "linked.log.gz")
		testutil.Symlink(t, sourcePath, symlinkPath)
		if _, err := store.Save(
			context.Background(),
			testSaveInput(symlinkPath, uploader.FailureCategoryRetryable),
		); err == nil {
			t.Fatal("Save(symlink source) error = nil")
		}
	})
}

func TestStoreRejectsSymlinkDirectoriesAndExistingArchivePaths(t *testing.T) {
	t.Parallel()

	rootPath := filepath.Join(t.TempDir(), "compressed_log_file_upload_failures")
	root, err := NewRoot(rootPath, objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
	if err != nil {
		t.Fatal(err)
	}
	root, err = root.ForDestination("destination-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(root.Path(), directoryMode); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	testutil.Symlink(t, outside, root.RetryableDir())
	store, err := NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureDirectories(context.Background()); err == nil {
		t.Fatal("EnsureDirectories() error = nil")
	}
	assertDirectoryEmpty(t, outside)

	store = newTestStore(t, pkglogger.LoggerContractSync)
	if err := store.EnsureDirectories(context.Background()); err != nil {
		t.Fatal(err)
	}
	sourcePath := writeTestCompressedLog(t, "existing.log.gz", []byte("source"))
	input := testSaveInput(sourcePath, uploader.FailureCategoryRetryable)
	existingPath, err := store.Root().archivedFilePath(input.FailureCategory, filepath.Base(sourcePath))
	if err != nil {
		t.Fatal(err)
	}
	testutil.Symlink(t, outside, existingPath)
	if _, err := store.Save(context.Background(), input); !errors.Is(err, ErrInvalidCompressedLogPath) {
		t.Fatalf("Save(existing symlink) error = %v, want ErrInvalidCompressedLogPath", err)
	}
	assertDirectoryEmpty(t, outside)
}

func TestStoreUsesResolvedRootAfterAncestorAliasChanges(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	outside := resolvedTempDirForTest(t)
	linkedRoot := filepath.Join(base, "archive-link")
	testutil.Symlink(t, outside, linkedRoot)
	root, err := NewRoot(
		filepath.Join(linkedRoot, "compressed_log_file_upload_failures"),
		objectstorage.UploadTargetS3,
		pkglogger.LoggerContractSync,
	)
	if err != nil {
		t.Fatal(err)
	}
	wantRoot := filepath.Join(outside, "compressed_log_file_upload_failures", "s3", "logger")
	if root.Path() != wantRoot {
		t.Fatalf("root = %q, want physical path %q", root.Path(), wantRoot)
	}
	assertDirectoryEmpty(t, outside)
	replacement := t.TempDir()
	if err := os.Remove(linkedRoot); err != nil {
		t.Fatal(err)
	}
	testutil.Symlink(t, replacement, linkedRoot)
	root, err = root.ForDestination("destination-a")
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureDirectories(context.Background()); err != nil {
		t.Fatal(err)
	}
	source := writeTestCompressedLog(t, "app_20260913_1.log.gz", []byte("fixed root"))
	published, err := store.Save(context.Background(), testSaveInput(source, uploader.FailureCategoryRetryable))
	if err != nil {
		t.Fatal(err)
	}
	if published.Path != filepath.Join(wantRoot, "destination-a", "retryable", filepath.Base(source)) {
		t.Fatalf("published path followed changed alias: %q", published.Path)
	}
	assertDirectoryEmpty(t, replacement)
}

func TestStoreCanceledPublishLeavesNoPublicOrTemporaryFile(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, pkglogger.LoggerContractSync)
	sourcePath := writeTestCompressedLog(t, "cancel.log.gz", []byte("preserve"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Save(
		ctx,
		testSaveInput(sourcePath, uploader.FailureCategoryRetryable),
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("Save(canceled) error = %v", err)
	}
	if _, err := os.Stat(store.Root().Path()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("archive root exists after canceled publish: %v", err)
	}
	assertFileBytes(t, sourcePath, []byte("preserve"))
}

// resolvedTempDirForTestは、macOSの/varなど一時ディレクトリの上位のリンクを解決したパスを返します。
// rootは解決済みのパスを返し、管理下の作成ではリンクを拒否するため、比べる側も解決しておきます。
func resolvedTempDirForTest(t testing.TB) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return directory
}

func newTestStore(t *testing.T, contract pkglogger.LoggerContractKind) *Store {
	t.Helper()
	root, err := NewRoot(
		filepath.Join(t.TempDir(), "target", "compressed_log_file_upload_failures"),
		objectstorage.UploadTargetS3,
		contract,
	)
	if err != nil {
		t.Fatalf("NewRoot() error = %v", err)
	}
	root, err = root.ForDestination("destination-a")
	if err != nil {
		t.Fatalf("ForDestination() error = %v", err)
	}
	store, err := NewStore(root)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	return store
}

func testSaveInput(
	sourcePath string,
	failureCategory uploader.FailureCategory,
) SaveInput {
	return SaveInput{
		SourcePath:      sourcePath,
		FailureCategory: failureCategory,
	}
}

func writeTestCompressedLog(t *testing.T, fileName string, body []byte) string {
	t.Helper()
	return writeTestCompressedLogIn(t, t.TempDir(), fileName, body)
}

func writeTestCompressedLogIn(t *testing.T, directory string, fileName string, body []byte) string {
	t.Helper()
	path := filepath.Join(directory, fileName)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
	return path
}

// createdDirectoryModeForTestは、process umaskを変更せずに、modeで新規作成した
// directoryの実際の権限を観測します。並行する試験のumaskにも影響しません。
// WindowsのRequirePrivatePermissionsはmodeではなくACLを検査します。
func createdDirectoryModeForTest(t *testing.T, mode os.FileMode) os.FileMode {
	t.Helper()
	control := filepath.Join(t.TempDir(), "creation-mode-control")
	if err := os.Mkdir(control, mode); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(control)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode().Perm()
}

func assertNoArchiveTemporaryFiles(t *testing.T, store *Store) {
	t.Helper()
	for _, directory := range []string{store.Root().RetryableDir(), store.Root().PermanentDir()} {
		entries, err := os.ReadDir(directory)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if isArchiveTemporaryFileName(entry.Name()) {
				t.Fatalf("temporary file remains: %s", filepath.Join(directory, entry.Name()))
			}
		}
	}
}

func assertDirectoryEmpty(t *testing.T, directory string) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("ReadDir(%q) error = %v", directory, err)
	}
	if len(entries) != 0 {
		t.Fatalf("directory %q entries = %v, want empty", directory, entries)
	}
}

func assertFileBytes(t *testing.T, path string, want []byte) {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	if string(body) != string(want) {
		t.Fatalf("file %q body = %v, want %v", path, body, want)
	}
}
