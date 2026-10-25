package fsstore

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestJoinFileRemovalFailuresSkipsSuccessAndPreservesFailureCause(t *testing.T) {
	removeErr := errors.New("remove failed")
	results := []FileRemovalResult{
		{
			File: FileInfo{Path: "already-absent.log"},
		},
		{
			File: FileInfo{Path: "failed.log"},
			Err:  &FileRemovalError{Step: FileRemovalStepRemove, Path: "failed.log", Err: removeErr},
		},
	}
	err := JoinFileRemovalFailures(results)

	if !errors.Is(err, removeErr) {
		t.Fatalf("joined error = %v, want wrapped remove error", err)
	}
	requireFileRemovalFailure(t, err, FileInfo{Path: "failed.log"})
	requireFileRemovalStep(t, err, FileRemovalStepRemove, "failed.log")
	if !HasFileRemovalFailures(results) {
		t.Fatal("HasFileRemovalFailures = false, want true")
	}
	if err := JoinFileRemovalFailures([]FileRemovalResult{{
		File: FileInfo{Path: "already-absent.log"},
	}}); err != nil {
		t.Fatalf("successful removal result produced error: %v", err)
	}
	if HasFileRemovalFailures(results[:1]) {
		t.Fatal("HasFileRemovalFailures = true for successful result")
	}
}

func TestRemoveObservedFileReturnsInputSnapshotAndConfirmsAbsence(t *testing.T) {
	dir := t.TempDir()
	observed := writePurgeTestFile(t, dir, "existing.log", time.Now())
	removed := removeObservedFile(observed)
	if !reflect.DeepEqual(removed.File, observed) ||
		removed.Err != nil {
		t.Fatalf("existing file removal = %+v, want successful remove of input snapshot", removed)
	}
	if _, err := os.Lstat(observed.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("removed file still exists: %v", err)
	}

	missing := FileInfo{
		Path:    filepath.Join(dir, "app_20260824_2.log"),
		Size:    42,
		ModTime: time.Now().Add(-time.Hour),
	}
	absent := removeObservedFile(missing)
	if !reflect.DeepEqual(absent.File, missing) ||
		absent.Err != nil {
		t.Fatalf("missing file removal = %+v, want successful stat of input snapshot", absent)
	}
}

func TestPurgeNoOpReturnsIndependentRemainingCopy(t *testing.T) {
	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		run  func([]FileInfo) PurgeResult
	}{
		{
			name: "age already ran today",
			run: func(files []FileInfo) PurgeResult {
				return PurgeByFileNameDateIfNeeded(
					files,
					1,
					now,
					now.Add(-time.Hour),
				)
			},
		},
		{
			name: "generation within limit",
			run: func(files []FileInfo) PurgeResult {
				return PurgeByGenerationsFromFileNameIfNeeded(files, len(files))
			},
		},
		{
			name: "directory size within limit",
			run: func(files []FileInfo) PurgeResult {
				return PurgeByDirectorySizeFromFileNameIfNeeded(files, 1, nil)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			files := []FileInfo{{Path: "not-observed.log", Size: 1, ModTime: now}}
			result := test.run(files)
			if result.Ran {
				t.Fatal("Ran = true, want false")
			}
			if !reflect.DeepEqual(result.Remaining, files) {
				t.Fatalf("Remaining = %+v, want %+v", result.Remaining, files)
			}
			if len(result.RemovalResults) != 0 {
				t.Fatalf("unexpected purge result: %+v", result)
			}

			result.Remaining[0].Path = "changed.log"
			if files[0].Path != "not-observed.log" {
				t.Fatalf("no-op Remaining aliases input: input path = %q", files[0].Path)
			}
		})
	}
}

func TestPurgeByFileNameDateIfNeededRanDoesNotMeanFileDeleted(t *testing.T) {
	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	result := PurgeByFileNameDateIfNeeded(
		nil,
		1,
		now,
		time.Time{},
	)
	if !result.Ran {
		t.Fatal("Ran = false, want true when the daily gate is due")
	}
	if len(result.Remaining) != 0 || len(result.RemovalResults) != 0 {
		t.Fatalf("unexpected empty-input result: %+v", result)
	}
}

func TestPurgeByGenerationsFromFileNameIfNeededPreservesInputAndResultOrder(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 8, 27, 9, 0, 0, 0, time.UTC)
	oldest := writePurgeTestFile(t, dir, "app_20260824_1.log", base)
	middle := writePurgeTestFile(t, dir, "app_20260824_2.log", base.Add(time.Hour))
	newest := writePurgeTestFile(t, dir, "app_20260824_3.log", base.Add(2*time.Hour))

	files := []FileInfo{newest, oldest, middle}
	original := append([]FileInfo(nil), files...)
	result := PurgeByGenerationsFromFileNameIfNeeded(files, 1)

	if !result.Ran {
		t.Fatal("Ran = false, want true")
	}
	if !reflect.DeepEqual(files, original) {
		t.Fatalf("input was modified: got %+v, want %+v", files, original)
	}
	if got := fileRemovalResultPaths(result.RemovalResults); !reflect.DeepEqual(got, []string{oldest.Path, middle.Path}) {
		t.Fatalf("removal paths = %v, want oldest-first", got)
	}
	for _, removal := range result.RemovalResults {
		if removal.Err != nil {
			t.Fatalf("removal result = %+v, want successful remove", removal)
		}
	}
	if got := fileInfoPaths(result.Remaining); !reflect.DeepEqual(got, []string{newest.Path}) {
		t.Fatalf("Remaining paths = %v, want input-order newest", got)
	}
}

func TestPurgeByFileNameDateIfNeededKeepsFailuresAndRecordsMissingFilesAsDeleted(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	old := now.AddDate(0, 0, -3)
	failingPath := filepath.Join(dir, "app_20260824_1.log")
	if err := os.Mkdir(failingPath, 0o700); err != nil {
		t.Fatalf("mkdir failing path: %v", err)
	}
	if err := os.WriteFile(filepath.Join(failingPath, "child"), []byte("keep"), 0o600); err != nil {
		t.Fatalf("write child: %v", err)
	}
	if err := os.Chtimes(failingPath, old, old); err != nil {
		t.Fatalf("set failing path time: %v", err)
	}
	retained := writePurgeTestFile(t, dir, "app_20260827_1.log", now)
	missing := FileInfo{Path: filepath.Join(dir, "app_20260824_2.log"), Size: 10, ModTime: old}
	files := []FileInfo{
		{Path: failingPath, Size: 5, ModTime: old},
		missing,
		retained,
	}

	result := PurgeByFileNameDateIfNeeded(
		files,
		1,
		now,
		time.Time{},
	)

	if !result.Ran {
		t.Fatal("Ran = false, want true")
	}
	if got := fileInfoPaths(result.Remaining); !reflect.DeepEqual(got, []string{failingPath, retained.Path}) {
		t.Fatalf("Remaining paths = %v, want failed and retained input files", got)
	}
	if len(result.RemovalResults) != 2 {
		t.Fatalf("RemovalResults = %+v, want failure and missing-file success", result.RemovalResults)
	}
	failed := result.RemovalResults[0]
	if failed.File.Path != failingPath || failed.Err == nil {
		t.Fatalf("first removal = %+v, want non-regular file rejection", failed)
	}
	absent := result.RemovalResults[1]
	if !reflect.DeepEqual(absent.File, missing) || absent.Err != nil {
		t.Fatalf("second removal = %+v, want missing-file success", absent)
	}
}

func TestPurgeByFileNameDateIfNeededKeepsStatFailure(t *testing.T) {
	failingPath, restore := makePurgeStatFailure(t)
	_, probeErr := os.Lstat(failingPath)
	var pathErr *os.PathError
	if !errors.As(probeErr, &pathErr) || errors.Is(probeErr, os.ErrNotExist) {
		t.Fatalf("fixture stat error = %v, want a failure other than not-exist", probeErr)
	}
	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	input := FileInfo{Path: failingPath, Size: 1, ModTime: now.AddDate(0, 0, -3)}

	result := PurgeByFileNameDateIfNeeded(
		[]FileInfo{input},
		1,
		now,
		time.Time{},
	)

	if !result.Ran {
		t.Fatal("Ran = false, want true")
	}
	if !reflect.DeepEqual(result.Remaining, []FileInfo{input}) {
		t.Fatalf("Remaining = %+v, want stat failure input", result.Remaining)
	}
	if len(result.RemovalResults) != 1 ||
		!reflect.DeepEqual(result.RemovalResults[0].File, input) ||
		!errors.Is(result.RemovalResults[0].Err, pathErr.Err) {
		t.Fatalf("RemovalResults = %+v, want one stat failure", result.RemovalResults)
	}
	removalErr := result.RemovalResults[0].Err
	var reportedPathErr *os.PathError
	if !errors.As(removalErr, &reportedPathErr) || reportedPathErr.Path != failingPath {
		t.Fatalf("stat error no longer exposes its original path error: %v", removalErr)
	}
	requireFileRemovalStep(t, removalErr, FileRemovalStepInspect, failingPath)
	// 情報取得の失敗を解除した後も、元ファイルが残っていることを確認します。
	restore()
	if _, err := os.Lstat(failingPath); err != nil {
		t.Fatalf("stat failure fixture was removed: %v", err)
	}
}

func TestPurgeByGenerationsFromFileNameIfNeededDoesNotDeleteNewerFileAfterFailure(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 8, 27, 9, 0, 0, 0, time.UTC)
	failingPath := filepath.Join(dir, "app_20260824_1.log")
	if err := os.Mkdir(failingPath, 0o700); err != nil {
		t.Fatalf("mkdir failing path: %v", err)
	}
	if err := os.WriteFile(filepath.Join(failingPath, "child"), []byte("keep"), 0o600); err != nil {
		t.Fatalf("write child: %v", err)
	}
	if err := os.Chtimes(failingPath, base, base); err != nil {
		t.Fatalf("set failing path time: %v", err)
	}
	middle := writePurgeTestFile(t, dir, "app_20260824_2.log", base.Add(time.Hour))
	newest := writePurgeTestFile(t, dir, "app_20260824_3.log", base.Add(2*time.Hour))
	files := []FileInfo{
		{Path: failingPath, Size: 1, ModTime: base},
		middle,
		newest,
	}

	result := PurgeByGenerationsFromFileNameIfNeeded(files, 2)
	if !result.Ran {
		t.Fatal("Ran = false, want true")
	}
	if !reflect.DeepEqual(result.Remaining, files) {
		t.Fatalf("Remaining = %+v, want all input files", result.Remaining)
	}
	if len(result.RemovalResults) != 1 || result.RemovalResults[0].File.Path != failingPath ||
		result.RemovalResults[0].Err == nil {
		t.Fatalf("RemovalResults = %+v, want oldest failure only", result.RemovalResults)
	}
}

func TestPurgeByDirectorySizeFromFileNameIfNeededContinuesAfterFailure(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 8, 27, 9, 0, 0, 0, time.UTC)
	failingPath := filepath.Join(dir, "app_20260824_1.log")
	if err := os.Mkdir(failingPath, 0o700); err != nil {
		t.Fatalf("mkdir failing path: %v", err)
	}
	if err := os.WriteFile(filepath.Join(failingPath, "child"), []byte("keep"), 0o600); err != nil {
		t.Fatalf("write child: %v", err)
	}
	if err := os.Chtimes(failingPath, base, base); err != nil {
		t.Fatalf("set failing path time: %v", err)
	}
	deletable := writePurgeTestFile(t, dir, "app_20260824_2.log", base.Add(time.Hour))
	deletable.Size = 5
	files := []FileInfo{
		{Path: failingPath, Size: 8, ModTime: base},
		deletable,
	}

	result := PurgeByDirectorySizeFromFileNameIfNeeded(files, 8, nil)
	if got := fileInfoPaths(result.Remaining); !reflect.DeepEqual(got, []string{failingPath}) {
		t.Fatalf("Remaining paths = %v, want failed file", got)
	}
	if got := fileRemovalResultPaths(result.RemovalResults); !reflect.DeepEqual(got, []string{failingPath, deletable.Path}) {
		t.Fatalf("removal paths = %v, want attempted order", got)
	}
	if result.RemovalResults[0].Err == nil || result.RemovalResults[1].Err != nil {
		t.Fatalf("RemovalResults = %+v, want failure followed by success", result.RemovalResults)
	}
}

func TestPurgeByDirectorySizeFromFileNameIfNeededMissingFileReducesObservedExcess(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 8, 27, 9, 0, 0, 0, time.UTC)
	missing := FileInfo{
		Path:    filepath.Join(dir, "app_20260824_2.log"),
		Size:    5,
		ModTime: base,
	}
	retained := writePurgeTestFile(t, dir, "app_20260827_1.log", base.Add(time.Hour))
	retained.Size = 5

	result := PurgeByDirectorySizeFromFileNameIfNeeded(
		[]FileInfo{missing, retained},
		5,
		nil,
	)
	if !result.Ran {
		t.Fatal("Ran = false, want true")
	}
	if !reflect.DeepEqual(result.Remaining, []FileInfo{retained}) {
		t.Fatalf("Remaining = %+v, want retained file", result.Remaining)
	}
	if !reflect.DeepEqual(result.RemovalResults, []FileRemovalResult{{
		File: missing,
	}}) {
		t.Fatalf("missing file result = %+v, want one confirmed-absent stat result", result)
	}
	if _, err := os.Stat(retained.Path); err != nil {
		t.Fatalf("later file was removed after missing file satisfied excess: %v", err)
	}
}

func TestDirectorySizeCalculationSaturatesUint64(t *testing.T) {
	files := []FileInfo{
		{Path: "first.log", Size: math.MaxUint64},
		{Path: "second.log", Size: 1},
	}
	if got := totalFileBytes(files); got != math.MaxUint64 {
		t.Fatalf("totalFileBytes = %d, want math.MaxUint64", got)
	}
	if !ShouldPurgeByDirectorySize(files, math.MaxUint64-1) {
		t.Fatal("ShouldPurgeByDirectorySize = false after saturated total exceeded limit")
	}
}

func TestJoinFileRemovalFailuresPreservesObservedTimeAndOperation(t *testing.T) {
	cause := errors.New("permission denied")
	modified := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	file := FileInfo{Path: "app_20260924_1.log", ModTime: modified}
	err := JoinFileRemovalFailures([]FileRemovalResult{{
		File: file,
		Err:  &FileRemovalError{Step: FileRemovalStepInspect, Path: file.Path, Err: cause},
	}})
	if !errors.Is(err, cause) {
		t.Fatalf("joined error lost cause: %v", err)
	}
	requireFileRemovalFailure(t, err, file)
	requireFileRemovalStep(t, err, FileRemovalStepInspect, file.Path)
}

func writePurgeTestFile(
	t *testing.T,
	dir string,
	name string,
	modTime time.Time,
) FileInfo {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("log"), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatalf("set %s time: %v", path, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return FileInfo{
		Path:    path,
		Size:    uint64(info.Size()),
		ModTime: info.ModTime(),
	}
}

func fileInfoPaths(files []FileInfo) []string {
	paths := make([]string, 0, len(files))
	for _, file := range files {
		paths = append(paths, file.Path)
	}
	return paths
}

func fileRemovalResultPaths(results []FileRemovalResult) []string {
	paths := make([]string, 0, len(results))
	for _, result := range results {
		paths = append(paths, result.File.Path)
	}
	return paths
}
