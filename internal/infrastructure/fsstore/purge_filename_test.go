package fsstore

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPurgeFileNameErrorPreservesOperationAndCause(t *testing.T) {
	cause := errors.New("invalid file name")
	file := FileInfo{Path: "unknown.log"}
	var result PurgeResult
	recordPurgeFileNameError(&result, file, cause)
	require.Len(t, result.RemovalResults, 1)
	require.ErrorIs(t, result.RemovalResults[0].Err, cause)
	requireFileRemovalStep(t, result.RemovalResults[0].Err, FileRemovalStepInspectName, file.Path)
	requireFileRemovalFailure(t, JoinFileRemovalFailures(result.RemovalResults), file)
}

func TestPurgeFileSortErrorReportsOperationWithoutFileSnapshot(t *testing.T) {
	var result PurgeResult
	require.False(t, sortValidatedFileIndexesByName([]FileInfo{{Path: "unknown.log"}}, []int{0}, &result))
	require.Len(t, result.RemovalResults, 1)
	requireFileRemovalStep(t, result.RemovalResults[0].Err, FileRemovalStepSortNames, "")
	err := JoinFileRemovalFailures(result.RemovalResults)
	require.ErrorIs(t, err, result.RemovalResults[0].Err)
	// 対象ファイルのない並べ替えの失敗には、空のファイル情報を付けません。
	var failure *FileRemovalFailure
	require.False(t, errors.As(err, &failure), "sort failure was attributed to file %+v", failure)
}

func TestPurgeByFileNameDateUsesLocalCalendarDayAndRetainsCutoffDay(t *testing.T) {
	t.Parallel()
	location := time.FixedZone("UTC+09", 9*60*60)
	for _, test := range []struct {
		name       string
		now        time.Time
		olderDate  string
		cutoffDate string
	}{
		{"midnight", time.Date(2026, 9, 21, 0, 30, 0, 0, location), "20260919", "20260920"},
		{"late evening", time.Date(2026, 9, 21, 23, 30, 0, 0, location), "20260919", "20260920"},
		{"year boundary", time.Date(2026, 1, 1, 0, 30, 0, 0, location), "20251230", "20251231"},
		{"leap day", time.Date(2024, 3, 1, 0, 30, 0, 0, location), "20240228", "20240229"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			cutoff := createFileNamePurgeTestFile(t, dir, "backend_"+test.cutoffDate+"_1.jsonl", test.now.AddDate(0, 0, -100))
			olderTen := createFileNamePurgeTestFile(t, dir, "backend_"+test.olderDate+"_10.jsonl.gz", test.now)
			newer := createFileNamePurgeTestFile(t, dir, "backend_"+test.now.Format("20060102")+"_1.jsonl", test.now.AddDate(0, 0, -100))
			olderTwo := createFileNamePurgeTestFile(t, dir, "backend_"+test.olderDate+"_2.jsonl", test.now.Add(time.Hour))
			files := []FileInfo{cutoff, olderTen, newer, olderTwo}

			result := PurgeByFileNameDateIfNeeded(files, 1, test.now, time.Time{})

			require.True(t, result.Ran)
			require.NoError(t, JoinFileRemovalFailures(result.RemovalResults))
			require.Equal(t, []FileInfo{cutoff, newer}, result.Remaining)
			require.Equal(t, []string{olderTwo.Path, olderTen.Path}, fileNamePurgeRemovedPaths(result))
			require.FileExists(t, cutoff.Path)
			require.FileExists(t, newer.Path)
			require.NoFileExists(t, olderTwo.Path)
			require.NoFileExists(t, olderTen.Path)
		})
	}
}

func TestPurgeByFileNameDateRetainsInvalidNamesAndContinues(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	invalid := createFileNamePurgeTestFile(t, dir, "backend_20260230_1.jsonl", now.AddDate(0, 0, -100))
	old := createFileNamePurgeTestFile(t, dir, "backend_20260301_1.jsonl.gz", now)
	result := PurgeByFileNameDateIfNeeded([]FileInfo{invalid, old}, 1, now, time.Time{})
	require.Equal(t, []FileInfo{invalid}, result.Remaining)
	require.Len(t, result.RemovalResults, 2)
	require.Equal(t, invalid, result.RemovalResults[0].File)
	requireFileRemovalStep(t, result.RemovalResults[0].Err, FileRemovalStepInspectName, invalid.Path)
	var nameErr *FileNameDateAndIndexError
	require.ErrorAs(t, result.RemovalResults[0].Err, &nameErr)
	require.Equal(t, invalid.Path, nameErr.Path)
	requireFileRemovalFailure(t, JoinFileRemovalFailures(result.RemovalResults), invalid)
	require.NoError(t, result.RemovalResults[1].Err)
	require.FileExists(t, invalid.Path)
	require.NoFileExists(t, old.Path)
}

func TestPurgeByFileNameDateReportsInvalidNamesEvenAfterRetentionBoundary(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.FixedZone("UTC+09", 9*60*60))
	dir := t.TempDir()
	cutoff := createFileNamePurgeTestFile(t, dir, "app_20260920_1.log", now)
	newer := createFileNamePurgeTestFile(t, dir, "app_20260921_1.log", now)
	invalid := createFileNamePurgeTestFile(t, dir, "app_20261301_1.log", now)
	old := createFileNamePurgeTestFile(t, dir, "app_20260919_1.log", now)

	result := PurgeByFileNameDateIfNeeded([]FileInfo{cutoff, newer, invalid, old}, 1, now, time.Time{})

	require.Equal(t, []FileInfo{cutoff, newer, invalid}, result.Remaining)
	require.Len(t, result.RemovalResults, 2)
	require.Equal(t, invalid, result.RemovalResults[0].File)
	requireFileRemovalStep(t, result.RemovalResults[0].Err, FileRemovalStepInspectName, invalid.Path)
	require.ErrorIs(t, result.RemovalResults[0].Err, ErrInvalidFileNameDateAndIndex)
	require.Equal(t, old, result.RemovalResults[1].File)
	require.NoError(t, result.RemovalResults[1].Err)
	require.NoFileExists(t, old.Path)
	for _, file := range result.Remaining {
		require.FileExists(t, file.Path)
	}
}

func TestPurgeByFileNameDateSkipsAlreadyObservedLocalDay(t *testing.T) {
	t.Parallel()
	location := time.FixedZone("UTC+09", 9*60*60)
	now := time.Date(2026, 9, 21, 0, 30, 0, 0, location)
	file := createFileNamePurgeTestFile(t, t.TempDir(), "backend_20200101_1.jsonl", now)
	result := PurgeByFileNameDateIfNeeded([]FileInfo{file}, 1, now, now.Add(-time.Minute).UTC())
	require.False(t, result.Ran)
	require.Equal(t, []FileInfo{file}, result.Remaining)
	require.Empty(t, result.RemovalResults)
	require.FileExists(t, file.Path)
}

func TestPurgeByGenerationsFromFileNameUsesNumericIndexInsteadOfModTime(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now()
	ten := createFileNamePurgeTestFile(t, dir, "backend_20260921_10.jsonl", now.Add(-time.Hour))
	three := createFileNamePurgeTestFile(t, dir, "backend_20260921_3.jsonl", now)
	two := createFileNamePurgeTestFile(t, dir, "backend_20260921_2.jsonl", now.Add(time.Hour))
	files := []FileInfo{ten, three, two}
	result := PurgeByGenerationsFromFileNameIfNeeded(files, 2)
	require.True(t, result.Ran)
	require.NoError(t, JoinFileRemovalFailures(result.RemovalResults))
	require.Equal(t, []string{two.Path}, fileNamePurgeRemovedPaths(result))
	require.Equal(t, []FileInfo{ten, three}, result.Remaining)
	require.Equal(t, []FileInfo{ten, three, two}, files, "input order must not change")
}

func TestPurgeByGenerationsFromFileNameKeepsInvalidNamesOutsideRetentionCount(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now()
	invalid := createFileNamePurgeTestFile(t, dir, "backend_20260921_01.jsonl", now)
	older := createFileNamePurgeTestFile(t, dir, "backend_20260920_99.jsonl", now)
	newer := createFileNamePurgeTestFile(t, dir, "backend_20260921_1.jsonl", now.Add(-time.Hour))
	result := PurgeByGenerationsFromFileNameIfNeeded([]FileInfo{invalid, newer, older}, 1)
	require.Equal(t, []FileInfo{invalid, newer}, result.Remaining)
	require.Len(t, result.RemovalResults, 2)
	requireFileRemovalStep(t, result.RemovalResults[0].Err, FileRemovalStepInspectName, invalid.Path)
	require.ErrorIs(t, result.RemovalResults[0].Err, ErrInvalidFileNameDateAndIndex)
	require.Equal(t, older.Path, result.RemovalResults[1].File.Path)
	require.NoError(t, result.RemovalResults[1].Err)
	// 不明な名前を保持するために、保持数以内の正常なファイルまで追加削除しません。
	require.FileExists(t, invalid.Path)
	require.FileExists(t, newer.Path)
}

func TestPurgeByGenerationsFromFileNameDoesNotSubstituteNewerFileAfterRemovalFailure(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	olderPath := filepath.Join(dir, "backend_20260920_1.jsonl")
	require.NoError(t, os.Mkdir(olderPath, 0o700))
	older := FileInfo{Path: olderPath}
	newer := createFileNamePurgeTestFile(t, dir, "backend_20260921_1.jsonl", time.Now())
	result := PurgeByGenerationsFromFileNameIfNeeded([]FileInfo{newer, older}, 1)
	require.Equal(t, []FileInfo{newer, older}, result.Remaining)
	require.Len(t, result.RemovalResults, 1)
	require.Equal(t, olderPath, result.RemovalResults[0].File.Path)
	require.Error(t, result.RemovalResults[0].Err)
	require.FileExists(t, newer.Path)
}

func TestPurgeByDirectorySizeFromFileNameCountsProtectedAndInvalidFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now()
	invalid := createFileNamePurgeTestFile(t, dir, "not-an-archive.jsonl", now)
	protected := createFileNamePurgeTestFile(t, dir, "source-delete-pending", now)
	oldest := createFileNamePurgeTestFile(t, dir, "backend_20260920_99.jsonl", now.Add(2*time.Hour))
	middle := createFileNamePurgeTestFile(t, dir, "backend_20260921_2.jsonl.gz", now.Add(time.Hour))
	newest := createFileNamePurgeTestFile(t, dir, "backend_20260921_10.jsonl.gz", now.Add(-time.Hour))
	files := []FileInfo{newest, invalid, middle, protected, oldest}
	result := PurgeByDirectorySizeFromFileNameIfNeeded(files, 12, map[string]struct{}{protected.Path: {}})
	require.True(t, result.Ran)
	require.Len(t, result.RemovalResults, 3)
	require.Equal(t, invalid, result.RemovalResults[0].File)
	requireFileRemovalStep(t, result.RemovalResults[0].Err, FileRemovalStepInspectName, invalid.Path)
	require.ErrorIs(t, result.RemovalResults[0].Err, ErrInvalidFileNameDateAndIndex)
	require.Equal(t, oldest.Path, result.RemovalResults[1].File.Path)
	require.Equal(t, middle.Path, result.RemovalResults[2].File.Path)
	require.NoError(t, result.RemovalResults[1].Err)
	require.NoError(t, result.RemovalResults[2].Err)
	require.Equal(t, []FileInfo{newest, invalid, protected}, result.Remaining)
	require.Equal(t, uint64(12), totalFileBytes(result.Remaining))
	require.FileExists(t, protected.Path)
	require.FileExists(t, invalid.Path)
	require.FileExists(t, newest.Path)
}

func TestPurgeFromFileNameSkipsThresholdsNotExceeded(t *testing.T) {
	t.Parallel()
	file := createFileNamePurgeTestFile(t, t.TempDir(), "backend_20260921_1.jsonl", time.Now())
	for _, result := range []PurgeResult{
		PurgeByGenerationsFromFileNameIfNeeded([]FileInfo{file}, 1),
		PurgeByDirectorySizeFromFileNameIfNeeded([]FileInfo{file}, file.Size, nil),
	} {
		require.False(t, result.Ran)
		require.Empty(t, result.RemovalResults)
		require.Equal(t, []FileInfo{file}, result.Remaining)
	}
	require.FileExists(t, file.Path)
}

func createFileNamePurgeTestFile(t *testing.T, dir, name string, modifiedAt time.Time) FileInfo {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte("data"), 0o600))
	require.NoError(t, os.Chtimes(path, modifiedAt, modifiedAt))
	return FileInfo{Path: path, Size: 4, ModTime: modifiedAt}
}

func fileNamePurgeRemovedPaths(result PurgeResult) []string {
	paths := make([]string, 0, len(result.RemovalResults))
	for _, removal := range result.RemovalResults {
		if removal.Err == nil {
			paths = append(paths, removal.File.Path)
		}
	}
	return paths
}
