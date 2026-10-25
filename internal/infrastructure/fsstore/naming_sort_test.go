package fsstore

import (
	"errors"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"
)

func TestSortFilesByNameDateAndIndexAsc(t *testing.T) {
	t.Parallel()

	// 接頭辞・ディレクトリ名・更新日時ではなく、名前末尾の日付と数値indexで並べます。
	paths := []string{
		filepath.Join("logs", "aaa_20260921_1.log"),
		filepath.Join("logs", "zzz_20260920_10.log.gz"),
		filepath.Join("logs", "error_api_v2_20260920_2.log.zst"),
		filepath.Join("logs", "app_20260920_2.log.gz"),
		filepath.Join("logs", "app_20260920_2.log"),
		filepath.Join("20991231", "zzz_20260919_99.jsonl.zst"),
		filepath.Join("logs", "app_20260920_1.jsonl.custom_format"),
		filepath.Join("logs", "prefix_20990101_99_app.prod_20260920_3.log.gz"),
	}
	files := make([]FileInfo, len(paths))
	for i, path := range paths {
		files[i] = FileInfo{Path: path, Size: uint64(i + 1), ModTime: time.Unix(int64(i), 0)}
	}
	want := []FileInfo{files[5], files[6], files[4], files[3], files[2], files[7], files[1], files[0]}
	pathCalls := 0
	if err := SortFilesByNameDateAndIndexAsc(files, func(file FileInfo) string {
		pathCalls++
		return file.Path
	}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(files, want) {
		t.Fatalf("files = %#v, want %#v", files, want)
	}
	if pathCalls != len(files) {
		t.Fatalf("pathOf calls = %d, want %d", pathCalls, len(files))
	}
}

func TestSortFilesByNameDateAndIndexAscPreservesEqualPathOrder(t *testing.T) {
	t.Parallel()

	files := []FileInfo{
		{Path: "app_20260920_10.log.gz", Size: 10},
		{Path: "app_20260920_2.log.gz", Size: 7},
		{Path: "app_20260920_2.log.gz", Size: 3},
	}
	want := []FileInfo{files[1], files[2], files[0]}
	if err := SortFilesByNameDateAndIndexAsc(files, func(file FileInfo) string { return file.Path }); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(files, want) {
		t.Fatalf("files = %#v, want %#v", files, want)
	}
}

func TestSortFilesByNameDateAndIndexAscSupportsPathsAndDateBoundaries(t *testing.T) {
	t.Parallel()

	maxIndex := strconv.Itoa(int(^uint(0) >> 1))
	paths := []string{
		"app_20250101_1.log",
		"app_20240229_" + maxIndex + ".log.custom-format",
		"app_20240301_1.log.gz",
		"app_20240229_10.log.gz",
		"app_20240228_99.jsonl",
		"app_20240229_2.log.gz",
		"app_20241231_99.log",
	}
	want := []string{paths[4], paths[5], paths[3], paths[1], paths[2], paths[6], paths[0]}
	if err := SortFilesByNameDateAndIndexAsc(paths, func(path string) string { return path }); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(paths, want) {
		t.Fatalf("paths = %v, want %v", paths, want)
	}
}

func TestSortFilesByNameDateAndIndexAscRejectsInvalidNamesWithoutReordering(t *testing.T) {
	t.Parallel()

	overIntIndex := strconv.FormatUint(uint64(^uint(0)>>1)+1, 10)
	for _, invalid := range []string{
		"",
		"memo.log.gz",
		"_20260920_1.log",
		"app_20260920_1",
		"app_20260920_1.",
		"app_20260920_1.log.gz.tmp",
		"app_20260229_1.log",
		"app_20261301_1.log.gz",
		"app_20260931_1.log.gz",
		"app_2026920_1.log.gz",
		"app_20260920_0.log.gz",
		"app_20260920_01.log.gz",
		"app_20260920_-1.log.gz",
		"app_20260920_+1.log.gz",
		"app_20260920_one.log.gz",
		"app_20260920_" + overIntIndex + ".log.gz",
	} {
		t.Run(invalid, func(t *testing.T) {
			for _, paths := range [][]string{
				{invalid},
				{invalid, "app_20260920_10.log", "app_20260920_2.log"},
				{"app_20260920_10.log", invalid, "app_20260920_2.log"},
				{"app_20260920_10.log", "app_20260920_2.log", invalid},
			} {
				before := slices.Clone(paths)
				err := SortFilesByNameDateAndIndexAsc(paths, func(path string) string { return path })
				var nameErr *FileNameDateAndIndexError
				if !errors.As(err, &nameErr) || nameErr.Path != invalid || !errors.Is(err, ErrInvalidFileNameDateAndIndex) {
					t.Fatalf("error = %v, want invalid path %q", err, invalid)
				}
				if !slices.Equal(paths, before) {
					t.Fatalf("paths changed on error: got %v, want %v", paths, before)
				}
			}
		})
	}
}

func TestSortFilesByNameDateAndIndexAscEmptyAndSingle(t *testing.T) {
	t.Parallel()

	for _, paths := range [][]string{nil, {}, {"app_20260920_1.log.gz"}} {
		before := slices.Clone(paths)
		if err := SortFilesByNameDateAndIndexAsc(paths, func(path string) string { return path }); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(paths, before) || (paths == nil) != (before == nil) {
			t.Fatalf("paths = %#v, want %#v", paths, before)
		}
	}
}
