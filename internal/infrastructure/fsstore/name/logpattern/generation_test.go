package logpattern

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
)

func TestPurgeGenerationsByFileNameIfNeededRemovesWholeExpiredGenerations(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 8, 29, 9, 0, 0, 0, time.UTC)
	rawOldest := writeGenerationSeriesTestFile(t, dir, "app_20260829_1.log", base)
	gzipOldest := writeGenerationSeriesTestFile(t, dir, "app_20260829_1.log.gz", base.Add(time.Minute))
	brOldest := writeGenerationSeriesTestFile(t, dir, "app_20260829_1.log.br", base.Add(2*time.Minute))
	rawMiddle := writeGenerationSeriesTestFile(t, dir, "app_20260829_2.log", base.Add(3*time.Minute))
	gzipMiddle := writeGenerationSeriesTestFile(t, dir, "app_20260829_2.log.gz", base.Add(4*time.Minute))
	brMiddle := writeGenerationSeriesTestFile(t, dir, "app_20260829_2.log.br", base.Add(5*time.Minute))
	rawNewest := writeGenerationSeriesTestFile(t, dir, "app_20260829_3.log", base.Add(6*time.Minute))
	gzipNewest := writeGenerationSeriesTestFile(t, dir, "app_20260829_3.log.gz", base.Add(7*time.Minute))
	temporary := writeGenerationSeriesTestFile(t, dir, "app_20260829_4.log.gz.tmp", base.Add(8*time.Minute))

	files := []fsstore.FileInfo{
		rawNewest,
		gzipNewest,
		brMiddle,
		gzipOldest,
		rawOldest,
		brOldest,
		temporary,
		rawMiddle,
		gzipMiddle,
	}
	original := append([]fsstore.FileInfo(nil), files...)
	// 3世代のうち古い1世代を、未圧縮・圧縮形式を問わずまとめて削除します。
	result := PurgeGenerationsByFileNameIfNeeded(files, "log", 2)

	if !result.Ran {
		t.Fatal("Ran = false, want true")
	}
	if !reflect.DeepEqual(files, original) {
		t.Fatalf("input was modified: got %+v, want %+v", files, original)
	}
	if got, want := generationRemovalPaths(result.RemovalResults), []string{
		gzipOldest.Path,
		rawOldest.Path,
		brOldest.Path,
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("removal paths = %v, want the oldest generation in input order %v", got, want)
	}
	if got, want := generationFilePaths(result.Remaining), []string{
		rawNewest.Path,
		gzipNewest.Path,
		brMiddle.Path,
		temporary.Path,
		rawMiddle.Path,
		gzipMiddle.Path,
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Remaining paths = %v, want input-order %v", got, want)
	}
	for _, removed := range []fsstore.FileInfo{rawOldest, gzipOldest, brOldest} {
		if _, err := os.Stat(removed.Path); !os.IsNotExist(err) {
			t.Fatalf("expired generation file %q still exists: %v", removed.Path, err)
		}
	}
	if _, err := os.Stat(temporary.Path); err != nil {
		t.Fatalf("temporary file must not be a generation candidate: %v", err)
	}
}

func TestPurgeGenerationsByFileNameIfNeededCountsFormatsOfOneGenerationOnce(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 8, 29, 9, 0, 0, 0, time.UTC)
	files := []fsstore.FileInfo{
		writeGenerationSeriesTestFile(t, dir, "app_20260829_1.jsonl", base),
		writeGenerationSeriesTestFile(t, dir, "app_20260829_2.jsonl", base.Add(time.Minute)),
		writeGenerationSeriesTestFile(t, dir, "app_20260829_1.jsonl.gz", base.Add(2*time.Minute)),
		writeGenerationSeriesTestFile(t, dir, "app_20260829_2.jsonl.gz", base.Add(3*time.Minute)),
		writeGenerationSeriesTestFile(t, dir, "app_20260829_1.jsonl.zst", base.Add(4*time.Minute)),
		writeGenerationSeriesTestFile(t, dir, "app_20260829_2.jsonl.zst", base.Add(5*time.Minute)),
	}

	result := PurgeGenerationsByFileNameIfNeeded(files, ".jsonl", 2)
	if result.Ran {
		t.Fatal("Ran = true although only two generations exist")
	}
	if !reflect.DeepEqual(result.Remaining, files) {
		t.Fatalf("Remaining = %+v, want all files in input order", result.Remaining)
	}
	if len(result.RemovalResults) != 0 {
		t.Fatalf("RemovalResults = %+v, want none", result.RemovalResults)
	}
	result.Remaining[0].Path = "changed"
	if files[0].Path == "changed" {
		t.Fatal("no-op Remaining aliases the input")
	}
}

func TestPurgeGenerationsByFileNameIfNeededKeepsFailureWithoutRemovingNewerGeneration(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 8, 29, 9, 0, 0, 0, time.UTC)
	failingRawPath := filepath.Join(dir, "app_20260829_1.log")
	if err := os.Mkdir(failingRawPath, 0o700); err != nil {
		t.Fatalf("mkdir failing raw path: %v", err)
	}
	if err := os.WriteFile(filepath.Join(failingRawPath, "child"), []byte("keep"), 0o600); err != nil {
		t.Fatalf("write failing raw child: %v", err)
	}
	if err := os.Chtimes(failingRawPath, base, base); err != nil {
		t.Fatalf("set failing raw time: %v", err)
	}
	failingRaw := fsstore.FileInfo{Path: failingRawPath, Size: 1, ModTime: base}
	rawNewest := writeGenerationSeriesTestFile(t, dir, "app_20260829_2.log", base.Add(3*time.Minute))
	gzipOldest := writeGenerationSeriesTestFile(t, dir, "app_20260829_1.log.gz", base.Add(time.Minute))
	gzipNewest := writeGenerationSeriesTestFile(t, dir, "app_20260829_2.log.gz", base.Add(2*time.Minute))
	files := []fsstore.FileInfo{failingRaw, gzipNewest, rawNewest, gzipOldest, gzipOldest}

	result := PurgeGenerationsByFileNameIfNeeded(files, "log", 1)
	if !result.Ran {
		t.Fatal("Ran = false, want true")
	}
	// 同じpathの重複観測は一度だけ削除し、削除に失敗しても新しい世代は削除しません。
	if got, want := generationRemovalPaths(result.RemovalResults), []string{
		failingRaw.Path,
		gzipOldest.Path,
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("removal paths = %v, want expired generation in input order %v", got, want)
	}
	if result.RemovalResults[0].Err == nil || result.RemovalResults[1].Err != nil {
		t.Fatalf("RemovalResults = %+v, want failure followed by success", result.RemovalResults)
	}
	if got, want := generationFilePaths(result.Remaining), []string{
		failingRaw.Path,
		gzipNewest.Path,
		rawNewest.Path,
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Remaining paths = %v, want failed raw and newest generation %v", got, want)
	}
}

func TestPurgeGenerationsByFileNameIfNeededKeepsUnparsableNamesWithError(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 8, 29, 9, 0, 0, 0, time.UTC)
	notes := writeGenerationSeriesTestFile(t, dir, "app_notes.log", base)
	notesCompressed := writeGenerationSeriesTestFile(t, dir, "app_notes.log.gz", base.Add(time.Minute))
	first := writeGenerationSeriesTestFile(t, dir, "app_20260829_1.log", base.Add(2*time.Minute))
	second := writeGenerationSeriesTestFile(t, dir, "app_20260829_2.log", base.Add(3*time.Minute))
	files := []fsstore.FileInfo{notes, notesCompressed, first, second}

	if got := CountGenerations(files, "log"); got != 3 {
		t.Fatalf("CountGenerations = %d, want unparsable stem and two generations", got)
	}
	result := PurgeGenerationsByFileNameIfNeeded(files, "log", 2)
	// 解析できない名前は削除する世代に含めず、エラーとともに保持します。
	if !result.Ran || len(result.RemovalResults) != 2 {
		t.Fatalf("result = %+v, want two inspection errors", result)
	}
	for index, want := range []string{notes.Path, notesCompressed.Path} {
		removal := result.RemovalResults[index]
		if removal.File.Path != want || removal.Err == nil {
			t.Fatalf("RemovalResults[%d] = %+v, want inspection error for %q", index, removal, want)
		}
	}
	if !reflect.DeepEqual(result.Remaining, files) {
		t.Fatalf("Remaining = %+v, want every file", result.Remaining)
	}
	for _, file := range files {
		if _, err := os.Stat(file.Path); err != nil {
			t.Fatalf("file %q was removed: %v", file.Path, err)
		}
	}
}

func TestCountGenerationsCountsEachDateAndIndexOnce(t *testing.T) {
	other := filepath.Join("other", "app_20260829_1.log")
	files := []fsstore.FileInfo{
		{Path: "app_20260829_1.log"},
		{Path: "app_20260829_2.log"},
		{Path: "app_20260829_3.log"},
		{Path: "app_20260829_1.log.gz"},
		{Path: "app_20260829_2.log.gz"},
		{Path: "app_20260829_4.log.gz"},
		{Path: "app_20260829_1.log.br"},
		{Path: "app_20260829_5.log.gz.tmp"},
		{Path: "app_x_20260829_1.log"},
		{Path: "app_20260829_1.jsonl"},
	}

	// 未圧縮・各圧縮形式は同じ世代として一度だけ数え、名前やディレクトリが異なる世代は区別します。
	if got := CountGenerations(files, "log"); got != 5 {
		t.Fatalf("CountGenerations = %d, want generations 1-4 and app_x", got)
	}
	if got := CountGenerations(append(files, fsstore.FileInfo{Path: other}), "log"); got != 6 {
		t.Fatalf("CountGenerations with another directory = %d, want 6", got)
	}
	if got := CountGenerations(files, "jsonl"); got != 1 {
		t.Fatalf("jsonl CountGenerations = %d, want 1", got)
	}
	if got := CountGenerations(files, ""); got != 0 {
		t.Fatalf("empty extension CountGenerations = %d, want 0", got)
	}
}

func writeGenerationSeriesTestFile(
	t *testing.T,
	dir string,
	name string,
	modTime time.Time,
) fsstore.FileInfo {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(name), 0o600); err != nil {
		t.Fatalf("write %q: %v", path, err)
	}
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatalf("set %q time: %v", path, err)
	}
	return fsstore.FileInfo{Path: path, Size: uint64(len(name)), ModTime: modTime}
}

func generationFilePaths(files []fsstore.FileInfo) []string {
	paths := make([]string, 0, len(files))
	for _, file := range files {
		paths = append(paths, file.Path)
	}
	return paths
}

func generationRemovalPaths(results []fsstore.FileRemovalResult) []string {
	paths := make([]string, 0, len(results))
	for _, result := range results {
		paths = append(paths, result.File.Path)
	}
	return paths
}
