package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	"github.com/go-jappe/heterolog/internal/testutil"
)

func TestProcessingCandidatesPreserveBucketOrderAndContinuation(t *testing.T) {
	t.Parallel()
	store := newCandidateTestStore(t)
	// Component 順を保つ。"2026!" は "2026" より後だが、path 全体の
	// 文字列比較では separator の値によって逆になるため、その回帰も検査する。
	relativePaths := []string{
		"2025/12/31/23/59/z",
		"2026/01/01/00/00/b",
		"2026/01/01/00/00/c",
		"2026/01/01/00/01/a",
		"2026/01/01/01/00/a",
		"2026/01/02/00/00/a",
		"2026/02/01/00/00/a",
		"2026!/01/01/00/00/a",
		"2027/01/01/00/00/a",
	}
	for i := range relativePaths {
		relativePaths[i] = filepath.FromSlash(relativePaths[i]) + spooldir.SpoolExt
	}
	for i := len(relativePaths) - 1; i >= 0; i-- {
		writeCandidateTestFile(t, store, relativePaths[i])
	}
	var got []string
	after := ""
	for {
		candidates, err := store.ListProcessingFileCandidatesAfter(context.Background(), after, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(candidates) == 0 {
			break
		}
		if len(candidates) > 2 {
			t.Fatalf("candidate count = %d, want at most 2", len(candidates))
		}
		for _, candidate := range candidates {
			if want := filepath.Join(store.RootDir.ProcessingDir().Path(), candidate.RelativePath); candidate.Path != want {
				t.Fatalf("operation path = %q, want %q", candidate.Path, want)
			}
			got = append(got, candidate.RelativePath)
		}
		after = candidates[len(candidates)-1].RelativePath
		if len(got) > len(relativePaths) {
			t.Fatal("continuation did not advance")
		}
	}
	if !slices.Equal(got, relativePaths) {
		t.Fatalf("ordered candidates = %v, want %v", got, relativePaths)
	}
}

func TestProcessingCandidatesContinueAfterDeletedBoundaryTree(t *testing.T) {
	t.Parallel()
	store := newCandidateTestStore(t)
	before := filepath.FromSlash("2025/12/31/23/59/old") + spooldir.SpoolExt
	boundary := filepath.FromSlash("2026/08/20/12/34/deleted") + spooldir.SpoolExt
	after := filepath.FromSlash("2027/01/01/00/00/next") + spooldir.SpoolExt
	for _, relativePath := range []string{before, boundary, after} {
		writeCandidateTestFile(t, store, relativePath)
	}
	for path := filepath.Join(store.RootDir.ProcessingDir().Path(), boundary); path != store.RootDir.ProcessingDir().Path(); path = filepath.Dir(path) {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	candidates, err := store.ListProcessingFileCandidatesAfter(context.Background(), boundary, 10)
	if err != nil || len(candidates) != 1 || candidates[0].RelativePath != after {
		t.Fatalf("candidates after deleted tree = %v, %v, want %q", candidates, err, after)
	}
	candidates, err = store.ListProcessingFileCandidatesAfter(context.Background(), "", 10)
	if err != nil || len(candidates) != 2 || candidates[0].RelativePath != before {
		t.Fatalf("candidates from beginning = %v, %v, want older file included", candidates, err)
	}
}

func TestProcessingCandidatesValidateBoundaryAndHonorCancellation(t *testing.T) {
	t.Parallel()
	store := newCandidateTestStore(t)
	for _, relativePath := range []string{
		"..", filepath.Join("..", "outside"),
		filepath.Join(store.RootDir.ProcessingDir().Path(), "absolute"+spooldir.SpoolExt),
		filepath.FromSlash("2026/01/../01/00/00/file") + spooldir.SpoolExt,
		filepath.FromSlash("2026/01/01/00/file") + spooldir.SpoolExt,
		filepath.FromSlash("2026/01/01/00/00/file.txt"),
	} {
		if candidates, err := store.ListProcessingFileCandidatesAfter(context.Background(), relativePath, 1); err == nil || candidates != nil {
			t.Fatalf("boundary %q = %v, %v, want validation error", relativePath, candidates, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if candidates, err := store.ListProcessingFileCandidatesAfter(ctx, "../invalid", 1); candidates != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled enumeration = %v, %v, want context.Canceled", candidates, err)
	}
	for _, limit := range []int{-1, 0, 1} {
		if candidates, err := store.ListProcessingFileCandidatesAfter(context.Background(), "", limit); len(candidates) != 0 || err != nil {
			t.Fatalf("missing state, limit %d = %v, %v, want empty", limit, candidates, err)
		}
	}
}

func TestProcessingCandidatesIgnoreDirectorySymlinks(t *testing.T) {
	t.Parallel()
	store := newCandidateTestStore(t)
	want := filepath.FromSlash("2026/08/20/12/34/regular") + spooldir.SpoolExt
	writeCandidateTestFile(t, store, want)
	outside := t.TempDir()
	outsidePath := filepath.Join(outside, filepath.FromSlash("01/01/00/00/ignored")+spooldir.SpoolExt)
	if err := os.MkdirAll(filepath.Dir(outsidePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outsidePath, []byte(regularSpoolBody), 0o600); err != nil {
		t.Fatal(err)
	}
	testutil.Symlink(t, outside, filepath.Join(store.RootDir.ProcessingDir().Path(), "2025"))
	candidates, err := store.ListProcessingFileCandidatesAfter(context.Background(), "", 10)
	if err != nil || len(candidates) != 1 || candidates[0].RelativePath != want {
		t.Fatalf("directory symlink enumeration = %v, %v, want %q", candidates, err, want)
	}
}

func TestCandidateScannerPrunesEveryEarlierSubtreeBeforeOpen(t *testing.T) {
	t.Parallel()
	root := filepath.Join("virtual", "processing")
	after := []string{"2026", "08", "20", "12", "34", "b" + spooldir.SpoolExt}
	readers := make(map[string]*candidateTestReader)
	for depth := 0; depth < spoolDirectoryDepth; depth++ {
		path := filepath.Join(append([]string{root}, after[:depth]...)...)
		readers[path] = candidateReaderEntries(
			candidateTestEntry{name: after[depth], mode: fs.ModeDir},
			candidateTestEntry{name: "0000", mode: fs.ModeDir},
		)
	}
	minuteDir := filepath.Join(append([]string{root}, after[:spoolDirectoryDepth]...)...)
	readers[minuteDir] = candidateReaderEntries(
		candidateTestEntry{name: "c" + spooldir.SpoolExt},
		candidateTestEntry{name: "b" + spooldir.SpoolExt},
		candidateTestEntry{name: "a" + spooldir.SpoolExt},
	)
	scanner := spoolCandidateScanner{openDirectory: func(path string) (spoolDirectoryReader, error) {
		reader, ok := readers[path]
		if !ok {
			return nil, fmt.Errorf("earlier subtree must not be opened: %s", path)
		}
		return reader, nil
	}}
	candidates, err := scanner.list(context.Background(), root, after, 10)
	if err != nil || len(candidates) != 1 || filepath.Base(candidates[0].Path) != "c"+spooldir.SpoolExt {
		t.Fatalf("pruned candidates = %v, %v, want only c", candidates, err)
	}
	for path, reader := range readers {
		if !reader.closed {
			t.Fatalf("reader for boundary prefix %s was not closed", path)
		}
	}
}

func TestCandidateScannerStreamsLargeUnsortedMinuteWithBoundedCandidates(t *testing.T) {
	t.Parallel()
	const total, limit = 20000, 7
	reader := &candidateTestReader{total: total, entry: func(i int) os.DirEntry {
		return candidateTestEntry{name: fmt.Sprintf("%08d%s", total-i, spooldir.SpoolExt)}
	}}
	scanner := spoolCandidateScanner{openDirectory: func(string) (spoolDirectoryReader, error) { return reader, nil }}
	names, err := scanner.smallestNames(context.Background(), "minute", "00000003"+spooldir.SpoolExt, false, limit, true)
	if err != nil || len(names) != limit || cap(names) > limit {
		t.Fatalf("streamed names = %v, %v, capacity %d, want %d bounded names", names, err, cap(names), limit)
	}
	for i, name := range names {
		if want := fmt.Sprintf("%08d%s", i+4, spooldir.SpoolExt); name != want {
			t.Fatalf("name[%d] = %q, want %q", i, name, want)
		}
	}
	if reader.reads <= 1 || reader.maxRequest != spoolReadBatchSize || reader.offset != total || !reader.closed {
		t.Fatalf("stream was not fully read in fixed batches and closed: %+v", reader)
	}
}

func TestCandidateScannerPaginatesOversizedDirectoryWithoutLosingLaterBuckets(t *testing.T) {
	t.Parallel()
	const yearCount = spoolReadBatchSize*2 + 3
	root := "processing"
	lastYear := fmt.Sprintf("%06d", yearCount)
	rootOpens := 0
	scanner := spoolCandidateScanner{openDirectory: func(path string) (spoolDirectoryReader, error) {
		if path == root {
			rootOpens++
			return &candidateTestReader{total: yearCount, entry: func(i int) os.DirEntry {
				return candidateTestEntry{name: fmt.Sprintf("%06d", yearCount-i), mode: fs.ModeDir}
			}}, nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return nil, err
		}
		parts := strings.Split(rel, string(os.PathSeparator))
		if parts[0] != lastYear {
			// 列挙の後に directory が消えた場合も次の候補を探す。
			return nil, os.ErrNotExist
		}
		if len(parts) < spoolDirectoryDepth {
			return candidateReaderEntries(candidateTestEntry{name: "01", mode: fs.ModeDir}), nil
		}
		return candidateReaderEntries(candidateTestEntry{name: "record" + spooldir.SpoolExt}), nil
	}}
	candidates, err := scanner.list(context.Background(), root, nil, 1)
	want := filepath.Join(lastYear, "01", "01", "01", "01", "record"+spooldir.SpoolExt)
	if err != nil || len(candidates) != 1 || candidates[0].RelativePath != want || rootOpens != 3 {
		t.Fatalf("directory pages = %v, %v, opens %d, want last bucket after 3 bounded pages", candidates, err, rootOpens)
	}
}

func TestCandidateScannerCancellationDuringEnumerationClosesReader(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := &candidateTestReader{total: spoolReadBatchSize * 4, entry: func(i int) os.DirEntry {
		return candidateTestEntry{name: fmt.Sprintf("%08d%s", i, spooldir.SpoolExt)}
	}}
	reader.onRead = func() {
		if reader.reads == 2 {
			cancel()
		}
	}
	scanner := spoolCandidateScanner{openDirectory: func(string) (spoolDirectoryReader, error) { return reader, nil }}
	names, err := scanner.smallestNames(ctx, "minute", "", false, 2, true)
	if names != nil || !errors.Is(err, context.Canceled) || !reader.closed || reader.reads != 2 {
		t.Fatalf("canceled scan = %v, %v, reader %+v", names, err, reader)
	}
}

func TestCandidateScannerPreservesIOErrorsAndSkipsDisappearedEntries(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"open", "read", "info"} {
		t.Run(phase, func(t *testing.T) {
			reader := candidateReaderEntries(candidateTestEntry{name: "record" + spooldir.SpoolExt, infoErr: fs.ErrPermission})
			if phase == "read" {
				reader.readErr = fs.ErrPermission
			}
			scanner := spoolCandidateScanner{openDirectory: func(string) (spoolDirectoryReader, error) {
				if phase == "open" {
					return nil, fs.ErrPermission
				}
				return reader, nil
			}}
			if names, err := scanner.smallestNames(context.Background(), "minute", "", false, 2, true); names != nil || !errors.Is(err, fs.ErrPermission) {
				t.Fatalf("%s failure = %v, %v, want permission error", phase, names, err)
			}
			if phase != "open" && !reader.closed {
				t.Fatal("failed scan did not close reader")
			}
		})
	}
	reader := candidateReaderEntries(
		candidateTestEntry{name: "a" + spooldir.SpoolExt, infoErr: fs.ErrNotExist},
		candidateTestEntry{name: "b" + spooldir.SpoolExt},
	)
	scanner := spoolCandidateScanner{openDirectory: func(string) (spoolDirectoryReader, error) { return reader, nil }}
	names, err := scanner.smallestNames(context.Background(), "minute", "", false, 2, true)
	if err != nil || !slices.Equal(names, []string{"b" + spooldir.SpoolExt}) {
		t.Fatalf("disappeared entry scan = %v, %v, want surviving b", names, err)
	}
}

func TestBoundedSpoolNamesCapacityAndOrdering(t *testing.T) {
	t.Parallel()
	for _, limit := range []int{1, 7, spoolReadBatchSize, 257} {
		names := boundedSpoolNames{limit: limit}
		for i := 20000; i >= 0; i-- {
			names.add(fmt.Sprintf("%08d", i))
			if len(names.values) > limit || cap(names.values) > limit {
				t.Fatalf("limit %d exceeded: len %d, cap %d", limit, len(names.values), cap(names.values))
			}
		}
		slices.Sort(names.values)
		for i, name := range names.values {
			if want := fmt.Sprintf("%08d", i); name != want {
				t.Fatalf("limit %d, name[%d] = %q, want %q", limit, i, name, want)
			}
		}
	}
}

// 生成 stream のため、入力一覧自体も backlog サイズの配列を作らない。
// B/op は全列挙で発生した累計 allocation であり、保持 candidate 数とは異なる。
func BenchmarkCandidateScannerLargeMinute(b *testing.B) {
	for _, total := range []int{1000, 100000} {
		b.Run(fmt.Sprintf("entries_%d", total), func(b *testing.B) {
			b.ReportAllocs()
			maxCandidateCapacity, maxReadBatch := 0, 0
			for b.Loop() {
				reader := &candidateTestReader{total: total, entry: func(i int) os.DirEntry {
					return candidateTestEntry{name: fmt.Sprintf("%08d%s", total-i, spooldir.SpoolExt)}
				}}
				scanner := spoolCandidateScanner{openDirectory: func(string) (spoolDirectoryReader, error) { return reader, nil }}
				names, err := scanner.smallestNames(context.Background(), "minute", "", false, 32, true)
				if err != nil || len(names) != 32 || cap(names) > 32 {
					b.Fatalf("scan = %v, %v", names, err)
				}
				maxCandidateCapacity = max(maxCandidateCapacity, cap(names))
				maxReadBatch = max(maxReadBatch, reader.maxRequest)
			}
			b.ReportMetric(float64(maxCandidateCapacity), "candidate-cap")
			b.ReportMetric(float64(maxReadBatch), "read-batch")
		})
	}
}

func newCandidateTestStore(t *testing.T) *StateStore {
	t.Helper()
	store, err := NewStateStore(processingTestConfig{spoolPath: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func writeCandidateTestFile(t *testing.T, store *StateStore, relativePath string) {
	t.Helper()
	path := filepath.Join(store.RootDir.ProcessingDir().Path(), relativePath)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(regularSpoolBody), 0o600); err != nil {
		t.Fatal(err)
	}
}

type candidateTestReader struct {
	total      int
	entry      func(int) os.DirEntry
	offset     int
	reads      int
	maxRequest int
	onRead     func()
	readErr    error
	closed     bool
}

func (r *candidateTestReader) ReadDir(n int) ([]os.DirEntry, error) {
	r.reads++
	r.maxRequest = max(r.maxRequest, n)
	if n < 1 || n > spoolReadBatchSize {
		return nil, fmt.Errorf("unbounded directory read: %d", n)
	}
	if r.onRead != nil {
		r.onRead()
	}
	if r.readErr != nil {
		return nil, r.readErr
	}
	entries := make([]os.DirEntry, 0, min(n, r.total-r.offset))
	for len(entries) < n && r.offset < r.total {
		entries = append(entries, r.entry(r.offset))
		r.offset++
	}
	if r.offset == r.total {
		return entries, io.EOF
	}
	return entries, nil
}

func (r *candidateTestReader) Close() error {
	r.closed = true
	return nil
}

func candidateReaderEntries(entries ...candidateTestEntry) *candidateTestReader {
	return &candidateTestReader{total: len(entries), entry: func(i int) os.DirEntry { return entries[i] }}
}

type candidateTestEntry struct {
	name    string
	mode    fs.FileMode
	infoErr error
}

func (e candidateTestEntry) Name() string               { return e.name }
func (e candidateTestEntry) IsDir() bool                { return e.mode.IsDir() }
func (e candidateTestEntry) Type() fs.FileMode          { return e.mode.Type() }
func (e candidateTestEntry) Info() (fs.FileInfo, error) { return e, e.infoErr }
func (e candidateTestEntry) Size() int64                { return 0 }
func (e candidateTestEntry) Mode() fs.FileMode          { return e.mode }
func (e candidateTestEntry) ModTime() time.Time         { return time.Time{} }
func (e candidateTestEntry) Sys() any                   { return nil }
