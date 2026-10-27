package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	spoolworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

const regularSpoolBody = "{\"OccurredAt\":\"2026-08-20T12:34:56Z\",\"RequestID\":\"regular\",\"Payload\":{\"message\":\"preserve\"}}\n"

func TestProcessingReadsRegularFile(t *testing.T) {
	t.Parallel()

	store, path := newProcessingReadFixture(t)
	if err := os.WriteFile(path, []byte(regularSpoolBody), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := store.ReadProcessingFile(context.Background(), path)
	if err != nil || result.Path != path || string(result.Body) != regularSpoolBody {
		t.Fatalf("ReadProcessingFile = %+v, %v, want original path and body", result, err)
	}
	records, err := store.ReadProcessingRecords(context.Background(), path)
	if err != nil || len(records) != 1 {
		t.Fatalf("ReadProcessingRecords = %+v, %v, want one record", records, err)
	}
	if records[0].RequestID != "regular" || string(records[0].Payload) != `{"message":"preserve"}` || records[0].OccurredAt.IsZero() {
		t.Fatalf("decoded record changed: %+v", records[0])
	}
}

func TestProcessingReadsRejectNonRegularFiles(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{"symlink", "dangling_symlink", "directory"} {
		t.Run(kind, func(t *testing.T) {
			store, path := newProcessingReadFixture(t)
			preserve := createNonRegularSpoolEntry(t, kind, path)
			assertProcessingReadRejected(t, store, path)
			preserve()
		})
	}
}

func TestProcessingReadErrorClassifications(t *testing.T) {
	t.Parallel()

	store, path := newProcessingReadFixture(t)
	t.Run("missing_file", func(t *testing.T) {
		assertProcessingReadRejected(t, store, path)
		_, err := store.ReadProcessingRecords(context.Background(), path)
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("missing file error = %v, want wrapped ErrNotExist", err)
		}
	})
	t.Run("invalid_path", func(t *testing.T) {
		outside := filepath.Join(store.RootDir.ActiveDir().Path(), "outside"+spooldir.SpoolExt)
		for _, read := range []func(context.Context, string) error{
			func(ctx context.Context, path string) error {
				_, err := store.ReadProcessingFile(ctx, path)
				return err
			},
			func(ctx context.Context, path string) error {
				_, err := store.ReadProcessingRecords(ctx, path)
				return err
			},
		} {
			if err := read(context.Background(), outside); !errors.Is(err, pkgspool.ErrValidateProcessingSpoolFilePath) || errors.Is(err, pkgspool.ErrReadProcessingSpoolFile) {
				t.Fatalf("outside path error = %v, want path validation error", err)
			}
		}
	})
	t.Run("canceled_before_validation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		records, err := store.ReadProcessingRecords(ctx, "outside")
		if records != nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("ReadProcessingRecords = %+v, %v, want nil, Canceled", records, err)
		}
	})
	t.Run("malformed_regular_file", func(t *testing.T) {
		if err := os.WriteFile(path, []byte("broken spool body"), 0o600); err != nil {
			t.Fatal(err)
		}
		records, err := store.ReadProcessingRecords(context.Background(), path)
		if records != nil || !errors.Is(err, pkgspool.ErrDecodeProcessingSpoolFile) || !errors.Is(err, pkgspool.ErrInvalidSpoolFile) || errors.Is(err, pkgspool.ErrReadProcessingSpoolFile) {
			t.Fatalf("ReadProcessingRecords = %+v, %v, want decode/invalid spool error", records, err)
		}
	})
}

func TestSpoolWorkerSkipsNonRegularEntries(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{"symlink", "dangling_symlink", "directory"} {
		t.Run(kind, func(t *testing.T) {
			testSpoolWorkerSkipsEntry(t, func(t *testing.T, path string) func() {
				return createNonRegularSpoolEntry(t, kind, path)
			})
		})
	}
}

func TestSpoolWorkerRejectsSymlinkIntroducedBeforeRead(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{"symlink", "dangling_symlink"} {
		t.Run(kind, func(t *testing.T) {
			store, path := newProcessingReadFixture(t)
			if err := os.WriteFile(path, []byte(regularSpoolBody), 0o600); err != nil {
				t.Fatal(err)
			}
			var preserve func()
			replacingStore := &replaceBeforeProcessingReadStore{
				StateStore: store,
				beforeRead: func() {
					// 発見した後の候補を置き換え、読み取り時の防御を確かめます。
					if err := os.Remove(path); err != nil {
						t.Fatal(err)
					}
					preserve = createNonRegularSpoolEntry(t, kind, path)
				},
			}
			activity := &unexpectedSpoolActivity{t: t}
			worker, err := spoolworker.New(replacingStore, activity, activity)
			if err != nil {
				t.Fatal(err)
			}
			report, err := worker.RunOnceSpoolFileProcess(context.Background())
			if !errors.Is(err, pkgspool.ErrReadProcessingSpoolFile) || !errors.Is(report.Err, pkgspool.ErrReadProcessingSpoolFile) || report.Status != spoolworker.ProcessStatusFailed {
				t.Fatalf("worker report = %+v, %v, want failed read report", report, err)
			}
			if report.ProcessingPath != path || report.RecordCount != 0 || report.QuarantineReason != "" || report.QuarantineRecordCount != 0 {
				t.Fatalf("unexpected processing or quarantine report: %+v", report)
			}
			if preserve == nil {
				t.Fatal("worker did not attempt the processing read")
			}
			preserve()
		})
	}
}

type replaceBeforeProcessingReadStore struct {
	*StateStore
	beforeRead func()
}

func (s *replaceBeforeProcessingReadStore) ReadProcessingRecords(ctx context.Context, path string) ([]pkgspool.SpoolRecord, error) {
	s.beforeRead()
	return s.StateStore.ReadProcessingRecords(ctx, path)
}

type unexpectedSpoolActivity struct{ t *testing.T }

func (s *unexpectedSpoolActivity) Send(context.Context, []pkgspool.SpoolRecord) (pkgspool.ExportReport, error) {
	s.t.Fatal("non-regular processing file reached the sender")
	return nil, nil
}

func (s *unexpectedSpoolActivity) StoreActive(context.Context, []pkgspool.SpoolRecord) error {
	s.t.Fatal("non-regular processing file reached the retry writer")
	return nil
}

func testSpoolWorkerSkipsEntry(t *testing.T, create func(*testing.T, string) func()) {
	t.Helper()
	for _, state := range []pkgspool.State{pkgspool.StateProcessing, pkgspool.StateActive} {
		t.Run(string(state), func(t *testing.T) {
			store, err := NewStateStore(processingTestConfig{spoolPath: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			stateDir, err := store.stateDir(state)
			if err != nil {
				t.Fatal(err)
			}
			bucket := filepath.Join(stateDir.Path(), "2026", "08", "20", "12", "34")
			if err := os.MkdirAll(bucket, 0o755); err != nil {
				t.Fatal(err)
			}
			preserve := create(t, filepath.Join(bucket, "00000_skipped"+spooldir.SpoolExt))
			want := []string{
				filepath.Join(bucket, "10000_first"+spooldir.SpoolExt),
				filepath.Join(bucket, "20000_second"+spooldir.SpoolExt),
			}
			for _, path := range want {
				if err := os.WriteFile(path, []byte(regularSpoolBody), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(bucket, "ignored.txt"), []byte(regularSpoolBody), 0o600); err != nil {
				t.Fatal(err)
			}
			paths, err := store.listOldestStatePaths(context.Background(), state, 2)
			if err != nil || !slices.Equal(paths, want) {
				t.Fatalf("list paths = %v, %v, want %v", paths, err, want)
			}
			for _, first := range []int{0, 1} {
				after := ""
				if first > 0 {
					after, err = filepath.Rel(stateDir.Path(), want[first-1])
					if err != nil {
						t.Fatal(err)
					}
				}
				candidates, err := store.listStateFileCandidatesAfter(context.Background(), state, after, 2)
				paths := make([]string, len(candidates))
				for i, candidate := range candidates {
					paths[i] = candidate.Path
				}
				if err != nil || !slices.Equal(paths, want[first:]) {
					t.Fatalf("list candidates after %q = %v, %v, want %v", after, candidates, err, want[first:])
				}
			}
			if state == pkgspool.StateActive {
				path, ok, err := store.ClaimOldestActiveFile(context.Background())
				if err != nil || !ok || filepath.Base(path) != filepath.Base(want[0]) {
					t.Fatalf("ClaimOldestActiveFile = %q, %v, %v, want first regular file", path, ok, err)
				}
			} else {
				result, ok, err := store.ReadOldestProcessingFile(context.Background())
				if err != nil || !ok || result.Path != want[0] || string(result.Body) != regularSpoolBody {
					t.Fatalf("ReadOldestProcessingFile = %+v, %v, %v, want first regular file", result, ok, err)
				}
			}
			preserve()
		})
	}
}

func newProcessingReadFixture(t *testing.T) (*StateStore, string) {
	t.Helper()
	store, err := NewStateStore(processingTestConfig{spoolPath: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(store.RootDir.ProcessingDir().Path(), "2026", "08", "20", "12", "34", "56789_record"+spooldir.SpoolExt)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	return store, path
}

func assertProcessingReadRejected(t *testing.T, store *StateStore, path string) {
	t.Helper()
	result, fileErr := store.ReadProcessingFile(context.Background(), path)
	if result.Path != "" || result.Body != nil {
		t.Fatalf("ReadProcessingFile returned data for non-regular path: %+v", result)
	}
	records, recordErr := store.ReadProcessingRecords(context.Background(), path)
	if records != nil {
		t.Fatalf("ReadProcessingRecords returned records for non-regular path: %+v", records)
	}
	for _, err := range []error{fileErr, recordErr} {
		if !errors.Is(err, pkgspool.ErrReadProcessingSpoolFile) || !errors.Is(err, pkgspool.ErrSpoolOperation) || errors.Is(err, pkgspool.ErrInvalidSpoolFile) || errors.Is(err, pkgspool.ErrDecodeProcessingSpoolFile) {
			t.Fatalf("read error = %v, want read/operation error without decode/invalid classification", err)
		}
	}
}

func createNonRegularSpoolEntry(t *testing.T, kind, path string) func() {
	t.Helper()
	target := filepath.Join(t.TempDir(), "external"+spooldir.SpoolExt)
	if kind == "directory" {
		if err := os.Mkdir(path, 0o755); err != nil {
			t.Fatal(err)
		}
	} else {
		if kind == "symlink" {
			if err := os.WriteFile(target, []byte(regularSpoolBody), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		testutil.Symlink(t, target, path)
	}
	before, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	return func() {
		t.Helper()
		after, err := os.Lstat(path)
		if err != nil || !os.SameFile(before, after) || before.Mode() != after.Mode() {
			t.Fatalf("non-regular entry changed: %v, %v", after, err)
		}
		if kind == "directory" {
			return
		}
		if got, err := os.Readlink(path); err != nil || got != target {
			t.Fatalf("link target = %q, %v, want %q", got, err, target)
		}
		if kind == "symlink" {
			if body, err := os.ReadFile(target); err != nil || string(body) != regularSpoolBody {
				t.Fatalf("symlink target changed: %q, %v", body, err)
			}
		} else if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("dangling symlink target error = %v, want ErrNotExist", err)
		}
	}
}
