package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

func TestStoreByStatePreservesObstructionAndStoresSameRecords(t *testing.T) {
	for _, state := range []pkgspool.State{pkgspool.StateActive, pkgspool.StateProcessing, pkgspool.StateDone, pkgspool.StateDead, pkgspool.StateQuarantine} {
		for _, kind := range []string{"file", "link", "broken link"} {
			t.Run(string(state)+"/"+kind, func(t *testing.T) {
				now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
				store := newTestSpoolStore(t, fixedClock{now: now}, testSpoolStoreConfig{spoolPath: t.TempDir(), target: "target", dir: "record"})
				root, outside := store.RootDir.String(), t.TempDir()
				require.NoError(t, os.MkdirAll(root, 0o755))
				path := filepath.Join(root, string(state))
				if kind == "file" {
					require.NoError(t, os.WriteFile(path, []byte("keep original"), 0o644))
				} else {
					target := outside
					if kind == "broken link" {
						target = filepath.Join(outside, "absent")
					}
					testutil.Symlink(t, target, path)
				}
				var warnings []*logging.LogEntry
				store.OnWarn = func(_ context.Context, entry *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
					warnings = append(warnings, entry)
				}
				records := []pkgspool.SpoolRecord{{OccurredAt: now, RequestID: "same-record", Payload: json.RawMessage(`{"message":"preserve then save"}`)}}
				require.NoError(t, store.StoreByState(context.Background(), state, records))
				files := listSpoolFiles(t, path)
				require.Len(t, files, 1)
				assertSpoolRecords(t, readSpoolRecords(t, files[0]), records)
				require.Len(t, warnings, 1)
				require.Equal(t, path, warnings[0].Fields["original_path"])
				require.Error(t, warnings[0].Err)
				retained := warnings[0].Fields["retained_path"].(string)
				require.Equal(t, filepath.Join(root, ".preserved"), filepath.Dir(retained))
				info, err := os.Lstat(retained)
				require.NoError(t, err)
				if kind == "file" {
					body, err := os.ReadFile(retained)
					require.NoError(t, err)
					require.Equal(t, "keep original", string(body))
				} else {
					require.NotZero(t, info.Mode()&os.ModeSymlink)
				}
				entries, err := os.ReadDir(outside)
				require.NoError(t, err)
				require.Empty(t, entries)
			})
		}
	}
}

func TestConcurrentStoresPreserveObstructionOnceWithoutLosingRecords(t *testing.T) {
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	cfg := testSpoolStoreConfig{spoolPath: t.TempDir(), target: "target", dir: "record"}
	stores := []*SpoolStore{newTestSpoolStore(t, fixedClock{now: now}, cfg), newTestSpoolStore(t, fixedClock{now: now}, cfg)}
	root := stores[0].RootDir.String()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "active"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "active", "2026"), []byte("keep"), 0o644))
	var warnings atomic.Int32
	for _, store := range stores {
		store.OnWarn = func(context.Context, *logging.LogEntry, observer.Tracer, observer.Meter) { warnings.Add(1) }
	}
	const count = 12
	start := make(chan struct{})
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for index := range count {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			errs <- stores[index%len(stores)].StoreByState(context.Background(), pkgspool.StateActive, []pkgspool.SpoolRecord{{
				OccurredAt: now, RequestID: fmt.Sprint(index), Payload: json.RawMessage(`{"message":"concurrent"}`),
			}})
		}(index)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	files := listSpoolFiles(t, stores[0].RootDir.ActiveDir().Path())
	require.Len(t, files, count)
	seen := make(map[string]bool)
	for _, path := range files {
		records := readSpoolRecords(t, path)
		require.Len(t, records, 1)
		require.False(t, seen[records[0].RequestID])
		seen[records[0].RequestID] = true
	}
	require.Equal(t, int32(1), warnings.Load())
	entries, err := os.ReadDir(filepath.Join(root, ".preserved"))
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

func TestStoreDirectoryWarningDoesNotHoldPreparationLock(t *testing.T) {
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	store := newTestSpoolStore(t, fixedClock{now: now}, testSpoolStoreConfig{spoolPath: t.TempDir(), target: "target", dir: "record"})
	require.NoError(t, os.MkdirAll(store.RootDir.String(), 0o755))
	require.NoError(t, os.WriteFile(store.RootDir.ActiveDir().Path(), []byte("keep"), 0o644))
	store.OnWarn = func(_ context.Context, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, store.EnsureCurrentTimePartitionDirectory(ctx, pkgspool.StateDead))
	}
	require.NoError(t, store.StoreByState(context.Background(), pkgspool.StateActive, []pkgspool.SpoolRecord{{OccurredAt: now, Payload: json.RawMessage(`{}`)}}))
}
