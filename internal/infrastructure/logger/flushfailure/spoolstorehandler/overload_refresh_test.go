package spoolstorehandler

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSpoolDirectoryStatsRefreshObservesFileGrowth(t *testing.T) {
	for _, tt := range []struct {
		name        string
		interval    time.Duration
		elapsed     time.Duration
		maxCalls    uint64
		wantRefresh bool
	}{
		{name: "zero interval without call limit", wantRefresh: true},
		{name: "negative interval without call limit", interval: -time.Second, wantRefresh: true},
		{name: "positive interval still valid", interval: time.Minute, elapsed: time.Second},
		{name: "positive interval elapsed", interval: time.Minute, elapsed: time.Minute, wantRefresh: true},
		{name: "call limit before positive interval", interval: time.Minute, maxCalls: 1, wantRefresh: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
			handler := newTestHandler(t, now, testOverloadPolicyConfig{
				statsScanInterval: tt.interval, maxCallsSinceScan: tt.maxCalls,
			})
			defer func() {
				if err := shutdownSpoolHandlerForTest(handler); err != nil {
					t.Error(err)
				}
			}()
			active := handler.Store.RootDir.ActiveDir()
			path := filepath.Join(active.Path(), "existing.log")
			writeFileWithModTime(t, path, []byte("old"), now)
			initial, err := handler.getDirStats(active)
			if err != nil || initial.TotalSize != 3 {
				t.Fatalf("initial stats = %+v, error = %v", initial, err)
			}

			const grown = "newly accumulated records"
			if err := os.WriteFile(path, []byte(grown), 0o600); err != nil {
				t.Fatal(err)
			}
			handler.clock = fixedClock{now: now.Add(tt.elapsed)}
			if _, err := handler.getDirStats(active); err != nil {
				t.Fatal(err)
			}
			// Refreshes remain asynchronous. Wait for publication instead of
			// assuming a particular scheduling order or a number of log calls.
			if done := handler.currentDirStatsScanDone(); done != nil {
				select {
				case <-done:
				case <-time.After(spoolHandlerHangTimeout):
					// 非同期scanが止まった場合の検出だけに使い、完了の順序は通知で確認します。
					t.Fatal("asynchronous directory stats refresh did not finish")
				}
			}
			handler.overloadCache.mu.RLock()
			stats := handler.overloadCache.snapshot.Stats
			handler.overloadCache.mu.RUnlock()
			wantSize := uint64(3)
			if tt.wantRefresh {
				wantSize = uint64(len(grown))
			}
			if stats.TotalSize != wantSize {
				t.Fatalf("cached directory size = %d, want %d after file growth", stats.TotalSize, wantSize)
			}
		})
	}
}
