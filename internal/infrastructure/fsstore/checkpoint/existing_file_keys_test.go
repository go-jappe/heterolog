package checkpoint

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestScanExistingFileKeysSeparatesRepresentationsAndIgnoresUnrelatedNames(t *testing.T) {
	t.Parallel()
	pattern := logpattern.FileNamePattern{Dir: t.TempDir(), Prefix: "error_", Target: "app", Extension: "log"}
	for _, name := range []string{
		"error_app_20260926_1.log", "error_app_20260926_1.log.gz", "error_app_20260926_1.log.zst",
		"error_app_20260926_2.log", "error_app_20260926_3.log.custom",
		"other_20260926_4.log.gz", "error_app_20260926_04.log.gz", "error_app_20260230_4.log.gz",
		"error_app_20260926_0.log.gz", "error_app_20260926_4.log.gz.tmp",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(pattern.Dir, name), nil, 0o600))
	}
	// The boundary uses names, even when an entry cannot currently be uploaded.
	require.NoError(t, os.Mkdir(filepath.Join(pattern.Dir, "error_app_20260926_5.log.gz"), 0o700))
	keys, err := ScanExistingFileKeys(pattern)
	require.NoError(t, err)
	require.Equal(t, []CheckpointKey{
		{Generation: "20260926-1", Compressed: false},
		{Generation: "20260926-1", Compressed: true},
		{Generation: "20260926-2", Compressed: false},
		{Generation: "20260926-3", Compressed: true},
		{Generation: "20260926-5", Compressed: true},
	}, keys)
	pattern.Dir = filepath.Join(pattern.Dir, "missing")
	keys, err = ScanExistingFileKeys(pattern)
	require.NoError(t, err)
	require.Empty(t, keys)
}

func TestScanExistingFileKeysIncludesDanglingCanonicalSymlink(t *testing.T) {
	t.Parallel()
	pattern := logpattern.FileNamePattern{Dir: t.TempDir(), Target: "app", Extension: "log"}
	testutil.Symlink(t, filepath.Join(pattern.Dir, "missing"), filepath.Join(pattern.Dir, "app_20260926_1.log.gz"))
	keys, err := ScanExistingFileKeys(pattern)
	require.NoError(t, err)
	require.Equal(t, []CheckpointKey{{Generation: "20260926-1", Compressed: true}}, keys)
}

func TestScanExistingFileKeysPropagatesDirectoryReadFailure(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(path, nil, 0o600))
	keys, err := ScanExistingFileKeys(logpattern.FileNamePattern{Dir: path, Target: "app", Extension: "log"})
	require.Error(t, err)
	require.Nil(t, keys)
}

func TestScanRegistrationFileKeysKeepsOnlyCurrentPeriodRawAppendable(t *testing.T) {
	t.Parallel()
	pattern := logpattern.FileNamePattern{Dir: t.TempDir(), Target: "app", Extension: "log"}
	for _, name := range []string{
		"app_20260920_1.log.gz", "app_20260920_2.log", "app_20260925_1.log", "app_20260925_1.log.gz",
		"app_20260926_1.log", "app_20260927_1.log",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(pattern.Dir, name), nil, 0o600))
	}

	keys, err := ScanRegistrationFileKeys(pattern, "20260926")
	require.NoError(t, err)
	// 現在の期間以降の元ログだけが追記され得ます。過去の期間の元ログは完結した世代です。
	require.Equal(t, []CheckpointKey{
		{Generation: "20260920-1", Compressed: true},
		{Generation: "20260920-2", Compressed: true},
		{Generation: "20260925-1", Compressed: true},
		{Generation: "20260926-1", Compressed: false},
		{Generation: "20260927-1", Compressed: false},
	}, keys)

	for _, invalid := range []string{"", "2026-09-26", "20260931"} {
		keys, err := ScanRegistrationFileKeys(pattern, invalid)
		require.ErrorIs(t, err, ErrInvalidActivePeriod)
		require.Nil(t, keys)
	}
}

func TestRegisterDestinationsLeavesOnlyCurrentPeriodRawInNewDestinationScope(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		files        []string
		activePeriod string
		wantBoundary string
	}{
		// 現在の期間の最新rawは書き込み中の可能性があるため、新しい宛先の対象に残します。
		{"current period raw", []string{"app_20260925_1.log.gz", "app_20260926_1.log"}, "20260926", "20260925-1"},
		// 過去の期間のrawはwriterが戻って追記しないため、他の登録前の世代と同じく対象外です。
		{"past period raw", []string{"app_20260925_1.log.gz", "app_20260926_1.log"}, "20260927", "20260926-1"},
		{"month old raw only", []string{"app_20260826_3.log"}, "20260926", "20260826-3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pattern := logpattern.FileNamePattern{Dir: t.TempDir(), Target: "app", Extension: "log"}
			for _, name := range tc.files {
				require.NoError(t, os.WriteFile(filepath.Join(pattern.Dir, name), nil, 0o600))
			}
			keys, err := ScanRegistrationFileKeys(pattern, tc.activePeriod)
			require.NoError(t, err)
			store := newCheckpointTestStore(t, "source")
			boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "store.Close()", store.Close, boundedwait.ForceCloserOf(store), boundedwait.NoError)
			ctx := context.Background()
			active := []objectstorage.DestinationID{"primary"}
			require.NoError(t, store.RegisterDestinations(ctx, active, keys))

			cursors, err := store.DestinationCursors(ctx)
			require.NoError(t, err)
			require.Equal(t, []DestinationCursor{{
				DestinationID: "primary", Generation: tc.wantBoundary, OutOfScopeGeneration: tc.wantBoundary,
			}}, cursors)
			for _, key := range keys {
				checkpoint, err := store.Load(ctx, key)
				require.NoError(t, err)
				inScope := CompareGenerations(key.Generation, tc.wantBoundary) > 0
				require.Equal(t, inScope, len(checkpoint.PendingDestinationIDs(active)) == 1, key.Generation)
				// 対象外の世代は送信済みではなく、upload完了を理由にした削除もしません。
				require.False(t, checkpoint.DeliveredToAny(active), key.Generation)
			}
		})
	}
}
