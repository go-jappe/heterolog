package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestInlinePendingSourceCleanupContinuesPurgeWithoutAnotherScan(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
	codec := &inlineRecoveryCompressionCodec{}
	logger := newInlineCompressionRecoveryTestLogger(t, now, "pending-only-observation", codec)
	fileName := logger.Fsstore.FileName
	stem := filepath.Join(fileName.Dir, fileName.Prefix+fileName.Target+"_20260913_")
	pendingPath := logpattern.CompressionSourceDeletePendingPath(stem + "1.log")
	require.NoError(t, os.WriteFile(pendingPath, []byte("retired source"), 0o600))
	old := now.AddDate(0, 0, -logger.inlinePurge.config.MaxFileAge()-1)
	oldCompressedPath := filepath.Join(fileName.Dir, fileName.Prefix+fileName.Target+"_"+old.Format("20060102")+"_2.log.gz")
	writeGzipFile(t, oldCompressedPath, "expired archive")
	require.NoError(t, os.Chtimes(oldCompressedPath, old, old))
	retainedCompressedPath := stem + "3.log.gz"
	writeGzipFile(t, retainedCompressedPath, "retained archive")
	require.NoError(t, os.Chtimes(retainedCompressedPath, now, now))
	scans := observeCleanupScans(logger)

	// 削除待ちは通常ログの容量・purge候補に含まれません。その片付け後も同じ観測を
	// 使って保持期限切れfileを削除し、残存fileから使用量の観測を完了できます。
	require.NoError(t, purgeFileLoggerForTest(ctx, logger))
	require.NoFileExists(t, pendingPath)
	require.NoFileExists(t, oldCompressedPath)
	require.FileExists(t, retainedCompressedPath)
	require.EqualValues(t, 1, scans.all.Load())
	require.Zero(t, codec.compressCalls.Load())
	require.Zero(t, codec.verifyCalls.Load())

	// pendingだけの削除を理由にcacheを無効化せず、TTL内の通常要求では再走査しません。
	for _, at := range []time.Time{now, now.Add(logger.inlinePurge.config.UsageCacheTTL() - time.Second)} {
		logger.Clock = pkgclock.NewFixedClock(at)
		require.NoError(t, purgeFileLoggerForTest(ctx, logger))
	}
	require.EqualValues(t, 1, scans.all.Load())
	require.EqualValues(t, 1, scans.reads.Load())
	require.Zero(t, scans.byTarget.Load())
}

func TestInlineFullScanSharesObservedTypesAndNamesThroughRecoveryUploadAndPurge(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
	codec := &inlineRecoveryCompressionCodec{}
	logger := newInlineCompressionRecoveryTestLogger(t, now, "one-file-observation", codec)
	logger.archiveUploadCheckpoints = newCompressedLogCheckpointTestStore(t, t.TempDir(), "one_observation")
	remote := &recordingArchiveUploader{}
	logger.ArchiveUploader = newMultiArchiveUploader(t, uploader.Destination{ID: "primary", Uploader: remote})
	fileName := logger.Fsstore.FileName
	fileName.Dir = filepath.Join(t.TempDir(), "snapshot-directory")
	raw := filepath.Join(fileName.Dir, fileName.Target+"_20260913_1.log")
	rawPeer := raw + ".zst"
	uploadRaw := filepath.Join(fileName.Dir, fileName.Target+"_20260913_2.log")
	uploadPeer := uploadRaw + ".gz"
	observation := fsstore.FileInspection{
		Paths: []string{raw, rawPeer, uploadPeer},
		RegularFiles: []fsstore.FileInfo{
			{Path: raw, Size: 12, ModTime: now},
			{Path: rawPeer, Size: 8, ModTime: now},
			{Path: uploadPeer, Size: 9, ModTime: now},
		},
	}
	// The scanner owns the filesystem inspection. A detached snapshot makes any
	// subsequent ReadDir/Lstat observable: its directory and paths do not exist.
	var scans int
	logger.inlinePurge.fileScanner = &fakePurgeFileScanner{observe: func(
		pattern logpattern.FileNamePattern, mode logpattern.GlobMode,
	) (fsstore.FileInspection, error) {
		scans++
		require.Equal(t, fileName, pattern)
		require.Equal(t, logpattern.GlobAllFiles, mode)
		return observation, nil
	}}
	tracker := logger.inlinePurge.directorySizeTracker
	require.True(t, tracker.TryStartObservation(now, logger.inlinePurge.config, true))
	owner := appendlog.NewDirectoryGuardToken()
	var protection []string
	var pending []archiveUploadGroup
	defer func() { logger.lifecycleGuard.UnprotectPaths(owner, protection...) }()
	require.NoError(t, logger.lifecycleGuard.RunWithLifecycleLock(func() error {
		return logger.purgeLogsWithinGuard(ctx, fileName, inlinePurgePlan{
			cleanupTriggeredAt: now, agePurgeReferenceDate: now,
			directorySizeScanAcquired: true, deferredRawRecoveryPath: raw,
		}, owner, &protection, &pending, runNotificationImmediately)
	}))
	require.Equal(t, 1, scans)
	require.ElementsMatch(t, []string{raw, rawPeer, uploadRaw, uploadPeer}, protection)
	require.Equal(t, []string{uploadPeer}, archiveUploadGroupPathsForTest(pending))
	require.False(t, tracker.TryStartObservation(now, logger.inlinePurge.config, false),
		"a mutation-free observation must finish the directory baseline")
	require.Zero(t, codec.compressCalls.Load())
	require.Zero(t, codec.verifyCalls.Load())
	require.Empty(t, remote.inputsSnapshot())
}

func TestInlinePendingUploadUsesTypedRegularFilesAndKeepsObservedRawNames(t *testing.T) {
	now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
	logger := newInlineCompressionRecoveryTestLogger(t, now, "typed-upload-candidates", &inlineRecoveryCompressionCodec{})
	logger.ArchiveUploader = newSingleArchiveUploader(t, &recordingArchiveUploader{})
	fileName := logger.Fsstore.FileName
	// 実在しない保存先の観測を渡し、候補選別のために再走査しないことも確認します。
	fileName.Dir = filepath.Join(t.TempDir(), "observed-only")
	raw := filepath.Join(fileName.Dir, fileName.Target+"_20260913_1.log")
	first, otherFormat := raw+".zst", raw+".gz"
	unretiredRaw := filepath.Join(fileName.Dir, fileName.Target+"_20260913_2.log")
	unretiredPeer := unretiredRaw + ".gz"
	nonCanonical := filepath.Join(fileName.Dir, fileName.Target+"_20260913_invalid.log.gz")
	unrelated := filepath.Join(fileName.Dir, "unrelated_20260913_1.log.gz")
	observation := fsstore.GroupFilesByCompressionSource(fsstore.FileInspection{
		Paths: []string{first, otherFormat, unretiredRaw, unretiredPeer, nonCanonical, unrelated},
		RegularFiles: []fsstore.FileInfo{
			{Path: first}, {Path: otherFormat}, {Path: unretiredPeer}, {Path: unrelated},
		},
		PathErrors: map[string]error{unretiredRaw: os.ErrNotExist, nonCanonical: os.ErrPermission},
	}, fileName.Extension)
	owner := appendlog.NewDirectoryGuardToken()
	var protection []string
	defer func() { logger.lifecycleGuard.UnprotectPaths(owner, protection...) }()
	require.NoError(t, logger.lifecycleGuard.RunWithLifecycleLock(func() error {
		pending, err := logger.retainRemainingLogFilesForOwner(context.Background(), observation, fileName, owner, &protection)
		require.Equal(t, []string{first}, archiveUploadGroupPathsForTest(pending),
			"最初の通常圧縮ファイルだけを選び、raw名の残る世代や対象外の名前は送信しない")
		return err
	}))
	require.ElementsMatch(t, []string{raw, first}, protection)
}

func TestInlinePendingUploadKeepsCandidatesWhenCheckpointReadFails(t *testing.T) {
	for _, test := range []struct {
		name   string
		suffix string
	}{
		{name: "raw checkpoint read failure"},
		{name: "compressed checkpoint read failure", suffix: ".gz"},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
			logger := newInlineUsageTestLogger(t, now, "checkpoint-selection")
			store := &failingCompressedLogCheckpointStore{err: errors.New("checkpoint temporarily unreadable")}
			logger.archiveUploadCheckpoints = store
			remote := &recordingArchiveUploader{}
			logger.ArchiveUploader = newSingleArchiveUploader(t, remote)
			fileName := logger.Fsstore.FileName
			fileName.Dir = filepath.Join(t.TempDir(), "observed-only")
			raw := filepath.Join(fileName.Dir, fileName.Prefix+fileName.Target+"_20260913_1.log")
			path := raw + test.suffix
			observation := fsstore.GroupFilesByCompressionSource(fsstore.FileInspection{
				Paths:        []string{path},
				RegularFiles: []fsstore.FileInfo{{Path: path}},
			}, fileName.Extension)
			owner := appendlog.NewDirectoryGuardToken()
			var protection []string
			var pending []archiveUploadGroup
			defer func() { logger.lifecycleGuard.UnprotectPaths(owner, protection...) }()
			require.NoError(t, logger.lifecycleGuard.RunWithLifecycleLock(func() error {
				var err error
				pending, err = logger.retainRemainingLogFilesForOwner(context.Background(), observation, fileName, owner, &protection)
				return err
			}))
			// 完了情報を読めない場合は、送信済みか登録前の対象外かを確認できないため、
			// 未圧縮・圧縮済みのどちらも保護を維持して後段で再確認します。
			require.Equal(t, []string{path}, archiveUploadGroupPathsForTest(pending),
				"完了情報を読めない候補は、送信済みか対象外かを確認できるまで保護を維持する")
			require.Contains(t, protection, raw)
			require.Contains(t, protection, path)
			logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
				require.Contains(t, protected, raw)
				require.Contains(t, protected, path)
			})
			require.Equal(t, 1, store.loadCalls)
			require.Empty(t, remote.inputsSnapshot())
		})
	}
}

func TestInlinePendingUploadDoesNotDropSourceInspectionErrors(t *testing.T) {
	now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
	logger := newInlineCompressionRecoveryTestLogger(t, now, "typed-upload-inspection-error", &inlineRecoveryCompressionCodec{})
	logger.ArchiveUploader = newSingleArchiveUploader(t, &recordingArchiveUploader{})
	fileName := logger.Fsstore.FileName
	raw := filepath.Join(fileName.Dir, fileName.Target+"_20260913_1.log")
	peer, rejectedPeer := raw+".gz", raw+".zst"
	observation := fsstore.GroupFilesByCompressionSource(fsstore.FileInspection{
		Paths:        []string{peer, rejectedPeer},
		RegularFiles: []fsstore.FileInfo{{Path: peer}},
		PathErrors:   map[string]error{rejectedPeer: os.ErrPermission},
	}, fileName.Extension)
	owner := appendlog.NewDirectoryGuardToken()
	var protection []string
	defer func() { logger.lifecycleGuard.UnprotectPaths(owner, protection...) }()
	err := logger.lifecycleGuard.RunWithLifecycleLock(func() error {
		pending, err := logger.retainRemainingLogFilesForOwner(context.Background(), observation, fileName, owner, &protection)
		require.Empty(t, pending)
		return err
	})
	require.ErrorIs(t, err, os.ErrPermission,
		"通常ファイルが先に見つかっても、同じ元ログに対応する検査エラーを無視しない")
	require.Empty(t, protection)
}

func TestInlineCheckpointCursorSurvivesRejectedFileInspection(t *testing.T) {
	for _, pathErr := range []error{os.ErrNotExist, os.ErrPermission} {
		t.Run(pathErr.Error(), func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
			logger := newInlineCompressionRecoveryTestLogger(t, now, "rejected-observed-name", &inlineRecoveryCompressionCodec{})
			store := newCompressedLogCheckpointTestStore(t, t.TempDir(), "rejected_names")
			logger.archiveUploadCheckpoints = store
			fileName := logger.Fsstore.FileName
			raw := filepath.Join(fileName.Dir, fileName.Target+"_20260913_1.log")
			generation, err := checkpointstore.ArchiveCheckpointGenerationKeyForPath(raw, fileName)
			require.NoError(t, err)
			key := checkpointstore.CheckpointKey{Generation: generation, Compressed: true}
			_, err = store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"primary"})
			require.NoError(t, err)
			observation := fsstore.FileInspection{
				Paths:      []string{raw},
				PathErrors: map[string]error{raw: &os.PathError{Op: "lstat", Path: raw, Err: pathErr}},
			}
			owner := appendlog.NewDirectoryGuardToken()
			var protection []string
			require.NoError(t, logger.lifecycleGuard.RunWithLifecycleLock(func() error {
				pending, err := logger.retainRemainingLogFilesForOwner(ctx, fsstore.GroupFilesByCompressionSource(observation, fileName.Extension), fileName, owner, &protection)
				require.Empty(t, pending)
				return err
			}))
			logger.lifecycleGuard.UnprotectPaths(owner, protection...)
			checkpoint, err := store.Load(ctx, key)
			require.NoError(t, err)
			require.Equal(t, []objectstorage.DestinationID{"primary"}, checkpoint.CompletedDestinationIDs)
		})
	}
}
