package fsarchivehandler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestFailureArchiveDestinationRegistrationPreservesInitialScopeAcrossRestart(t *testing.T) {
	for _, category := range []failurearchive.FailureCategory{failurearchive.FailureCategoryRetryable, failurearchive.FailureCategoryPermanent} {
		t.Run(string(category), func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
			checkpointCfg := pkgfsstore.ArchiveCheckpointConfig{Directory: t.TempDir(), SourcePrefix: "scope", AutoRecoverMissingCheckpoint: true}
			// upload無効で構築し、宛先の登録前から存在する世代を用意します。
			initial := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
				cfg.archiveCheckpoint = checkpointCfg
			})
			pattern, err := initial.handler.archiveFileNamePattern(category)
			require.NoError(t, err)
			old := filepath.Join(pattern.Dir, "test-app_20260920_1.jsonl.gz")
			raw := filepath.Join(pattern.Dir, "test-app_20260919_1.jsonl")
			require.NoError(t, os.WriteFile(old, []byte("old compressed"), 0o600))
			require.NoError(t, os.WriteFile(raw, []byte("old raw"), 0o600))
			// uploadを有効にして再起動し、構築時に宛先と既存の世代を登録します。
			remote := newDestinationFailureArchiveUploader("archive")
			pipeline := restartTestArchiveHandler(t, initial, now, func(cfg *testArchiveConfig) {
				cfg.setArchiveUploader(category, remote)
			})
			handler := pipeline.handler
			// 登録前から存在したarchiveは新しい宛先へ送信せず、upload完了を理由に削除もしません。
			outcome := handler.uploadCheckpointedArchiveGroup(ctx, []string{old}, category, pattern)
			require.False(t, outcome.retryRequired)
			require.False(t, outcome.completion.Attempted)
			require.Zero(t, remote.totalAttemptCount(old))
			require.FileExists(t, old)

			compressedRaw := raw + ".gz"
			require.NoError(t, os.WriteFile(compressedRaw, []byte("compressed after addition"), 0o600))
			require.NoError(t, os.Remove(raw))
			newPath := filepath.Join(pattern.Dir, "test-app_20260921_1.jsonl.gz")
			require.NoError(t, os.WriteFile(newPath, []byte("new compressed"), 0o600))
			// upload無効で再起動するとStoreを作らず、同じ宛先で再び有効にした時点の登録も
			// 最初の登録範囲を保ちます。
			disabled := restartTestArchiveHandler(t, pipeline, now, nil)
			require.Nil(t, disabled.handler.compressedArchiveUploadCheckpointStore(category))
			pipeline = restartTestArchiveHandler(t, disabled, now, func(cfg *testArchiveConfig) {
				cfg.setArchiveUploader(category, remote)
			})
			handler = pipeline.handler
			for _, path := range []string{newPath} {
				outcome = handler.uploadCheckpointedArchiveGroup(ctx, []string{path}, category, pattern)
				require.False(t, outcome.retryRequired)
				require.Equal(t, 1, remote.attemptCount(path, "archive"))
				require.NoFileExists(t, path)
			}
			// 登録時に対象外だったraw世代は、登録後に圧縮しても送信・削除しません。
			outcome = handler.uploadCheckpointedArchiveGroup(ctx, []string{compressedRaw}, category, pattern)
			require.False(t, outcome.retryRequired)
			require.False(t, outcome.completion.Attempted)
			require.Zero(t, remote.totalAttemptCount(compressedRaw))
			require.FileExists(t, compressedRaw)
			// 送信も削除もしない世代はupload待ちとして保護せず、通常の保持・purgeへ任せます。
			observation := infrafsstore.GroupFilesByCompressionSource(infrafsstore.InspectFilePaths([]string{old, compressedRaw}), pattern.Extension)
			protection, err := handler.retainArchivePathsForUpload(ctx, category, pattern, observation)
			require.NoError(t, err)
			require.Empty(t, protection.paths, "登録前の世代をupload待ちとしてretainしない")
			require.FileExists(t, old)
			require.FileExists(t, compressedRaw)
		})
	}
}

func TestFailureArchiveInitiallyOutOfScopeArchiveUsesNormalAgeRetention(t *testing.T) {
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	// upload無効で構築し、宛先の登録前から存在する世代を用意します。
	initial := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.maxFileAge = 30
		cfg.archiveCheckpoint = pkgfsstore.ArchiveCheckpointConfig{Directory: t.TempDir(), SourcePrefix: "scope"}
	})
	category := failurearchive.FailureCategoryRetryable
	pattern := initial.handler.Fsstore.FileNameRetryable
	expired := filepath.Join(pattern.Dir, "test-app_20260801_1.jsonl.gz")
	retained := filepath.Join(pattern.Dir, "test-app_20260926_1.jsonl.gz")
	for _, path := range []string{expired, retained} {
		require.NoError(t, os.WriteFile(path, []byte("old compressed"), 0o600))
	}
	remote := newDestinationFailureArchiveUploader("new")
	pipeline := restartTestArchiveHandler(t, initial, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(category, remote)
	})
	handler := pipeline.handler
	require.NoError(t, triggerFailureArchiveCleanupForFailureCategory(handler, category))
	// uploadの初回target-only観測に続く通常観測でretentionを実行します。
	require.NoError(t, triggerFailureArchiveCleanupForFailureCategory(handler, category))
	// 登録前のarchiveは送信せず、upload完了では削除しません。保持期間を過ぎたfileだけを
	// 通常のage purgeで削除し、期間内のfileは残します。
	require.NoFileExists(t, expired)
	require.FileExists(t, retained)
	require.Zero(t, remote.totalAttemptCount(expired))
	require.Zero(t, remote.totalAttemptCount(retained))
}

// 失敗した宛先の再送は再開処理の間隔で行うため、仮想時間で待ちます。
func TestFailureArchiveNewDestinationPreservesExistingDestinationRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
		category := failurearchive.FailureCategoryRetryable
		// 既存の宛先だけで構築して登録してから、送信前の世代を用意します。
		initial := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.archiveCheckpoint = pkgfsstore.ArchiveCheckpointConfig{Directory: t.TempDir(), SourcePrefix: "scope"}
			cfg.setArchiveUploader(category, newDestinationFailureArchiveUploader("existing"))
		})
		pattern := initial.handler.Fsstore.FileNameRetryable
		path := filepath.Join(pattern.Dir, "test-app_20260920_1.jsonl.gz")
		require.NoError(t, os.WriteFile(path, []byte("pending compressed"), 0o600))
		// 宛先を追加して再起動します。追加した宛先にとって、この世代は登録前から存在した世代です。
		remote := newDestinationFailureArchiveUploader("existing", "new")
		remote.failNext("existing", 1)
		pipeline := restartTestArchiveHandler(t, initial, now, func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(category, remote)
		})
		handler := pipeline.handler
		outcome := handler.uploadCheckpointedArchiveGroup(ctx, []string{path}, category, pattern)
		require.True(t, outcome.retryRequired)
		require.FileExists(t, path)
		require.Equal(t, 1, remote.attemptCount(path, "existing"))
		require.Zero(t, remote.attemptCount(path, "new"))
		drainArchiveRecoveryForTest(t, handler)
		outcome = handler.uploadCheckpointedArchiveGroup(ctx, []string{path}, category, pattern)
		require.False(t, outcome.retryRequired)
		require.NoFileExists(t, path)
		require.Equal(t, 2, remote.attemptCount(path, "existing"))
		require.Zero(t, remote.attemptCount(path, "new"))
	})
}
