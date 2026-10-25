package logspool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	fsworker "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/worker"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 実ファイルの相対パス順で探索を進め、待機記録が期限切れになっても、
// 分ディレクトリをまたぐ400件の読み込み失敗より後の正常なファイルを送信します。
func TestProcessingScanReachesHealthyFileAfterReadFailuresAndBackoffExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const failedCount = 400
		ctx := context.Background()
		cfg := integrationSpoolConfig{
			spoolPath: resolvedTempDirForTest(t),
			target:    "processing-scan-integration",
			dir:       "test-backend",
		}
		stateStore, err := fsworker.NewStateStore(cfg)
		require.NoError(t, err)
		processingRoot := stateRoot(cfg, pkgspool.StateProcessing)
		firstFileAt := time.Date(2026, 10, 21, 12, 34, 50, 0, time.UTC)
		paths := make([]string, failedCount+1)
		for i := range paths {
			fileAt := firstFileAt.Add(time.Duration(i) * 100 * time.Millisecond)
			name := fmt.Sprintf("%02d%03d_%04d%s", fileAt.Second(), fileAt.Nanosecond()/int(time.Millisecond), i, spooldir.SpoolExt)
			paths[i] = filepath.Join(processingRoot, filepath.FromSlash(fileAt.Format("2006/01/02/15/04")), name)
			require.NoError(t, os.MkdirAll(filepath.Dir(paths[i]), 0o755))
			record := integrationSpoolRecord(t, fileAt, fmt.Sprintf("record-%04d", i), "processing scan integration")
			body, err := json.Marshal(record)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(paths[i], append(body, '\n'), 0o600))
		}
		require.NotEqual(t, filepath.Dir(paths[0]), filepath.Dir(paths[failedCount]))

		readErr := errors.New("persistent processing read failure")
		store := &processingScanReadFailureStore{
			StateStore:  stateStore,
			healthyPath: paths[failedCount],
			readErr:     readErr,
		}
		sender := &recordingSuccessSender{now: firstFileAt.Add(time.Minute)}
		spoolWorker, err := worker.New(store, &spoolActiveStoreStub{}, sender)
		require.NoError(t, err)
		startedAt := time.Now()
		for _, path := range paths[:failedCount] {
			report, err := spoolWorker.RunOnceSpoolFileProcess(ctx)
			require.ErrorIs(t, err, readErr)
			require.Equal(t, worker.ProcessStatusFailed, report.Status)
			require.Equal(t, path, report.ProcessingPath)
			time.Sleep(100 * time.Millisecond)
		}
		require.Greater(t, time.Since(startedAt), 30*time.Second)
		assert.False(t, spoolWorker.ProcessingPathBackoff.ShouldSkipPath(paths[0], time.Now()))
		require.Empty(t, sender.records)

		report, err := spoolWorker.RunOnceSpoolFileProcess(ctx)
		require.NoError(t, err)
		assert.Equal(t, worker.ProcessStatusCompleted, report.Status)
		assert.Equal(t, paths[failedCount], report.ProcessingPath)
		assert.Equal(t, 1, report.SuccessRecordCount)
		require.Len(t, sender.records, 1)
		assert.Equal(t, "record-0400", sender.records[0].RequestID)
		_, err = os.Stat(paths[failedCount])
		assert.ErrorIs(t, err, os.ErrNotExist)

		doneRecords := readIntegrationSpoolRecords(t, stateRoot(cfg, pkgspool.StateDone))
		require.Len(t, doneRecords, 1)
		assert.Equal(t, sender.records[0].RequestID, doneRecords[0].RequestID)
		assert.False(t, doneRecords[0].FinalizedAt.IsZero())
		remaining, err := stateStore.ListProcessingFileCandidatesAfter(ctx, "", failedCount+1)
		require.NoError(t, err)
		assert.Len(t, remaining, failedCount)
	})
}

type processingScanReadFailureStore struct {
	*fsworker.StateStore
	healthyPath string
	readErr     error
}

func (s *processingScanReadFailureStore) ReadProcessingRecords(ctx context.Context, path string) ([]pkgspool.SpoolRecord, error) {
	if path != s.healthyPath {
		return nil, s.readErr
	}
	return s.StateStore.ReadProcessingRecords(ctx, path)
}
