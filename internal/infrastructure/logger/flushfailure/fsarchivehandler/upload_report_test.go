package fsarchivehandler

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

type incompleteArchiveReportUploader struct {
	destinationIDs []objectstorage.DestinationID
}

func (u incompleteArchiveReportUploader) DestinationIDs() []objectstorage.DestinationID {
	if u.destinationIDs != nil {
		return u.destinationIDs
	}
	return []objectstorage.DestinationID{"first", "second"}
}

func (incompleteArchiveReportUploader) Upload(context.Context, uploader.UploadRequest) error {
	return nil
}

func (incompleteArchiveReportUploader) UploadDestinations(context.Context, uploader.UploadRequest, []objectstorage.DestinationID) uploader.UploadReport {
	return uploader.UploadReport{Results: []uploader.DestinationResult{{DestinationID: "first"}}}
}

// 宛先別の結果が不正なreportからは完了記録を作らず、圧縮fileを残します。
// 集約したUploadのnilではなく宛先別の結果を検証し、同じ検証エラーを一度だけ通知します。
func TestFailureArchiveUploadRejectsInvalidDestinationReport(t *testing.T) {
	for _, testCase := range []struct {
		name           string
		destinationIDs []objectstorage.DestinationID
		sentinel       error
	}{
		{name: "incomplete_report", sentinel: uploader.ErrInvalidUploadReport},
		{
			name:           "duplicate_requested_destinations",
			destinationIDs: []objectstorage.DestinationID{"first", "first"},
			sentinel:       uploader.ErrDuplicateDestinationID,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			category := failurearchive.FailureCategoryRetryable
			var (
				notifiedMu sync.Mutex
				notified   []error
			)
			pipeline := newTestArchiveHandler(t, time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
				cfg.setArchiveUploader(category, incompleteArchiveReportUploader{destinationIDs: testCase.destinationIDs})
			}, InternalLogHandlers{OnError: func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
				notifiedMu.Lock()
				defer notifiedMu.Unlock()
				notified = append(notified, err)
			}})
			f := pipeline.handler
			store := f.compressedArchiveUploadCheckpointStore(category)
			require.NotNil(t, store)
			fileName, err := f.archiveFileNamePattern(category)
			require.NoError(t, err)
			path := filepath.Join(fileName.Dir, "test-app_20261010_1.jsonl.gz")
			require.NoError(t, os.WriteFile(path, []byte("preserve until every destination completes"), 0o600))

			failed := f.retryRetainedArchiveUploads(ctx, category, fileName, archivePathProtection{paths: []string{path}})
			require.True(t, failed)
			require.FileExists(t, path, "aggregate Uploadのnilではなく宛先別結果を検証して削除を判断する")
			notifiedMu.Lock()
			errs := append([]error(nil), notified...)
			notifiedMu.Unlock()
			require.Len(t, errs, 1, "同じ結果の検証エラーを二重通知しない")
			require.ErrorIs(t, errs[0], errInvalidFailureArchiveUploadReport)
			require.ErrorIs(t, errs[0], testCase.sentinel)
			checkpoint, err := store.Load(ctx, failureArchiveCompressedLogCheckpointKey(t, f, category, path))
			require.NoError(t, err)
			require.Empty(t, checkpoint.CompletedDestinationIDs, "不正な結果から完了記録を作らない")
		})
	}
}
