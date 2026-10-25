package fsarchivehandler

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestFinishArchiveUploadOperationCompletionResult(t *testing.T) {
	for _, name := range []string{
		"compressed",
		"raw",
		"without_destinations",
		"pre_canceled",
		"absent_parent",
		"non_regular",
	} {
		t.Run(name, func(t *testing.T) {
			now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
			pipeline := newTestArchiveHandler(t, now, nil)
			handler := pipeline.handler
			category := failurearchive.FailureCategoryRetryable
			tracker := handler.directorySizeTracker(category)
			require.NotNil(t, tracker)
			require.True(t, tracker.TryStartObservation(now, handler.Config, true))
			tracker.FinishObservation(64)
			require.False(t, tracker.TryStartObservation(now, handler.Config, false))

			path := filepath.Join(handler.Fsstore.FileNameRetryable.Dir, "test-app_20261017_1.jsonl.gz")
			compressed := name != "raw"
			if !compressed {
				path = path[:len(path)-len(".gz")]
			}
			payload := []byte("completed archive")
			switch name {
			case "absent_parent":
				// 親directoryも存在しない場合は、削除確認と同期失敗を区別します。
				path = filepath.Join(t.TempDir(), "missing-directory", filepath.Base(path))
			case "non_regular":
				require.NoError(t, os.Mkdir(path, 0o700))
			default:
				require.NoError(t, os.WriteFile(path, payload, 0o600))
			}
			ctx := context.Background()
			if name == "pre_canceled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			destinations := []objectstorage.DestinationID{"destination"}
			if name == "without_destinations" {
				destinations = nil
			}
			var notifications atomic.Int64
			handler.OnError = func(context.Context, error, *logging.LogEntry, observer.Tracer, observer.Meter) {
				notifications.Add(1)
			}

			completion, err := handler.finishArchiveUploadOperation(
				ctx, path, compressed, destinations, category,
			)

			switch name {
			case "compressed":
				require.NoError(t, err)
				require.True(t, completion.Attempted)
				require.NoError(t, completion.Removal.Err)
				require.Equal(t, path, completion.Removal.File.Path)
				require.Equal(t, uint64(len(payload)), completion.Removal.File.Size)
				require.NoFileExists(t, path)
				require.Zero(t, notifications.Load())
			case "raw", "without_destinations":
				require.NoError(t, err)
				require.False(t, completion.Attempted)
				require.NoError(t, completion.Removal.Err)
				require.FileExists(t, path)
				require.Zero(t, notifications.Load())
			case "pre_canceled":
				require.ErrorIs(t, err, context.Canceled)
				require.False(t, completion.Attempted)
				require.NoError(t, completion.Removal.Err)
				require.FileExists(t, path)
				require.EqualValues(t, 1, notifications.Load())
			case "absent_parent":
				require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
				require.True(t, completion.Attempted)
				require.NoError(t, completion.Removal.Err)
				require.Equal(t, path, completion.Removal.File.Path)
				require.NoFileExists(t, path)
				require.EqualValues(t, 1, notifications.Load())
			case "non_regular":
				require.ErrorIs(t, err, infrafsstore.ErrNotRegularFile)
				require.True(t, completion.Attempted)
				require.ErrorIs(t, completion.Removal.Err, infrafsstore.ErrNotRegularFile)
				require.NotErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
				require.DirExists(t, path)
				require.EqualValues(t, 1, notifications.Load())
			}
			// helperは概算値を無効化せず、呼び出し側がAttemptedを判断します。
			require.False(t, tracker.TryStartObservation(now, handler.Config, false),
				"completion helper must preserve the directory usage observation")
		})
	}
}
