package compressedlogarchive

import (
	"context"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"

	"github.com/stretchr/testify/require"
)

func TestPurgerCancellationWhileWaitingForStoreLocks(t *testing.T) {
	for _, lockName := range []string{"operation", "layout"} {
		t.Run(lockName, func(t *testing.T) {
			store := newTestStore(t, pkglogger.LoggerContractSync)
			source := writeTestCompressedLog(t, "app_20260916_1.log.gz", []byte("retained file"))
			file, err := store.Save(context.Background(), testSaveInput(source, uploader.FailureCategoryRetryable))
			require.NoError(t, err)
			purger := newTestPurger(t, store, PurgeConfig{Retryable: enabledArchivePurgeConfig(1, 1)})
			synctest.Test(t, func(t *testing.T) {
				var held sync.Locker = &store.operationMu
				if lockName == "layout" {
					held = &store.directorySetupMu
				}
				held.Lock()
				defer held.Unlock()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				report, err := purger.Run(ctx)
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.True(t, report.ByFailureCategory[uploader.FailureCategoryRetryable].Enabled)
			})
			data, err := os.ReadFile(file.Path)
			require.NoError(t, err)
			require.Equal(t, "retained file", string(data))
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err = purger.Run(ctx)
			require.NoError(t, err, "cancellation must release the outer operation lock")
		})
	}
}
