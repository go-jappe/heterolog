package compressedlogfailure

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
)

func TestArchiveLifecycleForceCancellationWhileHandoffIsStillProtected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stopContext, stop := context.WithCancel(context.Background())
		defer stop()
		operationContext, forceCancel := context.WithCancel(context.Background())
		defer forceCancel()
		type observedPurge struct {
			ctx context.Context
			err error
		}
		reported := make(chan observedPurge, 1)
		coordinator := &sharedArchiveLifecycle{
			interval:         time.Hour,
			operationContext: operationContext,
			purgeLoopStopped: make(chan struct{}),
			purgeObservers: map[*archiveLifecycleOwner]ArchivePurgeObserver{
				{}: func(ctx context.Context, _ compressedlogarchive.PurgeReport, err error) {
					reported <- observedPurge{ctx: ctx, err: err}
				},
			},
		}
		if err := coordinator.storeOrPurgeMu.RLockContext(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer coordinator.storeOrPurgeMu.RUnlock()
		go coordinator.run(stopContext)
		synctest.Wait()
		select {
		case report := <-reported:
			t.Fatalf("purge bypassed handoff protection: %v", report.err)
		default:
		}
		// 通常の停止要求だけでは、受理済みpurgeのlock待ちを中止しません。
		stop()
		synctest.Wait()
		select {
		case report := <-reported:
			t.Fatalf("normal stop canceled accepted purge: %v", report.err)
		case <-coordinator.purgeLoopStopped:
			t.Fatal("normal stop abandoned accepted purge")
		default:
		}
		// 明示的な強制終了だけが受理済み操作用のcontextを中止します。
		forceCancel()
		<-coordinator.purgeLoopStopped
		report := <-reported
		if report.ctx != operationContext {
			t.Fatal("purge observer did not receive the accepted operation context")
		}
		if !errors.Is(report.err, context.Canceled) {
			t.Fatalf("canceled lifecycle report = %v", report.err)
		}
	})
}
