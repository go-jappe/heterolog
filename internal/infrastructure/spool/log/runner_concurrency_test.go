package logspool

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle"
	lifecyclereport "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle/report"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/require"
)

func TestRunnerRunOnceSerializesCompleteCyclesAndCancelsWaitingCalls(t *testing.T) {
	for _, phase := range []string{"lifecycle", "transition retry", "send", "finalization"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				entered := make(chan struct{})
				release := make(chan struct{})
				var enterOnce, releaseOnce sync.Once
				t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
				block := func(ctx context.Context) error {
					enterOnce.Do(func() { close(entered) })
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				cycles := make(chan time.Time, 4)
				lifecycleStore := &observedRunnerLifecycleStore{started: cycles}
				workerStore := &observedRunnerWorkerStore{spoolWorkerStoreStub: &spoolWorkerStoreStub{}}
				sender := &spoolSenderStub{report: newSpoolExportReport(pkgspool.RecordOutcomeStatusSuccess, 1, time.Now())}
				spoolWorker := &worker.Worker{
					Store:             workerStore,
					ActiveSpoolWriter: &spoolActiveStoreStub{},
					Sender:            sender,
				}
				switch phase {
				case "lifecycle":
					lifecycleStore.beforeDone = block
				case "transition retry":
					retry, err := worker.NewProcessingFileTransitionRetry(worker.NewProcessingFileTransitionRetryConfigAdapter(worker.ProcessingFileTransitionRetryConfig{
						RetryDelay: time.Nanosecond,
						MaxEntries: 1,
					}))
					require.NoError(t, err)
					retry.Upsert("processing/retry", worker.ProcessingFileTransitionRetryOpMoveProcessingToDone, errors.New("previous move failed"), time.Now().Add(-time.Second))
					spoolWorker.ProcessingFileTransitionRetry = retry
					workerStore.beforeMoveDone = block
				case "send", "finalization":
					workerStore.processingPaths = []string{"processing/record"}
					workerStore.readRecords = map[string][]pkgspool.SpoolRecord{"processing/record": {spoolRecord("record")}}
					if phase == "send" {
						spoolWorker.Sender = runnerSenderFunc(func(ctx context.Context, records []pkgspool.SpoolRecord) (pkgspool.ExportReport, error) {
							if err := block(ctx); err != nil {
								return nil, err
							}
							return sender.Send(ctx, records)
						})
					} else {
						workerStore.beforeRemove = block
					}
				}
				runner := newTestRunner(t, spoolWorker, &lifecycle.Manager{Store: lifecycleStore}, nil)
				firstDone := runOnceForTest(runner, context.Background())
				select {
				case <-entered:
				case <-time.After(time.Minute):
					t.Fatal("first cycle did not reach blocked phase")
				}
				<-cycles
				secondDone := runOnceForTest(runner, context.Background())
				// 先行する周期が終わるまで、競合するRunOnceはlifecycleへ入りません。
				synctest.Wait()
				select {
				case <-cycles:
					t.Fatal("competing RunOnce entered lifecycle before the preceding cycle completed")
				default:
				}

				ctx, cancel := context.WithCancel(context.Background())
				canceledDone := runOnceForTest(runner, ctx)
				// 待機中の呼び出しを中止します。
				synctest.Wait()
				cancel()
				require.ErrorIs(t, waitRunnerResult(t, canceledDone), context.Canceled)
				require.Empty(t, cycles, "canceled waiting call must not enter lifecycle")
				releaseOnce.Do(func() { close(release) })
				require.NoError(t, waitRunnerResult(t, firstDone))
				require.NoError(t, waitRunnerResult(t, secondDone))
				require.Len(t, cycles, 1, "only the accepted competing cycle should run")
				if phase == "send" || phase == "finalization" {
					require.Equal(t, 1, sender.calls)
				}
			})
		})
	}
}

func TestRunnerRunWaitsForIntervalAfterLongCycleCompletes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const interval = 40 * time.Millisecond
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		cycles := make(chan time.Time, 4)
		release := make(chan struct{})
		var releaseOnce sync.Once
		t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
		var firstCycle sync.Once
		lifecycleStore := &observedRunnerLifecycleStore{
			started: cycles,
			beforeDone: func(ctx context.Context) error {
				var err error
				firstCycle.Do(func() {
					select {
					case <-release:
					case <-ctx.Done():
						err = ctx.Err()
					}
				})
				return err
			},
		}
		runner := newTestRunner(t,
			newTestWorker(&spoolWorkerStoreStub{}, &spoolSenderStub{}),
			&lifecycle.Manager{Store: lifecycleStore},
			runnerConfigStub{interval: interval},
		)
		done := make(chan error, 1)
		go func() { done <- runner.Run(ctx) }()
		select {
		case <-cycles:
		case <-time.After(time.Minute):
			t.Fatal("first cycle did not start")
		}
		// Keep work active beyond an interval so a ticker would accumulate a tick.
		time.Sleep(2 * interval)
		synctest.Wait()
		select {
		case <-cycles:
			t.Fatal("cycles overlapped")
		default:
		}
		completedAfter := time.Now()
		releaseOnce.Do(func() { close(release) })
		select {
		case started := <-cycles:
			require.GreaterOrEqual(t, started.Sub(completedAfter), interval)
		case <-time.After(time.Minute):
			t.Fatal("second cycle did not start")
		}
		cancel()
		require.ErrorIs(t, waitRunnerResult(t, done), context.Canceled)
	})
}

type observedRunnerLifecycleStore struct {
	spoolLifecycleStoreStub
	started    chan<- time.Time
	beforeDone func(context.Context) error
}

func (s *observedRunnerLifecycleStore) PurgeDoneIfNeed(ctx context.Context) (lifecyclereport.PurgeReport, error) {
	s.started <- time.Now()
	if s.beforeDone != nil {
		if err := s.beforeDone(ctx); err != nil {
			return lifecyclereport.PurgeReport{}, err
		}
	}
	return s.spoolLifecycleStoreStub.PurgeDoneIfNeed(ctx)
}

type observedRunnerWorkerStore struct {
	*spoolWorkerStoreStub
	beforeMoveDone func(context.Context) error
	beforeRemove   func(context.Context) error
}

func (s *observedRunnerWorkerStore) MoveProcessingToDone(ctx context.Context, path string) error {
	if s.beforeMoveDone != nil {
		if err := s.beforeMoveDone(ctx); err != nil {
			return err
		}
	}
	return s.spoolWorkerStoreStub.MoveProcessingToDone(ctx, path)
}

func (s *observedRunnerWorkerStore) RemoveProcessingFile(ctx context.Context, path string) error {
	if s.beforeRemove != nil {
		if err := s.beforeRemove(ctx); err != nil {
			return err
		}
	}
	s.processingPaths = nil
	return s.spoolWorkerStoreStub.RemoveProcessingFile(ctx, path)
}

type runnerSenderFunc func(context.Context, []pkgspool.SpoolRecord) (pkgspool.ExportReport, error)

func (f runnerSenderFunc) Send(ctx context.Context, records []pkgspool.SpoolRecord) (pkgspool.ExportReport, error) {
	return f(ctx, records)
}

func runOnceForTest(runner *Runner, ctx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := runner.RunOnce(ctx)
		done <- err
	}()
	return done
}

func waitRunnerResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(hangDetectionTimeout):
		t.Fatal("runner operation did not complete")
		return nil
	}
}
