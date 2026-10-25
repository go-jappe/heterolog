package lifecycle

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle/report"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type lifecycleStoreStub struct {
	calls    []string
	contexts []context.Context

	doneReport       report.PurgeReport
	deadReport       report.PurgeReport
	quarantineReport report.PurgeReport

	doneErr       error
	deadErr       error
	quarantineErr error
}

var _ Store = (*lifecycleStoreStub)(nil)

func (s *lifecycleStoreStub) PurgeDoneIfNeed(ctx context.Context) (report.PurgeReport, error) {
	s.calls = append(s.calls, "done")
	s.contexts = append(s.contexts, ctx)

	return s.doneReport, s.doneErr
}

func (s *lifecycleStoreStub) PurgeDeadIfNeed(ctx context.Context) (report.PurgeReport, error) {
	s.calls = append(s.calls, "dead")
	s.contexts = append(s.contexts, ctx)

	return s.deadReport, s.deadErr
}

func (s *lifecycleStoreStub) PurgeQuarantineIfNeed(ctx context.Context) (report.PurgeReport, error) {
	s.calls = append(s.calls, "quarantine")
	s.contexts = append(s.contexts, ctx)

	return s.quarantineReport, s.quarantineErr
}

func TestNewManagerRejectsNilStore(t *testing.T) {
	t.Parallel()

	manager, err := NewManager(nil, nil, nil)

	require.ErrorIs(t, err, ErrNilStore)
	assert.Nil(t, manager)
}

func TestNewManagerKeepsStore(t *testing.T) {
	t.Parallel()

	store := &lifecycleStoreStub{}

	manager, err := NewManager(store, nil, nil)

	require.NoError(t, err)
	require.NotNil(t, manager)
	assert.Same(t, store, manager.Store)
}

func TestManagerRunOnceRunsAllPurgesInOrder(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	doneReport := purgeReportWithDeletedFiles(1)
	deadReport := purgeReportWithDeletedFiles(2)
	quarantineReport := purgeReportWithDeletedFiles(3)
	store := &lifecycleStoreStub{
		doneReport:       doneReport,
		deadReport:       deadReport,
		quarantineReport: quarantineReport,
	}
	manager := &Manager{Store: store}

	actual, err := manager.RunOnce(ctx)

	require.NoError(t, err)
	assert.Equal(t, report.RunReport{
		DonePurgeReport:       doneReport,
		DeadPurgeReport:       deadReport,
		QuarantinePurgeReport: quarantineReport,
	}, actual)
	assert.Equal(t, []string{"done", "dead", "quarantine"}, store.calls)
	assertSameContexts(t, ctx, store.contexts)
}

func TestManagerRunOnceJoinsNonContextErrorsAfterAllPurges(t *testing.T) {
	t.Parallel()

	doneErr := errors.New("done failed")
	deadErr := errors.New("dead failed")
	quarantineErr := errors.New("quarantine failed")
	doneReport := purgeReportWithDeletedFiles(1)
	deadReport := purgeReportWithDeletedFiles(2)
	quarantineReport := purgeReportWithDeletedFiles(3)
	store := &lifecycleStoreStub{
		doneReport:       doneReport,
		deadReport:       deadReport,
		quarantineReport: quarantineReport,
		doneErr:          doneErr,
		deadErr:          deadErr,
		quarantineErr:    quarantineErr,
	}
	manager := &Manager{Store: store}

	actual, err := manager.RunOnce(context.Background())

	require.Error(t, err)
	assert.ErrorIs(t, err, doneErr)
	assert.ErrorIs(t, err, deadErr)
	assert.ErrorIs(t, err, quarantineErr)
	// 失敗をstateごとに区別し、それぞれの原因を保持します。
	assert.Equal(t, map[pkgspool.State]error{
		pkgspool.StateDone:       doneErr,
		pkgspool.StateDead:       deadErr,
		pkgspool.StateQuarantine: quarantineErr,
	}, purgeErrorsByState(t, err))
	assert.Equal(t, report.RunReport{
		DonePurgeReport:       doneReport,
		DeadPurgeReport:       deadReport,
		QuarantinePurgeReport: quarantineReport,
	}, actual)
	assert.Equal(t, []string{"done", "dead", "quarantine"}, store.calls)
}

func TestManagerRunOnceStopsOnContextError(t *testing.T) {
	t.Parallel()

	doneReport := purgeReportWithDeletedFiles(1)
	deadReport := purgeReportWithDeletedFiles(2)
	quarantineReport := purgeReportWithDeletedFiles(3)

	tests := []struct {
		name             string
		store            lifecycleStoreStub
		wantReport       report.RunReport
		wantErr          error
		wantState        pkgspool.State
		wantCalls        []string
		wantContextCalls int
	}{
		{
			name: "done canceled",
			store: lifecycleStoreStub{
				doneReport: doneReport,
				doneErr:    context.Canceled,
			},
			wantReport: report.RunReport{
				DonePurgeReport: doneReport,
			},
			wantErr:          context.Canceled,
			wantState:        pkgspool.StateDone,
			wantCalls:        []string{"done"},
			wantContextCalls: 1,
		},
		{
			name: "dead deadline exceeded",
			store: lifecycleStoreStub{
				doneReport: doneReport,
				deadReport: deadReport,
				deadErr:    context.DeadlineExceeded,
			},
			wantReport: report.RunReport{
				DonePurgeReport: doneReport,
				DeadPurgeReport: deadReport,
			},
			wantErr:          context.DeadlineExceeded,
			wantState:        pkgspool.StateDead,
			wantCalls:        []string{"done", "dead"},
			wantContextCalls: 2,
		},
		{
			name: "quarantine canceled",
			store: lifecycleStoreStub{
				doneReport:       doneReport,
				deadReport:       deadReport,
				quarantineReport: quarantineReport,
				quarantineErr:    context.Canceled,
			},
			wantReport: report.RunReport{
				DonePurgeReport:       doneReport,
				DeadPurgeReport:       deadReport,
				QuarantinePurgeReport: quarantineReport,
			},
			wantErr:          context.Canceled,
			wantState:        pkgspool.StateQuarantine,
			wantCalls:        []string{"done", "dead", "quarantine"},
			wantContextCalls: 3,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			store := tt.store
			manager := &Manager{Store: &store}

			actual, err := manager.RunOnce(ctx)

			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErr)
			var purgeErr *PurgeError
			require.ErrorAs(t, err, &purgeErr)
			assert.Equal(t, tt.wantState, purgeErr.State)
			assert.Equal(t, tt.wantReport, actual)
			assert.Equal(t, tt.wantCalls, store.calls)
			require.Len(t, store.contexts, tt.wantContextCalls)
			assertSameContexts(t, ctx, store.contexts)
		})
	}
}

func purgeReportWithDeletedFiles(count int) report.PurgeReport {
	return report.PurgeReport{
		ByAge: report.PurgeStepReport{
			Executed:         true,
			DeletedFileCount: count,
			DeletedTotalSize: uint64(count * 10),
		},
	}
}

func assertSameContexts(t *testing.T, want context.Context, got []context.Context) {
	t.Helper()

	for i, actual := range got {
		if actual != want {
			t.Fatalf("contexts[%d] = %v, want %v", i, actual, want)
		}
	}
}

// purgeErrorsByStateは、errors.Joinでまとめた削除失敗をstateごとの原因へ分けます。
func purgeErrorsByState(t *testing.T, err error) map[pkgspool.State]error {
	t.Helper()
	joined, ok := err.(interface{ Unwrap() []error })
	require.True(t, ok, "RunOnce error = %v, want joined purge failures", err)
	causes := make(map[pkgspool.State]error)
	for _, wrapped := range joined.Unwrap() {
		var purgeErr *PurgeError
		require.ErrorAs(t, wrapped, &purgeErr)
		causes[purgeErr.State] = purgeErr.Err
	}
	return causes
}
