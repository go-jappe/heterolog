package report

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPurgeReportSkippedAndExecuted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		purge        PurgeReport
		wantSkipped  bool
		wantExecuted bool
	}{
		{
			name: "state skipped",
			purge: PurgeReport{
				SkipReason: PurgeSkipReasonStateDisabled,
			},
			wantSkipped: true,
		},
		{
			name: "by age executed",
			purge: PurgeReport{
				ByAge: PurgeStepReport{Executed: true},
			},
			wantExecuted: true,
		},
		{
			name: "by size executed",
			purge: PurgeReport{
				BySize: PurgeStepReport{Executed: true},
			},
			wantExecuted: true,
		},
		{
			name:  "zero value is neither skipped nor executed",
			purge: PurgeReport{},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.wantSkipped, tt.purge.Skipped())
			assert.Equal(t, tt.wantExecuted, tt.purge.Executed())
		})
	}
}

func TestPurgeReportAggregatesStepCounts(t *testing.T) {
	t.Parallel()

	purge := PurgeReport{
		ByAge: PurgeStepReport{
			Executed:             true,
			DeletedFileCount:     2,
			DeletedTotalSize:     30,
			RemovedEmptyDirCount: 1,
		},
		BySize: PurgeStepReport{
			Executed:              true,
			DeletedFileCount:      3,
			DeletedTotalSize:      70,
			RemovedEmptyDirCount:  4,
			InvalidBucketDirCount: 1,
		},
	}

	assert.True(t, purge.HasInvalidBucketDirs())
	assert.Equal(t, 5, purge.DeletedFileCount())
	assert.Equal(t, uint64(100), purge.DeletedTotalSize())
	assert.Equal(t, 5, purge.RemovedEmptyDirCount())
}

func TestPurgeStepReportSkippedAndInvalidBucketDirs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		step           PurgeStepReport
		wantSkipped    bool
		wantHasInvalid bool
	}{
		{
			name: "skipped",
			step: PurgeStepReport{
				SkipReason: PurgeSkipReasonSizeIntervalNotDue,
			},
			wantSkipped: true,
		},
		{
			name: "executed with skip reason is not skipped",
			step: PurgeStepReport{
				Executed:   true,
				SkipReason: PurgeSkipReasonAgeAlreadyRunToday,
			},
		},
		{
			name: "has invalid bucket dirs",
			step: PurgeStepReport{
				InvalidBucketDirCount: 1,
			},
			wantHasInvalid: true,
		},
		{
			name: "zero value",
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.wantSkipped, tt.step.Skipped())
			assert.Equal(t, tt.wantHasInvalid, tt.step.HasInvalidBucketDirs())
		})
	}
}

func TestPurgeStepReportAddInvalidBucketDirRecordsCountAndKeepsFirstReason(t *testing.T) {
	t.Parallel()

	firstErr := errors.New("first invalid bucket")
	secondErr := errors.New("second invalid bucket")
	step := PurgeStepReport{}

	step.AddInvalidBucketDir("/spool/done/invalid-1", firstErr)
	step.AddInvalidBucketDir("/spool/done/invalid-2", secondErr)
	step.AddInvalidBucketDir("/spool/done/invalid-3", nil)

	assert.Equal(t, 3, step.InvalidBucketDirCount)
	assert.Equal(t, "/spool/done/invalid-1", step.FirstInvalidBucketDir)
	assert.Equal(t, "first invalid bucket", step.FirstInvalidBucketDirReason)
	assert.True(t, step.HasInvalidBucketDirs())
}

func TestPurgeStepReportAddInvalidBucketDirAllowsNilReceiver(t *testing.T) {
	t.Parallel()

	var step *PurgeStepReport

	assert.NotPanics(t, func() {
		step.AddInvalidBucketDir("/spool/done/invalid", errors.New("invalid bucket"))
	})
}
