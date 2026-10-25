package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	lifecyclereport "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle/report"
	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
	"github.com/go-jappe/heterolog/pkg/spool/lifecyclemanager/lifecyclefsstore"
)

func TestNewFileOperatorRejectsNonPositiveSizePurgeInterval(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second} {
		t.Run(interval.String(), func(t *testing.T) {
			cfg := newFileOperatorTestConfig(t, interval)

			_, err := NewFileOperator(clock.NewUTCClock(), cfg)
			if !errors.Is(err, ErrInvalidSizePurgeInterval) {
				t.Fatalf("NewFileOperator() error = %v, want non-positive size purge interval error", err)
			}
		})
	}
}

func TestNewFileOperatorRejectsNonPositiveActiveSettings(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*lifecyclefsstore.LifeCycleFsstoreConfig)
		wantErr   error
		// wantStateは、stateごとの設定の不正で期待するstateです。
		wantState pkgspool.State
	}{
		{
			name: "zero full scan checks",
			configure: func(cfg *lifecyclefsstore.LifeCycleFsstoreConfig) {
				cfg.SizeFullScanEveryNChecks = 0
			},
			wantErr: ErrInvalidSizeFullScanChecks,
		},
		{
			name: "zero full scan max interval",
			configure: func(cfg *lifecyclefsstore.LifeCycleFsstoreConfig) {
				cfg.SizeFullScanMaxInterval = 0
			},
			wantErr: ErrInvalidSizeFullScanMaxInterval,
		},
		{
			name: "zero done max age",
			configure: func(cfg *lifecyclefsstore.LifeCycleFsstoreConfig) {
				cfg.DoneMaxFileAge = 0
			},
			wantErr:   ErrInvalidStateMaxFileAge,
			wantState: pkgspool.StateDone,
		},
		{
			name: "zero done max dir size",
			configure: func(cfg *lifecyclefsstore.LifeCycleFsstoreConfig) {
				cfg.DoneMaxDirSize = 0
			},
			wantErr:   ErrInvalidStateMaxDirSize,
			wantState: pkgspool.StateDone,
		},
		{
			name: "zero dead max age",
			configure: func(cfg *lifecyclefsstore.LifeCycleFsstoreConfig) {
				cfg.DeadMaxFileAge = 0
			},
			wantErr:   ErrInvalidStateMaxFileAge,
			wantState: pkgspool.StateDead,
		},
		{
			name: "zero dead max dir size",
			configure: func(cfg *lifecyclefsstore.LifeCycleFsstoreConfig) {
				cfg.DeadMaxDirSize = 0
			},
			wantErr:   ErrInvalidStateMaxDirSize,
			wantState: pkgspool.StateDead,
		},
		{
			name: "zero quarantine max age",
			configure: func(cfg *lifecyclefsstore.LifeCycleFsstoreConfig) {
				cfg.QuarantineMaxFileAge = 0
			},
			wantErr:   ErrInvalidStateMaxFileAge,
			wantState: pkgspool.StateQuarantine,
		},
		{
			name: "zero quarantine max dir size",
			configure: func(cfg *lifecyclefsstore.LifeCycleFsstoreConfig) {
				cfg.QuarantineMaxDirSize = 0
			},
			wantErr:   ErrInvalidStateMaxDirSize,
			wantState: pkgspool.StateQuarantine,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validFileOperatorConfig(t)
			tt.configure(&cfg)

			_, err := NewFileOperator(clock.NewUTCClock(), NewConfigAdapter(cfg))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("NewFileOperator() error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantState != "" {
				if _, found := errortree.Find(err, func(got *StatePurgeConfigError) bool {
					return got.State == tt.wantState && errors.Is(got, tt.wantErr)
				}); !found {
					t.Fatalf("NewFileOperator() error = %v, want %v for state %q", err, tt.wantErr, tt.wantState)
				}
			}
		})
	}
}

func TestNewFileOperatorDoesNotValidateSettingsForDisabledFeatures(t *testing.T) {
	cfg := lifecyclefsstore.LifeCycleFsstoreConfig{
		SpoolPath:                t.TempDir(),
		Target:                   "test-target",
		Dir:                      "test-backend",
		SizePurgeInterval:        time.Second,
		EnableSizeEstimateCache:  false,
		SizeFullScanEveryNChecks: 0,
		SizeFullScanMaxInterval:  0,
		EnablePurgeDone:          false,
		DoneMaxFileAge:           0,
		DoneMaxDirSize:           0,
		EnablePurgeDead:          false,
		DeadMaxFileAge:           0,
		DeadMaxDirSize:           0,
		EnablePurgeQuarantine:    false,
		QuarantineMaxFileAge:     0,
		QuarantineMaxDirSize:     0,
	}

	if _, err := NewFileOperator(clock.NewUTCClock(), NewConfigAdapter(cfg)); err != nil {
		t.Fatalf("NewFileOperator() error = %v, want nil for disabled feature settings", err)
	}
}

func TestAgePurgeCalendarBoundaryUsesClockTimezone(t *testing.T) {
	t.Parallel()

	location := time.FixedZone("JST", 9*60*60)
	lastRunAt := time.Date(2026, 6, 10, 23, 30, 0, 0, location)
	now := time.Date(2026, 6, 11, 0, 30, 0, 0, location)

	if !shouldRunAgePurge(now, lastRunAt) {
		t.Fatal("shouldRunAgePurge = false, want true across the configured calendar day")
	}
	wantStart := time.Date(2026, 6, 11, 0, 0, 0, 0, location)
	if got := startOfDay(now); !got.Equal(wantStart) || got.Location() != location {
		t.Fatalf("startOfDay = %s (%s), want %s (%s)", got, got.Location(), wantStart, location)
	}
}

func TestCountStateFileSizeSinceUsesClockTimezoneForBucketTraversal(t *testing.T) {
	t.Parallel()

	location := time.FixedZone("UTC-4", -4*60*60)
	// 前回のClock値が持つUTC-4の日付・時刻でbucketを探索します。
	since := time.Date(2026, 6, 10, 22, 0, 0, 0, location)
	stateDir := t.TempDir()
	bucketDir := filepath.Join(stateDir, "2026", "06", "10", "22", "05")
	if err := os.MkdirAll(bucketDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error: %v", err)
	}
	path := filepath.Join(bucketDir, "00000_test"+spooldir.SpoolExt)
	body := []byte("new spool data")
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("WriteFile() error: %v", err)
	}
	if err := os.Chtimes(path, since.Add(time.Minute), since.Add(time.Minute)); err != nil {
		t.Fatalf("Chtimes() error: %v", err)
	}

	report := lifecyclereport.PurgeStepReport{}
	got, err := countStateFileSizeSince(
		context.Background(),
		stateDir,
		since,
		&report,
	)
	if err != nil {
		t.Fatalf("countStateFileSizeSince() error: %v", err)
	}
	if got != uint64(len(body)) {
		t.Fatalf("countStateFileSizeSince() = %d, want %d", got, len(body))
	}
}

func newFileOperatorTestConfig(t *testing.T, sizePurgeInterval time.Duration) ConfigProvider {
	t.Helper()

	return NewConfigAdapter(lifecyclefsstore.LifeCycleFsstoreConfig{
		SpoolPath:         t.TempDir(),
		Target:            "test-target",
		Dir:               "test-backend",
		DoneMaxDirSize:    1,
		SizePurgeInterval: sizePurgeInterval,
	})
}

func validFileOperatorConfig(t *testing.T) lifecyclefsstore.LifeCycleFsstoreConfig {
	t.Helper()
	return lifecyclefsstore.LifeCycleFsstoreConfig{
		SpoolPath:                t.TempDir(),
		Target:                   "test-target",
		Dir:                      "test-backend",
		EnablePurgeDone:          true,
		DoneMaxFileAge:           1,
		DoneMaxDirSize:           1,
		EnablePurgeDead:          true,
		DeadMaxFileAge:           1,
		DeadMaxDirSize:           1,
		EnablePurgeQuarantine:    true,
		QuarantineMaxFileAge:     1,
		QuarantineMaxDirSize:     1,
		SizePurgeInterval:        time.Second,
		EnableSizeEstimateCache:  true,
		SizeFullScanEveryNChecks: 1,
		SizeFullScanMaxInterval:  time.Second,
	}
}
