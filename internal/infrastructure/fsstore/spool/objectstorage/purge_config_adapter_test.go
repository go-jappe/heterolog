package objectstoragespool

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	pkgobjectstorage "github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestPurgeConfigAdapterPreservesValues(t *testing.T) {
	for _, config := range []TerminalPurgeConfig{
		{},
		{
			Done:                        enabledPurgeConfig(3, 10),
			Dead:                        enabledPurgeConfig(5, 20),
			Quarantine:                  enabledPurgeConfig(7, 30),
			CapacityPurgeMinimumFileAge: 17 * time.Minute,
		},
		{
			Done:                        pkgobjectstorage.CompressedLogFileUploadSpoolPurgeConfig{Enabled: true},
			Dead:                        pkgobjectstorage.CompressedLogFileUploadSpoolPurgeConfig{MaxFileAgeDays: -2},
			CapacityPurgeMinimumFileAge: -time.Second,
		},
	} {
		adapter := NewPurgeConfigAdapter(config)
		want := config
		config = TerminalPurgeConfig{}
		got := TerminalPurgeConfig{
			Done:                        statePurgeConfigValues(adapter.DonePurge()),
			Dead:                        statePurgeConfigValues(adapter.DeadPurge()),
			Quarantine:                  statePurgeConfigValues(adapter.QuarantinePurge()),
			CapacityPurgeMinimumFileAge: adapter.CapacityPurgeMinimumFileAge(),
		}
		if got != want {
			t.Fatalf("adapter config = %+v, want %+v", got, want)
		}
	}
}

type testPurgeConfigProvider struct {
	nilState   pkgspool.State
	done       pkgobjectstorage.CompressedLogFileUploadSpoolPurgeConfig
	dead       pkgobjectstorage.CompressedLogFileUploadSpoolPurgeConfig
	quarantine pkgobjectstorage.CompressedLogFileUploadSpoolPurgeConfig
	minimumAge time.Duration
}

func (p testPurgeConfigProvider) DonePurge() StatePurgeConfigProvider {
	if p.nilState == pkgspool.StateDone {
		return nil
	}
	return testStatePurgeConfigProvider{values: p.done}
}

func (p testPurgeConfigProvider) DeadPurge() StatePurgeConfigProvider {
	if p.nilState == pkgspool.StateDead {
		return nil
	}
	return testStatePurgeConfigProvider{values: p.dead}
}

func (p testPurgeConfigProvider) QuarantinePurge() StatePurgeConfigProvider {
	if p.nilState == pkgspool.StateQuarantine {
		return nil
	}
	return testStatePurgeConfigProvider{values: p.quarantine}
}

func (p testPurgeConfigProvider) CapacityPurgeMinimumFileAge() time.Duration {
	return p.minimumAge
}

func TestTerminalPurgerValidatesConfigProvider(t *testing.T) {
	stores, _ := newTestDestinationStores(t)
	for _, test := range []struct {
		name     string
		provider PurgeConfigProvider
		wantErr  error
		// wantStateErrは、stateごとの保持設定で期待する不正です。
		wantStateErr *StatePurgeConfigError
	}{
		{name: "nil", wantErr: ErrNilTerminalPurgeConfig},
		{
			name:         "done nil provider",
			provider:     testPurgeConfigProvider{nilState: pkgspool.StateDone},
			wantStateErr: &StatePurgeConfigError{State: pkgspool.StateDone, Err: ErrNilStatePurgeConfig},
		},
		{
			name:         "dead nil provider",
			provider:     testPurgeConfigProvider{nilState: pkgspool.StateDead},
			wantStateErr: &StatePurgeConfigError{State: pkgspool.StateDead, Err: ErrNilStatePurgeConfig},
		},
		{
			name:         "quarantine nil provider",
			provider:     testPurgeConfigProvider{nilState: pkgspool.StateQuarantine},
			wantStateErr: &StatePurgeConfigError{State: pkgspool.StateQuarantine, Err: ErrNilStatePurgeConfig},
		},
		{
			name:     "negative minimum age",
			provider: testPurgeConfigProvider{minimumAge: -time.Second},
			wantErr:  ErrNegativeCapacityPurgeMinimumFileAge,
		},
		{
			name:         "done age",
			provider:     testPurgeConfigProvider{done: enabledPurgeConfig(0, 1)},
			wantStateErr: &StatePurgeConfigError{State: pkgspool.StateDone, Err: ErrInvalidPurgeMaxFileAgeDays},
		},
		{
			name:         "dead size",
			provider:     testPurgeConfigProvider{dead: enabledPurgeConfig(1, 0)},
			wantStateErr: &StatePurgeConfigError{State: pkgspool.StateDead, Err: ErrInvalidPurgeMaxDirSize},
		},
		{
			name:         "quarantine age",
			provider:     testPurgeConfigProvider{quarantine: enabledPurgeConfig(-1, 1)},
			wantStateErr: &StatePurgeConfigError{State: pkgspool.StateQuarantine, Err: ErrInvalidPurgeMaxFileAgeDays, MaxFileAgeDays: -1},
		},
		{
			name: "disabled values",
			provider: testPurgeConfigProvider{
				done: pkgobjectstorage.CompressedLogFileUploadSpoolPurgeConfig{MaxFileAgeDays: -1},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			purger, err := NewTerminalPurger(stores, test.provider)
			switch {
			case test.wantErr == nil && test.wantStateErr == nil:
				if err != nil || purger == nil {
					t.Fatalf("NewTerminalPurger() = %v, %v", purger, err)
				}
			case test.wantStateErr != nil:
				_, found := errortree.Find(err, func(got *StatePurgeConfigError) bool { return *got == *test.wantStateErr })
				if !found || purger != nil {
					t.Fatalf("NewTerminalPurger() = %v, %v, want nil and %+v", purger, err, *test.wantStateErr)
				}
			default:
				if !errors.Is(err, test.wantErr) || purger != nil {
					t.Fatalf("NewTerminalPurger() = %v, %v, want nil and %v", purger, err, test.wantErr)
				}
			}
		})
	}
}

func TestTerminalPurgerUsesCustomConfigProvider(t *testing.T) {
	stores, store := newTestDestinationStores(t)
	done := saveTerminalPurgeTestFile(t, store, "app_20260906_40.log.gz", "done", pkgspool.StateDone)
	dead := saveTerminalPurgeTestFile(t, store, "app_20260906_41.log.gz", "dead", pkgspool.StateDead)
	quarantine := saveTerminalPurgeTestFile(t, store, "app_20260906_42.log.gz", "quarantine", pkgspool.StateQuarantine)
	now := time.Now().UTC().Truncate(time.Second)
	old := now.AddDate(0, 0, -10)
	for _, file := range []CompressedLogFile{done, dead} {
		if err := os.Chtimes(file.Path, old, old); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chtimes(quarantine.Path, now, now); err != nil {
		t.Fatal(err)
	}
	purger, err := NewTerminalPurger(stores, testPurgeConfigProvider{
		done:       enabledPurgeConfig(1, 100),
		quarantine: enabledPurgeConfig(365, 1),
		minimumAge: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	purger.now = func() time.Time { return now }
	report, err := purger.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.ByState[pkgspool.StateDone].DeletedFileCount != 1 {
		t.Fatalf("done report = %+v, want one age deletion", report.ByState[pkgspool.StateDone])
	}
	if _, err := os.Lstat(done.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired done file remains: %v", err)
	}
	if report.ByState[pkgspool.StateDead].Enabled {
		t.Fatalf("dead report = %+v, want disabled", report.ByState[pkgspool.StateDead])
	}
	if _, err := os.Lstat(dead.Path); err != nil {
		t.Fatalf("disabled dead file was removed: %v", err)
	}
	if stateReport := report.ByState[pkgspool.StateQuarantine]; !stateReport.Enabled ||
		stateReport.DeletedFileCount != 0 || stateReport.RemainingBytes <= 1 {
		t.Fatalf("quarantine report = %+v, want protected over capacity", stateReport)
	}
	if _, err := os.Lstat(quarantine.Path); err != nil {
		t.Fatalf("protected quarantine file was removed: %v", err)
	}

	now = now.Add(time.Hour)
	report, err = purger.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.ByState[pkgspool.StateQuarantine].DeletedFileCount != 1 {
		t.Fatalf("quarantine report = %+v, want one capacity deletion", report.ByState[pkgspool.StateQuarantine])
	}
	if _, err := os.Lstat(quarantine.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("aged over-capacity quarantine file remains: %v", err)
	}
}

func statePurgeConfigValues(config StatePurgeConfigProvider) pkgobjectstorage.CompressedLogFileUploadSpoolPurgeConfig {
	return pkgobjectstorage.CompressedLogFileUploadSpoolPurgeConfig{
		Enabled: config.Enabled(), MaxFileAgeDays: config.MaxFileAgeDays(), MaxDirSize: config.MaxDirSize(),
	}
}

// 独自providerでもpurgerが具体型に依存せずメソッドから条件を取得することを確認します。
type testStatePurgeConfigProvider struct {
	values pkgobjectstorage.CompressedLogFileUploadSpoolPurgeConfig
}

func (p testStatePurgeConfigProvider) Enabled() bool {
	return p.values.Enabled
}
func (p testStatePurgeConfigProvider) MaxFileAgeDays() int {
	return p.values.MaxFileAgeDays
}
func (p testStatePurgeConfigProvider) MaxDirSize() uint64 {
	return p.values.MaxDirSize
}
