package lifecycle

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

type FileOperator struct {
	clock                clock.Clock
	RootDir              spool.Dir
	Config               ConfigProvider
	purgeTrackersByState map[pkgspool.State]*StatePurgeTracker
}

func NewFileOperator(
	timeSource clock.Clock,
	cfg ConfigProvider,
) (*FileOperator, error) {
	if timeSource == nil {
		return nil, errors.New("spool lifecycle clock is nil")
	}
	if cfg == nil {
		return nil, errors.New("spool lifecycle config is nil")
	}
	if err := validateFileOperatorConfig(cfg); err != nil {
		return nil, err
	}
	rootDir, err := spool.NewRootDir(
		cfg.SpoolPath(),
		cfg.Target(),
		cfg.Dir(),
	)
	if err != nil {
		return nil, err
	}
	return &FileOperator{
		clock:                timeSource,
		RootDir:              rootDir,
		Config:               cfg,
		purgeTrackersByState: make(map[pkgspool.State]*StatePurgeTracker),
	}, nil
}

func validateFileOperatorConfig(cfg ConfigProvider) error {
	var errs []error
	if interval := cfg.SizePurgeInterval(); interval <= 0 {
		errs = append(errs, fmt.Errorf("%w: %s", ErrInvalidSizePurgeInterval, interval))
	}
	if cfg.EnableSizeEstimateCache() {
		if checks := cfg.SizeFullScanEveryNChecks(); checks <= 0 {
			errs = append(errs, fmt.Errorf("%w: %d", ErrInvalidSizeFullScanChecks, checks))
		}
		if interval := cfg.SizeFullScanMaxInterval(); interval <= 0 {
			errs = append(errs, fmt.Errorf("%w: %s", ErrInvalidSizeFullScanMaxInterval, interval))
		}
	}

	for _, state := range []struct {
		name       pkgspool.State
		enabled    bool
		maxAge     int
		maxDirSize uint64
	}{
		{
			name:       pkgspool.StateDone,
			enabled:    cfg.EnablePurgeDone(),
			maxAge:     cfg.DoneMaxFileAge(),
			maxDirSize: cfg.DoneMaxDirSize(),
		},
		{
			name:       pkgspool.StateDead,
			enabled:    cfg.EnablePurgeDead(),
			maxAge:     cfg.DeadMaxFileAge(),
			maxDirSize: cfg.DeadMaxDirSize(),
		},
		{
			name:       pkgspool.StateQuarantine,
			enabled:    cfg.EnablePurgeQuarantine(),
			maxAge:     cfg.QuarantineMaxFileAge(),
			maxDirSize: cfg.QuarantineMaxDirSize(),
		},
	} {
		if !state.enabled {
			continue
		}
		if state.maxAge <= 0 {
			errs = append(errs, &StatePurgeConfigError{State: state.name, Err: ErrInvalidStateMaxFileAge, MaxFileAge: state.maxAge})
		}
		if state.maxDirSize == 0 {
			errs = append(errs, &StatePurgeConfigError{State: state.name, Err: ErrInvalidStateMaxDirSize, MaxDirSize: state.maxDirSize})
		}
	}

	return errors.Join(errs...)
}

func (f *FileOperator) purgeTrackerFor(state pkgspool.State) *StatePurgeTracker {
	if f.purgeTrackersByState == nil {
		f.purgeTrackersByState = make(map[pkgspool.State]*StatePurgeTracker)
	}

	tracker := f.purgeTrackersByState[state]
	if tracker == nil {
		tracker = &StatePurgeTracker{}
		f.purgeTrackersByState[state] = tracker
	}

	return tracker
}
