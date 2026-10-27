package logger

import (
	"errors"
	"sync"
	"sync/atomic"
)

// LiveSettings holds a log level and sampling settings that can be changed while
// an output runs. When it is set in RuntimeConfig.Live, the output uses these
// values instead of RuntimeConfig.LogLevel and the SamplingConfig of the same
// backend configuration, and changes take effect from the next log. New and
// NewAsync of heterolog create one for each output that does not set Live.
// Create it with NewLiveSettings. It is safe for concurrent use.
type LiveSettings struct {
	level    atomic.Int64
	sampling atomic.Pointer[SamplingConfig]
	// updateMuは、levelとsamplingを置き換える処理を直列化します。読み取りは待ちません。
	updateMu sync.Mutex
}

// NewLiveSettings returns a LiveSettings that starts with level and sampling. It
// returns a ConfigFieldError with ErrInvalidLogLevel if level is not a defined
// LogLevel, and one with ErrInvalidSamplingRate if sampling does not satisfy the
// rules of ValidateSamplingConfig.
func NewLiveSettings(level LogLevel, sampling SamplingConfig) (*LiveSettings, error) {
	settings := &LiveSettings{}
	if err := settings.Set(level, sampling); err != nil {
		return nil, err
	}
	return settings, nil
}

// Level returns the current log level.
func (s *LiveSettings) Level() LogLevel {
	return LogLevel(s.level.Load())
}

// Sampling returns a copy of the current sampling settings.
func (s *LiveSettings) Sampling() SamplingConfig {
	if sampling := s.sampling.Load(); sampling != nil {
		return *sampling
	}
	return SamplingConfig{}
}

// Set validates level and sampling and then replaces both. On error, it changes
// neither. The validation is the same as in NewLiveSettings.
func (s *LiveSettings) Set(level LogLevel, sampling SamplingConfig) error {
	if err := errors.Join(
		ValidateLogLevel("", "", level),
		ValidateSamplingConfig("", "", sampling),
	); err != nil {
		return err
	}
	s.updateMu.Lock()
	defer s.updateMu.Unlock()
	stored := sampling
	s.sampling.Store(&stored)
	s.level.Store(int64(level))
	return nil
}
