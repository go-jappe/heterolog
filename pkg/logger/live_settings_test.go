package logger

import (
	"errors"
	"sync"
	"testing"
)

func TestNewLiveSettingsRejectsInvalidValues(t *testing.T) {
	if _, err := NewLiveSettings(LogLevel(0), SamplingConfig{}); !errors.Is(err, ErrInvalidLogLevel) {
		t.Errorf("NewLiveSettings(level 0) error = %v, want ErrInvalidLogLevel", err)
	}
	if _, err := NewLiveSettings(LogLevelInfo, SamplingConfig{IsSamplingEnabled: true}); !errors.Is(err, ErrInvalidSamplingRate) {
		t.Errorf("NewLiveSettings(sampling rate 0) error = %v, want ErrInvalidSamplingRate", err)
	}
}

// Setは検証に失敗するとどちらの値も変えません。
func TestLiveSettingsSetKeepsBothValuesWhenRejected(t *testing.T) {
	settings, err := NewLiveSettings(LogLevelInfo, SamplingConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := settings.Set(LogLevelDebug, SamplingConfig{IsSamplingEnabled: true}); !errors.Is(err, ErrInvalidSamplingRate) {
		t.Fatalf("Set(invalid sampling) error = %v, want ErrInvalidSamplingRate", err)
	}
	if settings.Level() != LogLevelInfo || settings.Sampling().IsSamplingEnabled {
		t.Errorf("settings = %v, %+v after a rejected Set, want the original INFO without sampling", settings.Level(), settings.Sampling())
	}
	enabled := SamplingConfig{IsSamplingEnabled: true, SamplingDebugRate: 1, SamplingInfoRate: 2, SamplingWarnRate: 3, SamplingErrorRate: 4}
	if err := settings.Set(LogLevelError, enabled); err != nil {
		t.Fatal(err)
	}
	if settings.Level() != LogLevelError || settings.Sampling() != enabled {
		t.Errorf("settings = %v, %+v, want ERROR with the new sampling", settings.Level(), settings.Sampling())
	}
}

// 読み取りと変更を並行して行えます（go test -raceで確認します）。
func TestLiveSettingsIsSafeForConcurrentUse(t *testing.T) {
	settings, err := NewLiveSettings(LogLevelInfo, SamplingConfig{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				_ = settings.Set(LogLevelDebug+LogLevel(j%4), SamplingConfig{})
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				if !settings.Level().IsValid() {
					t.Error("read an invalid level")
					return
				}
				_ = settings.Sampling()
			}
		}()
	}
	wg.Wait()
}
