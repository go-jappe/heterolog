package pressure

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/pkg/memorypressure"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sequenceMemoryPressureReader struct {
	mu        sync.Mutex
	values    []memorypressure.MemoryPressure
	errs      []error
	callCount int
}

func (r *sequenceMemoryPressureReader) ReadMemoryPressure(ctx context.Context) (memorypressure.MemoryPressure, error) {
	_ = ctx

	r.mu.Lock()
	defer r.mu.Unlock()

	idx := r.callCount
	r.callCount++

	if idx < len(r.errs) && r.errs[idx] != nil {
		return memorypressure.MemoryPressure{}, r.errs[idx]
	}
	if idx < len(r.values) {
		return r.values[idx], nil
	}
	if len(r.values) > 0 {
		return r.values[len(r.values)-1], nil
	}

	return memorypressure.MemoryPressure{}, nil
}

func (r *sequenceMemoryPressureReader) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.callCount
}

// newTestMonitor は、検証済みの policy で monitor を生成します。
func newTestMonitor(t *testing.T, reader memorypressure.Provider, policy memorypressure.CriticalPolicy, interval time.Duration) memorypressure.Monitor {
	t.Helper()
	monitor, err := New(reader, policy, interval)
	require.NoError(t, err)
	return monitor
}

func TestMemoryPressureMonitorCurrentAndIsCriticalBeforeStartReturnError(t *testing.T) {
	monitor := newTestMonitor(t, &sequenceMemoryPressureReader{},
		memorypressure.CriticalPolicy{CriticalRatio: 0.90}, time.Hour)

	_, err := monitor.Current(context.Background())
	assert.ErrorIs(t, err, ErrPressureNotRead)

	critical, err := monitor.IsCritical(context.Background())
	assert.False(t, critical)
	assert.ErrorIs(t, err, ErrPressureNotRead)
}

func TestMemoryPressureMonitorStartPollsInitialValueAndCachesSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := &sequenceMemoryPressureReader{
		values: []memorypressure.MemoryPressure{
			{Ratio: 0.95, AvailableBytes: 64, RSSBytes: 1024, Reliable: true},
		},
	}
	monitor := newTestMonitor(t, reader, memorypressure.CriticalPolicy{
		CriticalRatio:           0.90,
		ConsecutiveCriticalHits: 1,
	}, time.Hour)

	require.NoError(t, monitor.Start(ctx))

	current, err := monitor.Current(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0.95, current.Ratio)

	critical, err := monitor.IsCritical(context.Background())
	require.NoError(t, err)
	assert.True(t, critical)

	snapshot := monitor.Last()
	assert.Equal(t, current, snapshot.Pressure)
	assert.True(t, snapshot.LastCritical)
	assert.Equal(t, 1, snapshot.ConsecutiveHits)
	assert.False(t, snapshot.LastReadAt.IsZero())
	assert.False(t, snapshot.CriticalSince.IsZero())
	assert.Equal(t, 1, reader.calls())
}

func TestMemoryPressureMonitorStartReturnsInitialReadError(t *testing.T) {
	readErr := errors.New("read failed")
	reader := &sequenceMemoryPressureReader{
		errs: []error{readErr},
	}
	monitor := newTestMonitor(t, reader, memorypressure.CriticalPolicy{CriticalRatio: 0.90}, time.Hour)

	assert.ErrorIs(t, monitor.Start(context.Background()), readErr)

	_, err := monitor.Current(context.Background())
	assert.ErrorIs(t, err, readErr)

	critical, err := monitor.IsCritical(context.Background())
	assert.False(t, critical)
	assert.ErrorIs(t, err, readErr)
}

// New は入力の誤りを既定値で隠さず、生成時にエラーを返すことを確認する。
func TestNewRejectsInputThatWouldHideMistakes(t *testing.T) {
	provider := &sequenceMemoryPressureReader{}
	valid := memorypressure.CriticalPolicy{CriticalRatio: 0.90}
	for _, tc := range []struct {
		name     string
		provider memorypressure.Provider
		policy   memorypressure.CriticalPolicy
		interval time.Duration
	}{
		{"nil provider", nil, valid, time.Second},
		{"zero interval", provider, valid, 0},
		{"negative interval", provider, valid, -time.Second},
		{"no threshold", provider, memorypressure.CriticalPolicy{ConsecutiveCriticalHits: 2}, time.Second},
		{"ratio above one", provider, memorypressure.CriticalPolicy{CriticalRatio: 1.5}, time.Second},
		{"NaN ratio", provider, memorypressure.CriticalPolicy{CriticalRatio: math.NaN(), MinAvailableBytes: 1}, time.Second},
		{"negative hits", provider, memorypressure.CriticalPolicy{CriticalRatio: 0.90, ConsecutiveCriticalHits: -1}, time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			monitor, err := New(tc.provider, tc.policy, tc.interval)
			assert.ErrorIs(t, err, ErrInvalidMonitorConfig)
			assert.Nil(t, monitor)
		})
	}
	// CriticalPolicy の文書どおりの無効値は、もう一方の閾値が有効なら受け付ける。
	for _, policy := range []memorypressure.CriticalPolicy{
		{CriticalRatio: -1, MinAvailableBytes: 1},
		{CriticalRatio: 1},
		{MinAvailableBytes: 1, ConsecutiveCriticalHits: 0},
	} {
		_, err := New(provider, policy, time.Second)
		assert.NoError(t, err, "policy %+v", policy)
	}
}

func TestMemoryPressureMonitorResetClearsCachedState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	monitor := newTestMonitor(t, &sequenceMemoryPressureReader{
		values: []memorypressure.MemoryPressure{
			{Ratio: 0.95, Reliable: true},
		},
	}, memorypressure.CriticalPolicy{
		CriticalRatio:           0.90,
		ConsecutiveCriticalHits: 1,
	}, time.Hour)
	require.NoError(t, monitor.Start(ctx))

	monitor.Reset()

	snapshot := monitor.Last()
	assert.Equal(t, memorypressure.MemoryPressure{}, snapshot.Pressure)
	assert.True(t, snapshot.LastReadAt.IsZero())
	assert.False(t, snapshot.LastCritical)
	assert.Equal(t, 0, snapshot.ConsecutiveHits)
	assert.True(t, snapshot.CriticalSince.IsZero())

	_, err := monitor.Current(context.Background())
	assert.ErrorIs(t, err, ErrPressureNotRead)
}

func TestMemoryPressureMonitorPollsInBackground(t *testing.T) {
	// 取得間隔の経過は synctest の時計で進め、実時間の待機に依存しません。
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		reader := &sequenceMemoryPressureReader{
			values: []memorypressure.MemoryPressure{
				{Ratio: 0.95, Reliable: true},
				{Ratio: 0.96, Reliable: true},
			},
		}
		monitor := newTestMonitor(t, reader, memorypressure.CriticalPolicy{
			CriticalRatio:           0.90,
			ConsecutiveCriticalHits: 2,
		}, time.Second)

		require.NoError(t, monitor.Start(ctx))
		// 起動時の一回だけでは、二回連続の条件に達していない。
		critical, err := monitor.IsCritical(context.Background())
		require.NoError(t, err)
		assert.False(t, critical)

		time.Sleep(time.Second)
		synctest.Wait()
		critical, err = monitor.IsCritical(context.Background())
		require.NoError(t, err)
		assert.True(t, critical)
		assert.Equal(t, 2, reader.calls())

		// Start の context を終えると、以後は取得しない。
		cancel()
		synctest.Wait()
		time.Sleep(5 * time.Second)
		synctest.Wait()
		assert.Equal(t, 2, reader.calls())
	})
}
