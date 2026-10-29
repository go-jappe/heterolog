package pressure

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/pkg/memorypressure"
)

// observedPollは、PollObserverが受け取った1回分の観測と判定です。
type observedPoll struct {
	ratio    float64
	critical bool
}

// TestNewObservedReportsEachSuccessfulReadは、取得に成功した回だけ、その回の観測と判定の
// 結果をobserveへ渡すことを確認します。
func TestNewObservedReportsEachSuccessfulRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const interval = time.Second
		reader := &sequenceMemoryPressureReader{
			values: []memorypressure.MemoryPressure{
				{Ratio: 0.95, Reliable: true},
				{},
				{Ratio: 0.96, Reliable: true},
			},
			errs: []error{nil, errors.New("read failed")},
		}
		var mu sync.Mutex
		var polls []observedPoll
		monitor, err := NewObserved(reader, memorypressure.CriticalPolicy{
			CriticalRatio: 0.9, ConsecutiveCriticalHits: 2,
		}, interval, func(_ context.Context, pressure memorypressure.MemoryPressure, critical bool) {
			mu.Lock()
			defer mu.Unlock()
			polls = append(polls, observedPoll{ratio: pressure.Ratio, critical: critical})
		})
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		require.NoError(t, monitor.Start(ctx))
		time.Sleep(2 * interval)
		synctest.Wait()
		cancel()

		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, []observedPoll{{ratio: 0.95}, {ratio: 0.96, critical: true}}, polls)
	})
}
