package loki

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"

	"github.com/stretchr/testify/require"
)

func TestLokiLoggerPreservesInFlightBatchWhileAcceptingNextEntries(t *testing.T) {
	ctx := context.Background()
	logger := newLifecycleLokiLogger(t, time.Hour)
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFlush := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() {
		releaseFlush()
		require.NoError(t, logger.Close(ctx))
	})

	var firstFlush sync.Once
	var recordedMu sync.Mutex
	var recorded [][]string
	logger.mu.Lock()
	logger.flushManager = &snapshotLokiFlushManager{
		FlushManager: logger.flushManager,
		flush: func(_ context.Context, entries []*logging.LogEntry) error {
			firstFlush.Do(func() {
				close(started)
				<-release
			})
			// 実際のsnapshotは、新しいentryがqueueに入った後で初めて読みます。
			// 待機の前にcopyすると、backing arrayの再利用が隠れてしまいます。
			messages := make([]string, 0, len(entries))
			for _, entry := range entries {
				messages = append(messages, entry.Message)
			}
			recordedMu.Lock()
			recorded = append(recorded, messages)
			recordedMu.Unlock()
			return nil
		},
	}
	logger.mu.Unlock()

	require.NoError(t, logger.testInfo(ctx, "in-flight first"))
	require.NoError(t, logger.testInfo(ctx, "in-flight second"))
	flushDone := make(chan error, 1)
	go func() { flushDone <- logger.doFlush(ctx) }()
	waitLokiLifecycleSignal(t, started, "first batch did not reach FlushManager")

	enqueueDone := make(chan error, 1)
	go func() {
		if err := logger.testInfo(ctx, "next first"); err != nil {
			enqueueDone <- err
			return
		}
		enqueueDone <- logger.testInfo(ctx, "next second")
	}()
	waitLokiLifecycleError(t, enqueueDone, "next entries could not be accepted during Flush")

	releaseFlush()
	waitLokiLifecycleError(t, flushDone, "first batch did not finish")
	require.NoError(t, logger.Close(ctx))

	recordedMu.Lock()
	defer recordedMu.Unlock()
	require.NotEmpty(t, recorded)
	require.Equal(t, []string{"in-flight first", "in-flight second"}, recorded[0])
	var allMessages []string
	for _, batch := range recorded {
		allMessages = append(allMessages, batch...)
	}
	require.ElementsMatch(t, []string{
		"in-flight first", "in-flight second", "next first", "next second",
	}, allMessages, "each accepted log must reach the flush manager exactly once")
}

type snapshotLokiFlushManager struct {
	FlushManager
	flush func(context.Context, []*logging.LogEntry) error
}

func (m *snapshotLokiFlushManager) Flush(ctx context.Context, entries []*logging.LogEntry) error {
	return m.flush(ctx, entries)
}
