package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessingFileTransitionRetryConfigAdapterPreservesValues(t *testing.T) {
	for _, original := range []ProcessingFileTransitionRetryConfig{
		{RetryDelay: 7 * time.Second, MaxEntries: 13},
		{},
		{RetryDelay: -time.Second, MaxEntries: -1},
	} {
		cfg := original
		adapter := NewProcessingFileTransitionRetryConfigAdapter(cfg)
		cfg.RetryDelay += time.Hour
		cfg.MaxEntries++
		assert.Equal(t, original.RetryDelay, adapter.RetryDelay())
		assert.Equal(t, original.MaxEntries, adapter.MaxEntries())
	}
}

func TestNewProcessingFileTransitionRetryRejectsNilConfigProvider(t *testing.T) {
	retry, err := NewProcessingFileTransitionRetry(nil)
	require.ErrorIs(t, err, ErrNilProcessingFileTransitionRetryConfig)
	assert.Nil(t, retry)
}

type overridingTransitionRetryConfigProvider struct {
	ProcessingFileTransitionRetryConfigAdapter
}

func (overridingTransitionRetryConfigProvider) RetryDelay() time.Duration {
	return 7 * time.Second
}

func (overridingTransitionRetryConfigProvider) MaxEntries() int { return 1 }

func TestProcessingFileTransitionRetryUsesConfigProviderGetters(t *testing.T) {
	// The embedded adapter has invalid zero values. The overrides must govern
	// constructor validation, scheduling, and capacity checks.
	retry, err := NewProcessingFileTransitionRetry(overridingTransitionRetryConfigProvider{})
	require.NoError(t, err)
	assert.False(t, retry.IsFull())
	now := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	retry.Upsert("processing/a", ProcessingFileTransitionRetryOpMoveProcessingToDone, nil, now)
	assert.True(t, retry.IsFull())
	_, _, ready := retry.FindReady(now.Add(7*time.Second - time.Nanosecond))
	assert.False(t, ready)
	path, entry, ready := retry.FindReady(now.Add(7 * time.Second))
	require.True(t, ready)
	assert.Equal(t, "processing/a", path)

	retry.Requeue(path, entry, nil, now.Add(7*time.Second))
	_, _, ready = retry.FindReady(now.Add(14*time.Second - time.Nanosecond))
	assert.False(t, ready)
	_, requeued, ready := retry.FindReady(now.Add(14 * time.Second))
	require.True(t, ready)
	assert.Equal(t, entry.Operation, requeued.Operation)
	assert.True(t, retry.IsFull())
	retry.Remove(path)
	assert.False(t, retry.IsFull())
}

func TestProcessingFileTransitionRetryZeroValueBehavior(t *testing.T) {
	var nilRetry *ProcessingFileTransitionRetry
	assert.False(t, nilRetry.IsFull())
	assert.Zero(t, nilRetry.retryDelay())

	var retry ProcessingFileTransitionRetry
	assert.True(t, retry.IsFull())
	now := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	retry.Upsert("processing/a", ProcessingFileTransitionRetryOpMoveProcessingToDone, nil, now)
	path, entry, ready := retry.FindReady(now)
	require.True(t, ready)
	assert.Equal(t, "processing/a", path)
	assert.Equal(t, now, entry.NextRetryAt)
}
