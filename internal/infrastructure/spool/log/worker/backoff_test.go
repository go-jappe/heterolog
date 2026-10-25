package worker

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewProcessingPathBackoffAppliesDefaults(t *testing.T) {
	t.Parallel()

	backoff := NewProcessingPathBackoff(ProcessingPathBackoffConfig{})

	assert.Equal(t, defaultProcessingBackoffMaxPathCacheEntries, backoff.cfg.MaxPathCacheEntries)
	assert.Equal(t, defaultProcessingPathSkipHoldDuration, backoff.cfg.PathSkipHoldDuration)
	assert.Zero(t, backoff.PathCacheCount())
}

func TestProcessingPathBackoffBlocksUnblocksAndExpiresPath(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	backoff := NewProcessingPathBackoff(ProcessingPathBackoffConfig{
		MaxPathCacheEntries:  2,
		PathSkipHoldDuration: time.Minute,
	})

	backoff.BlockPath("processing/a", ProcessingPathBackoffOpReadProcessingRecords, errors.New("read failed"), now)

	assert.True(t, backoff.ShouldSkipPath("processing/a", now.Add(30*time.Second)))
	assert.Equal(t, 1, backoff.PathCacheCount())
	assert.Equal(t, ProcessingPathBackoffOpReadProcessingRecords, backoff.pathSkipCache["processing/a"].Operation)
	assert.Equal(t, "read failed", backoff.pathSkipCache["processing/a"].ErrorText)

	backoff.BlockPath("processing/a", ProcessingPathBackoffOpMoveInvalidProcessingToQuarantine, errors.New("move failed"), now.Add(40*time.Second))

	assert.Equal(t, 1, backoff.PathCacheCount())
	assert.Equal(t, ProcessingPathBackoffOpReadProcessingRecords, backoff.pathSkipCache["processing/a"].Operation)

	backoff.UnblockPath("processing/a")

	assert.False(t, backoff.ShouldSkipPath("processing/a", now.Add(50*time.Second)))
	assert.Zero(t, backoff.PathCacheCount())

	backoff.BlockPath("processing/b", ProcessingPathBackoffOpReadProcessingRecords, nil, now)

	assert.True(t, backoff.ShouldSkipPath("processing/b", now.Add(59*time.Second)))
	assert.False(t, backoff.ShouldSkipPath("processing/b", now.Add(time.Minute)))
	assert.Zero(t, backoff.PathCacheCountAt(now.Add(time.Minute)))
}

func TestProcessingPathBackoffOverflowKeepsBoundedPathMembership(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 9, 11, 0, 0, 0, time.UTC)
	backoff := NewProcessingPathBackoff(ProcessingPathBackoffConfig{
		MaxPathCacheEntries:  2,
		PathSkipHoldDuration: time.Minute,
	})

	backoff.BlockPath("processing/a", ProcessingPathBackoffOpReadProcessingRecords, nil, now)
	backoff.BlockPath("processing/b", ProcessingPathBackoffOpReadProcessingRecords, nil, now)
	backoff.BlockPath("processing/c", ProcessingPathBackoffOpReadProcessingRecords, nil, now)

	assert.Equal(t, 2, backoff.PathCacheCount())
	assert.True(t, backoff.ShouldSkipPath("processing/a", now.Add(10*time.Second)))
	assert.True(t, backoff.ShouldSkipPath("processing/b", now.Add(10*time.Second)))
	assert.False(t, backoff.ShouldSkipPath("processing/c", now.Add(10*time.Second)))

	backoff.BlockPath("processing/d", ProcessingPathBackoffOpReadProcessingRecords, nil, now.Add(20*time.Second))

	assert.Equal(t, 2, backoff.PathCacheCount())
	assert.False(t, backoff.ShouldSkipPath("processing/d", now.Add(30*time.Second)))
	assert.Equal(t, now.Add(time.Minute), backoff.pathSkipCacheExpireAt, "overflow must not extend the shared expiry")

	assert.Zero(t, backoff.PathCacheCountAt(now.Add(time.Minute)))
	backoff.BlockPath("processing/c", ProcessingPathBackoffOpReadProcessingRecords, nil, now.Add(time.Minute))
	assert.True(t, backoff.ShouldSkipPath("processing/c", now.Add(90*time.Second)))
	assert.Equal(t, 1, backoff.PathCacheCount())
}

func TestProcessingPathBackoffLaterEntriesShareOriginalExpiry(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	backoff := NewProcessingPathBackoff(ProcessingPathBackoffConfig{})
	backoff.BlockPath("processing/a", ProcessingPathBackoffOpReadProcessingRecords, nil, now)
	backoff.BlockPath("processing/b", ProcessingPathBackoffOpReadProcessingRecords, nil, now.Add(29*time.Second))

	assert.True(t, backoff.ShouldSkipPath("processing/b", now.Add(29*time.Second)))
	assert.False(t, backoff.ShouldSkipPath("processing/a", now.Add(30*time.Second)))
	assert.False(t, backoff.ShouldSkipPath("processing/b", now.Add(30*time.Second)))
	assert.Zero(t, backoff.PathCacheCount())
}
