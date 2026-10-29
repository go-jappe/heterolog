package multi

import (
	"context"
	"errors"
	"testing"
	"time"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

type entryLimitSyncConfig struct {
	timeout          time.Duration
	maxLogEntryBytes int
}

func (c entryLimitSyncConfig) Timeout() time.Duration { return c.timeout }
func (c entryLimitSyncConfig) MaxLogEntryBytes() int  { return c.maxLogEntryBytes }

type entryLimitAsyncConfig struct {
	timeout          time.Duration
	maxParallel      int
	maxLogEntryBytes int
}

func (c entryLimitAsyncConfig) Timeout() time.Duration { return c.timeout }
func (c entryLimitAsyncConfig) MaxParallel() int       { return c.maxParallel }
func (c entryLimitAsyncConfig) MaxLogEntryBytes() int  { return c.maxLogEntryBytes }

type entryLimitDeliveryConfig struct {
	timeout          time.Duration
	maxLogEntryBytes int
}

func (c entryLimitDeliveryConfig) Timeout() time.Duration { return c.timeout }
func (c entryLimitDeliveryConfig) MaxLogEntryBytes() int  { return c.maxLogEntryBytes }

func TestConfiguredMultiLoggersRejectNonPositiveApplicationEntryLimit(t *testing.T) {
	for _, maxBytes := range []int{0, -1} {
		t.Run("sync", func(t *testing.T) {
			_, err := NewMultiLogger(entryLimitSyncConfig{
				timeout: time.Second, maxLogEntryBytes: maxBytes,
			})
			require.ErrorIs(t, err, pkglogger.ErrInvalidMaxLogEntryBytes)
		})

		t.Run("async", func(t *testing.T) {
			_, err := NewAsyncBestEffortMultiLogger(entryLimitAsyncConfig{
				timeout: time.Second, maxParallel: 1, maxLogEntryBytes: maxBytes,
			})
			require.ErrorIs(t, err, pkglogger.ErrInvalidMaxLogEntryBytes)
		})

		t.Run("delivery", func(t *testing.T) {
			_, err := newTestDeliveryMultiLoggerWithConfig(entryLimitDeliveryConfig{
				timeout: time.Second, maxLogEntryBytes: maxBytes,
			})
			require.ErrorIs(t, err, pkglogger.ErrInvalidMaxLogEntryBytes)
		})
	}
}

// asyncのconstructorも、設定の欠落・timeout・同時実行数の不備を種類ごとのsentinelで返します。
func TestNewAsyncBestEffortMultiLoggerRejectsInvalidConfig(t *testing.T) {
	_, err := NewAsyncBestEffortMultiLogger(nil)
	require.ErrorIs(t, err, ErrConfigRequired)
	for _, timeout := range []time.Duration{0, -time.Second} {
		_, err := NewAsyncBestEffortMultiLogger(entryLimitAsyncConfig{
			timeout: timeout, maxParallel: 1, maxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
		})
		require.ErrorIs(t, err, pkglogger.ErrNonPositiveSetting)
	}
	for _, maxParallel := range []int{0, -1} {
		_, err := NewAsyncBestEffortMultiLogger(entryLimitAsyncConfig{
			timeout: time.Second, maxParallel: maxParallel, maxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
		})
		require.ErrorIs(t, err, pkglogger.ErrNonPositiveSetting)
	}
}

func TestMultiLoggersApplyApplicationEntryLimitBeforeDispatch(t *testing.T) {
	acceptedBytes, err := pkglogger.MeasureLogEntrySize("ok", nil)
	require.NoError(t, err)
	oversizedMessage := "this entry is larger than ok"

	t.Run("sync returns validation error", func(t *testing.T) {
		child := &syncHandlerStub{}
		logger, err := NewMultiLogger(entryLimitSyncConfig{
			timeout:          time.Second,
			maxLogEntryBytes: acceptedBytes,
		}, child)
		require.NoError(t, err)

		err = logger.Info(context.Background(), oversizedMessage)

		require.ErrorIs(t, err, pkglogger.ErrLogEntryTooLarge)
		require.Zero(t, child.handleCalls.Load())
	})

	t.Run("async silently drops invalid entry", func(t *testing.T) {
		child := &syncHandlerStub{}
		logger, err := NewAsyncBestEffortMultiLogger(entryLimitAsyncConfig{
			timeout:          time.Second,
			maxParallel:      1,
			maxLogEntryBytes: acceptedBytes,
		}, child)
		require.NoError(t, err)

		logger.Info(context.Background(), oversizedMessage)
		require.NoError(t, logger.Close())

		require.Zero(t, child.handleCalls.Load())
	})

	t.Run("delivery rejects every target", func(t *testing.T) {
		first := &deliveryHandlerStub{backend: "first"}
		second := &deliveryHandlerStub{backend: "second"}
		logger, err := newTestDeliveryMultiLoggerWithConfig(entryLimitDeliveryConfig{
			timeout:          time.Second,
			maxLogEntryBytes: acceptedBytes,
		}, first, second)
		require.NoError(t, err)

		report, err := logger.Info(context.Background(), oversizedMessage)

		require.ErrorIs(t, err, pkglogger.ErrLogEntryTooLarge)
		require.Zero(t, first.handleCalls.Load())
		require.Zero(t, second.handleCalls.Load())
		require.Len(t, report.Targets, 2)
		for _, target := range report.Targets {
			require.Equal(t, pkglogger.DeliveryRejected, target.Status)
			require.ErrorIs(t, target.Err, pkglogger.ErrLogEntryTooLarge)
		}
	})
}

func TestMultiLoggersDoNotReconstructOrRemeasureCompletedEntry(t *testing.T) {
	// application入力のsize validationは各MultiLoggerのapplication向けmethodで行います。
	// runAllは生成済みentryを全childへ渡す内部処理なので、config上限より
	// 大きいMessageでも再測定せずchildへ渡します。
	largeMessage := string(make([]byte, pkglogger.DefaultMaxLogEntryBytes+1))

	t.Run("sync", func(t *testing.T) {
		child := &syncHandlerStub{}
		m, err := NewMultiLogger(entryLimitSyncConfig{
			timeout: time.Second, maxLogEntryBytes: 1,
		}, child)
		require.NoError(t, err)
		require.NoError(t, m.runAll(context.Background(), testLogEntry(pkglogger.LogLevelInfo, largeMessage)))
		require.Equal(t, int32(1), child.handleCalls.Load())
	})

	t.Run("async", func(t *testing.T) {
		child := &syncHandlerStub{}
		m, err := NewAsyncBestEffortMultiLogger(entryLimitAsyncConfig{
			timeout: time.Second, maxParallel: 1, maxLogEntryBytes: 1,
		}, child)
		require.NoError(t, err)
		m.runAll(context.Background(), testLogEntry(pkglogger.LogLevelInfo, largeMessage))
		require.NoError(t, m.Close())
		require.Equal(t, int32(1), child.handleCalls.Load())
	})

	t.Run("delivery", func(t *testing.T) {
		child := &deliveryHandlerStub{backend: "child"}
		m, err := newTestDeliveryMultiLoggerWithConfig(entryLimitDeliveryConfig{
			timeout: time.Second, maxLogEntryBytes: 1,
		}, child)
		require.NoError(t, err)
		report, err := m.runAll(context.Background(), testLogEntry(pkglogger.LogLevelInfo, largeMessage))
		require.NoError(t, err)
		require.Len(t, report.Targets, 1)
		require.Equal(t, int32(1), child.handleCalls.Load())
	})
}

func TestDeliveryMultiLoggerConvertsTopLevelValidationFailureBeforeDispatch(t *testing.T) {
	first := &deliveryHandlerStub{backend: "first"}
	second := &deliveryHandlerStub{backend: "second"}
	m := newTestDeliveryMultiLogger(first, second)
	cause := fmtTestEntryTooLargeError()

	report, err := m.rejectBeforeDispatch("delivery.Info", cause)
	require.ErrorIs(t, err, cause)
	require.Len(t, report.Targets, 2)
	for _, target := range report.Targets {
		require.Equal(t, pkglogger.DeliveryRejected, target.Status)
		require.ErrorIs(t, target.Err, cause)
	}
	require.Equal(t, int32(0), first.handleCalls.Load())
	require.Equal(t, int32(0), second.handleCalls.Load())
}

func fmtTestEntryTooLargeError() error {
	return errors.Join(pkglogger.ErrLogEntryTooLarge, errors.New("test application boundary"))
}
