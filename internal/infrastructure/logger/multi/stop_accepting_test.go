package multi

import (
	"context"
	"testing"
	"time"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

func TestMultiLoggerStopAcceptingRejectsWithoutClosingChildren(t *testing.T) {
	child := &syncHandlerStub{}
	logger := newTestMultiLogger(child)

	logger.StopAccepting()
	logger.StopAccepting()

	err := logger.Info(context.Background(), "rejected after intake stopped")
	require.ErrorIs(t, err, pkglogger.ErrLoggerClosed)
	require.Equal(t, int32(0), child.handleCalls.Load())
	require.Equal(t, int32(0), child.closeCalls.Load())

	require.NoError(t, logger.Close())
	require.Equal(t, int32(1), child.closeCalls.Load())
	require.NoError(t, logger.Close())
	require.Equal(t, int32(1), child.closeCalls.Load())
}

func TestAsyncBestEffortMultiLoggerStopAcceptingRejectsWithoutClosingChildren(t *testing.T) {
	child := &syncHandlerStub{}
	logger, err := NewAsyncBestEffortMultiLogger(
		asyncBestEffortConfigStub{
			timeout:     time.Second,
			maxParallel: 1,
		},
		child,
	)
	require.NoError(t, err)

	logger.StopAccepting()
	logger.StopAccepting()
	logger.Info(context.Background(), "rejected after intake stopped")

	require.Equal(t, int32(0), child.closeCalls.Load())
	require.NoError(t, logger.Close())
	// Closeは受理済みの非同期処理を待つため、ここで0件なら停止後の要求は受理されていません。
	require.Equal(t, int32(0), child.handleCalls.Load())
	require.Equal(t, int32(1), child.closeCalls.Load())
	require.NoError(t, logger.Close())
	require.Equal(t, int32(1), child.closeCalls.Load())
}

func TestDeliveryMultiLoggerStopAcceptingRejectsWithoutClosingChildren(t *testing.T) {
	child := &deliveryHandlerStub{backend: "delivery-test"}
	logger := newTestDeliveryMultiLogger(child)

	logger.StopAccepting()
	logger.StopAccepting()

	report, err := logger.Info(context.Background(), "rejected after intake stopped")
	require.ErrorIs(t, err, pkglogger.ErrLoggerClosed)
	require.Len(t, report.Targets, 1)
	require.Equal(t, pkglogger.DeliveryRejected, report.Targets[0].Status)
	require.Equal(t, int32(0), child.handleCalls.Load())
	require.Equal(t, int32(0), child.closeCalls.Load())

	require.NoError(t, logger.Close())
	require.Equal(t, int32(1), child.closeCalls.Load())
	require.NoError(t, logger.Close())
	require.Equal(t, int32(1), child.closeCalls.Load())
}
