package clickhouse

import (
	"context"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

type capacityTestClickHouseFlushManager struct{}

func (capacityTestClickHouseFlushManager) Flush(
	context.Context,
	[]*logging.LogEntry,
) error {
	return nil
}

func (capacityTestClickHouseFlushManager) FlushWithNoFallback(
	context.Context,
	[]*logging.LogEntry,
) {
}

func (capacityTestClickHouseFlushManager) Close(context.Context) error {
	return nil
}

func TestFlushBufferedResetsCapacityFromConfiguredLimit(t *testing.T) {
	const (
		configuredCapacity = 3
		expandedCapacity   = 12
	)
	cfg := NewClickHouseLoggerConfigAdapter(pkglogger.ClickHouseLoggerBaseConfig{
		MaxEntries: configuredCapacity,
	})
	logger := &ClickHouseLogger{
		LoggerBase:         &LoggerBase{config: cfg},
		entries:            make([]*logging.LogEntry, 1, expandedCapacity),
		entryBytes:         make([]int, 1, expandedCapacity),
		entryRequiresFlush: make([]bool, 1, expandedCapacity),
		flushManager:       capacityTestClickHouseFlushManager{},
		Tracer:             observer.EnsureTracer(nil),
	}

	require.NoError(t, logger.flushBuffered(context.Background()))
	require.Empty(t, logger.entries)
	require.Empty(t, logger.entryBytes)
	require.Empty(t, logger.entryRequiresFlush)
	require.Equal(t, configuredCapacity, cap(logger.entries))
	require.Equal(t, configuredCapacity, cap(logger.entryBytes))
	require.Equal(t, configuredCapacity, cap(logger.entryRequiresFlush))
}
