package cloudwatchlogs

import (
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"

	"github.com/stretchr/testify/require"
)

func TestDetachBufferedBatchUsesRequestedContractCapacity(t *testing.T) {
	const (
		nextBufferCapacity = 3
		expandedCapacity   = 12
	)
	entry := &logging.LogEntry{Message: "buffered"}
	logger := &CloudWatchLogsLogger{
		entries: append(make([]*logging.LogEntry, 0, expandedCapacity), entry),
	}

	snapshot := logger.detachBufferedBatch(nextBufferCapacity)

	// 切り離したsnapshotの容量は実装詳細なので検証せず、内容と次のbufferの容量だけを検証します。
	require.Equal(t, []*logging.LogEntry{entry}, snapshot)
	require.Empty(t, logger.entries)
	require.Equal(t, nextBufferCapacity, cap(logger.entries))
}
