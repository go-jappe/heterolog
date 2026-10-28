package opensearch

import (
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

func TestDetachBufferedBatchResetsCapacityFromConfiguredLimit(t *testing.T) {
	const (
		configuredCapacity = 3
		expandedCapacity   = 12
	)
	cfg := NewOpenSearchLoggerConfigAdapter(pkglogger.OpenSearchLoggerBaseConfig{
		MaxBulkDocument: configuredCapacity,
	})
	entry := &logging.LogEntry{Message: "buffered"}
	logger := &OpenSearchLogger{
		LoggerBase: &LoggerBase{config: cfg},
		entries:    append(make([]*logging.LogEntry, 0, expandedCapacity), entry),
	}

	batch, _ := logger.detachBufferedBatch()

	// 切り離したsnapshotの容量は実装詳細なので検証せず、内容と次のbufferの容量だけを検証します。
	require.Equal(t, []*logging.LogEntry{entry}, batch)
	require.Empty(t, logger.entries)
	require.Equal(t, configuredCapacity, cap(logger.entries))
}
