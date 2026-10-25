package logger

import (
	"context"
	"sync"
	"testing"
	"time"

	clickhouselogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/clickhouse"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/multi"
	loggerresource "github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

func TestMultiLoggerCloseFlushesBufferedClickHouseEntries(t *testing.T) {
	client := &recordingClickHouseClient{}
	cfg := clickhouselogger.NewClickHouseLoggerConfigAdapter(pkglogger.ClickHouseLoggerBaseConfig{
		TimeConfig: pkglogger.TimeConfig{},
		RuntimeConfig: pkglogger.RuntimeConfig{
			AppName:  "clickhouse-close-test",
			Target:   "clickhouse-close-test",
			LogLevel: pkglogger.LogLevelDebug,
		},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat:        time.RFC3339Nano,
			ErrorTraceDepth:          1,
			ErrorTraceDepthDebugMode: 1,
		},
		FlushInterval:           time.Hour,
		MaxEntries:              1000,
		BufferFlushBytes:        1024 * 1024,
		FallbackQueueMaxEntries: 1000,
		MaxInsertRequestBytes:   16 * 1024 * 1024,
	})
	fmCfg := clickhouselogger.NewFlushManagerConfigAdapter(pkglogger.ClickHouseFlushManagerConfig{
		Resource: pkglogger.Resource{
			ServiceName: "clickhouse-close-test",
			Cloud:       &pkglogger.CloudResourceInfo{},
		},
	})
	res := loggerresource.NewResourceAdapter(pkglogger.Resource{
		ServiceName: "clickhouse-close-test",
		Cloud:       &pkglogger.CloudResourceInfo{},
	})

	base, err := clickhouselogger.NewClickHouseLogger(
		context.Background(),
		cfg,
		client,
		fmCfg,
		res,
		nil,
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	logger, err := multi.NewMultiLogger(rootMultiLoggerTestConfig{}, base)
	require.NoError(t, err)
	require.NoError(t, logger.Info(context.Background(), "flush me from app close"))
	require.Equal(t, 0, client.batchCount())

	require.NoError(t, logger.Close())
	require.Equal(t, 1, client.batchCount())
	require.Equal(t, "flush me from app close", client.firstMessage())
}

type recordingClickHouseClient struct {
	mu      sync.Mutex
	batches [][]pkgclickhouse.LogRecord
}

func (c *recordingClickHouseClient) InsertLogs(
	ctx context.Context,
	records []pkgclickhouse.LogRecord,
) (*pkgclickhouse.InsertResponse, error) {
	return c.InsertLogsWithOptions(ctx, records, pkgclickhouse.InsertOptions{})
}

func (c *recordingClickHouseClient) InsertLogsWithOptions(
	ctx context.Context,
	records []pkgclickhouse.LogRecord,
	opts pkgclickhouse.InsertOptions,
) (*pkgclickhouse.InsertResponse, error) {
	_ = opts
	c.mu.Lock()
	defer c.mu.Unlock()

	c.batches = append(c.batches, append([]pkgclickhouse.LogRecord(nil), records...))
	return &pkgclickhouse.InsertResponse{StatusCode: 200}, nil
}

func (c *recordingClickHouseClient) InsertLog(
	ctx context.Context,
	record pkgclickhouse.LogRecord,
) (*pkgclickhouse.InsertResponse, error) {
	return c.InsertLogs(ctx, []pkgclickhouse.LogRecord{record})
}

func (c *recordingClickHouseClient) InsertLogWithOptions(
	ctx context.Context,
	record pkgclickhouse.LogRecord,
	opts pkgclickhouse.InsertOptions,
) (*pkgclickhouse.InsertResponse, error) {
	return c.InsertLogsWithOptions(ctx, []pkgclickhouse.LogRecord{record}, opts)
}

func (c *recordingClickHouseClient) batchCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.batches)
}

func (c *recordingClickHouseClient) firstMessage() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.batches) == 0 || len(c.batches[0]) == 0 {
		return ""
	}
	return c.batches[0][0].Message
}
