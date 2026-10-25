package logger

import (
	"testing"
	"time"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/assert"
)

func TestMultiLoggerAdaptersExposeTopLevelConfig(t *testing.T) {
	syncCfg := pkglogger.LoggerConfig{
		Timeout:          5 * time.Second,
		MaxLogEntryBytes: 101,
	}
	syncAdapter := NewMultiLoggerAdapter(syncCfg)
	syncCfg.Timeout = time.Second
	assert.Equal(t, 5*time.Second, syncAdapter.Timeout())
	assert.Equal(t, 101, syncAdapter.MaxLogEntryBytes())

	asyncCfg := pkglogger.AsyncBestEffortMultiLoggerConfig{
		Timeout:          3 * time.Second,
		MaxLogEntryBytes: 202,
		MaxParallel:      64,
	}
	asyncAdapter := NewAsyncBestEffortMultiLoggerAdapter(asyncCfg)
	asyncCfg.Timeout = time.Second
	asyncCfg.MaxParallel = 1
	assert.Equal(t, 3*time.Second, asyncAdapter.Timeout())
	assert.Equal(t, 64, asyncAdapter.MaxParallel())
	assert.Equal(t, 202, asyncAdapter.MaxLogEntryBytes())

	deliveryCfg := pkglogger.DeliveryLoggerConfig{
		Timeout:          7 * time.Second,
		MaxLogEntryBytes: 303,
	}
	deliveryAdapter := NewDeliveryMultiLoggerAdapter(deliveryCfg)
	deliveryCfg.Timeout = time.Second
	assert.Equal(t, 7*time.Second, deliveryAdapter.Timeout())
	assert.Equal(t, 303, deliveryAdapter.MaxLogEntryBytes())
}
