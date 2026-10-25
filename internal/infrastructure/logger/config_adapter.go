package logger

import (
	multi "github.com/go-jappe/heterolog/internal/infrastructure/logger/multi"
	"github.com/go-jappe/heterolog/pkg/logger"
	"time"
)

type multiLoggerAdapter struct {
	timeout          time.Duration
	maxLogEntryBytes int
}

func NewMultiLoggerAdapter(cfg logger.LoggerConfig) multi.MultiLoggerConfigProvider {
	return multiLoggerAdapter{
		timeout:          cfg.Timeout,
		maxLogEntryBytes: cfg.MaxLogEntryBytes,
	}
}

func (a multiLoggerAdapter) Timeout() time.Duration {
	return a.timeout
}

func (a multiLoggerAdapter) MaxLogEntryBytes() int {
	return a.maxLogEntryBytes
}

type asyncBestEffortMultiLoggerAdapter struct {
	timeout          time.Duration
	maxParallel      int
	maxLogEntryBytes int
}

func NewAsyncBestEffortMultiLoggerAdapter(cfg logger.AsyncBestEffortMultiLoggerConfig) multi.AsyncBestEffortMultiLoggerConfigProvider {
	return asyncBestEffortMultiLoggerAdapter{
		timeout:          cfg.Timeout,
		maxParallel:      cfg.MaxParallel,
		maxLogEntryBytes: cfg.MaxLogEntryBytes,
	}
}

func (a asyncBestEffortMultiLoggerAdapter) Timeout() time.Duration {
	return a.timeout
}

func (a asyncBestEffortMultiLoggerAdapter) MaxParallel() int {
	return a.maxParallel
}

func (a asyncBestEffortMultiLoggerAdapter) MaxLogEntryBytes() int {
	return a.maxLogEntryBytes
}

type deliveryMultiLoggerAdapter struct {
	timeout          time.Duration
	maxLogEntryBytes int
}

func NewDeliveryMultiLoggerAdapter(cfg logger.DeliveryLoggerConfig) multi.DeliveryMultiLoggerConfigProvider {
	return deliveryMultiLoggerAdapter{
		timeout:          cfg.Timeout,
		maxLogEntryBytes: cfg.MaxLogEntryBytes,
	}
}

func (a deliveryMultiLoggerAdapter) Timeout() time.Duration {
	return a.timeout
}

func (a deliveryMultiLoggerAdapter) MaxLogEntryBytes() int {
	return a.maxLogEntryBytes
}
