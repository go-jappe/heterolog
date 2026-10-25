package trace

import "time"

const (
	DefaultIndexNamePrefixOpenSearchSpan    = "otel-span-"
	DefaultOpenSearchFlushTimeout           = 5 * time.Second
	DefaultOpenSearchBatchSize              = 1000
	DefaultOpenSearchSamplingEnabled        = true
	DefaultOpenSearchRecordByTraceIdEnabled = true
	DefaultRecordByTraceIdRate              = 10
	DefaultOpenSearchSamplingRate           = 1
	OpenSearchMaxRetryAttempts              = 3
	OpenSearchRetryWaitTime                 = 100 * time.Millisecond
)

type OpenSearchSpanConfig struct {
	IndexNamePrefix        string
	FlushTimeout           time.Duration
	BatchSize              int
	SamplingEnabled        bool
	RecordByTraceIdEnabled bool
	RecordByTraceIdRate    int
	SamplingRate           int
	MaxRetryAttempts       int
	RetryWaitTime          time.Duration
	OnFallbackLogConfig    []string
}
