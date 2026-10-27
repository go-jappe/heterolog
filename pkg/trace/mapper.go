package trace

import "time"

type OpenSearchSpanConfig struct {
	IsDebug                bool
	AppName                string
	Target                 string
	IndexNamePrefix        string
	FlushTimeout           time.Duration
	BatchSize              int
	SamplingEnabled        bool
	RecordByTraceIdEnabled bool
	RecordByTraceIdRate    int
	SamplingRate           int
	MaxRetries             int
	RetryWaitTime          time.Duration
	OnFallbackLogConfig    []string
}
