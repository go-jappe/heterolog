package traces

import (
	"time"

	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/spool/spoollifecycle"
	"github.com/go-jappe/heterolog/pkg/spool/spoolrunner"
	"github.com/go-jappe/heterolog/pkg/spool/spoolsupervisor"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
)

// traceの配送の既定値です。.env.exampleと文書の値も合わせます。待ち行列とbatchの大きさ、
// 送る間隔は、OpenTelemetry SDKのBatchSpanProcessorの既定値と同じです。
const (
	DefaultSamplingRatio  = 1.0
	DefaultQueueSize      = 2048
	DefaultBatchSize      = 512
	DefaultExportInterval = 5 * time.Second

	// DefaultOTLPEndpointは、OpenTelemetryの既定のOTLP/HTTPの受信先です。
	DefaultOTLPEndpoint       = "http://localhost:4318/v1/traces"
	DefaultOTLPCompression    = "gzip"
	DefaultOTLPTimeout        = 10 * time.Second
	DefaultOTLPRetries        = 3
	DefaultOTLPRetryBaseDelay = time.Second
	DefaultOTLPRetryMaxDelay  = 5 * time.Second

	// failure storeの同時実行の上限と、spoolから送り直す設定の既定値です。ログの出力先の
	// 既定値と同じです。
	DefaultFailureStoreMaxConcurrent = 8
	DefaultReplayInterval            = 100 * time.Millisecond
	DefaultReplayFinalizeRetryDelay  = time.Second
	DefaultReplayMaxFinalizeEntries  = 4098
	DefaultReplayMaxRetryAfter       = time.Hour
	DefaultReplaySizePurgeInterval   = time.Minute
	DefaultReplayRestartDelay        = time.Second
)

// DefaultConfigは、送り先を選んでいないtraceの既定の設定を返します。
func DefaultConfig() pkgtelemetry.TracesConfig {
	return pkgtelemetry.TracesConfig{
		SamplingRatio:  DefaultSamplingRatio,
		QueueSize:      DefaultQueueSize,
		BatchSize:      DefaultBatchSize,
		ExportInterval: DefaultExportInterval,
		OTLP: pkgtelemetry.OTLPConfig{
			Endpoint:       DefaultOTLPEndpoint,
			Compression:    DefaultOTLPCompression,
			Timeout:        DefaultOTLPTimeout,
			Retries:        DefaultOTLPRetries,
			RetryBaseDelay: DefaultOTLPRetryBaseDelay,
			RetryMaxDelay:  DefaultOTLPRetryMaxDelay,
			FailureStore: pkgtelemetry.FailureStoreConfig{
				Store: pkgdurable.FailureStoreConfig{
					MaxConcurrent: DefaultFailureStoreMaxConcurrent,
					SpillType:     []pkgdurable.SpillType{pkgdurable.SpillType(pkgdurable.SpillDestinationStdout)},
				},
				Replay: spoolrunner.BackendSpoolRunnerConfig{
					Runner: spoolrunner.RunnerConfig{Interval: DefaultReplayInterval},
					Worker: spoolrunner.WorkerConfig{
						Finalize: spoolrunner.FinalizeConfig{
							RetryDelay: DefaultReplayFinalizeRetryDelay,
							MaxEntries: DefaultReplayMaxFinalizeEntries,
						},
						MaxRetryAfter: DefaultReplayMaxRetryAfter,
					},
					LifeCycle: spoollifecycle.LifecycleConfig{SizePurgeInterval: DefaultReplaySizePurgeInterval},
				},
				ReplaySupervisor: spoolsupervisor.RunnerSupervisorConfig{RestartDelay: DefaultReplayRestartDelay},
			},
		},
	}
}
