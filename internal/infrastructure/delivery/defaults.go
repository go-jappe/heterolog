package delivery

import (
	"time"

	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/spool/spoollifecycle"
	"github.com/go-jappe/heterolog/pkg/spool/spoolrunner"
	"github.com/go-jappe/heterolog/pkg/spool/spoolsupervisor"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
)

// spanとmetricsのOTLP/HTTPの送り先と、failure storeの既定値です。.env.exampleと文書の値も
// 合わせます。
const (
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

// DefaultOTLPConfigは、endpointへ送るOTLP/HTTPの送り先の既定の設定を返します。failure
// storeは、何も残さない設定（DurableTypeが空）で、選んだときに使う同時実行の上限と
// 送り直しの設定を埋めます。
func DefaultOTLPConfig(endpoint string) pkgtelemetry.OTLPConfig {
	return pkgtelemetry.OTLPConfig{
		Endpoint:       endpoint,
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
	}
}
