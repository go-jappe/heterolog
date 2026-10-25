package spoolrunner

import (
	"github.com/go-jappe/heterolog/internal/config/spool/spoollifecycle"
	"time"
)

const (
	DefaultRunnerInterval = 100 * time.Millisecond
	DefaultStopOnError    = false

	DefaultFinalizeRetryDelay = 1000 * time.Millisecond
	DefaultMaxFinalizeEntries = 4098

	// DefaultMaxRetryAfterは、backendがRetry-Afterで指定した再送待機に適用する上限の既定値です。
	DefaultMaxRetryAfter = time.Hour
)

type SpoolRunnerConfig struct {
	OpensearchConfig     OpensearchSpoolRunnerConfig
	LokiConfig           LokiSpoolRunnerConfig
	CloudWatchLogsConfig CloudWatchLogsSpoolRunnerConfig
	ClickHouseConfig     ClickHouseSpoolRunnerConfig
}

type BackendSpoolRunnerConfig struct {
	Runner    RunnerConfig
	LifeCycle spoollifecycle.LifecycleConfig // spool lifecycle fsstoreの設定
	Worker    WorkerConfig
}

type OpensearchSpoolRunnerConfig = BackendSpoolRunnerConfig
type LokiSpoolRunnerConfig = BackendSpoolRunnerConfig
type CloudWatchLogsSpoolRunnerConfig = BackendSpoolRunnerConfig
type ClickHouseSpoolRunnerConfig = BackendSpoolRunnerConfig

type RunnerConfig struct {
	// Interval は、1回の RunOnce が終わってから次の RunOnce を開始するまでの待機時間。
	// 正の値が必須で、0や負値を既定値へ補正しません。
	Interval time.Duration

	// StopOnError は、RunOnce が error を返したときに loop を停止するかどうか。
	StopOnError bool
}

type WorkerConfig struct {
	Finalize FinalizeConfig
	// MaxRetryAfterは、backendがRetry-Afterで指定した再送待機の上限です。より長い指定は
	// この時間に短縮し、内部ログへ警告します。通常送信で受け取った指定と、spoolに保存済みの
	// 再送待機にも適用します。0はこのfieldを持たない既存設定との互換のため
	// DefaultMaxRetryAfterとして扱い、無制限にはしません。負値は設定errorです。
	MaxRetryAfter time.Duration
}

type FinalizeConfig struct {
	// RetryDelayはprocessing fileの状態遷移に失敗してから再試行するまでの待機時間です。
	// 正の値が必須で、0や負値を既定値へ補正しません。
	RetryDelay time.Duration
	// MaxEntriesは状態遷移retryをmemory上に保持する最大件数です。
	// 正の値が必須です。
	MaxEntries int
}
