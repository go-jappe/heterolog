package config

import (
	"errors"
	"path/filepath"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	"github.com/go-jappe/heterolog/internal/infrastructure/storageowner"
	"github.com/go-jappe/heterolog/internal/infrastructure/traces"
	pkgenv "github.com/go-jappe/heterolog/pkg/env"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
)

// defaultTracesFileDirは、spanのfileを置くdirectoryの既定値です。ログのfileと同じく、
// HETEROLOG_SYSTEM_LOG_DIRとHETEROLOG_TARGETを基準に解決します。
const defaultTracesFileDir = "/traces"

// tracesFileNameは、spanを書くfileの名前です。
const tracesFileName = "spans.jsonl"

// TracesConfigは、HETEROLOG_ENABLE_TRACESとHETEROLOG_TRACES_*から読んだtraceの設定です。
// 値の検証は、公開APIのLoadEnvConfigが行います。
type TracesConfig struct {
	// Enabledは、HETEROLOG_ENABLE_TRACESでtraceを有効にしたことを表します。無効なら、
	// ほかの項目は読み込まず、ゼロ値のままです。
	Enabled bool
	// Tracesは、送り先などの設定です。File.Pathは空で、MapTelemetryConfigがFileDirから
	// 組み立てます。
	Traces pkgtelemetry.TracesConfig
	// FileDirは、spanのfileを置くdirectoryです。
	FileDir string
	// FailureStoreは、otlpの送り先のfailure storeの設定です。保存先のpathなど、ログと共通の
	// 項目は、MapTelemetryConfigが組み立てます。
	FailureStore TracesFailureStoreConfig
}

// loadTracesConfigは、traceの設定を読み込みます。HETEROLOG_ENABLE_TRACESがfalseなら、
// ほかのkeyは読みません。
func loadTracesConfig(env pkgenv.Lookup) (TracesConfig, error) {
	enabled, err := env.GetEnvBool("HETEROLOG_ENABLE_TRACES", false)
	if err != nil || !enabled {
		return TracesConfig{}, err
	}
	defaults := traces.DefaultConfig()
	cfg := TracesConfig{Enabled: true, Traces: defaults}
	for _, name := range env.GetEnvSliceString("HETEROLOG_TRACES_TYPE", []string{string(pkgtelemetry.TraceExporterOTLP)}) {
		cfg.Traces.Exporters = append(cfg.Traces.Exporters, pkgtelemetry.TraceExporter(name))
	}
	samplingRatio, samplingRatioErr := env.GetEnvFloat64("HETEROLOG_TRACES_SAMPLING_RATIO", defaults.SamplingRatio)
	queueSize, queueSizeErr := env.GetEnvInt("HETEROLOG_TRACES_QUEUE_SIZE", defaults.QueueSize)
	batchSize, batchSizeErr := env.GetEnvInt("HETEROLOG_TRACES_BATCH_SIZE", defaults.BatchSize)
	exportInterval, exportIntervalErr := env.GetEnvDurationMillisecond(
		"HETEROLOG_TRACES_EXPORT_INTERVAL_MILLISECOND", defaults.ExportInterval,
	)
	cfg.Traces.SamplingRatio = samplingRatio
	cfg.Traces.QueueSize = queueSize
	cfg.Traces.BatchSize = batchSize
	cfg.Traces.ExportInterval = exportInterval

	headers, headersErr := loadHeaderEnv(env, "HETEROLOG_TRACES_OTLP_HEADERS", nil)
	timeout, timeoutErr := env.GetEnvDuration("HETEROLOG_TRACES_OTLP_TIMEOUT_SECOND", defaults.OTLP.Timeout)
	retries, retriesErr := env.GetEnvInt("HETEROLOG_TRACES_OTLP_RETRIES", defaults.OTLP.Retries)
	retryBaseDelay, retryBaseDelayErr := env.GetEnvDurationMillisecond(
		"HETEROLOG_TRACES_OTLP_RETRY_BASE_DELAY_MILLISECOND", defaults.OTLP.RetryBaseDelay,
	)
	retryMaxDelay, retryMaxDelayErr := env.GetEnvDurationMillisecond(
		"HETEROLOG_TRACES_OTLP_RETRY_MAX_DELAY_MILLISECOND", defaults.OTLP.RetryMaxDelay,
	)
	circuitBreaker, circuitBreakerErr := env.GetEnvBool("HETEROLOG_ENABLE_TRACES_OTLP_CIRCUIT_BREAKER", defaults.OTLP.CircuitBreaker)
	cfg.Traces.OTLP = pkgtelemetry.OTLPConfig{
		Endpoint:       env.GetEnvString("HETEROLOG_TRACES_OTLP_ENDPOINT", defaults.OTLP.Endpoint),
		Headers:        headers,
		Compression:    env.GetEnvString("HETEROLOG_TRACES_OTLP_COMPRESSION", defaults.OTLP.Compression),
		Timeout:        timeout,
		Retries:        retries,
		RetryBaseDelay: retryBaseDelay,
		RetryMaxDelay:  retryMaxDelay,
		CircuitBreaker: circuitBreaker,
	}

	cfg.FileDir = env.GetEnvString("HETEROLOG_TRACES_FILE_DIR", defaultTracesFileDir)
	failureStore, failureStoreErr := loadTracesFailureStoreConfig(env)
	cfg.FailureStore = failureStore
	return cfg, errors.Join(
		samplingRatioErr, queueSizeErr, batchSizeErr, exportIntervalErr,
		headersErr, timeoutErr, retriesErr, retryBaseDelayErr, retryMaxDelayErr, circuitBreakerErr, failureStoreErr,
	)
}

// MapTelemetryConfigは、読み込んだ設定を、NewTelemetryへ渡す設定にします。Resourceは、
// ログと同じ項目（HETEROLOG_APP_NAME・HETEROLOG_APP_ENVなど）から作ります。traceを
// 有効にしていなければ、送り先のない設定を返します。
func MapTelemetryConfig(a AppConfig) pkgtelemetry.Config {
	cfg := pkgtelemetry.Config{Resource: newLoggerResource(&a)}
	if !a.Traces.Enabled {
		cfg.Traces = traces.DefaultConfig()
		return cfg
	}
	cfg.Traces = a.Traces.Traces
	cfg.Traces.File.Path = filepath.Join(a.FsstoreConfig.LogPath, a.RuntimeConfig.Target, a.Traces.FileDir, tracesFileName)
	cfg.Traces.OTLP.FailureStore = pkgtelemetry.FailureStoreConfig{
		Store: newFailureStoreConfig(
			&a, checkpointSourceTracesOTLP, storageowner.Telemetry, a.Traces.FailureStore.FailureHandler, logconfig.LogBodyConfig{},
		),
		Replay:           a.Traces.FailureStore.Replay,
		ReplaySupervisor: a.Traces.FailureStore.ReplaySupervisor,
	}
	return cfg
}
