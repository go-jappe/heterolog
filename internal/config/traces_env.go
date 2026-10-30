package config

import (
	"errors"
	"path/filepath"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	metricsdelivery "github.com/go-jappe/heterolog/internal/infrastructure/metrics"
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
	FailureStore TelemetryFailureStoreConfig
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

	otlpCfg, otlpErr := loadHTTPEnv(env, httpKeys{
		Endpoint:       "HETEROLOG_TRACES_OTLP_ENDPOINT",
		Headers:        "HETEROLOG_TRACES_OTLP_HEADERS",
		Compression:    "HETEROLOG_TRACES_OTLP_COMPRESSION",
		Timeout:        "HETEROLOG_TRACES_OTLP_TIMEOUT_SECOND",
		Retries:        "HETEROLOG_TRACES_OTLP_RETRIES",
		RetryBaseDelay: "HETEROLOG_TRACES_OTLP_RETRY_BASE_DELAY_MILLISECOND",
		RetryMaxDelay:  "HETEROLOG_TRACES_OTLP_RETRY_MAX_DELAY_MILLISECOND",
		CircuitBreaker: "HETEROLOG_ENABLE_TRACES_OTLP_CIRCUIT_BREAKER",
	}, defaults.OTLP)
	cfg.Traces.OTLP = otlpCfg

	cfg.FileDir = env.GetEnvString("HETEROLOG_TRACES_FILE_DIR", defaultTracesFileDir)
	failureStore, failureStoreErr := loadTelemetryFailureStoreConfig(env, tracesOTLPFailureStoreKeys)
	cfg.FailureStore = failureStore
	return cfg, errors.Join(
		samplingRatioErr, queueSizeErr, batchSizeErr, exportIntervalErr,
		otlpErr, failureStoreErr,
	)
}

// MapTelemetryConfigは、読み込んだ設定を、NewTelemetryへ渡す設定にします。Resourceは、
// ログと同じ項目（HETEROLOG_APP_NAME・HETEROLOG_APP_ENVなど）から作ります。traceや
// metricsを有効にしていなければ、その送り先のない設定にします。
func MapTelemetryConfig(a AppConfig) pkgtelemetry.Config {
	cfg := pkgtelemetry.Config{
		Resource: newLoggerResource(&a),
		Traces:   traces.DefaultConfig(),
		Metrics:  metricsdelivery.DefaultConfig(),
	}
	if a.Traces.Enabled {
		cfg.Traces = a.Traces.Traces
		cfg.Traces.File.Path = filepath.Join(a.FsstoreConfig.LogPath, a.RuntimeConfig.Target, a.Traces.FileDir, tracesFileName)
		cfg.Traces.OTLP.FailureStore = mapTelemetryFailureStoreConfig(&a, checkpointSourceTracesOTLP, a.Traces.FailureStore)
	}
	if a.Metrics.Enabled {
		cfg.Metrics = a.Metrics.Metrics
		cfg.Metrics.File.Path = filepath.Join(a.FsstoreConfig.LogPath, a.RuntimeConfig.Target, a.Metrics.FileDir, metricsFileName)
		cfg.Metrics.OTLP.FailureStore = mapTelemetryFailureStoreConfig(&a, checkpointSourceMetricsOTLP, a.Metrics.FailureStore)
		cfg.Metrics.VictoriaMetrics.FailureStore = mapTelemetryFailureStoreConfig(
			&a, checkpointSourceMetricsVictoriaMetrics, a.Metrics.VictoriaMetricsFailureStore,
		)
	}
	return cfg
}

// mapTelemetryFailureStoreConfigは、送り先のfailure storeの設定に、ログの出力先と同じく、
// 保存先のpathなどの共通の項目を足します。object storageへ送れなかった圧縮fileの保存先は、
// Telemetryが所有します。
func mapTelemetryFailureStoreConfig(a *AppConfig, checkpointSource archiveCheckpointSourcePrefix, fs TelemetryFailureStoreConfig) pkgtelemetry.FailureStoreConfig {
	return pkgtelemetry.FailureStoreConfig{
		Store:            newFailureStoreConfig(a, checkpointSource, storageowner.Telemetry, fs.FailureHandler, logconfig.LogBodyConfig{}),
		Replay:           fs.Replay,
		ReplaySupervisor: fs.ReplaySupervisor,
	}
}

// httpKeysは、HTTPの送り先（OTLP/HTTPとVictoriaMetrics）の環境変数のkeyです。
type httpKeys struct {
	Endpoint, Headers, Compression, Timeout, Retries, RetryBaseDelay, RetryMaxDelay, CircuitBreaker string
}

// loadHTTPEnvは、HTTPの送り先の設定を、keysのkeyから読みます。keyがなければ、defaultsの
// 値を使います。failure storeの設定は、defaultsのままです。
func loadHTTPEnv(env pkgenv.Lookup, keys httpKeys, defaults pkgtelemetry.HTTPConfig) (pkgtelemetry.HTTPConfig, error) {
	headers, headersErr := loadHeaderEnv(env, keys.Headers, nil)
	timeout, timeoutErr := env.GetEnvDuration(keys.Timeout, defaults.Timeout)
	retries, retriesErr := env.GetEnvInt(keys.Retries, defaults.Retries)
	retryBaseDelay, retryBaseDelayErr := env.GetEnvDurationMillisecond(keys.RetryBaseDelay, defaults.RetryBaseDelay)
	retryMaxDelay, retryMaxDelayErr := env.GetEnvDurationMillisecond(keys.RetryMaxDelay, defaults.RetryMaxDelay)
	circuitBreaker, circuitBreakerErr := env.GetEnvBool(keys.CircuitBreaker, defaults.CircuitBreaker)
	cfg := defaults
	cfg.Endpoint = env.GetEnvString(keys.Endpoint, defaults.Endpoint)
	cfg.Headers = headers
	cfg.Compression = env.GetEnvString(keys.Compression, defaults.Compression)
	cfg.Timeout = timeout
	cfg.Retries = retries
	cfg.RetryBaseDelay = retryBaseDelay
	cfg.RetryMaxDelay = retryMaxDelay
	cfg.CircuitBreaker = circuitBreaker
	return cfg, errors.Join(headersErr, timeoutErr, retriesErr, retryBaseDelayErr, retryMaxDelayErr, circuitBreakerErr)
}
