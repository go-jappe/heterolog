package config

import (
	"errors"
	"strconv"
	"strings"

	metricsdelivery "github.com/go-jappe/heterolog/internal/infrastructure/metrics"
	pkgenv "github.com/go-jappe/heterolog/pkg/env"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
)

// defaultMetricsFileDirは、metricsのfileを置くdirectoryの既定値です。ログのfileと同じく、
// HETEROLOG_SYSTEM_LOG_DIRとHETEROLOG_TARGETを基準に解決します。
const defaultMetricsFileDir = "/metrics"

// metricsFileNameは、metricsを書くfileの名前です。
const metricsFileName = "metrics.jsonl"

// MetricsConfigは、HETEROLOG_ENABLE_METRICSとHETEROLOG_METRICS_*から読んだmetricsの設定です。
// 値の検証は、公開APIのLoadEnvConfigが行います。
type MetricsConfig struct {
	// Enabledは、HETEROLOG_ENABLE_METRICSでmetricsを有効にしたことを表します。無効なら、
	// ほかの項目は読み込まず、ゼロ値のままです。
	Enabled bool
	// Metricsは、送り先などの設定です。File.Pathは空で、MapTelemetryConfigがFileDirから
	// 組み立てます。
	Metrics pkgtelemetry.MetricsConfig
	// FileDirは、metricsのfileを置くdirectoryです。
	FileDir string
	// FailureStoreとVictoriaMetricsFailureStoreは、otlpとvictoriametricsの送り先のfailure
	// storeの設定です。保存先のpathなど、ログと共通の項目は、MapTelemetryConfigが組み立てます。
	FailureStore                TelemetryFailureStoreConfig
	VictoriaMetricsFailureStore TelemetryFailureStoreConfig
}

// loadMetricsConfigは、metricsの設定を読み込みます。HETEROLOG_ENABLE_METRICSがfalseなら、
// ほかのkeyは読みません。
func loadMetricsConfig(env pkgenv.Lookup) (MetricsConfig, error) {
	enabled, err := env.GetEnvBool("HETEROLOG_ENABLE_METRICS", false)
	if err != nil || !enabled {
		return MetricsConfig{}, err
	}
	defaults := metricsdelivery.DefaultConfig()
	cfg := MetricsConfig{Enabled: true, Metrics: defaults}
	for _, name := range env.GetEnvSliceString("HETEROLOG_METRICS_TYPE", []string{string(pkgtelemetry.MetricExporterOTLP)}) {
		cfg.Metrics.Exporters = append(cfg.Metrics.Exporters, pkgtelemetry.MetricExporter(name))
	}
	exportInterval, exportIntervalErr := env.GetEnvDurationMillisecond(
		"HETEROLOG_METRICS_EXPORT_INTERVAL_MILLISECOND", defaults.ExportInterval,
	)
	queueSize, queueSizeErr := env.GetEnvInt("HETEROLOG_METRICS_QUEUE_SIZE", defaults.QueueSize)
	cardinalityLimit, cardinalityLimitErr := env.GetEnvInt("HETEROLOG_METRICS_CARDINALITY_LIMIT", defaults.CardinalityLimit)
	bounds, boundsErr := loadHistogramBoundsEnv(env, "HETEROLOG_METRICS_HISTOGRAM_BOUNDS", defaults.HistogramBounds)
	cfg.Metrics.ExportInterval = exportInterval
	cfg.Metrics.QueueSize = queueSize
	cfg.Metrics.CardinalityLimit = cardinalityLimit
	cfg.Metrics.HistogramBounds = bounds

	otlpCfg, otlpErr := loadHTTPEnv(env, httpKeys{
		Endpoint:       "HETEROLOG_METRICS_OTLP_ENDPOINT",
		Headers:        "HETEROLOG_METRICS_OTLP_HEADERS",
		Compression:    "HETEROLOG_METRICS_OTLP_COMPRESSION",
		Timeout:        "HETEROLOG_METRICS_OTLP_TIMEOUT_SECOND",
		Retries:        "HETEROLOG_METRICS_OTLP_RETRIES",
		RetryBaseDelay: "HETEROLOG_METRICS_OTLP_RETRY_BASE_DELAY_MILLISECOND",
		RetryMaxDelay:  "HETEROLOG_METRICS_OTLP_RETRY_MAX_DELAY_MILLISECOND",
		CircuitBreaker: "HETEROLOG_ENABLE_METRICS_OTLP_CIRCUIT_BREAKER",
	}, defaults.OTLP)
	cfg.Metrics.OTLP = otlpCfg
	victoriaMetricsCfg, victoriaMetricsErr := loadHTTPEnv(env, httpKeys{
		Endpoint:       "HETEROLOG_METRICS_VICTORIAMETRICS_ENDPOINT",
		Headers:        "HETEROLOG_METRICS_VICTORIAMETRICS_HEADERS",
		Compression:    "HETEROLOG_METRICS_VICTORIAMETRICS_COMPRESSION",
		Timeout:        "HETEROLOG_METRICS_VICTORIAMETRICS_TIMEOUT_SECOND",
		Retries:        "HETEROLOG_METRICS_VICTORIAMETRICS_RETRIES",
		RetryBaseDelay: "HETEROLOG_METRICS_VICTORIAMETRICS_RETRY_BASE_DELAY_MILLISECOND",
		RetryMaxDelay:  "HETEROLOG_METRICS_VICTORIAMETRICS_RETRY_MAX_DELAY_MILLISECOND",
		CircuitBreaker: "HETEROLOG_ENABLE_METRICS_VICTORIAMETRICS_CIRCUIT_BREAKER",
	}, defaults.VictoriaMetrics)
	cfg.Metrics.VictoriaMetrics = victoriaMetricsCfg

	cfg.FileDir = env.GetEnvString("HETEROLOG_METRICS_FILE_DIR", defaultMetricsFileDir)
	failureStore, failureStoreErr := loadTelemetryFailureStoreConfig(env, metricsOTLPFailureStoreKeys)
	cfg.FailureStore = failureStore
	victoriaMetricsFailureStore, victoriaMetricsFailureStoreErr := loadTelemetryFailureStoreConfig(env, metricsVictoriaMetricsFailureStoreKeys)
	cfg.VictoriaMetricsFailureStore = victoriaMetricsFailureStore
	return cfg, errors.Join(
		exportIntervalErr, queueSizeErr, cardinalityLimitErr, boundsErr,
		otlpErr, victoriaMetricsErr, failureStoreErr, victoriaMetricsFailureStoreErr,
	)
}

// loadHistogramBoundsEnvは、カンマ区切りの数の並びを読みます。keyがなければdefaultsを
// 返します。並びの順と値の範囲は、pkg/telemetryの検証で確かめます。
func loadHistogramBoundsEnv(env pkgenv.Lookup, key string, defaults []float64) ([]float64, error) {
	values := env.GetEnvSliceString(key, nil)
	if values == nil {
		return defaults, nil
	}
	bounds := make([]float64, 0, len(values))
	for _, value := range values {
		bound, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil {
			return defaults, &pkgenv.VariableError{Key: key, Err: err}
		}
		bounds = append(bounds, bound)
	}
	return bounds, nil
}
