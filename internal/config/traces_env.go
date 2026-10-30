package config

import (
	"errors"
	"path/filepath"

	clickhousecfg "github.com/go-jappe/heterolog/internal/config/clickhouse"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	"github.com/go-jappe/heterolog/internal/config/opensearch"
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
	// FailureStore、OpenSearchFailureStore、ClickHouseFailureStoreは、otlp、opensearch、
	// clickhouseの送り先のfailure storeの設定です。保存先のpathなど、ログと共通の項目は、
	// MapTelemetryConfigが組み立てます。
	FailureStore           TelemetryFailureStoreConfig
	OpenSearchFailureStore TelemetryFailureStoreConfig
	ClickHouseFailureStore TelemetryFailureStoreConfig
}

// tracesOpenSearchClientEnvKeysは、spanを書くOpenSearchへの接続のkeyです。ログの出力先の
// OpenSearch（HETEROLOG_OPENSEARCH_*）とは別に設定します。
var tracesOpenSearchClientEnvKeys = openSearchClientKeys{
	Endpoint:           "HETEROLOG_TRACES_OPENSEARCH_ENDPOINT",
	Timeout:            "HETEROLOG_TRACES_OPENSEARCH_TIMEOUT_SECOND",
	SkipVerifySSL:      "HETEROLOG_TRACES_OPENSEARCH_SKIP_VERIFY_SSL",
	CompressRequest:    "HETEROLOG_ENABLE_TRACES_OPENSEARCH_COMPRESS_REQUEST",
	AuthType:           "HETEROLOG_TRACES_OPENSEARCH_AUTH_TYPE",
	Username:           "HETEROLOG_TRACES_OPENSEARCH_USERNAME",
	Password:           "HETEROLOG_TRACES_OPENSEARCH_PASSWORD",
	APIKeyID:           "HETEROLOG_TRACES_OPENSEARCH_API_KEY_ID",
	APIKeySecret:       "HETEROLOG_TRACES_OPENSEARCH_API_KEY_SECRET",
	AWSRegion:          "HETEROLOG_TRACES_OPENSEARCH_AWS_REGION",
	AWSAccessKeyID:     "HETEROLOG_TRACES_OPENSEARCH_AWS_ACCESS_KEY_ID",
	AWSSecretAccessKey: "HETEROLOG_TRACES_OPENSEARCH_AWS_SECRET_ACCESS_KEY",
	AWSSessionToken:    "HETEROLOG_TRACES_OPENSEARCH_AWS_SESSION_TOKEN",
	SAMLToken:          "HETEROLOG_TRACES_OPENSEARCH_SAML_TOKEN",
	OIDCToken:          "HETEROLOG_TRACES_OPENSEARCH_OIDC_TOKEN",
	CertFile:           "HETEROLOG_TRACES_OPENSEARCH_CERT_FILE",
	KeyFile:            "HETEROLOG_TRACES_OPENSEARCH_KEY_FILE",
	CAFile:             "HETEROLOG_TRACES_OPENSEARCH_CA_FILE",
}

// tracesClickHouseClientEnvKeysは、spanを書くClickHouseへの接続のkeyです。ログの出力先の
// ClickHouse（HETEROLOG_CLICKHOUSE_*）とは別に設定します。
var tracesClickHouseClientEnvKeys = clickHouseClientKeys{
	Endpoint:        "HETEROLOG_TRACES_CLICKHOUSE_ENDPOINT",
	Database:        "HETEROLOG_TRACES_CLICKHOUSE_DATABASE",
	Table:           "HETEROLOG_TRACES_CLICKHOUSE_TABLE",
	Timeout:         "HETEROLOG_TRACES_CLICKHOUSE_TIMEOUT_SECOND",
	Username:        "HETEROLOG_TRACES_CLICKHOUSE_USERNAME",
	Password:        "HETEROLOG_TRACES_CLICKHOUSE_PASSWORD",
	Headers:         "HETEROLOG_TRACES_CLICKHOUSE_HEADERS",
	CompressRequest: "HETEROLOG_ENABLE_TRACES_CLICKHOUSE_COMPRESS_REQUEST",
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

	openSearchCfg, openSearchErr := loadTracesOpenSearchEnv(env, defaults.OpenSearch)
	cfg.Traces.OpenSearch = openSearchCfg
	clickHouseCfg, clickHouseErr := loadTracesClickHouseEnv(env, defaults.ClickHouse)
	cfg.Traces.ClickHouse = clickHouseCfg

	cfg.FileDir = env.GetEnvString("HETEROLOG_TRACES_FILE_DIR", defaultTracesFileDir)
	failureStore, failureStoreErr := loadTelemetryFailureStoreConfig(env, tracesOTLPFailureStoreKeys)
	cfg.FailureStore = failureStore
	openSearchFailureStore, openSearchFailureStoreErr := loadTelemetryFailureStoreConfig(env, tracesOpenSearchFailureStoreKeys)
	cfg.OpenSearchFailureStore = openSearchFailureStore
	clickHouseFailureStore, clickHouseFailureStoreErr := loadTelemetryFailureStoreConfig(env, tracesClickHouseFailureStoreKeys)
	cfg.ClickHouseFailureStore = clickHouseFailureStore
	return cfg, errors.Join(
		samplingRatioErr, queueSizeErr, batchSizeErr, exportIntervalErr,
		otlpErr, openSearchErr, clickHouseErr, failureStoreErr, openSearchFailureStoreErr, clickHouseFailureStoreErr,
	)
}

// loadTracesClickHouseEnvは、spanを書くClickHouseの送り先の設定を読みます。接続は、ログの
// 出力先と同じ読み方で、HETEROLOG_TRACES_CLICKHOUSE_*から読みます。failure storeの設定は、
// defaultsのままです。
func loadTracesClickHouseEnv(env pkgenv.Lookup, defaults pkgtelemetry.ClickHouseConfig) (pkgtelemetry.ClickHouseConfig, error) {
	client, clientErr := loadClickHouseClientEnv(env, tracesClickHouseClientEnvKeys, clickhousecfg.ClientConfig{
		Endpoint:        defaults.Client.Endpoint,
		Database:        defaults.Client.Database,
		Table:           defaults.Client.Table,
		Timeout:         defaults.Client.Timeout,
		Username:        defaults.Client.Username,
		Password:        defaults.Client.Password,
		Headers:         defaults.Client.Headers,
		CompressRequest: defaults.Client.CompressRequest,
	})
	retries, retriesErr := env.GetEnvInt("HETEROLOG_TRACES_CLICKHOUSE_RETRIES", defaults.Retries)
	retryBaseDelay, retryBaseDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_TRACES_CLICKHOUSE_RETRY_BASE_DELAY_MILLISECOND", defaults.RetryBaseDelay)
	retryMaxDelay, retryMaxDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_TRACES_CLICKHOUSE_RETRY_MAX_DELAY_MILLISECOND", defaults.RetryMaxDelay)
	circuitBreaker, circuitBreakerErr := env.GetEnvBool("HETEROLOG_ENABLE_TRACES_CLICKHOUSE_CIRCUIT_BREAKER", defaults.CircuitBreaker)
	cfg := defaults
	cfg.Client = *newClickHouseClientConfig(client)
	cfg.Retries = retries
	cfg.RetryBaseDelay = retryBaseDelay
	cfg.RetryMaxDelay = retryMaxDelay
	cfg.CircuitBreaker = circuitBreaker
	return cfg, errors.Join(clientErr, retriesErr, retryBaseDelayErr, retryMaxDelayErr, circuitBreakerErr)
}

// loadTracesOpenSearchEnvは、spanを書くOpenSearchの送り先の設定を読みます。接続は、ログの
// 出力先と同じ読み方で、HETEROLOG_TRACES_OPENSEARCH_*から読みます。failure storeの設定は、
// defaultsのままです。
func loadTracesOpenSearchEnv(env pkgenv.Lookup, defaults pkgtelemetry.OpenSearchConfig) (pkgtelemetry.OpenSearchConfig, error) {
	client, clientErr := loadOpenSearchClientEnv(env, tracesOpenSearchClientEnvKeys, opensearch.OpenSearchClientConfig{
		Endpoint:        defaults.Client.Endpoint,
		Timeout:         defaults.Client.Timeout,
		SkipVerifySSL:   defaults.Client.SkipVerifySSL,
		Auth:            opensearch.OpenSearchAuthConfig{Type: defaults.Client.AuthConfig.Type},
		CompressRequest: defaults.Client.CompressRequest,
	})
	retries, retriesErr := env.GetEnvInt("HETEROLOG_TRACES_OPENSEARCH_RETRIES", defaults.Retries)
	retryBaseDelay, retryBaseDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_TRACES_OPENSEARCH_RETRY_BASE_DELAY_MILLISECOND", defaults.RetryBaseDelay)
	retryMaxDelay, retryMaxDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_TRACES_OPENSEARCH_RETRY_MAX_DELAY_MILLISECOND", defaults.RetryMaxDelay)
	circuitBreaker, circuitBreakerErr := env.GetEnvBool("HETEROLOG_ENABLE_TRACES_OPENSEARCH_CIRCUIT_BREAKER", defaults.CircuitBreaker)
	cfg := defaults
	cfg.Client = *newOpenSearchClientConfig(client)
	cfg.Index = env.GetEnvString("HETEROLOG_TRACES_OPENSEARCH_INDEX", defaults.Index)
	cfg.Retries = retries
	cfg.RetryBaseDelay = retryBaseDelay
	cfg.RetryMaxDelay = retryMaxDelay
	cfg.CircuitBreaker = circuitBreaker
	return cfg, errors.Join(clientErr, retriesErr, retryBaseDelayErr, retryMaxDelayErr, circuitBreakerErr)
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
		cfg.Traces.OpenSearch.FailureStore = mapTelemetryFailureStoreConfig(
			&a, checkpointSourceTracesOpenSearch, a.Traces.OpenSearchFailureStore,
		)
		cfg.Traces.ClickHouse.FailureStore = mapTelemetryFailureStoreConfig(
			&a, checkpointSourceTracesClickHouse, a.Traces.ClickHouseFailureStore,
		)
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
		MaxAge:           fs.MaxAge,
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
