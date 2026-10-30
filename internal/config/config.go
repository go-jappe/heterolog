package config

import (
	"errors"
	"fmt"
	"os"
	goruntime "runtime"
	"strings"
	"time"

	"github.com/go-jappe/heterolog/internal/config/app"
	"github.com/go-jappe/heterolog/internal/config/appendlogrunner"
	clickhousecfg "github.com/go-jappe/heterolog/internal/config/clickhouse"
	cloudwatchlogscfg "github.com/go-jappe/heterolog/internal/config/cloudwatchlogs"
	"github.com/go-jappe/heterolog/internal/config/failure"
	"github.com/go-jappe/heterolog/internal/config/fsstoreconfig"
	"github.com/go-jappe/heterolog/internal/config/internalerror"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	lokicfg "github.com/go-jappe/heterolog/internal/config/loki"
	objectstorages3cfg "github.com/go-jappe/heterolog/internal/config/objectstorage/s3"
	"github.com/go-jappe/heterolog/internal/config/objectstoragecompressedlog"
	"github.com/go-jappe/heterolog/internal/config/opensearch"
	"github.com/go-jappe/heterolog/internal/config/runtime"
	"github.com/go-jappe/heterolog/internal/config/slack"
	"github.com/go-jappe/heterolog/internal/config/spool/spoollifecycle"
	"github.com/go-jappe/heterolog/internal/config/spool/spoolrunner"
	"github.com/go-jappe/heterolog/internal/config/spool/spoolsupervisor"
	"github.com/go-jappe/heterolog/internal/config/timeoutbudget"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkgenv "github.com/go-jappe/heterolog/pkg/env"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
	"github.com/go-jappe/heterolog/pkg/path/appstatepath"
	"github.com/go-jappe/heterolog/pkg/path/logpath"
	pkgslack "github.com/go-jappe/heterolog/pkg/slack"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
	pkgspoollifecycle "github.com/go-jappe/heterolog/pkg/spool/spoollifecycle"
	pkgspoolrunner "github.com/go-jappe/heterolog/pkg/spool/spoolrunner"
	pkgspoolsupervisor "github.com/go-jappe/heterolog/pkg/spool/spoolsupervisor"

	"github.com/joho/godotenv"
)

// 数値をメガバイトに変換する定数
const BytesPerMegabyte uint64 = 1024 * 1024

// アプリケーション全体の設定を保持する構造体
type AppConfig struct {
	RuntimeEnv    runtime.RuntimeEnv
	RuntimeConfig runtime.RuntimeConfig
	AppConfig     app.AppConfig
	// MaxLogEntryBytesはsync、async、deliveryの全application logger契約に共通する
	// 一回のlog入力上限です。backend固有の送信上限とは独立しています。
	MaxLogEntryBytes int
	// LoggerEnabled、AsyncLoggerEnabled、DeliveryLoggerEnabledは、HETEROLOG_ENABLE_LOGGINGなどで
	// 各loggerを有効にしたかを表します。無効にしたloggerの設定は読み込まず、ゼロ値のままです。
	LoggerEnabled                    bool
	AsyncLoggerEnabled               bool
	DeliveryLoggerEnabled            bool
	Logger                           logconfig.LoggerConfig
	AsyncBestEffortMultiLoggerConfig logconfig.AsyncBestEffortMultiLoggerConfig // 非同期ロガー用設定
	DeliveryLogger                   logconfig.DeliveryLoggerConfig
	FsstoreConfig                    fsstoreconfig.CommonFileSystemStoreConfig
	AppendLogRunnerConfig            appendlogrunner.Config
	// ArchiveCheckpointAutoRecoveryは登録済みの現行upload宛先のcheckpoint欠落だけを
	// 空の進捗から復旧し、既存archiveの再uploadを許可します。
	ArchiveCheckpointAutoRecovery bool
	// CompressedLogFileUploadFailureStoreはrecord spoolとは独立した、
	// compressed log fileのupload失敗用file archive／spool設定です。
	CompressedLogFileUploadFailureStore objectstoragecompressedlog.Config
	SpoolConfig                         pkgspool.SpoolConfig
	Slack                               slack.SlackConfig
	OpenSearchClient                    opensearch.OpenSearchClientConfig
	AsyncOpenSearchClient               opensearch.OpenSearchClientConfig // 非同期ロガー用設定
	DeliveryOpenSearchClient            opensearch.OpenSearchClientConfig // 送達確認ロガー用設定
	LokiClient                          lokicfg.LokiClientConfig
	AsyncLokiClient                     lokicfg.LokiClientConfig
	DeliveryLokiClient                  lokicfg.LokiClientConfig
	CloudWatchLogsClient                cloudwatchlogscfg.CloudWatchLogsClientConfig
	AsyncCloudWatchLogsClient           cloudwatchlogscfg.CloudWatchLogsClientConfig
	DeliveryCloudWatchLogsClient        cloudwatchlogscfg.CloudWatchLogsClientConfig
	ClickHouseClient                    clickhousecfg.ClientConfig
	AsyncClickHouseClient               clickhousecfg.ClientConfig
	DeliveryClickHouseClient            clickhousecfg.ClientConfig
	// AdminServerは、管理用APIの管理用サーバーの設定です。loggerの設定には含めません。
	AdminServer AdminServerConfig
	// AdminAuditは、管理用APIの監査logを記録する送達確認loggerの設定です。管理用
	// サーバーを有効にした場合だけ読み込みます。
	AdminAudit AdminAuditConfig
	// Tracesは、NewTelemetryのtraceの設定です。HETEROLOG_ENABLE_TRACESで有効にした
	// 場合だけ読み込みます。
	Traces TracesConfig
}

// envSourceは、設定値の参照先です。process environmentの値を優先し、process environmentに
// ないkeyだけ環境設定fileの値を使います。godotenvで環境設定fileを読み込んだ場合と同じ優先順位
// ですが、process environmentは変更しません。
type envSource struct {
	file map[string]string
}

// newEnvSourceは、pathの環境設定fileを読み込んだenvSourceを返します。pathが空なら
// process environmentだけを参照します。fileが存在しない場合、requiredならEnvFileErrorを返し、
// そうでなければprocess environmentだけを参照します。存在するfileの構文不正や読み込み失敗は、
// requiredでなくてもEnvFileErrorです。
func newEnvSource(path string, required bool) (envSource, error) {
	if path == "" {
		return envSource{}, nil
	}
	values, err := godotenv.Read(path)
	if err != nil {
		if !required && errors.Is(err, os.ErrNotExist) {
			return envSource{}, nil
		}
		return envSource{}, &pkgenv.EnvFileError{Path: path, Err: err}
	}
	return envSource{file: values}, nil
}

// lookupは、keyの値と、keyが設定されているかを返します。
func (s envSource) lookup(key string) (string, bool) {
	if value, ok := os.LookupEnv(key); ok {
		return value, true
	}
	value, ok := s.file[key]
	return value, ok
}

// keysは、process environmentと環境設定fileにあるkey名を返します。
func (s envSource) keys() []string {
	keys := make([]string, 0, len(s.file))
	for _, entry := range os.Environ() {
		// Windowsの「=C:」のような項目は名前が空になるため、対象から外れます。
		if key, _, ok := strings.Cut(entry, "="); ok && key != "" {
			keys = append(keys, key)
		}
	}
	for key := range s.file {
		if _, ok := os.LookupEnv(key); !ok {
			keys = append(keys, key)
		}
	}
	return keys
}

// loadLogCompressFormatは読み込み済みの圧縮形式を解析します。keyはエラー表示にだけ使います。
func loadLogCompressFormat(value, key string, defaultValue fsstore.CompressFormat) (fsstore.CompressFormat, error) {
	format, err := logconfig.ParseLogCompressFormat(value)
	if err != nil {
		return defaultValue, newInvalidEnvironmentValueError(
			key, value, err, "environment variable %q has invalid value %q: %v", key, value, err,
		)
	}
	return format, nil
}

// loadLogLevelは読み込み済みのログレベルを解析します。keyはエラー表示にだけ使います。
func loadLogLevel(value, key string, defaultValue pkglogger.LogLevel) (pkglogger.LogLevel, error) {
	level, err := pkglogger.ParseLogLevel(value)
	if err != nil {
		return defaultValue, newInvalidEnvironmentValueError(
			key, value, err, "environment variable %q has invalid value %q: %v", key, value, err,
		)
	}
	return level, nil
}

// loadLogRotationは読み込み済みの設定値を解析し、不正値なら既定の周期を返します。
func loadLogRotation(
	raw string,
	defaultValue fsstore.FileRotationType,
) fsstore.FileRotationType {
	rotation, err := fsstore.ParseFileRotation(raw)
	if err != nil {
		// rotationの誤指定は保存を停止させず、呼び出し元が選んだ安全な既定周期へ戻す。
		return defaultValue
	}
	return rotation
}

// OpenSearch認証設定を構築する
// 認証情報は暗黙に補完しません。basic認証の利用者名・passwordが未設定の場合、
// OpenSearchを出力先またはinternal logの委譲先として使う構成は、mapper後の
// validationで設定エラーになります。
func buildOpenSearchAuthConfig(env pkgenv.Lookup) opensearch.OpenSearchAuthConfig {
	authType := pkgopensearch.AuthType(env.GetEnvString("HETEROLOG_OPENSEARCH_AUTH_TYPE", string(opensearch.DefaultAuthType)))

	authConfig := opensearch.OpenSearchAuthConfig{
		Type: authType,
	}

	switch authType {
	case pkgopensearch.AuthTypeBasic:
		authConfig.Basic = &opensearch.BasicAuthConfig{
			Username: env.GetEnvString("HETEROLOG_OPENSEARCH_USERNAME", ""),
			Password: env.GetEnvString("HETEROLOG_OPENSEARCH_PASSWORD", ""),
		}
	case pkgopensearch.AuthTypeAPIKey:
		authConfig.APIKey = &opensearch.APIKeyAuthConfig{
			ID:     env.GetEnvString("HETEROLOG_OPENSEARCH_API_KEY_ID", ""),
			Secret: env.GetEnvString("HETEROLOG_OPENSEARCH_API_KEY_SECRET", ""),
		}
	case pkgopensearch.AuthTypeAWSIAM:
		authConfig.AWSIAM = &opensearch.AWSIAMAuthConfig{
			Region:          env.GetEnvString("HETEROLOG_OPENSEARCH_AWS_REGION", ""),
			AccessKeyID:     env.GetEnvString("HETEROLOG_OPENSEARCH_AWS_ACCESS_KEY_ID", ""),
			SecretAccessKey: env.GetEnvString("HETEROLOG_OPENSEARCH_AWS_SECRET_ACCESS_KEY", ""),
			SessionToken:    env.GetEnvString("HETEROLOG_OPENSEARCH_AWS_SESSION_TOKEN", ""),
		}
	case pkgopensearch.AuthTypeSAML:
		authConfig.SAML = &opensearch.SAMLAuthConfig{
			Token: env.GetEnvString("HETEROLOG_OPENSEARCH_SAML_TOKEN", ""),
		}
	case pkgopensearch.AuthTypeOIDC:
		authConfig.OIDC = &opensearch.OIDCAuthConfig{
			Token: env.GetEnvString("HETEROLOG_OPENSEARCH_OIDC_TOKEN", ""),
		}
	case pkgopensearch.AuthTypeCertificate:
		authConfig.Certificate = &opensearch.CertificateAuthConfig{
			CertFile: env.GetEnvString("HETEROLOG_OPENSEARCH_CERT_FILE", ""),
			KeyFile:  env.GetEnvString("HETEROLOG_OPENSEARCH_KEY_FILE", ""),
			CAFile:   env.GetEnvString("HETEROLOG_OPENSEARCH_CA_FILE", ""),
		}
	case pkgopensearch.AuthTypeNone:
		// 何もしない（全てnil）
	}

	return authConfig
}

// 非同期logger用のOpenSearch認証設定を構築する
// 認証情報の扱いはbuildOpenSearchAuthConfigと同じで、未設定値を補完しません。
func buildAsyncOpenSearchAuthConfig(env pkgenv.Lookup) opensearch.OpenSearchAuthConfig {
	authType := pkgopensearch.AuthType(env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_AUTH_TYPE", string(opensearch.DefaultAuthType)))

	authConfig := opensearch.OpenSearchAuthConfig{
		Type: authType,
	}

	switch authType {
	case pkgopensearch.AuthTypeBasic:
		authConfig.Basic = &opensearch.BasicAuthConfig{
			Username: env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_USERNAME", ""),
			Password: env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_PASSWORD", ""),
		}
	case pkgopensearch.AuthTypeAPIKey:
		authConfig.APIKey = &opensearch.APIKeyAuthConfig{
			ID:     env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_API_KEY_ID", ""),
			Secret: env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_API_KEY_SECRET", ""),
		}
	case pkgopensearch.AuthTypeAWSIAM:
		authConfig.AWSIAM = &opensearch.AWSIAMAuthConfig{
			Region:          env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_AWS_REGION", ""),
			AccessKeyID:     env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_AWS_ACCESS_KEY_ID", ""),
			SecretAccessKey: env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_AWS_SECRET_ACCESS_KEY", ""),
			SessionToken:    env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_AWS_SESSION_TOKEN", ""),
		}
	case pkgopensearch.AuthTypeSAML:
		authConfig.SAML = &opensearch.SAMLAuthConfig{
			Token: env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_SAML_TOKEN", ""),
		}
	case pkgopensearch.AuthTypeOIDC:
		authConfig.OIDC = &opensearch.OIDCAuthConfig{
			Token: env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_OIDC_TOKEN", ""),
		}
	case pkgopensearch.AuthTypeCertificate:
		authConfig.Certificate = &opensearch.CertificateAuthConfig{
			CertFile: env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_CERT_FILE", ""),
			KeyFile:  env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_KEY_FILE", ""),
			CAFile:   env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_CA_FILE", ""),
		}
	case pkgopensearch.AuthTypeNone:
		// 何もしない（全てnil）
	}

	return authConfig
}

// buildDeliveryOpenSearchAuthConfigはDelivery専用の認証設定を読み込みます。
// sync/asyncの設定は継承せず、認証方式の未指定にはOpenSearchの既定値を使います。
// 認証情報は補完せず、不足はmapper後のvalidationで設定エラーにします。
func buildDeliveryOpenSearchAuthConfig(env pkgenv.Lookup) opensearch.OpenSearchAuthConfig {
	authType := pkgopensearch.AuthType(env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_AUTH_TYPE", string(opensearch.DefaultAuthType)))
	authConfig := opensearch.OpenSearchAuthConfig{Type: authType}

	switch authType {
	case pkgopensearch.AuthTypeBasic:
		authConfig.Basic = &opensearch.BasicAuthConfig{
			Username: env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_USERNAME", ""),
			Password: env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_PASSWORD", ""),
		}
	case pkgopensearch.AuthTypeAPIKey:
		authConfig.APIKey = &opensearch.APIKeyAuthConfig{
			ID:     env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_API_KEY_ID", ""),
			Secret: env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_API_KEY_SECRET", ""),
		}
	case pkgopensearch.AuthTypeAWSIAM:
		authConfig.AWSIAM = &opensearch.AWSIAMAuthConfig{
			Region:          env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_AWS_REGION", ""),
			AccessKeyID:     env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_AWS_ACCESS_KEY_ID", ""),
			SecretAccessKey: env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_AWS_SECRET_ACCESS_KEY", ""),
			SessionToken:    env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_AWS_SESSION_TOKEN", ""),
		}
	case pkgopensearch.AuthTypeSAML:
		authConfig.SAML = &opensearch.SAMLAuthConfig{
			Token: env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_SAML_TOKEN", ""),
		}
	case pkgopensearch.AuthTypeOIDC:
		authConfig.OIDC = &opensearch.OIDCAuthConfig{
			Token: env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_OIDC_TOKEN", ""),
		}
	case pkgopensearch.AuthTypeCertificate:
		authConfig.Certificate = &opensearch.CertificateAuthConfig{
			CertFile: env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_CERT_FILE", ""),
			KeyFile:  env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_KEY_FILE", ""),
			CAFile:   env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_CA_FILE", ""),
		}
	case pkgopensearch.AuthTypeNone:
		// 認証設定は不要です。
	default:
		// 未知のtypeはそのまま保持し、mapper後のvalidationで拒否します。
	}
	return authConfig
}

// ランタイム実行環境を読み込む
func LoadRuntimeEnv(env pkgenv.Lookup) runtime.RuntimeEnv {
	host, _ := os.Hostname()

	return runtime.RuntimeEnv{
		CoreInfo: runtime.CoreRuntimeEnv{
			HostName:          host,
			OSType:            goruntime.GOOS,
			Environment:       env.GetEnvString("HETEROLOG_APP_ENV", "DEV"),          // 例: dev・stg・prod
			RuntimeType:       env.GetEnvString("HETEROLOG_RUNTIME_TYPE", "unknown"), // 例: ecs・ec2・k8s・docker・local
			RuntimeInstanceID: env.GetEnvString("HETEROLOG_INSTANCE_ID", "unknown"),  // 例: task ID・pod名・instance ID・hostname
		},
		Cloud: &runtime.CloudInfo{
			Provider: env.GetEnvString("HETEROLOG_CLOUD_PROVIDER", ""),
			Region:   env.GetEnvString("HETEROLOG_CLOUD_PROVIDER_REGION", ""),
		},
	}
}

// ランタイム設定を読み込む
func loadRuntimeConfig(env pkgenv.Lookup) (runtime.RuntimeConfig, error) {
	var environmentErrors []error
	memoryCriticalRatio, memoryCriticalRatioErr := env.GetEnvFloat64("HETEROLOG_MEMORY_CRITICAL_RATIO", runtime.DefaultMemoryCriticalRatio)
	if memoryCriticalRatioErr != nil {
		environmentErrors = append(environmentErrors, memoryCriticalRatioErr)
	}
	minAvailableMemoryBytesMb, minAvailableMemoryBytesMbErr := env.GetEnvUint64Scaled("HETEROLOG_MIN_AVAILABLE_MEMORY_BYTES_MB", runtime.DefaultMinAvailableMemoryMegaBytes, BytesPerMegabyte)
	if minAvailableMemoryBytesMbErr != nil {
		environmentErrors = append(environmentErrors, minAvailableMemoryBytesMbErr)
	}
	memoryCriticalConsecutiveHits, memoryCriticalConsecutiveHitsErr := env.GetEnvInt("HETEROLOG_MEMORY_CRITICAL_CONSECUTIVE_HITS", runtime.DefaultMemoryCriticalConsecutiveHits)
	if memoryCriticalConsecutiveHitsErr != nil {
		environmentErrors = append(environmentErrors, memoryCriticalConsecutiveHitsErr)
	}
	return runtime.RuntimeConfig{
		Target:  env.GetEnvString("HETEROLOG_TARGET", app.DefaultTarget),
		IsDebug: false,
		Health: runtime.RuntimeHealthConfig{
			Memory: runtime.MemoryHealthConfig{
				CriticalRatio:           memoryCriticalRatio,
				MinAvailableBytes:       minAvailableMemoryBytesMb,
				CriticalConsecutiveHits: memoryCriticalConsecutiveHits,
			},
		},
	}, errors.Join(environmentErrors...)
}

// アプリケーション設定を読み込む
func loadAppConfig(env pkgenv.Lookup) app.AppConfig {
	return app.AppConfig{
		AppName:    env.GetEnvString("HETEROLOG_APP_NAME", app.DefaultAppName), // アプリケーション名
		AppVersion: app.AppVersion,
	}
}

func loadFsstoreConfig(env pkgenv.Lookup) (fsstoreconfig.CommonFileSystemStoreConfig, error) {
	var environmentErrors []error
	minAvailableDiskSpaceGb, minAvailableDiskSpaceGbErr := env.GetEnvUint64Scaled("HETEROLOG_MIN_AVAILABLE_DISK_SPACE_GB", failure.DefaultMinAvailableDiskSpaceGB, BytesPerMegabyte*1024)
	if minAvailableDiskSpaceGbErr != nil {
		environmentErrors = append(environmentErrors, minAvailableDiskSpaceGbErr)
	}
	usageCacheTTL, err := env.GetEnvDuration(
		"HETEROLOG_INLINE_FILESYSTEM_USAGE_CACHE_TTL_SECOND",
		fsstoreconfig.DefaultUsageCacheTTL,
	)
	if err != nil {
		environmentErrors = append(environmentErrors, err)
	}
	return fsstoreconfig.CommonFileSystemStoreConfig{
		MinAvailableDiskSpace: minAvailableDiskSpaceGb,
		UsageCacheTTL:         usageCacheTTL,
		LogPath:               env.GetEnvString("HETEROLOG_SYSTEM_LOG_DIR", logpath.DefaultLogBaseDir()),
		SpoolPath:             env.GetEnvString("HETEROLOG_SYSTEM_LIB_DIR", appstatepath.DefaultAppStateBaseDir()),
		// TargetはCLIで上書きされ得るため、ここではbase directoryだけを保持する。
		// failure archiveとarchive checkpointはmapper後に最終RuntimeConfig.Targetを付与する。
		AppStatePath:   env.GetEnvString("HETEROLOG_APP_STATE_DIR", appstatepath.DefaultAppStateBaseDir()),
		FileNameFormat: fsstore.FileNameFormat,
		FileDateFormat: fsstore.FileDateFormat,
	}, errors.Join(environmentErrors...)
}

func loadAppendLogRunnerConfig(env pkgenv.Lookup) (appendlogrunner.Config, error) {
	var environmentErrors []error
	appendlogRunnerIntervalSecond, appendlogRunnerIntervalSecondErr := env.GetEnvDuration("HETEROLOG_APPENDLOG_RUNNER_INTERVAL_SECOND", appendlogrunner.DefaultInterval)
	if appendlogRunnerIntervalSecondErr != nil {
		environmentErrors = append(environmentErrors, appendlogRunnerIntervalSecondErr)
	}
	enableAppendlogRunnerStopOnError, enableAppendlogRunnerStopOnErrorErr := env.GetEnvBool("HETEROLOG_ENABLE_APPENDLOG_RUNNER_STOP_ON_ERROR", appendlogrunner.DefaultStopOnError)
	if enableAppendlogRunnerStopOnErrorErr != nil {
		environmentErrors = append(environmentErrors, enableAppendlogRunnerStopOnErrorErr)
	}
	enableAppendlogRunnerSupervisor, enableAppendlogRunnerSupervisorErr := env.GetEnvBool(
		"HETEROLOG_ENABLE_APPENDLOG_RUNNER_SUPERVISOR",
		appendlogrunner.DefaultSupervisorEnabled)
	if enableAppendlogRunnerSupervisorErr != nil {
		environmentErrors = append(environmentErrors, enableAppendlogRunnerSupervisorErr)
	}
	appendlogRunnerSupervisorRestartDelayMillisecond, appendlogRunnerSupervisorRestartDelayMillisecondErr := env.GetEnvDurationMillisecond(
		"HETEROLOG_APPENDLOG_RUNNER_SUPERVISOR_RESTART_DELAY_MILLISECOND",
		appendlogrunner.DefaultRestartDelay,
	)
	if appendlogRunnerSupervisorRestartDelayMillisecondErr != nil {
		environmentErrors = append(environmentErrors, appendlogRunnerSupervisorRestartDelayMillisecondErr)
	}
	maxAppendlogRunnerSupervisorRestarts, maxAppendlogRunnerSupervisorRestartsErr := env.GetEnvInt(
		"HETEROLOG_MAX_APPENDLOG_RUNNER_SUPERVISOR_RESTARTS",
		appendlogrunner.DefaultMaxRestarts)
	if maxAppendlogRunnerSupervisorRestartsErr != nil {
		environmentErrors = append(environmentErrors, maxAppendlogRunnerSupervisorRestartsErr)
	}
	return appendlogrunner.Config{
		Interval:    appendlogRunnerIntervalSecond,
		StopOnError: enableAppendlogRunnerStopOnError,
		Supervisor: appendlogrunner.SupervisorConfig{
			Enabled: enableAppendlogRunnerSupervisor,

			RestartDelay: appendlogRunnerSupervisorRestartDelayMillisecond,

			MaxRestarts: maxAppendlogRunnerSupervisorRestarts,
		},
	}, errors.Join(environmentErrors...)
}

// loadLogOutputDestinationsはHETEROLOG_LOG_TYPE・HETEROLOG_ASYNC_LOG_TYPE・HETEROLOG_DELIVERY_LOG_TYPEを同じ規則で
// 読み込みます。未設定・空、または区切り文字だけで出力先を一つも含まない場合は、
// 通常・非同期・Delivery共通の既定値である標準出力だけを選択します。
// Slackなど認証情報を必要とする出力先は利用者が明示した場合だけ有効になり、
// その認証情報はmapper後のvalidationで必須として検証します。
func loadLogOutputDestinations(env pkgenv.Lookup, key string) (pkglogger.LogOutputDestinations, error) {
	return loadLogOutputDestinationsWithDefault(env, key, logconfig.LogDefaultOutputs)
}

// loadLogOutputDestinationsWithDefaultは、keyの出力先を読み込みます。未設定か空の場合は
// defaultsを使います。
func loadLogOutputDestinationsWithDefault(env pkgenv.Lookup, key string, defaults []string) (pkglogger.LogOutputDestinations, error) {
	rawDestinations := env.GetEnvSliceString(key, defaults)
	destinations, err := pkglogger.ParseLogOutputDestinations(rawDestinations)
	if err != nil {
		return nil, newInvalidEnvironmentValueError(
			key, fmt.Sprint(rawDestinations), err, "invalid %s %v: %v", key, rawDestinations, err,
		)
	}
	if len(destinations) == 0 {
		// 出力先なしの構成ではログが黙って失われるため、未設定と同じ既定値へ戻します。
		return pkglogger.ParseLogOutputDestinations(defaults)
	}
	return destinations, nil
}

// ロガー設定を読み込む
func loadLoggerConfig(env pkgenv.Lookup) (logconfig.LoggerConfig, error) {
	var environmentErrors []error
	destinations, err := loadLogOutputDestinations(env, "HETEROLOG_LOG_TYPE")
	if err != nil {
		return logconfig.LoggerConfig{}, err
	}

	timeoutBudgetPolicy, err := loadTimeoutBudgetPolicy(
		env.GetEnvString("HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY", string(timeoutbudget.DefaultTimeoutBudgetPolicy)),
		"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY",
	)
	if err != nil {
		return logconfig.LoggerConfig{}, err
	}

	logTimeoutSecond, logTimeoutSecondErr := env.GetEnvDuration("HETEROLOG_LOG_TIMEOUT_SECOND", logconfig.DefaultMultiLoggerTimout)
	if logTimeoutSecondErr != nil {
		environmentErrors = append(environmentErrors, logTimeoutSecondErr)
	}
	var errs []error
	clockConfig, clockErr := loadClockConfig(env)
	if clockErr != nil {
		errs = append(errs, clockErr)
	}
	cfg := logconfig.LoggerConfig{
		LogOutputDestinations: destinations,
		ClockConfig:           clockConfig,
		// MultiLogger の親 timeout。子 logger が使う HTTP client と retryable client の最大予算より長いことを LoadConfig 後に検証します。
		Timeout:             logTimeoutSecond,
		TimeoutBudgetPolicy: timeoutBudgetPolicy,
	}
	if destinations.Has(logconfig.LogOutputStdout) {
		stdoutConfig, loadErr := loadStdoutConfig(env)
		cfg.StdoutConfig = stdoutConfig
		if loadErr != nil {
			errs = append(errs, loadErr)
		}
	}

	if destinations.Has(logconfig.LogOutputFile) {
		fileConfig, loadErr := loadFileConfig(env)
		cfg.FileConfig = fileConfig
		if loadErr != nil {
			errs = append(errs, loadErr)
		}
	}

	if destinations.Has(logconfig.LogOutputSlack) {
		slackConfig, loadErr := loadSlackConfig(env)
		cfg.SlackConfig = slackConfig
		if loadErr != nil {
			errs = append(errs, loadErr)
		}
	}

	if destinations.Has(logconfig.LogOutputOpenSearch) {
		retryConfig, retryErr := loadOpenSearchRetryableClientConfig(env)
		if retryErr != nil {
			errs = append(errs, fmt.Errorf("load logger OpenSearch retry config: %w", retryErr))
		}
		baseConfig, baseErr := loadOpenSearchConfig(env)
		flushManagerConfig, flushErr := loadOpenSearchFlushManagerConfig(env)
		errs = append(errs, baseErr, flushErr)
		cfg.OpenSearchConfig = logconfig.OpenSearchConfig{
			OpenSearchLoggerBaseConfig:      baseConfig,
			OpenSearchFlushManagerConfig:    flushManagerConfig,
			OpenSearchRetryAbleClientConfig: retryConfig,
		}
	}

	if destinations.Has(logconfig.LogOutputLoki) {
		retryConfig, retryErr := loadLokiRetryableClientConfig(env)
		if retryErr != nil {
			errs = append(errs, fmt.Errorf("load logger Loki retry config: %w", retryErr))
		}
		baseConfig, baseErr := loadLokiConfig(env)
		flushManagerConfig, flushErr := loadLokiFlushManagerConfig(env)
		errs = append(errs, baseErr, flushErr)
		cfg.LokiConfig = logconfig.LokiConfig{
			LokiLoggerBaseConfig:      baseConfig,
			LokiFlushManagerConfig:    flushManagerConfig,
			LokiRetryAbleClientConfig: retryConfig,
		}
	}
	if destinations.Has(logconfig.LogOutputCloudWatchLogs) {
		retryConfig, retryErr := loadCloudWatchLogsRetryableClientConfig(env)
		if retryErr != nil {
			errs = append(errs, fmt.Errorf("load logger CloudWatch Logs retry config: %w", retryErr))
		}
		baseConfig, baseErr := loadCloudWatchLogsConfig(env)
		flushManagerConfig, flushErr := loadCloudWatchLogsFlushManagerConfig(env)
		errs = append(errs, baseErr, flushErr)
		cfg.CloudWatchLogsConfig = logconfig.CloudWatchLogsConfig{
			CloudWatchLogsLoggerBaseConfig:      baseConfig,
			CloudWatchLogsFlushManagerConfig:    flushManagerConfig,
			CloudWatchLogsRetryAbleClientConfig: retryConfig,
		}
	}
	if destinations.Has(logconfig.LogOutputClickHouse) {
		retryConfig, retryErr := loadClickHouseRetryableClientConfig(env)
		if retryErr != nil {
			errs = append(errs, fmt.Errorf("load logger ClickHouse retry config: %w", retryErr))
		}
		baseConfig, baseErr := loadClickHouseConfig(env)
		flushManagerConfig, flushErr := loadClickHouseFlushManagerConfig(env)
		errs = append(errs, baseErr, flushErr)
		cfg.ClickHouseConfig = logconfig.ClickHouseConfig{
			ClickHouseLoggerBaseConfig:      baseConfig,
			ClickHouseFlushManagerConfig:    flushManagerConfig,
			ClickHouseRetryAbleClientConfig: retryConfig,
		}
	}

	cfg.HandlerTargetDestinations = collectInternalHandlerTargets(
		destinations,
		func(target pkglogger.LogOutputDestination) bool {
			return loggerUsesInternalHandlerTarget(cfg, target)
		},
	)
	internalLogDestinationConfig, internalLogDestinationErr := loadLoggerInternalLogDestinationConfig(
		env,
		requiresLoggerTargetConfig(
			cfg.LogOutputDestinations,
			cfg.HandlerTargetDestinations,
			logconfig.LogOutputLoki,
		),
	)
	cfg.InternalLogDestinationConfig = internalLogDestinationConfig
	if internalLogDestinationErr != nil {
		errs = append(errs, internalLogDestinationErr)
	}
	if err := loadLoggerHandlerTargetConfigs(env, &cfg); err != nil {
		errs = append(errs, err)
	}

	errs = append(errs, environmentErrors...)
	return cfg, errors.Join(errs...)
}

func loadAsyncBestEffortMultiLoggerConfig(env pkgenv.Lookup) (logconfig.AsyncBestEffortMultiLoggerConfig, error) {
	var environmentErrors []error
	destinations, err := loadLogOutputDestinations(env, "HETEROLOG_ASYNC_LOG_TYPE")
	if err != nil {
		return logconfig.AsyncBestEffortMultiLoggerConfig{}, err
	}
	timeoutBudgetPolicy, err := loadTimeoutBudgetPolicy(
		env.GetEnvString("HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY", string(timeoutbudget.DefaultTimeoutBudgetPolicy)),
		"HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY",
	)
	if err != nil {
		return logconfig.AsyncBestEffortMultiLoggerConfig{}, err
	}

	asyncLogTimeoutSecond, asyncLogTimeoutSecondErr := env.GetEnvDuration("HETEROLOG_ASYNC_LOG_TIMEOUT_SECOND", logconfig.AsyncBestEffortMultiLoggerTimout)
	if asyncLogTimeoutSecondErr != nil {
		environmentErrors = append(environmentErrors, asyncLogTimeoutSecondErr)
	}
	maxAsyncLogParallel, maxAsyncLogParallelErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_LOG_PARALLEL", logconfig.AsyncBestEffortMultiLoggerMaxParallel)
	if maxAsyncLogParallelErr != nil {
		environmentErrors = append(environmentErrors, maxAsyncLogParallelErr)
	}
	var errs []error
	clockConfig, clockErr := loadClockConfig(env)
	if clockErr != nil {
		errs = append(errs, clockErr)
	}
	cfg := logconfig.AsyncBestEffortMultiLoggerConfig{
		LogOutputDestinations: destinations,
		ClockConfig:           clockConfig,
		// best-effort は呼び出し元が結果を待たないため、retryable client が完走できるよう通常 logger より長めに取ります。
		Timeout:             asyncLogTimeoutSecond,
		TimeoutBudgetPolicy: timeoutBudgetPolicy,
		MaxParallel:         maxAsyncLogParallel,
	}
	if destinations.Has(logconfig.LogOutputStdout) {
		stdoutConfig, loadErr := loadAsyncStdoutConfig(env)
		cfg.StdoutConfig = stdoutConfig
		if loadErr != nil {
			errs = append(errs, loadErr)
		}
	}

	if destinations.Has(logconfig.LogOutputFile) {
		fileConfig, loadErr := loadAsyncFileConfig(env)
		cfg.FileConfig = fileConfig
		if loadErr != nil {
			errs = append(errs, loadErr)
		}
	}

	if destinations.Has(logconfig.LogOutputSlack) {
		slackConfig, loadErr := loadAsyncSlackConfig(env)
		cfg.SlackConfig = slackConfig
		if loadErr != nil {
			errs = append(errs, loadErr)
		}
	}

	if destinations.Has(logconfig.LogOutputOpenSearch) {
		retryConfig, retryErr := loadAsyncOpenSearchRetryableClientConfig(env)
		if retryErr != nil {
			errs = append(errs, fmt.Errorf("load async logger OpenSearch retry config: %w", retryErr))
		}
		baseConfig, baseErr := loadAsyncOpenSearchConfig(env)
		flushManagerConfig, flushErr := loadAsyncOpenSearchFlushManagerConfig(env)
		errs = append(errs, baseErr, flushErr)
		cfg.OpenSearchConfig = logconfig.OpenSearchConfig{
			OpenSearchLoggerBaseConfig:      baseConfig,
			OpenSearchFlushManagerConfig:    flushManagerConfig,
			OpenSearchRetryAbleClientConfig: retryConfig,
		}
	}

	if destinations.Has(logconfig.LogOutputLoki) {
		retryConfig, retryErr := loadAsyncLokiRetryableClientConfig(env)
		if retryErr != nil {
			errs = append(errs, fmt.Errorf("load async logger Loki retry config: %w", retryErr))
		}
		baseConfig, baseErr := loadAsyncLokiConfig(env)
		flushManagerConfig, flushErr := loadAsyncLokiFlushManagerConfig(env)
		errs = append(errs, baseErr, flushErr)
		cfg.LokiConfig = logconfig.LokiConfig{
			LokiLoggerBaseConfig:      baseConfig,
			LokiFlushManagerConfig:    flushManagerConfig,
			LokiRetryAbleClientConfig: retryConfig,
		}
	}
	if destinations.Has(logconfig.LogOutputCloudWatchLogs) {
		retryConfig, retryErr := loadAsyncCloudWatchLogsRetryableClientConfig(env)
		if retryErr != nil {
			errs = append(errs, fmt.Errorf("load async logger CloudWatch Logs retry config: %w", retryErr))
		}
		baseConfig, baseErr := loadAsyncCloudWatchLogsConfig(env)
		flushManagerConfig, flushErr := loadAsyncCloudWatchLogsFlushManagerConfig(env)
		errs = append(errs, baseErr, flushErr)
		cfg.CloudWatchLogsConfig = logconfig.CloudWatchLogsConfig{
			CloudWatchLogsLoggerBaseConfig:      baseConfig,
			CloudWatchLogsFlushManagerConfig:    flushManagerConfig,
			CloudWatchLogsRetryAbleClientConfig: retryConfig,
		}
	}
	if destinations.Has(logconfig.LogOutputClickHouse) {
		retryConfig, retryErr := loadAsyncClickHouseRetryableClientConfig(env)
		if retryErr != nil {
			errs = append(errs, fmt.Errorf("load async logger ClickHouse retry config: %w", retryErr))
		}
		baseConfig, baseErr := loadAsyncClickHouseConfig(env)
		flushManagerConfig, flushErr := loadAsyncClickHouseFlushManagerConfig(env)
		errs = append(errs, baseErr, flushErr)
		cfg.ClickHouseConfig = logconfig.ClickHouseConfig{
			ClickHouseLoggerBaseConfig:      baseConfig,
			ClickHouseFlushManagerConfig:    flushManagerConfig,
			ClickHouseRetryAbleClientConfig: retryConfig,
		}
	}

	cfg.HandlerTargetDestinations = collectInternalHandlerTargets(
		destinations,
		func(target pkglogger.LogOutputDestination) bool {
			return asyncLoggerUsesInternalHandlerTarget(cfg, target)
		},
	)
	internalLogDestinationConfig, internalLogDestinationErr := loadAsyncLoggerInternalLogDestinationConfig(
		env,
		requiresLoggerTargetConfig(
			cfg.LogOutputDestinations,
			cfg.HandlerTargetDestinations,
			logconfig.LogOutputLoki,
		),
	)
	cfg.InternalLogDestinationConfig = internalLogDestinationConfig
	if internalLogDestinationErr != nil {
		errs = append(errs, internalLogDestinationErr)
	}
	if err := loadAsyncLoggerHandlerTargetConfigs(env, &cfg); err != nil {
		errs = append(errs, err)
	}

	errs = append(errs, environmentErrors...)
	return cfg, errors.Join(errs...)
}

func loadDeliveryLoggerConfig(env pkgenv.Lookup) (logconfig.DeliveryLoggerConfig, error) {
	destinations, err := loadLogOutputDestinations(env, "HETEROLOG_DELIVERY_LOG_TYPE")
	if err != nil {
		return logconfig.DeliveryLoggerConfig{}, err
	}
	return loadDeliveryLoggerConfigWithDestinations(env, destinations)
}

// loadDeliveryLoggerConfigWithDestinationsは、destinationsを出力先として、Delivery loggerの
// 設定を読み込みます。管理用APIの監査用loggerも、出力先だけを変えてこの設定を使います。
func loadDeliveryLoggerConfigWithDestinations(
	env pkgenv.Lookup,
	destinations pkglogger.LogOutputDestinations,
) (logconfig.DeliveryLoggerConfig, error) {
	var environmentErrors []error
	timeoutBudgetPolicy, err := loadTimeoutBudgetPolicy(
		env.GetEnvString("HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY", string(timeoutbudget.DefaultTimeoutBudgetPolicy)),
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY",
	)
	if err != nil {
		return logconfig.DeliveryLoggerConfig{}, err
	}

	deliveryLogTimeoutSecond, deliveryLogTimeoutSecondErr := env.GetEnvDuration("HETEROLOG_DELIVERY_LOG_TIMEOUT_SECOND", logconfig.DefaultDeliveryLoggerTimout)
	if deliveryLogTimeoutSecondErr != nil {
		environmentErrors = append(environmentErrors, deliveryLogTimeoutSecondErr)
	}
	var errs []error
	clockConfig, clockErr := loadClockConfig(env)
	if clockErr != nil {
		errs = append(errs, clockErr)
	}
	cfg := logconfig.DeliveryLoggerConfig{
		LogOutputDestinations: destinations,
		ClockConfig:           clockConfig,
		// delivery は送達結果を返すため、長すぎない範囲で retryable client の最大予算より長くします。
		Timeout:             deliveryLogTimeoutSecond,
		TimeoutBudgetPolicy: timeoutBudgetPolicy,
	}
	if destinations.Has(logconfig.LogOutputStdout) {
		stdoutConfig, loadErr := loadDeliveryStdoutConfig(env)
		cfg.DeliveryStdoutConfig = stdoutConfig
		if loadErr != nil {
			errs = append(errs, loadErr)
		}
	}
	if destinations.Has(logconfig.LogOutputFile) {
		fileConfig, loadErr := loadDeliveryFileConfig(env)
		cfg.DeliveryFileConfig = fileConfig
		if loadErr != nil {
			errs = append(errs, loadErr)
		}
	}
	if destinations.Has(logconfig.LogOutputSlack) {
		slackConfig, loadErr := loadDeliverySlackConfig(env)
		cfg.DeliverySlackConfig = slackConfig
		if loadErr != nil {
			errs = append(errs, loadErr)
		}
	}
	if destinations.Has(logconfig.LogOutputOpenSearch) {
		retryConfig, retryErr := loadDeliveryOpenSearchRetryableClientConfig(env)
		if retryErr != nil {
			errs = append(errs, fmt.Errorf("load delivery logger OpenSearch retry config: %w", retryErr))
		}
		baseConfig, baseErr := loadDeliveryOpenSearchConfig(env)
		flushManagerConfig, flushErr := loadDeliveryOpenSearchFlushManagerConfig(env)
		errs = append(errs, baseErr, flushErr)
		cfg.DeliveryOpenSearchConfig = logconfig.DeliveryOpenSearchConfig{
			DeliveryOpenSearchLoggerBaseConfig:   baseConfig,
			DeliveryOpenSearchFlushManagerConfig: flushManagerConfig,
			OpenSearchRetryAbleClientConfig:      retryConfig,
		}
	}
	if destinations.Has(logconfig.LogOutputLoki) {
		retryConfig, retryErr := loadDeliveryLokiRetryableClientConfig(env)
		if retryErr != nil {
			errs = append(errs, fmt.Errorf("load delivery logger Loki retry config: %w", retryErr))
		}
		baseConfig, baseErr := loadDeliveryLokiConfig(env)
		flushManagerConfig, flushErr := loadDeliveryLokiFlushManagerConfig(env)
		errs = append(errs, baseErr, flushErr)
		cfg.DeliveryLokiConfig = logconfig.DeliveryLokiConfig{
			DeliveryLokiLoggerBaseConfig:   baseConfig,
			DeliveryLokiFlushManagerConfig: flushManagerConfig,
			LokiRetryAbleClientConfig:      retryConfig,
		}
	}
	if destinations.Has(logconfig.LogOutputCloudWatchLogs) {
		retryConfig, retryErr := loadDeliveryCloudWatchLogsRetryableClientConfig(env)
		if retryErr != nil {
			errs = append(errs, fmt.Errorf("load delivery logger CloudWatch Logs retry config: %w", retryErr))
		}
		baseConfig, baseErr := loadDeliveryCloudWatchLogsConfig(env)
		flushManagerConfig, flushErr := loadDeliveryCloudWatchLogsFlushManagerConfig(env)
		errs = append(errs, baseErr, flushErr)
		cfg.DeliveryCloudWatchLogsConfig = logconfig.DeliveryCloudWatchLogsConfig{
			DeliveryCloudWatchLogsLoggerBaseConfig:   baseConfig,
			DeliveryCloudWatchLogsFlushManagerConfig: flushManagerConfig,
			CloudWatchLogsRetryAbleClientConfig:      retryConfig,
		}
	}
	if destinations.Has(logconfig.LogOutputClickHouse) {
		retryConfig, retryErr := loadDeliveryClickHouseRetryableClientConfig(env)
		if retryErr != nil {
			errs = append(errs, fmt.Errorf("load delivery logger ClickHouse retry config: %w", retryErr))
		}
		baseConfig, baseErr := loadDeliveryClickHouseConfig(env)
		flushManagerConfig, flushErr := loadDeliveryClickHouseFlushManagerConfig(env)
		errs = append(errs, baseErr, flushErr)
		cfg.DeliveryClickHouseConfig = logconfig.DeliveryClickHouseConfig{
			DeliveryClickHouseLoggerBaseConfig:   baseConfig,
			DeliveryClickHouseFlushManagerConfig: flushManagerConfig,
			ClickHouseRetryAbleClientConfig:      retryConfig,
		}
	}

	cfg.HandlerTargetDestinations = collectInternalHandlerTargets(
		destinations,
		func(target pkglogger.LogOutputDestination) bool {
			return deliveryLoggerUsesInternalHandlerTarget(cfg, target)
		},
	)
	internalLogBatchConfig, internalLogBatchErr := loadDeliveryInternalLogBatchConfig(
		env,
		func(target pkglogger.LogOutputDestination) bool {
			return deliveryLoggerUsesInternalHandlerTarget(cfg, target)
		},
	)
	cfg.InternalLogBatchConfig = internalLogBatchConfig
	if internalLogBatchErr != nil {
		errs = append(errs, internalLogBatchErr)
	}
	internalLogDestinationConfig, internalLogDestinationErr := loadDeliveryLoggerInternalLogDestinationConfig(
		env,
		requiresLoggerTargetConfig(
			cfg.LogOutputDestinations,
			cfg.HandlerTargetDestinations,
			logconfig.LogOutputLoki,
		),
	)
	cfg.InternalLogDestinationConfig = internalLogDestinationConfig
	if internalLogDestinationErr != nil {
		errs = append(errs, internalLogDestinationErr)
	}
	if err := loadDeliveryLoggerHandlerTargetConfigs(env, &cfg); err != nil {
		errs = append(errs, err)
	}
	errs = append(errs, environmentErrors...)
	return cfg, errors.Join(errs...)

}

// loadDeliveryInternalLogBatchConfigはDelivery loggerからinternal-log fallbackへ
// enqueueしたrecordだけに適用するbatch設定を読み込みます。Delivery本体および
// sync/async loggerの送信設定には適用しません。
func loadDeliveryInternalLogBatchConfig(
	env pkgenv.Lookup,
	usesTarget func(pkglogger.LogOutputDestination) bool,
) (logconfig.DeliveryInternalLogBatchConfig, error) {
	var errs []error
	cfg := logconfig.DeliveryInternalLogBatchConfig{
		OpenSearch: logconfig.DeliveryInternalLogOpenSearchBatchConfig{
			FlushInterval:       logconfig.DefaultDeliveryInternalLogOpenSearchFlushInterval,
			MaxBulkDocument:     logconfig.DefaultDeliveryInternalLogOpenSearchMaxBulkDocument,
			BufferFlushBytes:    logconfig.DefaultDeliveryInternalLogOpenSearchBufferFlushBytes,
			MaxBulkRequestBytes: logconfig.DefaultDeliveryInternalLogOpenSearchMaxBulkRequestBytes,
		},
		Loki: logconfig.DeliveryInternalLogLokiBatchConfig{
			FlushInterval:           logconfig.DefaultDeliveryInternalLogLokiFlushInterval,
			MaxEntries:              logconfig.DefaultDeliveryInternalLogLokiMaxEntries,
			BufferFlushBytes:        logconfig.DefaultDeliveryInternalLogLokiBufferFlushBytes,
			FallbackQueueMaxEntries: logconfig.DefaultDeliveryInternalLogLokiFallbackQueueMaxEntries,
		},
		CloudWatchLogs: logconfig.DeliveryInternalLogCloudWatchLogsBatchConfig{
			FlushInterval:          logconfig.DefaultDeliveryInternalLogCloudWatchLogsFlushInterval,
			BufferFlushMaxEvents:   logconfig.DefaultDeliveryInternalLogCloudWatchLogsBufferFlushMaxEvents,
			BufferFlushBytes:       logconfig.DefaultDeliveryInternalLogCloudWatchLogsBufferFlushBytes,
			FallbackQueueMaxEvents: logconfig.DefaultDeliveryInternalLogCloudWatchLogsFallbackQueueMaxEvents,
		},
		Slack: logconfig.DeliveryInternalLogSlackBatchConfig{
			Intervals: append(
				[]time.Duration(nil),
				logconfig.DefaultDeliveryInternalLogSlackIntervals...,
			),
			MaxLogsPerNotification: logconfig.DefaultDeliveryInternalLogSlackMaxLogsPerNotification,
			MaxRestThreshold:       logconfig.DefaultDeliveryInternalLogSlackMaxRestThreshold,
		},
	}

	if usesTarget != nil && usesTarget(logconfig.LogOutputOpenSearch) {
		value, err := env.GetEnvDuration(
			"HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_OPENSEARCH_FLUSH_INTERVAL_SECOND",
			cfg.OpenSearch.FlushInterval,
		)
		cfg.OpenSearch.FlushInterval = value
		if err != nil {
			errs = append(errs, err)
		}
		maxBulkDocument, err := env.GetEnvInt(
			"HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_OPENSEARCH_BULK_DOCUMENTS",
			cfg.OpenSearch.MaxBulkDocument,
		)
		cfg.OpenSearch.MaxBulkDocument = maxBulkDocument
		if err != nil {
			errs = append(errs, err)
		}
		bufferFlushBytes, err := env.GetEnvInt(
			"HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_OPENSEARCH_BUFFER_FLUSH_BYTES",
			cfg.OpenSearch.BufferFlushBytes,
		)
		cfg.OpenSearch.BufferFlushBytes = bufferFlushBytes
		if err != nil {
			errs = append(errs, err)
		}
		maxBulkRequestBytes, err := env.GetEnvInt(
			"HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_OPENSEARCH_BULK_REQUEST_BYTES",
			cfg.OpenSearch.MaxBulkRequestBytes,
		)
		cfg.OpenSearch.MaxBulkRequestBytes = maxBulkRequestBytes
		if err != nil {
			errs = append(errs, err)
		}
	}

	if usesTarget != nil && usesTarget(logconfig.LogOutputLoki) {
		value, err := env.GetEnvDuration(
			"HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_LOKI_FLUSH_INTERVAL_SECOND",
			cfg.Loki.FlushInterval,
		)
		cfg.Loki.FlushInterval = value
		if err != nil {
			errs = append(errs, err)
		}
		maxEntries, err := env.GetEnvInt(
			"HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_LOKI_PUSH_ENTRIES",
			cfg.Loki.MaxEntries,
		)
		cfg.Loki.MaxEntries = maxEntries
		if err != nil {
			errs = append(errs, err)
		}
		bufferFlushBytes, err := env.GetEnvInt(
			"HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_LOKI_BUFFER_FLUSH_BYTES",
			cfg.Loki.BufferFlushBytes,
		)
		cfg.Loki.BufferFlushBytes = bufferFlushBytes
		if err != nil {
			errs = append(errs, err)
		}
		fallbackQueueMaxEntries, err := env.GetEnvInt(
			"HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_LOKI_FALLBACK_QUEUE_ENTRIES",
			cfg.Loki.FallbackQueueMaxEntries,
		)
		cfg.Loki.FallbackQueueMaxEntries = fallbackQueueMaxEntries
		if err != nil {
			errs = append(errs, err)
		}
	}

	if usesTarget != nil && usesTarget(logconfig.LogOutputCloudWatchLogs) {
		value, err := env.GetEnvDuration(
			"HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_CLOUDWATCH_FLUSH_INTERVAL_SECOND",
			cfg.CloudWatchLogs.FlushInterval,
		)
		cfg.CloudWatchLogs.FlushInterval = value
		if err != nil {
			errs = append(errs, err)
		}
		bufferFlushMaxEvents, err := env.GetEnvInt(
			"HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_CLOUDWATCH_BUFFER_FLUSH_MAX_EVENTS",
			cfg.CloudWatchLogs.BufferFlushMaxEvents,
		)
		cfg.CloudWatchLogs.BufferFlushMaxEvents = bufferFlushMaxEvents
		if err != nil {
			errs = append(errs, err)
		}
		bufferFlushBytes, err := env.GetEnvInt(
			"HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_CLOUDWATCH_BUFFER_FLUSH_BYTES",
			cfg.CloudWatchLogs.BufferFlushBytes,
		)
		cfg.CloudWatchLogs.BufferFlushBytes = bufferFlushBytes
		if err != nil {
			errs = append(errs, err)
		}
		fallbackQueueMaxEvents, err := env.GetEnvInt(
			"HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_CLOUDWATCH_FALLBACK_QUEUE_EVENTS",
			cfg.CloudWatchLogs.FallbackQueueMaxEvents,
		)
		cfg.CloudWatchLogs.FallbackQueueMaxEvents = fallbackQueueMaxEvents
		if err != nil {
			errs = append(errs, err)
		}
	}

	if usesTarget != nil && usesTarget(logconfig.LogOutputSlack) {
		intervals, err := env.GetEnvDurationSlice(
			"HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_SLACK_INTERVALS",
			cfg.Slack.Intervals,
			time.Minute,
		)
		cfg.Slack.Intervals = intervals
		if err != nil {
			errs = append(errs, err)
		}
		maxLogs, err := env.GetEnvInt(
			"HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_SLACK_LOGS_PER_NOTIFICATION",
			cfg.Slack.MaxLogsPerNotification,
		)
		cfg.Slack.MaxLogsPerNotification = maxLogs
		if err != nil {
			errs = append(errs, err)
		}
		maxRestThreshold, err := env.GetEnvDurationMinute(
			"HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_SLACK_REST_THRESHOLD_MINUTE",
			cfg.Slack.MaxRestThreshold,
		)
		cfg.Slack.MaxRestThreshold = maxRestThreshold
		if err != nil {
			errs = append(errs, err)
		}
	}

	return cfg, errors.Join(errs...)
}

// loadTimeoutBudgetPolicyは読み込み済みの値を解析します。keyはエラー表示にだけ使います。
func loadTimeoutBudgetPolicy(value, key string) (timeoutbudget.TimeoutBudgetPolicy, error) {
	policy, err := timeoutbudget.ParseTimeoutBudgetPolicy(value)
	if err != nil {
		return "", newInvalidEnvironmentValueError(key, value, err, "invalid %s: %v", key, err)
	}
	return policy, nil
}

func loadSpoolConfig(env pkgenv.Lookup, appCfg AppConfig) (pkgspool.SpoolConfig, error) {
	var environmentErrors []error
	var cfg pkgspool.SpoolConfig
	spoolSelected := false

	if failureSpoolSelectedForBackend(
		appCfg.Logger.LogOutputDestinations.Has(logconfig.LogOutputOpenSearch),
		appCfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig,
	) || failureSpoolSelectedForBackend(
		appCfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations.Has(logconfig.LogOutputOpenSearch),
		appCfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig,
	) {
		runnerConfig, err := loadOpensearchSpoolRunnerConfig(env)
		cfg.RunnerConfig.OpensearchConfig = runnerConfig
		if err != nil {
			environmentErrors = append(environmentErrors, err)
		}
		spoolSelected = true
	}
	if failureSpoolSelectedForBackend(
		appCfg.Logger.LogOutputDestinations.Has(logconfig.LogOutputLoki),
		appCfg.Logger.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig,
	) || failureSpoolSelectedForBackend(
		appCfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations.Has(logconfig.LogOutputLoki),
		appCfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig,
	) {
		runnerConfig, err := loadLokiSpoolRunnerConfig(env)
		cfg.RunnerConfig.LokiConfig = runnerConfig
		if err != nil {
			environmentErrors = append(environmentErrors, err)
		}
		spoolSelected = true
	}
	if failureSpoolSelectedForBackend(
		appCfg.Logger.LogOutputDestinations.Has(logconfig.LogOutputCloudWatchLogs),
		appCfg.Logger.CloudWatchLogsConfig.CloudWatchLogsFlushManagerConfig.FailureHandlerConfig,
	) || failureSpoolSelectedForBackend(
		appCfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations.Has(logconfig.LogOutputCloudWatchLogs),
		appCfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.CloudWatchLogsFlushManagerConfig.FailureHandlerConfig,
	) {
		runnerConfig, err := loadCloudWatchLogsSpoolRunnerConfig(env)
		cfg.RunnerConfig.CloudWatchLogsConfig = runnerConfig
		if err != nil {
			environmentErrors = append(environmentErrors, err)
		}
		spoolSelected = true
	}
	if failureSpoolSelectedForBackend(
		appCfg.Logger.LogOutputDestinations.Has(logconfig.LogOutputClickHouse),
		appCfg.Logger.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig,
	) || failureSpoolSelectedForBackend(
		appCfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations.Has(logconfig.LogOutputClickHouse),
		appCfg.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig,
	) {
		runnerConfig, err := loadClickHouseSpoolRunnerConfig(env)
		cfg.RunnerConfig.ClickHouseConfig = runnerConfig
		if err != nil {
			environmentErrors = append(environmentErrors, err)
		}
		spoolSelected = true
	}

	if spoolSelected {
		maxRestartSpoolTimes, maxRestartSpoolTimesErr := env.GetEnvInt("HETEROLOG_MAX_RESTART_SPOOL_TIMES", spoolsupervisor.DefaultMaxRestart)
		if maxRestartSpoolTimesErr != nil {
			environmentErrors = append(environmentErrors, maxRestartSpoolTimesErr)
		}
		cfg.SupervisorConfig = pkgspoolsupervisor.RunnerSupervisorConfig{
			RestartDelay: spoolsupervisor.DefaultRestartDelay,
			MaxRestarts:  maxRestartSpoolTimes,
		}
	}
	return cfg, errors.Join(environmentErrors...)
}

func loadObjectStorageCompressedLogFileUploadFailureStoreConfig(
	env pkgenv.Lookup,
	appCfg AppConfig,
) (objectstoragecompressedlog.Config, error) {
	standard, standardErr := loadSelectedConfig(
		standardObjectStorageCompressedLogFileUploadFailureStoreRequired(appCfg),
		func() (objectstoragecompressedlog.FailureStoreConfig, error) {
			return loadStandardObjectStorageCompressedLogFileUploadFailureStore(env)
		},
	)
	delivery, deliveryErr := loadSelectedConfig(
		deliveryObjectStorageCompressedLogFileUploadFailureStoreRequired(appCfg),
		func() (objectstoragecompressedlog.FailureStoreConfig, error) {
			return loadDeliveryObjectStorageCompressedLogFileUploadFailureStore(env)
		},
	)

	cfg := objectstoragecompressedlog.Config{Standard: standard, Delivery: delivery}
	return cfg, errors.Join(
		standardErr,
		deliveryErr,
		objectstoragecompressedlog.ValidateConfig(cfg),
	)
}

func containsFailureStoreDurableType(
	types []failurestore.DurableType,
	want failurestore.DurableType,
) bool {
	for _, durableType := range types {
		if durableType == want {
			return true
		}
	}
	return false
}

func standardObjectStorageCompressedLogFileUploadFailureStoreRequired(appCfg AppConfig) bool {
	required := objectStorageCompressedLogFileUploadFailureStoreRequirements(appCfg)
	return required[pkglogger.LoggerContractSync] ||
		required[pkglogger.LoggerContractAsyncBestEffort]
}

func deliveryObjectStorageCompressedLogFileUploadFailureStoreRequired(appCfg AppConfig) bool {
	required := objectStorageCompressedLogFileUploadFailureStoreRequirements(appCfg)
	return required[pkglogger.LoggerContractDelivery]
}

// failureSpoolSelectedForBackendは、通常出力としてbackendが選択され、かつその
// DurableTypeにspoolが含まれる場合にtrueを返します。
func failureSpoolSelectedForBackend(selected bool, cfg failure.FailureHandlerConfig) bool {
	return selected && containsDurableType(cfg.DurableType, failure.DurableTypeSpool)
}

// loadSelectedConfigは選択されたsubconfigだけをenvironmentから読みます。
// 未選択時はloadを呼ばないため、残存する未使用keyの不正値を起動errorへ混入させません。
func loadSelectedConfig[T any](selected bool, load func() (T, error)) (T, error) {
	if selected {
		return load()
	}
	var zero T
	return zero, nil
}

// loadFailureHandlerSelectionはfailure handlerの選択条件を先に読みます。
// DurableTypeを保存先選択の正本とし、noneを単独指定した場合だけdirect degrade設定を読みます。
func loadFailureHandlerSelection(
	env pkgenv.Lookup,
	enableDegradeKey string,
	defaultEnableDegrade bool,
	maxConcurrentKey string,
	defaultMaxConcurrent int,
	durableTypesKey string,
	defaultDurableTypes []pkgloggerdurable.DurableType,
	spillTypesKey string,
	defaultSpillTypes []pkgloggerdurable.SpillType,
) (failure.FailureHandlerConfig, error) {
	var environmentErrors []error
	enableDegradeValue, enableDegradeValueErr := env.GetEnvBool(enableDegradeKey, defaultEnableDegrade)
	if enableDegradeValueErr != nil {
		environmentErrors = append(environmentErrors, enableDegradeValueErr)
	}
	maxConcurrentValue, maxConcurrentValueErr := env.GetEnvInt(maxConcurrentKey, defaultMaxConcurrent)
	if maxConcurrentValueErr != nil {
		environmentErrors = append(environmentErrors, maxConcurrentValueErr)
	}
	cfg := failure.FailureHandlerConfig{
		EnableDegrade: enableDegradeValue,
		MaxConcurrent: maxConcurrentValue,
		DurableType:   loadDurableTypes(env.GetEnvSliceString(durableTypesKey, nil), defaultDurableTypes),
	}
	if cfg.EnableDegrade && durableTypesSelectNone(cfg.DurableType) {
		cfg.SpillType = loadSpillTypes(env.GetEnvSliceString(spillTypesKey, nil), defaultSpillTypes)
	}
	return cfg, errors.Join(environmentErrors...)
}

// loadDeliveryFailureHandlerSelectionはDeliveryのfile archiveを読むか先に確定します。
// DurableType=noneを単独指定した場合だけdirect degrade設定を読みます。
func loadDeliveryFailureHandlerSelection(
	env pkgenv.Lookup,
	enableDegradeKey string,
	defaultEnableDegrade bool,
	durableTypesKey string,
	defaultDurableTypes []pkgloggerdurable.DurableType,
	spillTypesKey string,
	defaultSpillTypes []pkgloggerdurable.SpillType,
) (failure.DeliveryFailureHandlerConfig, error) {
	var environmentErrors []error
	enableDegradeValue, enableDegradeValueErr := env.GetEnvBool(enableDegradeKey, defaultEnableDegrade)
	if enableDegradeValueErr != nil {
		environmentErrors = append(environmentErrors, enableDegradeValueErr)
	}
	cfg := failure.DeliveryFailureHandlerConfig{
		EnableDegrade: enableDegradeValue,
		DurableType:   loadDurableTypes(env.GetEnvSliceString(durableTypesKey, nil), defaultDurableTypes),
	}
	if cfg.EnableDegrade && durableTypesSelectNone(cfg.DurableType) {
		cfg.SpillType = loadSpillTypes(env.GetEnvSliceString(spillTypesKey, nil), defaultSpillTypes)
	}
	return cfg, errors.Join(environmentErrors...)
}

func loadOpensearchSpoolRunnerConfig(env pkgenv.Lookup) (pkgspoolrunner.BackendSpoolRunnerConfig, error) {
	var environmentErrors []error
	opensearchSpoolRunnerIntervalMillisecond, opensearchSpoolRunnerIntervalMillisecondErr := env.GetEnvDurationMillisecond("HETEROLOG_OPENSEARCH_SPOOL_RUNNER_INTERVAL_MILLISECOND", spoolrunner.DefaultRunnerInterval)
	if opensearchSpoolRunnerIntervalMillisecondErr != nil {
		environmentErrors = append(environmentErrors, opensearchSpoolRunnerIntervalMillisecondErr)
	}
	enableOpensearchSpoolRunnerStopOnError, enableOpensearchSpoolRunnerStopOnErrorErr := env.GetEnvBool("HETEROLOG_ENABLE_OPENSEARCH_SPOOL_RUNNER_STOP_ON_ERROR", spoolrunner.DefaultStopOnError)
	if enableOpensearchSpoolRunnerStopOnErrorErr != nil {
		environmentErrors = append(environmentErrors, enableOpensearchSpoolRunnerStopOnErrorErr)
	}
	enablePurgeOpensearchSpoolDone, enablePurgeOpensearchSpoolDoneErr := env.GetEnvBool("HETEROLOG_ENABLE_PURGE_OPENSEARCH_SPOOL_DONE", spoollifecycle.DefaultEnablePurgeDone)
	if enablePurgeOpensearchSpoolDoneErr != nil {
		environmentErrors = append(environmentErrors, enablePurgeOpensearchSpoolDoneErr)
	}
	maxOpensearchSpoolDoneAge, maxOpensearchSpoolDoneAgeErr := env.GetEnvInt("HETEROLOG_MAX_OPENSEARCH_SPOOL_DONE_AGE", spoollifecycle.DefaultDoneMaxFileAge)
	if maxOpensearchSpoolDoneAgeErr != nil {
		environmentErrors = append(environmentErrors, maxOpensearchSpoolDoneAgeErr)
	}
	maxOpensearchSpoolDoneDirSizeMb, maxOpensearchSpoolDoneDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_OPENSEARCH_SPOOL_DONE_DIR_SIZE_MB", spoollifecycle.DefaultDoneMaxDirSizeMB, BytesPerMegabyte)
	if maxOpensearchSpoolDoneDirSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxOpensearchSpoolDoneDirSizeMbErr)
	}
	enablePurgeOpensearchSpoolDead, enablePurgeOpensearchSpoolDeadErr := env.GetEnvBool("HETEROLOG_ENABLE_PURGE_OPENSEARCH_SPOOL_DEAD", spoollifecycle.DefaultEnablePurgeDead)
	if enablePurgeOpensearchSpoolDeadErr != nil {
		environmentErrors = append(environmentErrors, enablePurgeOpensearchSpoolDeadErr)
	}
	maxOpensearchSpoolDeadAge, maxOpensearchSpoolDeadAgeErr := env.GetEnvInt("HETEROLOG_MAX_OPENSEARCH_SPOOL_DEAD_AGE", spoollifecycle.DefaultDeadMaxFileAges)
	if maxOpensearchSpoolDeadAgeErr != nil {
		environmentErrors = append(environmentErrors, maxOpensearchSpoolDeadAgeErr)
	}
	maxOpensearchSpoolDeadDirSizeMb, maxOpensearchSpoolDeadDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_OPENSEARCH_SPOOL_DEAD_DIR_SIZE_MB", spoollifecycle.DefaultDeadMaxDirSizes, BytesPerMegabyte)
	if maxOpensearchSpoolDeadDirSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxOpensearchSpoolDeadDirSizeMbErr)
	}
	enablePurgeOpensearchSpoolQuarantine, enablePurgeOpensearchSpoolQuarantineErr := env.GetEnvBool("HETEROLOG_ENABLE_PURGE_OPENSEARCH_SPOOL_QUARANTINE", spoollifecycle.DefaultEnablePurgeQuarantine)
	if enablePurgeOpensearchSpoolQuarantineErr != nil {
		environmentErrors = append(environmentErrors, enablePurgeOpensearchSpoolQuarantineErr)
	}
	maxOpensearchSpoolQuarantineAge, maxOpensearchSpoolQuarantineAgeErr := env.GetEnvInt("HETEROLOG_MAX_OPENSEARCH_SPOOL_QUARANTINE_AGE", spoollifecycle.DefaultQuarantineMaxFileAge)
	if maxOpensearchSpoolQuarantineAgeErr != nil {
		environmentErrors = append(environmentErrors, maxOpensearchSpoolQuarantineAgeErr)
	}
	maxOpensearchSpoolQuarantineDirSizeMb, maxOpensearchSpoolQuarantineDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_OPENSEARCH_SPOOL_QUARANTINE_DIR_SIZE_MB", spoollifecycle.DefaultQuarantineMaxDirSize, BytesPerMegabyte)
	if maxOpensearchSpoolQuarantineDirSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxOpensearchSpoolQuarantineDirSizeMbErr)
	}
	opensearchSpoolRunnerPurgeIntervalSecond, opensearchSpoolRunnerPurgeIntervalSecondErr := env.GetEnvDuration("HETEROLOG_OPENSEARCH_SPOOL_RUNNER_PURGE_INTERVAL_SECOND", spoollifecycle.DefaultSizePurgeInterval)
	if opensearchSpoolRunnerPurgeIntervalSecondErr != nil {
		environmentErrors = append(environmentErrors, opensearchSpoolRunnerPurgeIntervalSecondErr)
	}
	enableOpensearchSpoolSizeEstimateCache, enableOpensearchSpoolSizeEstimateCacheErr := env.GetEnvBool("HETEROLOG_ENABLE_OPENSEARCH_SPOOL_SIZE_ESTIMATE_CACHE", spoollifecycle.DefaultEnableSizeEstimateCache)
	if enableOpensearchSpoolSizeEstimateCacheErr != nil {
		environmentErrors = append(environmentErrors, enableOpensearchSpoolSizeEstimateCacheErr)
	}
	opensearchSpoolDirFullScanEveryNChecks, opensearchSpoolDirFullScanEveryNChecksErr := env.GetEnvInt("HETEROLOG_OPENSEARCH_SPOOL_DIR_FULL_SCAN_EVERY_N_CHECKS", spoollifecycle.DefaultSizeFullScanEveryNChecks)
	if opensearchSpoolDirFullScanEveryNChecksErr != nil {
		environmentErrors = append(environmentErrors, opensearchSpoolDirFullScanEveryNChecksErr)
	}
	opensearchSpoolFullScanMaxInterval, opensearchSpoolFullScanMaxIntervalErr := env.GetEnvDuration(
		"HETEROLOG_MAX_OPENSEARCH_SPOOL_FULL_SCAN_INTERVAL_SECOND",
		spoollifecycle.DefaultSizeFullScanMaxInterval,
	)
	if opensearchSpoolFullScanMaxIntervalErr != nil {
		environmentErrors = append(environmentErrors, opensearchSpoolFullScanMaxIntervalErr)
	}
	opensearchSpoolRunnerRetryFinalizeDelayMillisecond, opensearchSpoolRunnerRetryFinalizeDelayMillisecondErr := env.GetEnvDurationMillisecond("HETEROLOG_OPENSEARCH_SPOOL_RUNNER_RETRY_FINALIZE_DELAY_MILLISECOND", spoolrunner.DefaultFinalizeRetryDelay)
	if opensearchSpoolRunnerRetryFinalizeDelayMillisecondErr != nil {
		environmentErrors = append(environmentErrors, opensearchSpoolRunnerRetryFinalizeDelayMillisecondErr)
	}
	maxOpensearchSpoolFinalizeFailureEntries, maxOpensearchSpoolFinalizeFailureEntriesErr := env.GetEnvInt("HETEROLOG_MAX_OPENSEARCH_SPOOL_FINALIZE_FAILURE_ENTRIES", spoolrunner.DefaultMaxFinalizeEntries)
	if maxOpensearchSpoolFinalizeFailureEntriesErr != nil {
		environmentErrors = append(environmentErrors, maxOpensearchSpoolFinalizeFailureEntriesErr)
	}
	opensearchSpoolRunnerMaxRetryAfterSecond, opensearchSpoolRunnerMaxRetryAfterSecondErr := getEnvMaxRetryAfter(env, "HETEROLOG_OPENSEARCH_SPOOL_RUNNER_MAX_RETRY_AFTER_SECOND", spoolrunner.DefaultMaxRetryAfter)
	if opensearchSpoolRunnerMaxRetryAfterSecondErr != nil {
		environmentErrors = append(environmentErrors, opensearchSpoolRunnerMaxRetryAfterSecondErr)
	}
	return pkgspoolrunner.BackendSpoolRunnerConfig{
		Runner: pkgspoolrunner.RunnerConfig{
			Interval:    opensearchSpoolRunnerIntervalMillisecond,
			StopOnError: enableOpensearchSpoolRunnerStopOnError,
		},
		LifeCycle: pkgspoollifecycle.LifecycleConfig{
			EnablePurgeDone:          enablePurgeOpensearchSpoolDone,
			DoneMaxFileAge:           maxOpensearchSpoolDoneAge,
			DoneMaxDirSize:           maxOpensearchSpoolDoneDirSizeMb,
			EnablePurgeDead:          enablePurgeOpensearchSpoolDead,
			DeadMaxFileAge:           maxOpensearchSpoolDeadAge,
			DeadMaxDirSize:           maxOpensearchSpoolDeadDirSizeMb,
			EnablePurgeQuarantine:    enablePurgeOpensearchSpoolQuarantine,
			QuarantineMaxFileAge:     maxOpensearchSpoolQuarantineAge,
			QuarantineMaxDirSize:     maxOpensearchSpoolQuarantineDirSizeMb,
			SizePurgeInterval:        opensearchSpoolRunnerPurgeIntervalSecond,
			EnableSizeEstimateCache:  enableOpensearchSpoolSizeEstimateCache,
			SizeFullScanEveryNChecks: opensearchSpoolDirFullScanEveryNChecks,
			SizeFullScanMaxInterval:  opensearchSpoolFullScanMaxInterval,
		},
		Worker: pkgspoolrunner.WorkerConfig{
			Finalize: pkgspoolrunner.FinalizeConfig{
				RetryDelay: opensearchSpoolRunnerRetryFinalizeDelayMillisecond,
				MaxEntries: maxOpensearchSpoolFinalizeFailureEntries,
			},
			MaxRetryAfter: opensearchSpoolRunnerMaxRetryAfterSecond,
		},
	}, errors.Join(environmentErrors...)
}

func loadLokiSpoolRunnerConfig(env pkgenv.Lookup) (pkgspoolrunner.BackendSpoolRunnerConfig, error) {
	var environmentErrors []error
	lokiSpoolRunnerIntervalMillisecond, lokiSpoolRunnerIntervalMillisecondErr := env.GetEnvDurationMillisecond("HETEROLOG_LOKI_SPOOL_RUNNER_INTERVAL_MILLISECOND", spoolrunner.DefaultRunnerInterval)
	if lokiSpoolRunnerIntervalMillisecondErr != nil {
		environmentErrors = append(environmentErrors, lokiSpoolRunnerIntervalMillisecondErr)
	}
	enableLokiSpoolRunnerStopOnError, enableLokiSpoolRunnerStopOnErrorErr := env.GetEnvBool("HETEROLOG_ENABLE_LOKI_SPOOL_RUNNER_STOP_ON_ERROR", spoolrunner.DefaultStopOnError)
	if enableLokiSpoolRunnerStopOnErrorErr != nil {
		environmentErrors = append(environmentErrors, enableLokiSpoolRunnerStopOnErrorErr)
	}
	enablePurgeLokiSpoolDone, enablePurgeLokiSpoolDoneErr := env.GetEnvBool("HETEROLOG_ENABLE_PURGE_LOKI_SPOOL_DONE", spoollifecycle.DefaultEnablePurgeDone)
	if enablePurgeLokiSpoolDoneErr != nil {
		environmentErrors = append(environmentErrors, enablePurgeLokiSpoolDoneErr)
	}
	maxLokiSpoolDoneAge, maxLokiSpoolDoneAgeErr := env.GetEnvInt("HETEROLOG_MAX_LOKI_SPOOL_DONE_AGE", spoollifecycle.DefaultDoneMaxFileAge)
	if maxLokiSpoolDoneAgeErr != nil {
		environmentErrors = append(environmentErrors, maxLokiSpoolDoneAgeErr)
	}
	maxLokiSpoolDoneDirSizeMb, maxLokiSpoolDoneDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_LOKI_SPOOL_DONE_DIR_SIZE_MB", spoollifecycle.DefaultDoneMaxDirSizeMB, BytesPerMegabyte)
	if maxLokiSpoolDoneDirSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxLokiSpoolDoneDirSizeMbErr)
	}
	enablePurgeLokiSpoolDead, enablePurgeLokiSpoolDeadErr := env.GetEnvBool("HETEROLOG_ENABLE_PURGE_LOKI_SPOOL_DEAD", spoollifecycle.DefaultEnablePurgeDead)
	if enablePurgeLokiSpoolDeadErr != nil {
		environmentErrors = append(environmentErrors, enablePurgeLokiSpoolDeadErr)
	}
	maxLokiSpoolDeadAge, maxLokiSpoolDeadAgeErr := env.GetEnvInt("HETEROLOG_MAX_LOKI_SPOOL_DEAD_AGE", spoollifecycle.DefaultDeadMaxFileAges)
	if maxLokiSpoolDeadAgeErr != nil {
		environmentErrors = append(environmentErrors, maxLokiSpoolDeadAgeErr)
	}
	maxLokiSpoolDeadDirSizeMb, maxLokiSpoolDeadDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_LOKI_SPOOL_DEAD_DIR_SIZE_MB", spoollifecycle.DefaultDeadMaxDirSizes, BytesPerMegabyte)
	if maxLokiSpoolDeadDirSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxLokiSpoolDeadDirSizeMbErr)
	}
	enablePurgeLokiSpoolQuarantine, enablePurgeLokiSpoolQuarantineErr := env.GetEnvBool("HETEROLOG_ENABLE_PURGE_LOKI_SPOOL_QUARANTINE", spoollifecycle.DefaultEnablePurgeQuarantine)
	if enablePurgeLokiSpoolQuarantineErr != nil {
		environmentErrors = append(environmentErrors, enablePurgeLokiSpoolQuarantineErr)
	}
	maxLokiSpoolQuarantineAge, maxLokiSpoolQuarantineAgeErr := env.GetEnvInt("HETEROLOG_MAX_LOKI_SPOOL_QUARANTINE_AGE", spoollifecycle.DefaultQuarantineMaxFileAge)
	if maxLokiSpoolQuarantineAgeErr != nil {
		environmentErrors = append(environmentErrors, maxLokiSpoolQuarantineAgeErr)
	}
	maxLokiSpoolQuarantineDirSizeMb, maxLokiSpoolQuarantineDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_LOKI_SPOOL_QUARANTINE_DIR_SIZE_MB", spoollifecycle.DefaultQuarantineMaxDirSize, BytesPerMegabyte)
	if maxLokiSpoolQuarantineDirSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxLokiSpoolQuarantineDirSizeMbErr)
	}
	lokiSpoolRunnerPurgeIntervalSecond, lokiSpoolRunnerPurgeIntervalSecondErr := env.GetEnvDuration("HETEROLOG_LOKI_SPOOL_RUNNER_PURGE_INTERVAL_SECOND", spoollifecycle.DefaultSizePurgeInterval)
	if lokiSpoolRunnerPurgeIntervalSecondErr != nil {
		environmentErrors = append(environmentErrors, lokiSpoolRunnerPurgeIntervalSecondErr)
	}
	enableLokiSpoolSizeEstimateCache, enableLokiSpoolSizeEstimateCacheErr := env.GetEnvBool("HETEROLOG_ENABLE_LOKI_SPOOL_SIZE_ESTIMATE_CACHE", spoollifecycle.DefaultEnableSizeEstimateCache)
	if enableLokiSpoolSizeEstimateCacheErr != nil {
		environmentErrors = append(environmentErrors, enableLokiSpoolSizeEstimateCacheErr)
	}
	lokiSpoolDirFullScanEveryNChecks, lokiSpoolDirFullScanEveryNChecksErr := env.GetEnvInt("HETEROLOG_LOKI_SPOOL_DIR_FULL_SCAN_EVERY_N_CHECKS", spoollifecycle.DefaultSizeFullScanEveryNChecks)
	if lokiSpoolDirFullScanEveryNChecksErr != nil {
		environmentErrors = append(environmentErrors, lokiSpoolDirFullScanEveryNChecksErr)
	}
	lokiSpoolFullScanMaxInterval, lokiSpoolFullScanMaxIntervalErr := env.GetEnvDuration(
		"HETEROLOG_MAX_LOKI_SPOOL_FULL_SCAN_INTERVAL_SECOND",
		spoollifecycle.DefaultSizeFullScanMaxInterval,
	)
	if lokiSpoolFullScanMaxIntervalErr != nil {
		environmentErrors = append(environmentErrors, lokiSpoolFullScanMaxIntervalErr)
	}
	lokiSpoolRunnerRetryFinalizeDelayMillisecond, lokiSpoolRunnerRetryFinalizeDelayMillisecondErr := env.GetEnvDurationMillisecond("HETEROLOG_LOKI_SPOOL_RUNNER_RETRY_FINALIZE_DELAY_MILLISECOND", spoolrunner.DefaultFinalizeRetryDelay)
	if lokiSpoolRunnerRetryFinalizeDelayMillisecondErr != nil {
		environmentErrors = append(environmentErrors, lokiSpoolRunnerRetryFinalizeDelayMillisecondErr)
	}
	maxLokiSpoolFinalizeFailureEntries, maxLokiSpoolFinalizeFailureEntriesErr := env.GetEnvInt("HETEROLOG_MAX_LOKI_SPOOL_FINALIZE_FAILURE_ENTRIES", spoolrunner.DefaultMaxFinalizeEntries)
	if maxLokiSpoolFinalizeFailureEntriesErr != nil {
		environmentErrors = append(environmentErrors, maxLokiSpoolFinalizeFailureEntriesErr)
	}
	lokiSpoolRunnerMaxRetryAfterSecond, lokiSpoolRunnerMaxRetryAfterSecondErr := getEnvMaxRetryAfter(env, "HETEROLOG_LOKI_SPOOL_RUNNER_MAX_RETRY_AFTER_SECOND", spoolrunner.DefaultMaxRetryAfter)
	if lokiSpoolRunnerMaxRetryAfterSecondErr != nil {
		environmentErrors = append(environmentErrors, lokiSpoolRunnerMaxRetryAfterSecondErr)
	}
	return pkgspoolrunner.BackendSpoolRunnerConfig{
		Runner: pkgspoolrunner.RunnerConfig{
			Interval:    lokiSpoolRunnerIntervalMillisecond,
			StopOnError: enableLokiSpoolRunnerStopOnError,
		},
		LifeCycle: pkgspoollifecycle.LifecycleConfig{
			EnablePurgeDone:          enablePurgeLokiSpoolDone,
			DoneMaxFileAge:           maxLokiSpoolDoneAge,
			DoneMaxDirSize:           maxLokiSpoolDoneDirSizeMb,
			EnablePurgeDead:          enablePurgeLokiSpoolDead,
			DeadMaxFileAge:           maxLokiSpoolDeadAge,
			DeadMaxDirSize:           maxLokiSpoolDeadDirSizeMb,
			EnablePurgeQuarantine:    enablePurgeLokiSpoolQuarantine,
			QuarantineMaxFileAge:     maxLokiSpoolQuarantineAge,
			QuarantineMaxDirSize:     maxLokiSpoolQuarantineDirSizeMb,
			SizePurgeInterval:        lokiSpoolRunnerPurgeIntervalSecond,
			EnableSizeEstimateCache:  enableLokiSpoolSizeEstimateCache,
			SizeFullScanEveryNChecks: lokiSpoolDirFullScanEveryNChecks,
			SizeFullScanMaxInterval:  lokiSpoolFullScanMaxInterval,
		},
		Worker: pkgspoolrunner.WorkerConfig{
			Finalize: pkgspoolrunner.FinalizeConfig{
				RetryDelay: lokiSpoolRunnerRetryFinalizeDelayMillisecond,
				MaxEntries: maxLokiSpoolFinalizeFailureEntries,
			},
			MaxRetryAfter: lokiSpoolRunnerMaxRetryAfterSecond,
		},
	}, errors.Join(environmentErrors...)
}

func loadCloudWatchLogsSpoolRunnerConfig(env pkgenv.Lookup) (pkgspoolrunner.BackendSpoolRunnerConfig, error) {
	var environmentErrors []error
	cloudwatchSpoolRunnerIntervalMillisecond, cloudwatchSpoolRunnerIntervalMillisecondErr := env.GetEnvDurationMillisecond("HETEROLOG_CLOUDWATCH_SPOOL_RUNNER_INTERVAL_MILLISECOND", spoolrunner.DefaultRunnerInterval)
	if cloudwatchSpoolRunnerIntervalMillisecondErr != nil {
		environmentErrors = append(environmentErrors, cloudwatchSpoolRunnerIntervalMillisecondErr)
	}
	enableCloudwatchSpoolRunnerStopOnError, enableCloudwatchSpoolRunnerStopOnErrorErr := env.GetEnvBool("HETEROLOG_ENABLE_CLOUDWATCH_SPOOL_RUNNER_STOP_ON_ERROR", spoolrunner.DefaultStopOnError)
	if enableCloudwatchSpoolRunnerStopOnErrorErr != nil {
		environmentErrors = append(environmentErrors, enableCloudwatchSpoolRunnerStopOnErrorErr)
	}
	enablePurgeCloudwatchSpoolDone, enablePurgeCloudwatchSpoolDoneErr := env.GetEnvBool("HETEROLOG_ENABLE_PURGE_CLOUDWATCH_SPOOL_DONE", spoollifecycle.DefaultEnablePurgeDone)
	if enablePurgeCloudwatchSpoolDoneErr != nil {
		environmentErrors = append(environmentErrors, enablePurgeCloudwatchSpoolDoneErr)
	}
	maxCloudwatchSpoolDoneAge, maxCloudwatchSpoolDoneAgeErr := env.GetEnvInt("HETEROLOG_MAX_CLOUDWATCH_SPOOL_DONE_AGE", spoollifecycle.DefaultDoneMaxFileAge)
	if maxCloudwatchSpoolDoneAgeErr != nil {
		environmentErrors = append(environmentErrors, maxCloudwatchSpoolDoneAgeErr)
	}
	maxCloudwatchSpoolDoneDirSizeMb, maxCloudwatchSpoolDoneDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_CLOUDWATCH_SPOOL_DONE_DIR_SIZE_MB", spoollifecycle.DefaultDoneMaxDirSizeMB, BytesPerMegabyte)
	if maxCloudwatchSpoolDoneDirSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxCloudwatchSpoolDoneDirSizeMbErr)
	}
	enablePurgeCloudwatchSpoolDead, enablePurgeCloudwatchSpoolDeadErr := env.GetEnvBool("HETEROLOG_ENABLE_PURGE_CLOUDWATCH_SPOOL_DEAD", spoollifecycle.DefaultEnablePurgeDead)
	if enablePurgeCloudwatchSpoolDeadErr != nil {
		environmentErrors = append(environmentErrors, enablePurgeCloudwatchSpoolDeadErr)
	}
	maxCloudwatchSpoolDeadAge, maxCloudwatchSpoolDeadAgeErr := env.GetEnvInt("HETEROLOG_MAX_CLOUDWATCH_SPOOL_DEAD_AGE", spoollifecycle.DefaultDeadMaxFileAges)
	if maxCloudwatchSpoolDeadAgeErr != nil {
		environmentErrors = append(environmentErrors, maxCloudwatchSpoolDeadAgeErr)
	}
	maxCloudwatchSpoolDeadDirSizeMb, maxCloudwatchSpoolDeadDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_CLOUDWATCH_SPOOL_DEAD_DIR_SIZE_MB", spoollifecycle.DefaultDeadMaxDirSizes, BytesPerMegabyte)
	if maxCloudwatchSpoolDeadDirSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxCloudwatchSpoolDeadDirSizeMbErr)
	}
	enablePurgeCloudwatchSpoolQuarantine, enablePurgeCloudwatchSpoolQuarantineErr := env.GetEnvBool("HETEROLOG_ENABLE_PURGE_CLOUDWATCH_SPOOL_QUARANTINE", spoollifecycle.DefaultEnablePurgeQuarantine)
	if enablePurgeCloudwatchSpoolQuarantineErr != nil {
		environmentErrors = append(environmentErrors, enablePurgeCloudwatchSpoolQuarantineErr)
	}
	maxCloudwatchSpoolQuarantineAge, maxCloudwatchSpoolQuarantineAgeErr := env.GetEnvInt("HETEROLOG_MAX_CLOUDWATCH_SPOOL_QUARANTINE_AGE", spoollifecycle.DefaultQuarantineMaxFileAge)
	if maxCloudwatchSpoolQuarantineAgeErr != nil {
		environmentErrors = append(environmentErrors, maxCloudwatchSpoolQuarantineAgeErr)
	}
	maxCloudwatchSpoolQuarantineDirSizeMb, maxCloudwatchSpoolQuarantineDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_CLOUDWATCH_SPOOL_QUARANTINE_DIR_SIZE_MB", spoollifecycle.DefaultQuarantineMaxDirSize, BytesPerMegabyte)
	if maxCloudwatchSpoolQuarantineDirSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxCloudwatchSpoolQuarantineDirSizeMbErr)
	}
	cloudwatchSpoolRunnerPurgeIntervalSecond, cloudwatchSpoolRunnerPurgeIntervalSecondErr := env.GetEnvDuration("HETEROLOG_CLOUDWATCH_SPOOL_RUNNER_PURGE_INTERVAL_SECOND", spoollifecycle.DefaultSizePurgeInterval)
	if cloudwatchSpoolRunnerPurgeIntervalSecondErr != nil {
		environmentErrors = append(environmentErrors, cloudwatchSpoolRunnerPurgeIntervalSecondErr)
	}
	enableCloudwatchSpoolSizeEstimateCache, enableCloudwatchSpoolSizeEstimateCacheErr := env.GetEnvBool("HETEROLOG_ENABLE_CLOUDWATCH_SPOOL_SIZE_ESTIMATE_CACHE", spoollifecycle.DefaultEnableSizeEstimateCache)
	if enableCloudwatchSpoolSizeEstimateCacheErr != nil {
		environmentErrors = append(environmentErrors, enableCloudwatchSpoolSizeEstimateCacheErr)
	}
	cloudwatchSpoolDirFullScanEveryNChecks, cloudwatchSpoolDirFullScanEveryNChecksErr := env.GetEnvInt("HETEROLOG_CLOUDWATCH_SPOOL_DIR_FULL_SCAN_EVERY_N_CHECKS", spoollifecycle.DefaultSizeFullScanEveryNChecks)
	if cloudwatchSpoolDirFullScanEveryNChecksErr != nil {
		environmentErrors = append(environmentErrors, cloudwatchSpoolDirFullScanEveryNChecksErr)
	}
	cloudWatchSpoolFullScanMaxInterval, cloudWatchSpoolFullScanMaxIntervalErr := env.GetEnvDuration(
		"HETEROLOG_MAX_CLOUDWATCH_SPOOL_FULL_SCAN_INTERVAL_SECOND",
		spoollifecycle.DefaultSizeFullScanMaxInterval,
	)
	if cloudWatchSpoolFullScanMaxIntervalErr != nil {
		environmentErrors = append(environmentErrors, cloudWatchSpoolFullScanMaxIntervalErr)
	}
	cloudwatchSpoolRunnerRetryFinalizeDelayMillisecond, cloudwatchSpoolRunnerRetryFinalizeDelayMillisecondErr := env.GetEnvDurationMillisecond("HETEROLOG_CLOUDWATCH_SPOOL_RUNNER_RETRY_FINALIZE_DELAY_MILLISECOND", spoolrunner.DefaultFinalizeRetryDelay)
	if cloudwatchSpoolRunnerRetryFinalizeDelayMillisecondErr != nil {
		environmentErrors = append(environmentErrors, cloudwatchSpoolRunnerRetryFinalizeDelayMillisecondErr)
	}
	maxCloudwatchSpoolFinalizeFailureEntries, maxCloudwatchSpoolFinalizeFailureEntriesErr := env.GetEnvInt("HETEROLOG_MAX_CLOUDWATCH_SPOOL_FINALIZE_FAILURE_ENTRIES", spoolrunner.DefaultMaxFinalizeEntries)
	if maxCloudwatchSpoolFinalizeFailureEntriesErr != nil {
		environmentErrors = append(environmentErrors, maxCloudwatchSpoolFinalizeFailureEntriesErr)
	}
	cloudwatchSpoolRunnerMaxRetryAfterSecond, cloudwatchSpoolRunnerMaxRetryAfterSecondErr := getEnvMaxRetryAfter(env, "HETEROLOG_CLOUDWATCH_SPOOL_RUNNER_MAX_RETRY_AFTER_SECOND", spoolrunner.DefaultMaxRetryAfter)
	if cloudwatchSpoolRunnerMaxRetryAfterSecondErr != nil {
		environmentErrors = append(environmentErrors, cloudwatchSpoolRunnerMaxRetryAfterSecondErr)
	}
	return pkgspoolrunner.BackendSpoolRunnerConfig{
		Runner: pkgspoolrunner.RunnerConfig{
			Interval:    cloudwatchSpoolRunnerIntervalMillisecond,
			StopOnError: enableCloudwatchSpoolRunnerStopOnError,
		},
		LifeCycle: pkgspoollifecycle.LifecycleConfig{
			EnablePurgeDone:          enablePurgeCloudwatchSpoolDone,
			DoneMaxFileAge:           maxCloudwatchSpoolDoneAge,
			DoneMaxDirSize:           maxCloudwatchSpoolDoneDirSizeMb,
			EnablePurgeDead:          enablePurgeCloudwatchSpoolDead,
			DeadMaxFileAge:           maxCloudwatchSpoolDeadAge,
			DeadMaxDirSize:           maxCloudwatchSpoolDeadDirSizeMb,
			EnablePurgeQuarantine:    enablePurgeCloudwatchSpoolQuarantine,
			QuarantineMaxFileAge:     maxCloudwatchSpoolQuarantineAge,
			QuarantineMaxDirSize:     maxCloudwatchSpoolQuarantineDirSizeMb,
			SizePurgeInterval:        cloudwatchSpoolRunnerPurgeIntervalSecond,
			EnableSizeEstimateCache:  enableCloudwatchSpoolSizeEstimateCache,
			SizeFullScanEveryNChecks: cloudwatchSpoolDirFullScanEveryNChecks,
			SizeFullScanMaxInterval:  cloudWatchSpoolFullScanMaxInterval,
		},
		Worker: pkgspoolrunner.WorkerConfig{
			Finalize: pkgspoolrunner.FinalizeConfig{
				RetryDelay: cloudwatchSpoolRunnerRetryFinalizeDelayMillisecond,
				MaxEntries: maxCloudwatchSpoolFinalizeFailureEntries,
			},
			MaxRetryAfter: cloudwatchSpoolRunnerMaxRetryAfterSecond,
		},
	}, errors.Join(environmentErrors...)
}

func loadClickHouseSpoolRunnerConfig(env pkgenv.Lookup) (pkgspoolrunner.BackendSpoolRunnerConfig, error) {
	var environmentErrors []error
	clickhouseSpoolRunnerIntervalMillisecond, clickhouseSpoolRunnerIntervalMillisecondErr := env.GetEnvDurationMillisecond("HETEROLOG_CLICKHOUSE_SPOOL_RUNNER_INTERVAL_MILLISECOND", spoolrunner.DefaultRunnerInterval)
	if clickhouseSpoolRunnerIntervalMillisecondErr != nil {
		environmentErrors = append(environmentErrors, clickhouseSpoolRunnerIntervalMillisecondErr)
	}
	enableClickhouseSpoolRunnerStopOnError, enableClickhouseSpoolRunnerStopOnErrorErr := env.GetEnvBool("HETEROLOG_ENABLE_CLICKHOUSE_SPOOL_RUNNER_STOP_ON_ERROR", spoolrunner.DefaultStopOnError)
	if enableClickhouseSpoolRunnerStopOnErrorErr != nil {
		environmentErrors = append(environmentErrors, enableClickhouseSpoolRunnerStopOnErrorErr)
	}
	enablePurgeClickhouseSpoolDone, enablePurgeClickhouseSpoolDoneErr := env.GetEnvBool("HETEROLOG_ENABLE_PURGE_CLICKHOUSE_SPOOL_DONE", spoollifecycle.DefaultEnablePurgeDone)
	if enablePurgeClickhouseSpoolDoneErr != nil {
		environmentErrors = append(environmentErrors, enablePurgeClickhouseSpoolDoneErr)
	}
	maxClickhouseSpoolDoneAge, maxClickhouseSpoolDoneAgeErr := env.GetEnvInt("HETEROLOG_MAX_CLICKHOUSE_SPOOL_DONE_AGE", spoollifecycle.DefaultDoneMaxFileAge)
	if maxClickhouseSpoolDoneAgeErr != nil {
		environmentErrors = append(environmentErrors, maxClickhouseSpoolDoneAgeErr)
	}
	maxClickhouseSpoolDoneDirSizeMb, maxClickhouseSpoolDoneDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_CLICKHOUSE_SPOOL_DONE_DIR_SIZE_MB", spoollifecycle.DefaultDoneMaxDirSizeMB, BytesPerMegabyte)
	if maxClickhouseSpoolDoneDirSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxClickhouseSpoolDoneDirSizeMbErr)
	}
	enablePurgeClickhouseSpoolDead, enablePurgeClickhouseSpoolDeadErr := env.GetEnvBool("HETEROLOG_ENABLE_PURGE_CLICKHOUSE_SPOOL_DEAD", spoollifecycle.DefaultEnablePurgeDead)
	if enablePurgeClickhouseSpoolDeadErr != nil {
		environmentErrors = append(environmentErrors, enablePurgeClickhouseSpoolDeadErr)
	}
	maxClickhouseSpoolDeadAge, maxClickhouseSpoolDeadAgeErr := env.GetEnvInt("HETEROLOG_MAX_CLICKHOUSE_SPOOL_DEAD_AGE", spoollifecycle.DefaultDeadMaxFileAges)
	if maxClickhouseSpoolDeadAgeErr != nil {
		environmentErrors = append(environmentErrors, maxClickhouseSpoolDeadAgeErr)
	}
	maxClickhouseSpoolDeadDirSizeMb, maxClickhouseSpoolDeadDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_CLICKHOUSE_SPOOL_DEAD_DIR_SIZE_MB", spoollifecycle.DefaultDeadMaxDirSizes, BytesPerMegabyte)
	if maxClickhouseSpoolDeadDirSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxClickhouseSpoolDeadDirSizeMbErr)
	}
	enablePurgeClickhouseSpoolQuarantine, enablePurgeClickhouseSpoolQuarantineErr := env.GetEnvBool("HETEROLOG_ENABLE_PURGE_CLICKHOUSE_SPOOL_QUARANTINE", spoollifecycle.DefaultEnablePurgeQuarantine)
	if enablePurgeClickhouseSpoolQuarantineErr != nil {
		environmentErrors = append(environmentErrors, enablePurgeClickhouseSpoolQuarantineErr)
	}
	maxClickhouseSpoolQuarantineAge, maxClickhouseSpoolQuarantineAgeErr := env.GetEnvInt("HETEROLOG_MAX_CLICKHOUSE_SPOOL_QUARANTINE_AGE", spoollifecycle.DefaultQuarantineMaxFileAge)
	if maxClickhouseSpoolQuarantineAgeErr != nil {
		environmentErrors = append(environmentErrors, maxClickhouseSpoolQuarantineAgeErr)
	}
	maxClickhouseSpoolQuarantineDirSizeMb, maxClickhouseSpoolQuarantineDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_CLICKHOUSE_SPOOL_QUARANTINE_DIR_SIZE_MB", spoollifecycle.DefaultQuarantineMaxDirSize, BytesPerMegabyte)
	if maxClickhouseSpoolQuarantineDirSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxClickhouseSpoolQuarantineDirSizeMbErr)
	}
	clickhouseSpoolRunnerPurgeIntervalSecond, clickhouseSpoolRunnerPurgeIntervalSecondErr := env.GetEnvDuration("HETEROLOG_CLICKHOUSE_SPOOL_RUNNER_PURGE_INTERVAL_SECOND", spoollifecycle.DefaultSizePurgeInterval)
	if clickhouseSpoolRunnerPurgeIntervalSecondErr != nil {
		environmentErrors = append(environmentErrors, clickhouseSpoolRunnerPurgeIntervalSecondErr)
	}
	enableClickhouseSpoolSizeEstimateCache, enableClickhouseSpoolSizeEstimateCacheErr := env.GetEnvBool("HETEROLOG_ENABLE_CLICKHOUSE_SPOOL_SIZE_ESTIMATE_CACHE", spoollifecycle.DefaultEnableSizeEstimateCache)
	if enableClickhouseSpoolSizeEstimateCacheErr != nil {
		environmentErrors = append(environmentErrors, enableClickhouseSpoolSizeEstimateCacheErr)
	}
	clickhouseSpoolDirFullScanEveryNChecks, clickhouseSpoolDirFullScanEveryNChecksErr := env.GetEnvInt("HETEROLOG_CLICKHOUSE_SPOOL_DIR_FULL_SCAN_EVERY_N_CHECKS", spoollifecycle.DefaultSizeFullScanEveryNChecks)
	if clickhouseSpoolDirFullScanEveryNChecksErr != nil {
		environmentErrors = append(environmentErrors, clickhouseSpoolDirFullScanEveryNChecksErr)
	}
	clickHouseSpoolFullScanMaxInterval, clickHouseSpoolFullScanMaxIntervalErr := env.GetEnvDuration(
		"HETEROLOG_MAX_CLICKHOUSE_SPOOL_FULL_SCAN_INTERVAL_SECOND",
		spoollifecycle.DefaultSizeFullScanMaxInterval,
	)
	if clickHouseSpoolFullScanMaxIntervalErr != nil {
		environmentErrors = append(environmentErrors, clickHouseSpoolFullScanMaxIntervalErr)
	}
	clickhouseSpoolRunnerRetryFinalizeDelayMillisecond, clickhouseSpoolRunnerRetryFinalizeDelayMillisecondErr := env.GetEnvDurationMillisecond("HETEROLOG_CLICKHOUSE_SPOOL_RUNNER_RETRY_FINALIZE_DELAY_MILLISECOND", spoolrunner.DefaultFinalizeRetryDelay)
	if clickhouseSpoolRunnerRetryFinalizeDelayMillisecondErr != nil {
		environmentErrors = append(environmentErrors, clickhouseSpoolRunnerRetryFinalizeDelayMillisecondErr)
	}
	maxClickhouseSpoolFinalizeFailureEntries, maxClickhouseSpoolFinalizeFailureEntriesErr := env.GetEnvInt("HETEROLOG_MAX_CLICKHOUSE_SPOOL_FINALIZE_FAILURE_ENTRIES", spoolrunner.DefaultMaxFinalizeEntries)
	if maxClickhouseSpoolFinalizeFailureEntriesErr != nil {
		environmentErrors = append(environmentErrors, maxClickhouseSpoolFinalizeFailureEntriesErr)
	}
	clickhouseSpoolRunnerMaxRetryAfterSecond, clickhouseSpoolRunnerMaxRetryAfterSecondErr := getEnvMaxRetryAfter(env, "HETEROLOG_CLICKHOUSE_SPOOL_RUNNER_MAX_RETRY_AFTER_SECOND", spoolrunner.DefaultMaxRetryAfter)
	if clickhouseSpoolRunnerMaxRetryAfterSecondErr != nil {
		environmentErrors = append(environmentErrors, clickhouseSpoolRunnerMaxRetryAfterSecondErr)
	}
	return pkgspoolrunner.BackendSpoolRunnerConfig{
		Runner: pkgspoolrunner.RunnerConfig{
			Interval:    clickhouseSpoolRunnerIntervalMillisecond,
			StopOnError: enableClickhouseSpoolRunnerStopOnError,
		},
		LifeCycle: pkgspoollifecycle.LifecycleConfig{
			EnablePurgeDone:          enablePurgeClickhouseSpoolDone,
			DoneMaxFileAge:           maxClickhouseSpoolDoneAge,
			DoneMaxDirSize:           maxClickhouseSpoolDoneDirSizeMb,
			EnablePurgeDead:          enablePurgeClickhouseSpoolDead,
			DeadMaxFileAge:           maxClickhouseSpoolDeadAge,
			DeadMaxDirSize:           maxClickhouseSpoolDeadDirSizeMb,
			EnablePurgeQuarantine:    enablePurgeClickhouseSpoolQuarantine,
			QuarantineMaxFileAge:     maxClickhouseSpoolQuarantineAge,
			QuarantineMaxDirSize:     maxClickhouseSpoolQuarantineDirSizeMb,
			SizePurgeInterval:        clickhouseSpoolRunnerPurgeIntervalSecond,
			EnableSizeEstimateCache:  enableClickhouseSpoolSizeEstimateCache,
			SizeFullScanEveryNChecks: clickhouseSpoolDirFullScanEveryNChecks,
			SizeFullScanMaxInterval:  clickHouseSpoolFullScanMaxInterval,
		},
		Worker: pkgspoolrunner.WorkerConfig{
			Finalize: pkgspoolrunner.FinalizeConfig{
				RetryDelay: clickhouseSpoolRunnerRetryFinalizeDelayMillisecond,
				MaxEntries: maxClickhouseSpoolFinalizeFailureEntries,
			},
			MaxRetryAfter: clickhouseSpoolRunnerMaxRetryAfterSecond,
		},
	}, errors.Join(environmentErrors...)
}

// 時計設定を読み込む
func loadClockConfig(env pkgenv.Lookup) (logconfig.ClockConfig, error) {
	var environmentErrors []error
	enableLocalTimeLog, enableLocalTimeLogErr := env.GetEnvBool("HETEROLOG_ENABLE_LOCAL_TIME_LOG", logconfig.DefaultIsLocalTimeEnabled)
	if enableLocalTimeLogErr != nil {
		environmentErrors = append(environmentErrors, enableLocalTimeLogErr)
	}
	return logconfig.ClockConfig{
		IsLocalTimeEnabled: enableLocalTimeLog,
		TimeZone:           env.GetEnvString("HETEROLOG_TIMEZONE_LOG", logconfig.DefaultTimeZone),
	}, errors.Join(environmentErrors...)
}

func loadCommonObjectStorageS3Config(env pkgenv.Lookup) (objectstorage.S3Config, error) {
	var environmentErrors []error
	enableObjectStorageS3ForcePathStyle, enableObjectStorageS3ForcePathStyleErr := env.GetEnvBool("HETEROLOG_ENABLE_OBJECT_STORAGE_S3_FORCE_PATH_STYLE", objectstorages3cfg.DefaultForcePathStyle)
	if enableObjectStorageS3ForcePathStyleErr != nil {
		environmentErrors = append(environmentErrors, enableObjectStorageS3ForcePathStyleErr)
	}
	objectStorageS3TimeoutSecond, objectStorageS3TimeoutSecondErr := env.GetEnvDuration("HETEROLOG_OBJECT_STORAGE_S3_TIMEOUT_SECOND", objectstorages3cfg.DefaultTimeout)
	if objectStorageS3TimeoutSecondErr != nil {
		environmentErrors = append(environmentErrors, objectStorageS3TimeoutSecondErr)
	}
	return objectstorage.S3Config{
		Region:          env.GetEnvString("HETEROLOG_OBJECT_STORAGE_S3_REGION", env.GetEnvString("AWS_REGION", objectstorages3cfg.DefaultRegion)),
		Endpoint:        env.GetEnvString("HETEROLOG_OBJECT_STORAGE_S3_ENDPOINT", objectstorages3cfg.DefaultEndpoint),
		AccessKeyID:     env.GetEnvString("HETEROLOG_OBJECT_STORAGE_S3_ACCESS_KEY_ID", env.GetEnvString("AWS_ACCESS_KEY_ID", "")),
		SecretAccessKey: env.GetEnvString("HETEROLOG_OBJECT_STORAGE_S3_SECRET_ACCESS_KEY", env.GetEnvString("AWS_SECRET_ACCESS_KEY", "")),
		SessionToken:    env.GetEnvString("HETEROLOG_OBJECT_STORAGE_S3_SESSION_TOKEN", env.GetEnvString("AWS_SESSION_TOKEN", "")),
		ForcePathStyle:  enableObjectStorageS3ForcePathStyle,
		Timeout:         objectStorageS3TimeoutSecond,
	}, errors.Join(environmentErrors...)
}

func loadLogFileObjectStorageUploadConfig(env pkgenv.Lookup) (objectstorage.LogFileUploadConfig, error) {
	var errs []error
	enabled, err := env.GetEnvBool(
		"HETEROLOG_ENABLE_LOG_FILE_OBJECT_STORAGE_UPLOAD",
		objectstorages3cfg.DefaultUploadEnabled,
	)
	if err != nil {
		errs = append(errs, err)
	}

	var defaultEnabled, errorEnabled, debugEnabled bool
	var targets []objectstorage.UploadTarget
	var commonS3 objectstorage.S3Config
	if enabled {
		var commonS3Err error
		commonS3, commonS3Err = loadCommonObjectStorageS3Config(env)
		if commonS3Err != nil {
			errs = append(errs, commonS3Err)
		}
		defaultEnabled, err = env.GetEnvBool(
			"HETEROLOG_ENABLE_LOG_FILE_DEFAULT_OBJECT_STORAGE_UPLOAD",
			objectstorages3cfg.DefaultUploadTargetEnabled,
		)
		if err != nil {
			errs = append(errs, err)
		}
		errorEnabled, err = env.GetEnvBool(
			"HETEROLOG_ENABLE_LOG_FILE_ERROR_OBJECT_STORAGE_UPLOAD",
			objectstorages3cfg.DefaultUploadTargetEnabled,
		)
		if err != nil {
			errs = append(errs, err)
		}
		debugEnabled, err = env.GetEnvBool(
			"HETEROLOG_ENABLE_LOG_FILE_DEBUG_OBJECT_STORAGE_UPLOAD",
			objectstorages3cfg.DefaultUploadTargetEnabled,
		)
		if err != nil {
			errs = append(errs, err)
		}
		targets = objectStorageUploadTargets(env.GetEnvSliceString("HETEROLOG_LOG_FILE_OBJECT_STORAGE_UPLOAD_TYPE", nil))
	}
	defaultS3 := commonS3
	defaultS3.Bucket = env.GetEnvString("HETEROLOG_LOG_FILE_OBJECT_STORAGE_S3_BUCKET", objectstorages3cfg.DefaultBucket)
	defaultS3.Prefix = env.GetEnvString("HETEROLOG_LOG_FILE_OBJECT_STORAGE_S3_PREFIX", objectstorages3cfg.DefaultPrefix)
	errorS3 := commonS3
	errorS3.Bucket = env.GetEnvString("HETEROLOG_LOG_FILE_ERROR_OBJECT_STORAGE_S3_BUCKET", defaultS3.Bucket)
	errorS3.Prefix = env.GetEnvString("HETEROLOG_LOG_FILE_ERROR_OBJECT_STORAGE_S3_PREFIX", defaultS3.Prefix)
	debugS3 := commonS3
	debugS3.Bucket = env.GetEnvString("HETEROLOG_LOG_FILE_DEBUG_OBJECT_STORAGE_S3_BUCKET", defaultS3.Bucket)
	debugS3.Prefix = env.GetEnvString("HETEROLOG_LOG_FILE_DEBUG_OBJECT_STORAGE_S3_PREFIX", defaultS3.Prefix)

	cfg := newLogFileObjectStorageUploadConfig(
		enabled,
		defaultEnabled,
		errorEnabled,
		debugEnabled,
		targets,
		defaultS3,
		errorS3,
		debugS3,
	)
	cfg, destinationsErr := configureLogFileS3Destinations(cfg,
		env.GetEnvString("HETEROLOG_LOG_FILE_OBJECT_STORAGE_S3_DESTINATIONS", ""),
		env.GetEnvString("HETEROLOG_LOG_FILE_ERROR_OBJECT_STORAGE_S3_DESTINATIONS", ""),
		env.GetEnvString("HETEROLOG_LOG_FILE_DEBUG_OBJECT_STORAGE_S3_DESTINATIONS", ""),
	)
	return cfg, errors.Join(append(errs, destinationsErr)...)
}

func loadAsyncLogFileObjectStorageUploadConfig(env pkgenv.Lookup) (objectstorage.LogFileUploadConfig, error) {
	var errs []error
	enabled, err := env.GetEnvBool(
		"HETEROLOG_ENABLE_ASYNC_LOG_FILE_OBJECT_STORAGE_UPLOAD",
		objectstorages3cfg.DefaultUploadEnabled,
	)
	if err != nil {
		errs = append(errs, err)
	}

	var defaultEnabled, errorEnabled, debugEnabled bool
	var targets []objectstorage.UploadTarget
	var commonS3 objectstorage.S3Config
	if enabled {
		var commonS3Err error
		commonS3, commonS3Err = loadCommonObjectStorageS3Config(env)
		if commonS3Err != nil {
			errs = append(errs, commonS3Err)
		}
		defaultEnabled, err = env.GetEnvBool(
			"HETEROLOG_ENABLE_ASYNC_LOG_FILE_DEFAULT_OBJECT_STORAGE_UPLOAD",
			objectstorages3cfg.DefaultUploadTargetEnabled,
		)
		if err != nil {
			errs = append(errs, err)
		}
		errorEnabled, err = env.GetEnvBool(
			"HETEROLOG_ENABLE_ASYNC_LOG_FILE_ERROR_OBJECT_STORAGE_UPLOAD",
			objectstorages3cfg.DefaultUploadTargetEnabled,
		)
		if err != nil {
			errs = append(errs, err)
		}
		debugEnabled, err = env.GetEnvBool(
			"HETEROLOG_ENABLE_ASYNC_LOG_FILE_DEBUG_OBJECT_STORAGE_UPLOAD",
			objectstorages3cfg.DefaultUploadTargetEnabled,
		)
		if err != nil {
			errs = append(errs, err)
		}
		targets = objectStorageUploadTargets(env.GetEnvSliceString("HETEROLOG_ASYNC_LOG_FILE_OBJECT_STORAGE_UPLOAD_TYPE", nil))
	}
	defaultS3 := commonS3
	defaultS3.Bucket = env.GetEnvString("HETEROLOG_ASYNC_LOG_FILE_OBJECT_STORAGE_S3_BUCKET", objectstorages3cfg.DefaultBucket)
	defaultS3.Prefix = env.GetEnvString("HETEROLOG_ASYNC_LOG_FILE_OBJECT_STORAGE_S3_PREFIX", objectstorages3cfg.DefaultPrefix)
	errorS3 := commonS3
	errorS3.Bucket = env.GetEnvString("HETEROLOG_ASYNC_LOG_FILE_ERROR_OBJECT_STORAGE_S3_BUCKET", defaultS3.Bucket)
	errorS3.Prefix = env.GetEnvString("HETEROLOG_ASYNC_LOG_FILE_ERROR_OBJECT_STORAGE_S3_PREFIX", defaultS3.Prefix)
	debugS3 := commonS3
	debugS3.Bucket = env.GetEnvString("HETEROLOG_ASYNC_LOG_FILE_DEBUG_OBJECT_STORAGE_S3_BUCKET", defaultS3.Bucket)
	debugS3.Prefix = env.GetEnvString("HETEROLOG_ASYNC_LOG_FILE_DEBUG_OBJECT_STORAGE_S3_PREFIX", defaultS3.Prefix)

	cfg := newLogFileObjectStorageUploadConfig(
		enabled,
		defaultEnabled,
		errorEnabled,
		debugEnabled,
		targets,
		defaultS3,
		errorS3,
		debugS3,
	)
	cfg, destinationsErr := configureLogFileS3Destinations(cfg,
		env.GetEnvString("HETEROLOG_ASYNC_LOG_FILE_OBJECT_STORAGE_S3_DESTINATIONS", ""),
		env.GetEnvString("HETEROLOG_ASYNC_LOG_FILE_ERROR_OBJECT_STORAGE_S3_DESTINATIONS", ""),
		env.GetEnvString("HETEROLOG_ASYNC_LOG_FILE_DEBUG_OBJECT_STORAGE_S3_DESTINATIONS", ""),
	)
	return cfg, errors.Join(append(errs, destinationsErr)...)
}

func loadDeliveryLogFileObjectStorageUploadConfig(env pkgenv.Lookup) (objectstorage.LogFileUploadConfig, error) {
	var errs []error
	enabled, err := env.GetEnvBool(
		"HETEROLOG_ENABLE_DELIVERY_LOG_FILE_OBJECT_STORAGE_UPLOAD",
		objectstorages3cfg.DefaultUploadEnabled,
	)
	if err != nil {
		errs = append(errs, err)
	}

	var defaultEnabled, errorEnabled, debugEnabled bool
	var targets []objectstorage.UploadTarget
	var commonS3 objectstorage.S3Config
	if enabled {
		var commonS3Err error
		commonS3, commonS3Err = loadCommonObjectStorageS3Config(env)
		if commonS3Err != nil {
			errs = append(errs, commonS3Err)
		}
		defaultEnabled, err = env.GetEnvBool(
			"HETEROLOG_ENABLE_DELIVERY_LOG_FILE_DEFAULT_OBJECT_STORAGE_UPLOAD",
			objectstorages3cfg.DefaultUploadTargetEnabled,
		)
		if err != nil {
			errs = append(errs, err)
		}
		errorEnabled, err = env.GetEnvBool(
			"HETEROLOG_ENABLE_DELIVERY_LOG_FILE_ERROR_OBJECT_STORAGE_UPLOAD",
			objectstorages3cfg.DefaultUploadTargetEnabled,
		)
		if err != nil {
			errs = append(errs, err)
		}
		debugEnabled, err = env.GetEnvBool(
			"HETEROLOG_ENABLE_DELIVERY_LOG_FILE_DEBUG_OBJECT_STORAGE_UPLOAD",
			objectstorages3cfg.DefaultUploadTargetEnabled,
		)
		if err != nil {
			errs = append(errs, err)
		}
		targets = objectStorageUploadTargets(env.GetEnvSliceString("HETEROLOG_DELIVERY_LOG_FILE_OBJECT_STORAGE_UPLOAD_TYPE", nil))
	}
	defaultS3 := commonS3
	defaultS3.Bucket = env.GetEnvString("HETEROLOG_DELIVERY_LOG_FILE_OBJECT_STORAGE_S3_BUCKET", objectstorages3cfg.DefaultBucket)
	defaultS3.Prefix = env.GetEnvString("HETEROLOG_DELIVERY_LOG_FILE_OBJECT_STORAGE_S3_PREFIX", objectstorages3cfg.DefaultPrefix)
	errorS3 := commonS3
	errorS3.Bucket = env.GetEnvString("HETEROLOG_DELIVERY_LOG_FILE_ERROR_OBJECT_STORAGE_S3_BUCKET", defaultS3.Bucket)
	errorS3.Prefix = env.GetEnvString("HETEROLOG_DELIVERY_LOG_FILE_ERROR_OBJECT_STORAGE_S3_PREFIX", defaultS3.Prefix)
	debugS3 := commonS3
	debugS3.Bucket = env.GetEnvString("HETEROLOG_DELIVERY_LOG_FILE_DEBUG_OBJECT_STORAGE_S3_BUCKET", defaultS3.Bucket)
	debugS3.Prefix = env.GetEnvString("HETEROLOG_DELIVERY_LOG_FILE_DEBUG_OBJECT_STORAGE_S3_PREFIX", defaultS3.Prefix)

	cfg := newLogFileObjectStorageUploadConfig(
		enabled,
		defaultEnabled,
		errorEnabled,
		debugEnabled,
		targets,
		defaultS3,
		errorS3,
		debugS3,
	)
	cfg, destinationsErr := configureLogFileS3Destinations(cfg,
		env.GetEnvString("HETEROLOG_DELIVERY_LOG_FILE_OBJECT_STORAGE_S3_DESTINATIONS", ""),
		env.GetEnvString("HETEROLOG_DELIVERY_LOG_FILE_ERROR_OBJECT_STORAGE_S3_DESTINATIONS", ""),
		env.GetEnvString("HETEROLOG_DELIVERY_LOG_FILE_DEBUG_OBJECT_STORAGE_S3_DESTINATIONS", ""),
	)
	return cfg, errors.Join(append(errs, destinationsErr)...)
}

// objectStorageUploadTargetsは読み込み済みの宛先一覧を設定型へ変換します。
func objectStorageUploadTargets(values []string) []objectstorage.UploadTarget {
	// 対応providerの判定はmapperがpkg/objectstorageへ渡す直前に行います。
	// ここでは大文字小文字の表記揺れだけを正規化します。重複や空要素は除去せず、
	// 明示された値をそのまま利用側の設定検証へ渡します。
	targets := make([]objectstorage.UploadTarget, 0, len(values))
	for _, value := range values {
		normalized := strings.ToLower(value)
		targets = append(targets, objectstorage.UploadTarget(normalized))
	}
	return targets
}

func newLogFileObjectStorageUploadConfig(
	enabled bool,
	defaultEnabled bool,
	errorEnabled bool,
	debugEnabled bool,
	targets []objectstorage.UploadTarget,
	defaultS3 objectstorage.S3Config,
	errorS3 objectstorage.S3Config,
	debugS3 objectstorage.S3Config,
) objectstorage.LogFileUploadConfig {
	return objectstorage.LogFileUploadConfig{
		Enabled: enabled,
		Default: objectstorage.UploadTargetConfig{
			Enabled: defaultEnabled,
			Targets: targets,
			S3:      defaultS3,
		},
		Error: objectstorage.UploadTargetConfig{
			Enabled: errorEnabled,
			Targets: targets,
			S3:      errorS3,
		},
		Debug: objectstorage.UploadTargetConfig{
			Enabled: debugEnabled,
			Targets: targets,
			S3:      debugS3,
		},
	}
}

// ファイルロガー設定を読み込む
func loadFileConfig(env pkgenv.Lookup) (logconfig.FileConfig, error) {
	var environmentErrors []error
	maxLogSizeMb, maxLogSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_LOG_SIZE_MB", logconfig.DefaultMaxLogFileSizeMB, BytesPerMegabyte)
	if maxLogSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxLogSizeMbErr)
	}
	enableLogFileRotate, enableLogFileRotateErr := env.GetEnvBool("HETEROLOG_ENABLE_LOG_FILE_ROTATE", logconfig.DefaultEnableLogFileRotate)
	if enableLogFileRotateErr != nil {
		environmentErrors = append(environmentErrors, enableLogFileRotateErr)
	}
	maxLogFileBackups, maxLogFileBackupsErr := env.GetEnvInt("HETEROLOG_MAX_LOG_FILE_BACKUPS", logconfig.DefaultMaxLogFileBackups)
	if maxLogFileBackupsErr != nil {
		environmentErrors = append(environmentErrors, maxLogFileBackupsErr)
	}
	maxLogFileAge, maxLogFileAgeErr := env.GetEnvInt("HETEROLOG_MAX_LOG_FILE_AGE", logconfig.DefaultMaxLogFileAge)
	if maxLogFileAgeErr != nil {
		environmentErrors = append(environmentErrors, maxLogFileAgeErr)
	}
	maxLogFileDirSizeMb, maxLogFileDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_LOG_FILE_DIR_SIZE_MB", logconfig.DefaultMaxLogFileDirSizeMB, BytesPerMegabyte)
	if maxLogFileDirSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxLogFileDirSizeMbErr)
	}
	maxErrorLogFileBackups, maxErrorLogFileBackupsErr := env.GetEnvInt("HETEROLOG_MAX_ERROR_LOG_FILE_BACKUPS", maxLogFileBackups)
	if maxErrorLogFileBackupsErr != nil {
		environmentErrors = append(environmentErrors, maxErrorLogFileBackupsErr)
	}
	maxErrorLogFileAge, maxErrorLogFileAgeErr := env.GetEnvInt("HETEROLOG_MAX_ERROR_LOG_FILE_AGE", maxLogFileAge)
	if maxErrorLogFileAgeErr != nil {
		environmentErrors = append(environmentErrors, maxErrorLogFileAgeErr)
	}
	maxErrorLogFileDirSizeMb, maxErrorLogFileDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_ERROR_LOG_FILE_DIR_SIZE_MB", maxLogFileDirSizeMb/BytesPerMegabyte, BytesPerMegabyte)
	if maxErrorLogFileDirSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxErrorLogFileDirSizeMbErr)
	}
	maxDebugLogFileBackups, maxDebugLogFileBackupsErr := env.GetEnvInt("HETEROLOG_MAX_DEBUG_LOG_FILE_BACKUPS", maxLogFileBackups)
	if maxDebugLogFileBackupsErr != nil {
		environmentErrors = append(environmentErrors, maxDebugLogFileBackupsErr)
	}
	maxDebugLogFileAge, maxDebugLogFileAgeErr := env.GetEnvInt("HETEROLOG_MAX_DEBUG_LOG_FILE_AGE", maxLogFileAge)
	if maxDebugLogFileAgeErr != nil {
		environmentErrors = append(environmentErrors, maxDebugLogFileAgeErr)
	}
	maxDebugLogFileDirSizeMb, maxDebugLogFileDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_DEBUG_LOG_FILE_DIR_SIZE_MB", maxLogFileDirSizeMb/BytesPerMegabyte, BytesPerMegabyte)
	if maxDebugLogFileDirSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxDebugLogFileDirSizeMbErr)
	}
	enableLogFileCompress, enableLogFileCompressErr := env.GetEnvBool("HETEROLOG_ENABLE_LOG_FILE_COMPRESS", logconfig.DefaultEnableLogFileCompress)
	if enableLogFileCompressErr != nil {
		environmentErrors = append(environmentErrors, enableLogFileCompressErr)
	}
	enableLogFileSeparateErrorLogs, enableLogFileSeparateErrorLogsErr := env.GetEnvBool("HETEROLOG_ENABLE_LOG_FILE_SEPARATE_ERROR_LOGS", logconfig.DefaultIsSeparateErrorLogsEnabled)
	if enableLogFileSeparateErrorLogsErr != nil {
		environmentErrors = append(environmentErrors, enableLogFileSeparateErrorLogsErr)
	}
	enableLogFileSeparateDebugLogs, enableLogFileSeparateDebugLogsErr := env.GetEnvBool("HETEROLOG_ENABLE_LOG_FILE_SEPARATE_DEBUG_LOGS", logconfig.DefaultIsSeparateDebugLogsEnabled)
	if enableLogFileSeparateDebugLogsErr != nil {
		environmentErrors = append(environmentErrors, enableLogFileSeparateDebugLogsErr)
	}
	enableLogFileSeparateDebugLogDir, enableLogFileSeparateDebugLogDirErr := env.GetEnvBool("HETEROLOG_ENABLE_LOG_FILE_SEPARATE_DEBUG_LOG_DIR", logconfig.DefaultIsSeparateDebugLogDirEnabled)
	if enableLogFileSeparateDebugLogDirErr != nil {
		environmentErrors = append(environmentErrors, enableLogFileSeparateDebugLogDirErr)
	}
	compressFormat, compressFormatErr := loadLogCompressFormat(
		env.GetEnvString("HETEROLOG_LOG_COMPRESS_FORMAT", string(logconfig.DefaultLogCompressFormat)),
		"HETEROLOG_LOG_COMPRESS_FORMAT",
		logconfig.DefaultLogCompressFormat,
	)
	if compressFormatErr != nil {
		environmentErrors = append(environmentErrors, compressFormatErr)
	}
	uploadConfig, uploadConfigErr := loadLogFileObjectStorageUploadConfig(env)
	if uploadConfigErr != nil {
		environmentErrors = append(environmentErrors, uploadConfigErr)
	}
	storeCfg := logconfig.FileSystemStoreConfig{
		MaxFileSize:    maxLogSizeMb,
		LogDir:         env.GetEnvString("HETEROLOG_LOG_DIR", logconfig.LogDirCaseFile),
		ErrorLogDir:    env.GetEnvString("HETEROLOG_ERROR_LOG_DIR", logconfig.LogDirCaseErrorFile),
		DebugLogDir:    env.GetEnvString("HETEROLOG_DEBUG_LOG_DIR", ""),
		EnableRotate:   enableLogFileRotate,
		MaxFileBackups: maxLogFileBackups,
		MaxFileAge:     maxLogFileAge,
		MaxDirSize:     maxLogFileDirSizeMb,
		ErrorPurge: &fsstore.FilePurgeConfig{
			MaxFileBackups: maxErrorLogFileBackups,
			MaxFileAge:     maxErrorLogFileAge,
			MaxDirSize:     maxErrorLogFileDirSizeMb,
		},
		DebugPurge: &fsstore.FilePurgeConfig{
			MaxFileBackups: maxDebugLogFileBackups,
			MaxFileAge:     maxDebugLogFileAge,
			MaxDirSize:     maxDebugLogFileDirSizeMb,
		},
		EnableFileCompress: enableLogFileCompress,
		CompressFormat:     compressFormat,
		LifecycleMode:      logconfig.ParseLogFileLifecycleMode(env.GetEnvString("HETEROLOG_LOG_FILE_LIFECYCLE_MODE", string(logconfig.DefaultLogFileLifecycleMode)), logconfig.DefaultLogFileLifecycleMode),
		OverloadConfig: logconfig.FileOverLoadPolicyConfig{
			OverloadPolicy:   failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_LOG_FILE_OVERLOAD_POLICY", string(logconfig.DefaultLogFileOverloadPolicy)), logconfig.DefaultLogFileOverloadPolicy),
			SpillDestination: failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_LOG_FILE_SECONDARY_DESTINATION", string(logconfig.DefaultLogFileSecondaryDestination)), logconfig.DefaultLogFileSecondaryDestination),
		},
		RotationType:                       loadLogRotation(env.GetEnvString("HETEROLOG_LOG_FILE_ROTATION", string(logconfig.DefaultLogFileRotation)), logconfig.DefaultLogFileRotation),
		RotationDayOfWeek:                  logconfig.LogRotationDayOfWeek,
		WriteLogFileWarnThresholdDuration:  fsstoreconfig.WriteLogFileWarnThresholdDuration,
		WriteLogFileErrorThresholdDuration: fsstoreconfig.WriteLogFileErrorThresholdDuration,
		CompressWarnThresholdDuration:      fsstoreconfig.CompressWarnThresholdDuration,
		CompressErrorThresholdDuration:     fsstoreconfig.CompressErrorThresholdDuration,
		MaxWriteRetries:                    fsstoreconfig.MaxFileOperationRetries,
		FileOperationRetryWaitTime:         fsstoreconfig.FileOperationRetryWaitTime,
		IsSeparateErrorLogsEnabled:         enableLogFileSeparateErrorLogs,
		SeparateErrorLogsPrefix:            logconfig.SeparateErrorLogsPrefix,
		IsSeparateDebugLogsEnabled:         enableLogFileSeparateDebugLogs,
		IsSeparateDebugLogDirEnabled:       enableLogFileSeparateDebugLogDir,
		SeparateDebugLogsPrefix:            logconfig.SeparateDebugLogsPrefix,
		ObjectStorageUploadConfig:          uploadConfig,
		LifecycleHandlerConfig: internalerror.RoutingConfig{
			OnErrorConfig: env.GetEnvSliceString("HETEROLOG_LOG_FILE_LIFECYCLE_ERROR_HANDLER", nil),
			OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_LOG_FILE_LIFECYCLE_WARN_HANDLER", nil),
			OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_LOG_FILE_LIFECYCLE_INFO_HANDLER", nil),
			OnDebugConfig: env.GetEnvSliceString("HETEROLOG_LOG_FILE_LIFECYCLE_DEBUG_HANDLER", nil),
		},
	}
	enableLogFileSampling, enableLogFileSamplingErr := env.GetEnvBool("HETEROLOG_ENABLE_LOG_FILE_SAMPLING", logconfig.DefaultIsLogFileSamplingEnabled)
	if enableLogFileSamplingErr != nil {
		environmentErrors = append(environmentErrors, enableLogFileSamplingErr)
	}
	enableLoggingFileByRequestId, enableLoggingFileByRequestIdErr := env.GetEnvBool("HETEROLOG_ENABLE_LOGGING_FILE_BY_REQUEST_ID", logconfig.DefaultEnableFileLoggingByRequestID)
	if enableLoggingFileByRequestIdErr != nil {
		environmentErrors = append(environmentErrors, enableLoggingFileByRequestIdErr)
	}
	logFileRequestIdRate, logFileRequestIdRateErr := env.GetEnvInt("HETEROLOG_LOG_FILE_REQUEST_ID_RATE", logconfig.DefaultFileLoggingByRequestIDRate)
	if logFileRequestIdRateErr != nil {
		environmentErrors = append(environmentErrors, logFileRequestIdRateErr)
	}
	logFileDebugRate, logFileDebugRateErr := env.GetEnvInt("HETEROLOG_LOG_FILE_DEBUG_RATE", logconfig.DefaultLogFileSamplingDebugRate)
	if logFileDebugRateErr != nil {
		environmentErrors = append(environmentErrors, logFileDebugRateErr)
	}
	logFileInfoRate, logFileInfoRateErr := env.GetEnvInt("HETEROLOG_LOG_FILE_INFO_RATE", logconfig.DefaultLogFileSamplingInfoRate)
	if logFileInfoRateErr != nil {
		environmentErrors = append(environmentErrors, logFileInfoRateErr)
	}
	logFileWarnRate, logFileWarnRateErr := env.GetEnvInt("HETEROLOG_LOG_FILE_WARN_RATE", logconfig.DefaultLogFileSamplingWarnRate)
	if logFileWarnRateErr != nil {
		environmentErrors = append(environmentErrors, logFileWarnRateErr)
	}
	logFileErrorRate, logFileErrorRateErr := env.GetEnvInt("HETEROLOG_LOG_FILE_ERROR_RATE", logconfig.DefaultLogFileSamplingErrorRate)
	if logFileErrorRateErr != nil {
		environmentErrors = append(environmentErrors, logFileErrorRateErr)
	}
	writeLogFileLatencyEnabled, writeLogFileLatencyEnabledErr := env.GetEnvBool("HETEROLOG_WRITE_LOG_FILE_LATENCY_ENABLED", logconfig.DefaultWriteFileLoggerLatencyEnabled)
	if writeLogFileLatencyEnabledErr != nil {
		environmentErrors = append(environmentErrors, writeLogFileLatencyEnabledErr)
	}
	logLevel, logLevelErr := loadLogLevel(
		env.GetEnvString("HETEROLOG_FILE_LOG_LEVEL", logconfig.DefaultFileLogLevel.String()),
		"HETEROLOG_FILE_LOG_LEVEL",
		logconfig.DefaultFileLogLevel,
	)
	if logLevelErr != nil {
		environmentErrors = append(environmentErrors, logLevelErr)
	}
	return logconfig.FileConfig{
		LogLevel:              logLevel,
		FileSystemStoreConfig: storeCfg,
		LogBodyConfig: logconfig.LogBodyConfig{
			LogLineDateFormat:        logconfig.LogLineDateFormat,
			ErrorTraceDepth:          logconfig.DefaultErrorTraceDepth,
			ErrorTraceDepthDebugMode: logconfig.DefaultErrorTraceDepthDebugMode,
		},
		SamplingConfig: logconfig.SamplingConfig{
			IsSamplingEnabled:        enableLogFileSampling,
			EnableLoggingByRequestID: enableLoggingFileByRequestId,
			LoggingByRequestIDRate:   logFileRequestIdRate,
			SamplingDebugRate:        logFileDebugRate,
			SamplingInfoRate:         logFileInfoRate,
			SamplingWarnRate:         logFileWarnRate,
			SamplingErrorRate:        logFileErrorRate,
		},
		WriteFileLatencyEnabled: writeLogFileLatencyEnabled,
		HandlerConfig: internalerror.RoutingConfig{
			OnErrorConfig: env.GetEnvSliceString("HETEROLOG_FILE_LOGGER_ERROR_HANDLER", nil),
			OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_FILE_LOGGER_WARN_HANDLER", nil),
			OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_FILE_LOGGER_INFO_HANDLER", nil),
			OnDebugConfig: env.GetEnvSliceString("HETEROLOG_FILE_LOGGER_DEBUG_HANDLER", nil),
		},
	}, errors.Join(environmentErrors...)
}

// 非同期ファイルロガー設定を読み込む
func loadAsyncFileConfig(env pkgenv.Lookup) (logconfig.FileConfig, error) {
	var environmentErrors []error
	maxAsyncLogSizeMb, maxAsyncLogSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_ASYNC_LOG_SIZE_MB", logconfig.DefaultMaxAsyncLogFileSizeMB, BytesPerMegabyte)
	if maxAsyncLogSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxAsyncLogSizeMbErr)
	}
	enableAsyncLogFileRotate, enableAsyncLogFileRotateErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_LOG_FILE_ROTATE", logconfig.DefaultEnableAsyncLogFileRotate)
	if enableAsyncLogFileRotateErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncLogFileRotateErr)
	}
	maxAsyncLogFileBackups, maxAsyncLogFileBackupsErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_LOG_FILE_BACKUPS", logconfig.DefaultMaxAsyncLogFileBackups)
	if maxAsyncLogFileBackupsErr != nil {
		environmentErrors = append(environmentErrors, maxAsyncLogFileBackupsErr)
	}
	maxAsyncLogFileAge, maxAsyncLogFileAgeErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_LOG_FILE_AGE", logconfig.DefaultMaxAsyncLogFileAge)
	if maxAsyncLogFileAgeErr != nil {
		environmentErrors = append(environmentErrors, maxAsyncLogFileAgeErr)
	}
	maxAsyncLogFileDirSizeMb, maxAsyncLogFileDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_ASYNC_LOG_FILE_DIR_SIZE_MB", logconfig.DefaultMaxAsyncLogFileDirSizeMB, BytesPerMegabyte)
	if maxAsyncLogFileDirSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxAsyncLogFileDirSizeMbErr)
	}
	maxAsyncErrorLogFileBackups, maxAsyncErrorLogFileBackupsErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_ERROR_LOG_FILE_BACKUPS", maxAsyncLogFileBackups)
	if maxAsyncErrorLogFileBackupsErr != nil {
		environmentErrors = append(environmentErrors, maxAsyncErrorLogFileBackupsErr)
	}
	maxAsyncErrorLogFileAge, maxAsyncErrorLogFileAgeErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_ERROR_LOG_FILE_AGE", maxAsyncLogFileAge)
	if maxAsyncErrorLogFileAgeErr != nil {
		environmentErrors = append(environmentErrors, maxAsyncErrorLogFileAgeErr)
	}
	maxAsyncErrorLogFileDirSizeMb, maxAsyncErrorLogFileDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_ASYNC_ERROR_LOG_FILE_DIR_SIZE_MB", maxAsyncLogFileDirSizeMb/BytesPerMegabyte, BytesPerMegabyte)
	if maxAsyncErrorLogFileDirSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxAsyncErrorLogFileDirSizeMbErr)
	}
	maxAsyncDebugLogFileBackups, maxAsyncDebugLogFileBackupsErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_DEBUG_LOG_FILE_BACKUPS", maxAsyncLogFileBackups)
	if maxAsyncDebugLogFileBackupsErr != nil {
		environmentErrors = append(environmentErrors, maxAsyncDebugLogFileBackupsErr)
	}
	maxAsyncDebugLogFileAge, maxAsyncDebugLogFileAgeErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_DEBUG_LOG_FILE_AGE", maxAsyncLogFileAge)
	if maxAsyncDebugLogFileAgeErr != nil {
		environmentErrors = append(environmentErrors, maxAsyncDebugLogFileAgeErr)
	}
	maxAsyncDebugLogFileDirSizeMb, maxAsyncDebugLogFileDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_ASYNC_DEBUG_LOG_FILE_DIR_SIZE_MB", maxAsyncLogFileDirSizeMb/BytesPerMegabyte, BytesPerMegabyte)
	if maxAsyncDebugLogFileDirSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxAsyncDebugLogFileDirSizeMbErr)
	}
	enableAsyncLogFileCompress, enableAsyncLogFileCompressErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_LOG_FILE_COMPRESS", logconfig.DefaultEnableAsyncLogFileCompress)
	if enableAsyncLogFileCompressErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncLogFileCompressErr)
	}
	enableAsyncLogFileSeparateErrorLogs, enableAsyncLogFileSeparateErrorLogsErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_LOG_FILE_SEPARATE_ERROR_LOGS", logconfig.AsyncDefaultIsSeparateErrorLogsEnabled)
	if enableAsyncLogFileSeparateErrorLogsErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncLogFileSeparateErrorLogsErr)
	}
	enableAsyncLogFileSeparateDebugLogs, enableAsyncLogFileSeparateDebugLogsErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_LOG_FILE_SEPARATE_DEBUG_LOGS", logconfig.AsyncDefaultIsSeparateDebugLogsEnabled)
	if enableAsyncLogFileSeparateDebugLogsErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncLogFileSeparateDebugLogsErr)
	}
	enableAsyncLogFileSeparateDebugLogDir, enableAsyncLogFileSeparateDebugLogDirErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_LOG_FILE_SEPARATE_DEBUG_LOG_DIR", logconfig.DefaultIsSeparateDebugLogDirEnabled)
	if enableAsyncLogFileSeparateDebugLogDirErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncLogFileSeparateDebugLogDirErr)
	}
	compressFormat, compressFormatErr := loadLogCompressFormat(
		env.GetEnvString("HETEROLOG_ASYNC_LOG_COMPRESS_FORMAT", string(logconfig.DefaultAsyncLogCompressFormat)),
		"HETEROLOG_ASYNC_LOG_COMPRESS_FORMAT",
		logconfig.DefaultAsyncLogCompressFormat,
	)
	if compressFormatErr != nil {
		environmentErrors = append(environmentErrors, compressFormatErr)
	}
	uploadConfig, uploadConfigErr := loadAsyncLogFileObjectStorageUploadConfig(env)
	if uploadConfigErr != nil {
		environmentErrors = append(environmentErrors, uploadConfigErr)
	}
	storeCfg := logconfig.FileSystemStoreConfig{
		MaxFileSize:    maxAsyncLogSizeMb,
		LogDir:         env.GetEnvString("HETEROLOG_ASYNC_LOG_DIR", logconfig.AsyncLogDirCaseFile),
		ErrorLogDir:    env.GetEnvString("HETEROLOG_ASYNC_ERROR_LOG_DIR", logconfig.AsyncLogDirCaseErrorFile),
		DebugLogDir:    env.GetEnvString("HETEROLOG_ASYNC_DEBUG_LOG_DIR", ""),
		EnableRotate:   enableAsyncLogFileRotate,
		MaxFileBackups: maxAsyncLogFileBackups,
		MaxFileAge:     maxAsyncLogFileAge,
		MaxDirSize:     maxAsyncLogFileDirSizeMb,
		ErrorPurge: &fsstore.FilePurgeConfig{
			MaxFileBackups: maxAsyncErrorLogFileBackups,
			MaxFileAge:     maxAsyncErrorLogFileAge,
			MaxDirSize:     maxAsyncErrorLogFileDirSizeMb,
		},
		DebugPurge: &fsstore.FilePurgeConfig{
			MaxFileBackups: maxAsyncDebugLogFileBackups,
			MaxFileAge:     maxAsyncDebugLogFileAge,
			MaxDirSize:     maxAsyncDebugLogFileDirSizeMb,
		},
		EnableFileCompress: enableAsyncLogFileCompress,
		CompressFormat:     compressFormat,
		LifecycleMode:      logconfig.ParseLogFileLifecycleMode(env.GetEnvString("HETEROLOG_ASYNC_LOG_FILE_LIFECYCLE_MODE", string(logconfig.DefaultAsyncLogFileLifecycleMode)), logconfig.DefaultAsyncLogFileLifecycleMode),
		OverloadConfig: logconfig.FileOverLoadPolicyConfig{
			OverloadPolicy:   failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_ASYNC_LOG_FILE_OVERLOAD_POLICY", string(logconfig.DefaultLogFileOverloadPolicy)), logconfig.DefaultLogFileOverloadPolicy),
			SpillDestination: failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_ASYNC_LOG_FILE_SECONDARY_DESTINATION", string(logconfig.DefaultLogFileSecondaryDestination)), logconfig.DefaultLogFileSecondaryDestination),
		},
		RotationType:                       loadLogRotation(env.GetEnvString("HETEROLOG_ASYNC_LOG_FILE_ROTATION", string(logconfig.DefaultAsyncLogFileRotation)), logconfig.DefaultAsyncLogFileRotation),
		RotationDayOfWeek:                  logconfig.AsyncLogRotationDayOfWeek,
		WriteLogFileWarnThresholdDuration:  fsstoreconfig.WriteLogFileWarnThresholdDuration,
		WriteLogFileErrorThresholdDuration: fsstoreconfig.WriteLogFileErrorThresholdDuration,
		CompressWarnThresholdDuration:      fsstoreconfig.CompressWarnThresholdDuration,
		CompressErrorThresholdDuration:     fsstoreconfig.CompressErrorThresholdDuration,
		MaxWriteRetries:                    fsstoreconfig.MaxFileOperationRetries,
		FileOperationRetryWaitTime:         fsstoreconfig.FileOperationRetryWaitTime,
		IsSeparateErrorLogsEnabled:         enableAsyncLogFileSeparateErrorLogs,
		SeparateErrorLogsPrefix:            logconfig.SeparateErrorLogsPrefix,
		IsSeparateDebugLogsEnabled:         enableAsyncLogFileSeparateDebugLogs,
		IsSeparateDebugLogDirEnabled:       enableAsyncLogFileSeparateDebugLogDir,
		SeparateDebugLogsPrefix:            logconfig.SeparateDebugLogsPrefix,
		ObjectStorageUploadConfig:          uploadConfig,
		LifecycleHandlerConfig: internalerror.RoutingConfig{
			OnErrorConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_LOG_FILE_LIFECYCLE_ERROR_HANDLER", nil),
			OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_LOG_FILE_LIFECYCLE_WARN_HANDLER", nil),
			OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_LOG_FILE_LIFECYCLE_INFO_HANDLER", nil),
			OnDebugConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_LOG_FILE_LIFECYCLE_DEBUG_HANDLER", nil),
		},
	}
	enableAsyncLogFileSampling, enableAsyncLogFileSamplingErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_LOG_FILE_SAMPLING", logconfig.DefaultIsAsyncLogFileSamplingEnabled)
	if enableAsyncLogFileSamplingErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncLogFileSamplingErr)
	}
	enableAsyncLoggingFileByRequestId, enableAsyncLoggingFileByRequestIdErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_LOGGING_FILE_BY_REQUEST_ID", logconfig.DefaultEnableAsyncFileLoggingByRequestID)
	if enableAsyncLoggingFileByRequestIdErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncLoggingFileByRequestIdErr)
	}
	asyncLogFileRequestIdRate, asyncLogFileRequestIdRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_FILE_REQUEST_ID_RATE", logconfig.DefaultAsyncFileLoggingByRequestIDRate)
	if asyncLogFileRequestIdRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogFileRequestIdRateErr)
	}
	asyncLogFileDebugRate, asyncLogFileDebugRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_FILE_DEBUG_RATE", logconfig.DefaultAsyncLogFileSamplingDebugRate)
	if asyncLogFileDebugRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogFileDebugRateErr)
	}
	asyncLogFileInfoRate, asyncLogFileInfoRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_FILE_INFO_RATE", logconfig.DefaultAsyncLogFileSamplingInfoRate)
	if asyncLogFileInfoRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogFileInfoRateErr)
	}
	asyncLogFileWarnRate, asyncLogFileWarnRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_FILE_WARN_RATE", logconfig.DefaultAsyncLogFileSamplingWarnRate)
	if asyncLogFileWarnRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogFileWarnRateErr)
	}
	asyncLogFileErrorRate, asyncLogFileErrorRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_FILE_ERROR_RATE", logconfig.DefaultAsyncLogFileSamplingErrorRate)
	if asyncLogFileErrorRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogFileErrorRateErr)
	}
	writeAsyncLogFileLatencyEnabled, writeAsyncLogFileLatencyEnabledErr := env.GetEnvBool("HETEROLOG_WRITE_ASYNC_LOG_FILE_LATENCY_ENABLED", logconfig.DefaultWriteAsyncFileLoggerLatencyEnabled)
	if writeAsyncLogFileLatencyEnabledErr != nil {
		environmentErrors = append(environmentErrors, writeAsyncLogFileLatencyEnabledErr)
	}
	logLevel, logLevelErr := loadLogLevel(
		env.GetEnvString("HETEROLOG_ASYNC_FILE_LOG_LEVEL", logconfig.DefaultAsyncFileLogLevel.String()),
		"HETEROLOG_ASYNC_FILE_LOG_LEVEL",
		logconfig.DefaultAsyncFileLogLevel,
	)
	if logLevelErr != nil {
		environmentErrors = append(environmentErrors, logLevelErr)
	}
	return logconfig.FileConfig{
		LogLevel:              logLevel,
		FileSystemStoreConfig: storeCfg,
		LogBodyConfig: logconfig.LogBodyConfig{
			LogLineDateFormat:        logconfig.LogLineDateFormat,
			ErrorTraceDepth:          logconfig.DefaultErrorTraceDepth,
			ErrorTraceDepthDebugMode: logconfig.DefaultErrorTraceDepthDebugMode,
		},
		SamplingConfig: logconfig.SamplingConfig{
			IsSamplingEnabled:        enableAsyncLogFileSampling,
			EnableLoggingByRequestID: enableAsyncLoggingFileByRequestId,
			LoggingByRequestIDRate:   asyncLogFileRequestIdRate,
			SamplingDebugRate:        asyncLogFileDebugRate,
			SamplingInfoRate:         asyncLogFileInfoRate,
			SamplingWarnRate:         asyncLogFileWarnRate,
			SamplingErrorRate:        asyncLogFileErrorRate,
		},
		WriteFileLatencyEnabled: writeAsyncLogFileLatencyEnabled,
		HandlerConfig: internalerror.RoutingConfig{
			OnErrorConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_FILE_LOGGER_ERROR_HANDLER", nil),
			OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_FILE_LOGGER_WARN_HANDLER", nil),
			OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_FILE_LOGGER_INFO_HANDLER", nil),
			OnDebugConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_FILE_LOGGER_DEBUG_HANDLER", nil),
		},
	}, errors.Join(environmentErrors...)
}

// ファイルロガー設定を読み込む
func loadDeliveryFileConfig(env pkgenv.Lookup) (logconfig.DeliveryFileConfig, error) {
	var environmentErrors []error
	maxDeliveryLogSizeMb, maxDeliveryLogSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_DELIVERY_LOG_SIZE_MB", logconfig.DefaultMaxDeliveryLogFileSizeMB, BytesPerMegabyte)
	if maxDeliveryLogSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxDeliveryLogSizeMbErr)
	}
	maxDeliveryLogFileBackups, maxDeliveryLogFileBackupsErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_LOG_FILE_BACKUPS", logconfig.DefaultMaxDeliveryLogFileBackups)
	if maxDeliveryLogFileBackupsErr != nil {
		environmentErrors = append(environmentErrors, maxDeliveryLogFileBackupsErr)
	}
	maxDeliveryLogFileAge, maxDeliveryLogFileAgeErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_LOG_FILE_AGE", logconfig.DefaultMaxDeliveryLogFileAge)
	if maxDeliveryLogFileAgeErr != nil {
		environmentErrors = append(environmentErrors, maxDeliveryLogFileAgeErr)
	}
	maxDeliveryLogFileDirSizeMb, maxDeliveryLogFileDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_DELIVERY_LOG_FILE_DIR_SIZE_MB", logconfig.DefaultMaxDeliveryLogFileDirSizeMB, BytesPerMegabyte)
	if maxDeliveryLogFileDirSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxDeliveryLogFileDirSizeMbErr)
	}
	maxDeliveryErrorLogFileBackups, maxDeliveryErrorLogFileBackupsErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_ERROR_LOG_FILE_BACKUPS", maxDeliveryLogFileBackups)
	if maxDeliveryErrorLogFileBackupsErr != nil {
		environmentErrors = append(environmentErrors, maxDeliveryErrorLogFileBackupsErr)
	}
	maxDeliveryErrorLogFileAge, maxDeliveryErrorLogFileAgeErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_ERROR_LOG_FILE_AGE", maxDeliveryLogFileAge)
	if maxDeliveryErrorLogFileAgeErr != nil {
		environmentErrors = append(environmentErrors, maxDeliveryErrorLogFileAgeErr)
	}
	maxDeliveryErrorLogFileDirSizeMb, maxDeliveryErrorLogFileDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_DELIVERY_ERROR_LOG_FILE_DIR_SIZE_MB", maxDeliveryLogFileDirSizeMb/BytesPerMegabyte, BytesPerMegabyte)
	if maxDeliveryErrorLogFileDirSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxDeliveryErrorLogFileDirSizeMbErr)
	}
	maxDeliveryDebugLogFileBackups, maxDeliveryDebugLogFileBackupsErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_DEBUG_LOG_FILE_BACKUPS", maxDeliveryLogFileBackups)
	if maxDeliveryDebugLogFileBackupsErr != nil {
		environmentErrors = append(environmentErrors, maxDeliveryDebugLogFileBackupsErr)
	}
	maxDeliveryDebugLogFileAge, maxDeliveryDebugLogFileAgeErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_DEBUG_LOG_FILE_AGE", maxDeliveryLogFileAge)
	if maxDeliveryDebugLogFileAgeErr != nil {
		environmentErrors = append(environmentErrors, maxDeliveryDebugLogFileAgeErr)
	}
	maxDeliveryDebugLogFileDirSizeMb, maxDeliveryDebugLogFileDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_DELIVERY_DEBUG_LOG_FILE_DIR_SIZE_MB", maxDeliveryLogFileDirSizeMb/BytesPerMegabyte, BytesPerMegabyte)
	if maxDeliveryDebugLogFileDirSizeMbErr != nil {
		environmentErrors = append(environmentErrors, maxDeliveryDebugLogFileDirSizeMbErr)
	}
	enableDeliveryLogFileRotate, enableDeliveryLogFileRotateErr := env.GetEnvBool("HETEROLOG_ENABLE_DELIVERY_LOG_FILE_ROTATE", logconfig.DefaultEnableDeliveryLogFileRotate)
	if enableDeliveryLogFileRotateErr != nil {
		environmentErrors = append(environmentErrors, enableDeliveryLogFileRotateErr)
	}
	enableDeliveryLogFileCompress, enableDeliveryLogFileCompressErr := env.GetEnvBool("HETEROLOG_ENABLE_DELIVERY_LOG_FILE_COMPRESS", logconfig.DefaultEnableDeliveryLogFileCompress)
	if enableDeliveryLogFileCompressErr != nil {
		environmentErrors = append(environmentErrors, enableDeliveryLogFileCompressErr)
	}
	enableDeliveryLogFileSeparateErrorLogs, enableDeliveryLogFileSeparateErrorLogsErr := env.GetEnvBool("HETEROLOG_ENABLE_DELIVERY_LOG_FILE_SEPARATE_ERROR_LOGS", logconfig.DefaultIsSeparateErrorLogsEnabled)
	if enableDeliveryLogFileSeparateErrorLogsErr != nil {
		environmentErrors = append(environmentErrors, enableDeliveryLogFileSeparateErrorLogsErr)
	}
	enableDeliveryLogFileSeparateDebugLogs, enableDeliveryLogFileSeparateDebugLogsErr := env.GetEnvBool("HETEROLOG_ENABLE_DELIVERY_LOG_FILE_SEPARATE_DEBUG_LOGS", logconfig.DefaultIsSeparateDebugLogsEnabled)
	if enableDeliveryLogFileSeparateDebugLogsErr != nil {
		environmentErrors = append(environmentErrors, enableDeliveryLogFileSeparateDebugLogsErr)
	}
	enableDeliveryLogFileSeparateDebugLogDir, enableDeliveryLogFileSeparateDebugLogDirErr := env.GetEnvBool("HETEROLOG_ENABLE_DELIVERY_LOG_FILE_SEPARATE_DEBUG_LOG_DIR", logconfig.DefaultIsSeparateDebugLogDirEnabled)
	if enableDeliveryLogFileSeparateDebugLogDirErr != nil {
		environmentErrors = append(environmentErrors, enableDeliveryLogFileSeparateDebugLogDirErr)
	}
	compressFormat, compressFormatErr := loadLogCompressFormat(
		env.GetEnvString("HETEROLOG_DELIVERY_LOG_COMPRESS_FORMAT", string(logconfig.DefaultDeliveryLogCompressFormat)),
		"HETEROLOG_DELIVERY_LOG_COMPRESS_FORMAT",
		logconfig.DefaultDeliveryLogCompressFormat,
	)
	if compressFormatErr != nil {
		environmentErrors = append(environmentErrors, compressFormatErr)
	}
	uploadConfig, uploadConfigErr := loadDeliveryLogFileObjectStorageUploadConfig(env)
	if uploadConfigErr != nil {
		environmentErrors = append(environmentErrors, uploadConfigErr)
	}
	storeCfg := logconfig.DeliveryFileSystemStoreConfig{
		MaxFileSize:    maxDeliveryLogSizeMb,
		MaxFileBackups: maxDeliveryLogFileBackups,
		MaxFileAge:     maxDeliveryLogFileAge,
		MaxDirSize:     maxDeliveryLogFileDirSizeMb,
		ErrorPurge: &fsstore.FilePurgeConfig{
			MaxFileBackups: maxDeliveryErrorLogFileBackups,
			MaxFileAge:     maxDeliveryErrorLogFileAge,
			MaxDirSize:     maxDeliveryErrorLogFileDirSizeMb,
		},
		DebugPurge: &fsstore.FilePurgeConfig{
			MaxFileBackups: maxDeliveryDebugLogFileBackups,
			MaxFileAge:     maxDeliveryDebugLogFileAge,
			MaxDirSize:     maxDeliveryDebugLogFileDirSizeMb,
		},
		LogDir:                             env.GetEnvString("HETEROLOG_DELIVERY_LOG_DIR", logconfig.DeliveryLogDirCaseFile),
		ErrorLogDir:                        env.GetEnvString("HETEROLOG_DELIVERY_ERROR_LOG_DIR", logconfig.DeliveryLogDirCaseErrorFile),
		DebugLogDir:                        env.GetEnvString("HETEROLOG_DELIVERY_DEBUG_LOG_DIR", ""),
		EnableRotate:                       enableDeliveryLogFileRotate,
		RotationType:                       loadLogRotation(env.GetEnvString("HETEROLOG_DELIVERY_LOG_FILE_ROTATION", string(logconfig.DefaultLogFileRotation)), logconfig.DefaultLogFileRotation),
		RotationDayOfWeek:                  logconfig.DeliveryLogRotationDayOfWeek,
		EnableFileCompress:                 enableDeliveryLogFileCompress,
		CompressFormat:                     compressFormat,
		LifecycleMode:                      logconfig.ParseLogFileLifecycleMode(env.GetEnvString("HETEROLOG_DELIVERY_LOG_FILE_LIFECYCLE_MODE", string(logconfig.DefaultDeliveryLogFileLifecycleMode)), logconfig.DefaultDeliveryLogFileLifecycleMode),
		WriteLogFileWarnThresholdDuration:  fsstoreconfig.WriteLogFileWarnThresholdDuration,
		WriteLogFileErrorThresholdDuration: fsstoreconfig.WriteLogFileErrorThresholdDuration,
		CompressWarnThresholdDuration:      fsstoreconfig.CompressWarnThresholdDuration,
		CompressErrorThresholdDuration:     fsstoreconfig.CompressErrorThresholdDuration,
		MaxWriteRetries:                    fsstoreconfig.MaxFileOperationRetries,
		FileOperationRetryWaitTime:         fsstoreconfig.FileOperationRetryWaitTime,
		IsSeparateErrorLogsEnabled:         enableDeliveryLogFileSeparateErrorLogs,
		SeparateErrorLogsPrefix:            logconfig.SeparateErrorLogsPrefix,
		IsSeparateDebugLogsEnabled:         enableDeliveryLogFileSeparateDebugLogs,
		IsSeparateDebugLogDirEnabled:       enableDeliveryLogFileSeparateDebugLogDir,
		SeparateDebugLogsPrefix:            logconfig.SeparateDebugLogsPrefix,
		ObjectStorageUploadConfig:          uploadConfig,
		LifecycleHandlerConfig: internalerror.RoutingConfig{
			OnErrorConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_LOG_FILE_LIFECYCLE_ERROR_HANDLER", nil),
			OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_LOG_FILE_LIFECYCLE_WARN_HANDLER", nil),
			OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_LOG_FILE_LIFECYCLE_INFO_HANDLER", nil),
			OnDebugConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_LOG_FILE_LIFECYCLE_DEBUG_HANDLER", nil),
		},
	}
	writeDeliveryLogFileLatencyEnabled, writeDeliveryLogFileLatencyEnabledErr := env.GetEnvBool("HETEROLOG_WRITE_DELIVERY_LOG_FILE_LATENCY_ENABLED", logconfig.DefaultWriteDeliveryFileLoggerLatencyEnabled)
	if writeDeliveryLogFileLatencyEnabledErr != nil {
		environmentErrors = append(environmentErrors, writeDeliveryLogFileLatencyEnabledErr)
	}
	return logconfig.DeliveryFileConfig{
		FileSystemStoreConfig: storeCfg,
		LogBodyConfig: logconfig.LogBodyConfig{
			LogLineDateFormat:        logconfig.LogLineDateFormat,
			ErrorTraceDepth:          logconfig.DefaultErrorTraceDepth,
			ErrorTraceDepthDebugMode: logconfig.DefaultErrorTraceDepthDebugMode,
		},
		WriteFileLatencyEnabled: writeDeliveryLogFileLatencyEnabled,
		HandlerConfig: internalerror.RoutingConfig{
			OnErrorConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_FILE_LOGGER_ERROR_HANDLER", nil),
			OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_FILE_LOGGER_WARN_HANDLER", nil),
			OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_FILE_LOGGER_INFO_HANDLER", nil),
			OnDebugConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_FILE_LOGGER_DEBUG_HANDLER", nil),
		},
	}, errors.Join(environmentErrors...)
}

// 標準出力ロガー設定を読み込む
func loadStdoutConfig(env pkgenv.Lookup) (logconfig.StdoutConfig, error) {
	cfg, err := loadStdoutBaseConfig(env)
	cfg.HandlerConfig = internalerror.RoutingConfig{
		OnErrorConfig: env.GetEnvSliceString("HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER", nil),
		OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_STDOUT_LOGGER_WARN_HANDLER", nil),
		OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_STDOUT_LOGGER_INFO_HANDLER", nil),
		OnDebugConfig: env.GetEnvSliceString("HETEROLOG_STDOUT_LOGGER_DEBUG_HANDLER", nil),
	}
	return cfg, err
}

// loadAsyncStdoutConfig はstdout本体の設定を共有しつつ、internal log routingだけは
// sync stdoutとは独立したasync専用env keyから読み込みます。
func loadAsyncStdoutConfig(env pkgenv.Lookup) (logconfig.StdoutConfig, error) {
	cfg, err := loadStdoutBaseConfig(env)
	cfg.HandlerConfig = internalerror.RoutingConfig{
		OnErrorConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_STDOUT_LOGGER_ERROR_HANDLER", nil),
		OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_STDOUT_LOGGER_WARN_HANDLER", nil),
		OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_STDOUT_LOGGER_INFO_HANDLER", nil),
		OnDebugConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_STDOUT_LOGGER_DEBUG_HANDLER", nil),
	}
	return cfg, err
}

func loadStdoutBaseConfig(env pkgenv.Lookup) (logconfig.StdoutConfig, error) {
	var environmentErrors []error
	enableLogStdoutSampling, enableLogStdoutSamplingErr := env.GetEnvBool("HETEROLOG_ENABLE_LOG_STDOUT_SAMPLING", logconfig.DefaultIsLogStdoutSamplingEnabled)
	if enableLogStdoutSamplingErr != nil {
		environmentErrors = append(environmentErrors, enableLogStdoutSamplingErr)
	}
	enableLoggingStdoutByRequestId, enableLoggingStdoutByRequestIdErr := env.GetEnvBool("HETEROLOG_ENABLE_LOGGING_STDOUT_BY_REQUEST_ID", logconfig.DefaultEnableStdoutLoggingByRequestID)
	if enableLoggingStdoutByRequestIdErr != nil {
		environmentErrors = append(environmentErrors, enableLoggingStdoutByRequestIdErr)
	}
	logStdoutRequestIdRate, logStdoutRequestIdRateErr := env.GetEnvInt("HETEROLOG_LOG_STDOUT_REQUEST_ID_RATE", logconfig.DefaultStdoutLoggingByRequestIDRate)
	if logStdoutRequestIdRateErr != nil {
		environmentErrors = append(environmentErrors, logStdoutRequestIdRateErr)
	}
	logStdoutDebugRate, logStdoutDebugRateErr := env.GetEnvInt("HETEROLOG_LOG_STDOUT_DEBUG_RATE", logconfig.DefaultLogStdoutSamplingDebugRate)
	if logStdoutDebugRateErr != nil {
		environmentErrors = append(environmentErrors, logStdoutDebugRateErr)
	}
	logStdoutInfoRate, logStdoutInfoRateErr := env.GetEnvInt("HETEROLOG_LOG_STDOUT_INFO_RATE", logconfig.DefaultLogStdoutSamplingInfoRate)
	if logStdoutInfoRateErr != nil {
		environmentErrors = append(environmentErrors, logStdoutInfoRateErr)
	}
	logStdoutWarnRate, logStdoutWarnRateErr := env.GetEnvInt("HETEROLOG_LOG_STDOUT_WARN_RATE", logconfig.DefaultLogStdoutSamplingWarnRate)
	if logStdoutWarnRateErr != nil {
		environmentErrors = append(environmentErrors, logStdoutWarnRateErr)
	}
	logStdoutErrorRate, logStdoutErrorRateErr := env.GetEnvInt("HETEROLOG_LOG_STDOUT_ERROR_RATE", logconfig.DefaultLogStdoutSamplingErrorRate)
	if logStdoutErrorRateErr != nil {
		environmentErrors = append(environmentErrors, logStdoutErrorRateErr)
	}
	enableStdoutOtelFormat, enableStdoutOtelFormatErr := env.GetEnvBool("HETEROLOG_ENABLE_STDOUT_OTEL_FORMAT", logconfig.DefaultOTelFormatEnabledForStdout)
	if enableStdoutOtelFormatErr != nil {
		environmentErrors = append(environmentErrors, enableStdoutOtelFormatErr)
	}
	logLevel, logLevelErr := loadLogLevel(
		env.GetEnvString("HETEROLOG_STDOUT_LOG_LEVEL", logconfig.DefaultStdoutLogLevel.String()),
		"HETEROLOG_STDOUT_LOG_LEVEL",
		logconfig.DefaultStdoutLogLevel,
	)
	if logLevelErr != nil {
		environmentErrors = append(environmentErrors, logLevelErr)
	}
	return logconfig.StdoutConfig{
		LogLevel:                 logLevel,
		LogLineDateFormat:        logconfig.LogLineDateFormat,
		ErrorTraceDepth:          logconfig.DefaultErrorTraceDepth,
		ErrorTraceDepthDebugMode: logconfig.DefaultErrorTraceDepthDebugMode,
		SamplingConfig: logconfig.SamplingConfig{
			IsSamplingEnabled:        enableLogStdoutSampling,
			EnableLoggingByRequestID: enableLoggingStdoutByRequestId,
			LoggingByRequestIDRate:   logStdoutRequestIdRate,
			SamplingDebugRate:        logStdoutDebugRate,
			SamplingInfoRate:         logStdoutInfoRate,
			SamplingWarnRate:         logStdoutWarnRate,
			SamplingErrorRate:        logStdoutErrorRate,
		},
		OpenTelemetryFormatEnabled: enableStdoutOtelFormat,
	}, errors.Join(environmentErrors...)
}

func loadDeliveryStdoutConfig(env pkgenv.Lookup) (logconfig.DeliveryStdoutConfig, error) {
	var environmentErrors []error
	enableStdoutOtelFormat, enableStdoutOtelFormatErr := env.GetEnvBool("HETEROLOG_ENABLE_STDOUT_OTEL_FORMAT", logconfig.DefaultOTelFormatEnabledForStdout)
	if enableStdoutOtelFormatErr != nil {
		environmentErrors = append(environmentErrors, enableStdoutOtelFormatErr)
	}
	return logconfig.DeliveryStdoutConfig{
		LogLineDateFormat:          logconfig.LogLineDateFormat,
		ErrorTraceDepth:            logconfig.DefaultErrorTraceDepth,
		ErrorTraceDepthDebugMode:   logconfig.DefaultErrorTraceDepthDebugMode,
		OpenTelemetryFormatEnabled: enableStdoutOtelFormat,
		HandlerConfig: internalerror.RoutingConfig{
			OnErrorConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_STDOUT_LOGGER_ERROR_HANDLER", nil),
			OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_STDOUT_LOGGER_WARN_HANDLER", nil),
			OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_STDOUT_LOGGER_INFO_HANDLER", nil),
			OnDebugConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_STDOUT_LOGGER_DEBUG_HANDLER", nil),
		},
	}, errors.Join(environmentErrors...)
}

// Slackロガー設定を読み込む
func loadSlackConfig(env pkgenv.Lookup) (logconfig.SlackConfig, error) {
	var environmentErrors []error
	slackTimeoutSecond, slackTimeoutSecondErr := env.GetEnvDuration("HETEROLOG_SLACK_TIMEOUT_SECOND", pkgslack.DefaultHTTPTimeout)
	if slackTimeoutSecondErr != nil {
		environmentErrors = append(environmentErrors, slackTimeoutSecondErr)
	}
	slackIntervals, slackIntervalsErr := env.GetEnvDurationSlice("HETEROLOG_SLACK_INTERVALS", logconfig.DefaultSlackIntervals, time.Minute)
	if slackIntervalsErr != nil {
		environmentErrors = append(environmentErrors, slackIntervalsErr)
	}
	maxLogsPerSlackNotification, maxLogsPerSlackNotificationErr := env.GetEnvInt("HETEROLOG_MAX_LOGS_PER_SLACK_NOTIFICATION", logconfig.DefaultMaxLogsPerSlackNotification)
	if maxLogsPerSlackNotificationErr != nil {
		environmentErrors = append(environmentErrors, maxLogsPerSlackNotificationErr)
	}
	maxSlackRestThresholdMinute, maxSlackRestThresholdMinuteErr := env.GetEnvDurationMinute("HETEROLOG_MAX_SLACK_REST_THRESHOLD_MINUTE", logconfig.DefaultMaxRestThreshold)
	if maxSlackRestThresholdMinuteErr != nil {
		environmentErrors = append(environmentErrors, maxSlackRestThresholdMinuteErr)
	}
	enableLogSlackSampling, enableLogSlackSamplingErr := env.GetEnvBool("HETEROLOG_ENABLE_LOG_SLACK_SAMPLING", logconfig.DefaultIsLogSlackSamplingEnabled)
	if enableLogSlackSamplingErr != nil {
		environmentErrors = append(environmentErrors, enableLogSlackSamplingErr)
	}
	enableLoggingSlackByRequestId, enableLoggingSlackByRequestIdErr := env.GetEnvBool("HETEROLOG_ENABLE_LOGGING_SLACK_BY_REQUEST_ID", logconfig.DefaultEnableSlackLoggingByRequestID)
	if enableLoggingSlackByRequestIdErr != nil {
		environmentErrors = append(environmentErrors, enableLoggingSlackByRequestIdErr)
	}
	logSlackRequestIdRate, logSlackRequestIdRateErr := env.GetEnvInt("HETEROLOG_LOG_SLACK_REQUEST_ID_RATE", logconfig.DefaultSlackLoggingByRequestIDRate)
	if logSlackRequestIdRateErr != nil {
		environmentErrors = append(environmentErrors, logSlackRequestIdRateErr)
	}
	logSlackDebugRate, logSlackDebugRateErr := env.GetEnvInt("HETEROLOG_LOG_SLACK_DEBUG_RATE", logconfig.DefaultLogSlackSamplingDebugRate)
	if logSlackDebugRateErr != nil {
		environmentErrors = append(environmentErrors, logSlackDebugRateErr)
	}
	logSlackInfoRate, logSlackInfoRateErr := env.GetEnvInt("HETEROLOG_LOG_SLACK_INFO_RATE", logconfig.DefaultLogSlackSamplingInfoRate)
	if logSlackInfoRateErr != nil {
		environmentErrors = append(environmentErrors, logSlackInfoRateErr)
	}
	logSlackWarnRate, logSlackWarnRateErr := env.GetEnvInt("HETEROLOG_LOG_SLACK_WARN_RATE", logconfig.DefaultLogSlackSamplingWarnRate)
	if logSlackWarnRateErr != nil {
		environmentErrors = append(environmentErrors, logSlackWarnRateErr)
	}
	logSlackErrorRate, logSlackErrorRateErr := env.GetEnvInt("HETEROLOG_LOG_SLACK_ERROR_RATE", logconfig.DefaultLogSlackSamplingErrorRate)
	if logSlackErrorRateErr != nil {
		environmentErrors = append(environmentErrors, logSlackErrorRateErr)
	}
	logLevel, logLevelErr := loadLogLevel(
		env.GetEnvString("HETEROLOG_SLACK_LOG_LEVEL", logconfig.DefaultSlackLogLevel.String()),
		"HETEROLOG_SLACK_LOG_LEVEL",
		logconfig.DefaultSlackLogLevel,
	)
	if logLevelErr != nil {
		environmentErrors = append(environmentErrors, logLevelErr)
	}
	return logconfig.SlackConfig{
		LogLevel: logLevel, // デフォルト値はASYNCかどうかで分けるのは不要
		LogBodyConfig: logconfig.LogBodyConfig{
			LogLineDateFormat:        logconfig.LogLineDateFormat,
			ErrorTraceDepth:          logconfig.DefaultErrorTraceDepth,
			ErrorTraceDepthDebugMode: logconfig.DefaultErrorTraceDepthDebugMode,
		},
		Timeout:                slackTimeoutSecond,
		Intervals:              slackIntervals,
		MaxLogsPerNotification: maxLogsPerSlackNotification,
		MaxRestThreshold:       maxSlackRestThresholdMinute,
		Token:                  env.GetEnvString("HETEROLOG_SLACK_API_KEY", ""),
		Channel:                env.GetEnvString("HETEROLOG_SLACK_NORMAL_CHANNEL", ""),
		ErrorChannel:           env.GetEnvString("HETEROLOG_SLACK_ERROR_CHANNEL", env.GetEnvString("HETEROLOG_SLACK_NORMAL_CHANNEL", "")), // エラーチャネルが未定義の場合には通常のチャネルを使用
		WarnChannel:            env.GetEnvString("HETEROLOG_SLACK_WARN_CHANNEL", env.GetEnvString("HETEROLOG_SLACK_NORMAL_CHANNEL", "")),
		InfoChannel:            env.GetEnvString("HETEROLOG_SLACK_INFO_CHANNEL", env.GetEnvString("HETEROLOG_SLACK_NORMAL_CHANNEL", "")),
		DebugChannel:           env.GetEnvString("HETEROLOG_SLACK_DEBUG_CHANNEL", env.GetEnvString("HETEROLOG_SLACK_NORMAL_CHANNEL", "")),
		MaxMessageLength:       pkgslack.MaxMessageLength,
		SamplingConfig: logconfig.SamplingConfig{
			IsSamplingEnabled:        enableLogSlackSampling,
			EnableLoggingByRequestID: enableLoggingSlackByRequestId,
			LoggingByRequestIDRate:   logSlackRequestIdRate,
			SamplingDebugRate:        logSlackDebugRate,
			SamplingInfoRate:         logSlackInfoRate,
			SamplingWarnRate:         logSlackWarnRate,
			SamplingErrorRate:        logSlackErrorRate,
		},
		HandlerConfig: internalerror.RoutingConfig{
			OnErrorConfig: env.GetEnvSliceString("HETEROLOG_SLACK_LOGGER_ERROR_HANDLER", nil),
			OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_SLACK_LOGGER_WARN_HANDLER", nil),
			OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_SLACK_LOGGER_INFO_HANDLER", nil),
			OnDebugConfig: env.GetEnvSliceString("HETEROLOG_SLACK_LOGGER_DEBUG_HANDLER", nil),
		},
	}, errors.Join(environmentErrors...)
}

func loadAsyncSlackConfig(env pkgenv.Lookup) (logconfig.SlackConfig, error) {
	var environmentErrors []error
	asyncSlackTimeoutSecond, asyncSlackTimeoutSecondErr := env.GetEnvDuration("HETEROLOG_ASYNC_SLACK_TIMEOUT_SECOND", pkgslack.DefaultHTTPTimeout)
	if asyncSlackTimeoutSecondErr != nil {
		environmentErrors = append(environmentErrors, asyncSlackTimeoutSecondErr)
	}
	asyncSlackIntervals, asyncSlackIntervalsErr := env.GetEnvDurationSlice("HETEROLOG_ASYNC_SLACK_INTERVALS", logconfig.DefaultSlackIntervals, time.Minute)
	if asyncSlackIntervalsErr != nil {
		environmentErrors = append(environmentErrors, asyncSlackIntervalsErr)
	}
	maxAsyncLogsPerSlackNotification, maxAsyncLogsPerSlackNotificationErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_LOGS_PER_SLACK_NOTIFICATION", logconfig.DefaultMaxLogsPerSlackNotification)
	if maxAsyncLogsPerSlackNotificationErr != nil {
		environmentErrors = append(environmentErrors, maxAsyncLogsPerSlackNotificationErr)
	}
	maxAsyncSlackRestThresholdMinute, maxAsyncSlackRestThresholdMinuteErr := env.GetEnvDurationMinute("HETEROLOG_MAX_ASYNC_SLACK_REST_THRESHOLD_MINUTE", logconfig.DefaultMaxRestThreshold)
	if maxAsyncSlackRestThresholdMinuteErr != nil {
		environmentErrors = append(environmentErrors, maxAsyncSlackRestThresholdMinuteErr)
	}
	enableAsyncLogSlackSampling, enableAsyncLogSlackSamplingErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_LOG_SLACK_SAMPLING", logconfig.DefaultIsLogSlackSamplingEnabled)
	if enableAsyncLogSlackSamplingErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncLogSlackSamplingErr)
	}
	enableAsyncLoggingSlackByRequestId, enableAsyncLoggingSlackByRequestIdErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_LOGGING_SLACK_BY_REQUEST_ID", logconfig.DefaultEnableSlackLoggingByRequestID)
	if enableAsyncLoggingSlackByRequestIdErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncLoggingSlackByRequestIdErr)
	}
	asyncLogSlackRequestIdRate, asyncLogSlackRequestIdRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_SLACK_REQUEST_ID_RATE", logconfig.DefaultSlackLoggingByRequestIDRate)
	if asyncLogSlackRequestIdRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogSlackRequestIdRateErr)
	}
	asyncLogSlackDebugRate, asyncLogSlackDebugRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_SLACK_DEBUG_RATE", logconfig.DefaultLogSlackSamplingDebugRate)
	if asyncLogSlackDebugRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogSlackDebugRateErr)
	}
	asyncLogSlackInfoRate, asyncLogSlackInfoRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_SLACK_INFO_RATE", logconfig.DefaultLogSlackSamplingInfoRate)
	if asyncLogSlackInfoRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogSlackInfoRateErr)
	}
	asyncLogSlackWarnRate, asyncLogSlackWarnRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_SLACK_WARN_RATE", logconfig.DefaultLogSlackSamplingWarnRate)
	if asyncLogSlackWarnRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogSlackWarnRateErr)
	}
	asyncLogSlackErrorRate, asyncLogSlackErrorRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_SLACK_ERROR_RATE", logconfig.DefaultLogSlackSamplingErrorRate)
	if asyncLogSlackErrorRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogSlackErrorRateErr)
	}
	logLevel, logLevelErr := loadLogLevel(
		env.GetEnvString("HETEROLOG_ASYNC_SLACK_LOG_LEVEL", logconfig.DefaultSlackLogLevel.String()),
		"HETEROLOG_ASYNC_SLACK_LOG_LEVEL",
		logconfig.DefaultSlackLogLevel,
	)
	if logLevelErr != nil {
		environmentErrors = append(environmentErrors, logLevelErr)
	}
	return logconfig.SlackConfig{
		LogLevel: logLevel,
		LogBodyConfig: logconfig.LogBodyConfig{
			LogLineDateFormat:        logconfig.LogLineDateFormat,
			ErrorTraceDepth:          logconfig.DefaultErrorTraceDepth,
			ErrorTraceDepthDebugMode: logconfig.DefaultErrorTraceDepthDebugMode,
		},
		Timeout:                asyncSlackTimeoutSecond,
		Intervals:              asyncSlackIntervals,
		MaxLogsPerNotification: maxAsyncLogsPerSlackNotification,
		MaxRestThreshold:       maxAsyncSlackRestThresholdMinute,
		Token:                  env.GetEnvString("HETEROLOG_ASYNC_SLACK_API_KEY", ""),
		Channel:                env.GetEnvString("HETEROLOG_ASYNC_SLACK_NORMAL_CHANNEL", ""),
		ErrorChannel:           env.GetEnvString("HETEROLOG_ASYNC_SLACK_ERROR_CHANNEL", env.GetEnvString("HETEROLOG_ASYNC_SLACK_NORMAL_CHANNEL", "")), // エラーチャネルが未定義の場合には通常のチャネルを使用
		WarnChannel:            env.GetEnvString("HETEROLOG_ASYNC_SLACK_WARN_CHANNEL", env.GetEnvString("HETEROLOG_ASYNC_SLACK_NORMAL_CHANNEL", "")),
		InfoChannel:            env.GetEnvString("HETEROLOG_ASYNC_SLACK_INFO_CHANNEL", env.GetEnvString("HETEROLOG_ASYNC_SLACK_NORMAL_CHANNEL", "")),
		DebugChannel:           env.GetEnvString("HETEROLOG_ASYNC_SLACK_DEBUG_CHANNEL", env.GetEnvString("HETEROLOG_ASYNC_SLACK_NORMAL_CHANNEL", "")),
		MaxMessageLength:       pkgslack.MaxMessageLength,
		SamplingConfig: logconfig.SamplingConfig{
			IsSamplingEnabled:        enableAsyncLogSlackSampling,
			EnableLoggingByRequestID: enableAsyncLoggingSlackByRequestId,
			LoggingByRequestIDRate:   asyncLogSlackRequestIdRate,
			SamplingDebugRate:        asyncLogSlackDebugRate,
			SamplingInfoRate:         asyncLogSlackInfoRate,
			SamplingWarnRate:         asyncLogSlackWarnRate,
			SamplingErrorRate:        asyncLogSlackErrorRate,
		},
		HandlerConfig: internalerror.RoutingConfig{
			OnErrorConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_SLACK_LOGGER_ERROR_HANDLER", nil),
			OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_SLACK_LOGGER_WARN_HANDLER", nil),
			OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_SLACK_LOGGER_INFO_HANDLER", nil),
			OnDebugConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_SLACK_LOGGER_DEBUG_HANDLER", nil),
		},
	}, errors.Join(environmentErrors...)
}

func loadDeliverySlackConfig(env pkgenv.Lookup) (logconfig.DeliverySlackConfig, error) {
	var environmentErrors []error
	deliverySlackTimeoutSecond, deliverySlackTimeoutSecondErr := env.GetEnvDuration("HETEROLOG_DELIVERY_SLACK_TIMEOUT_SECOND", pkgslack.DefaultHTTPTimeout)
	if deliverySlackTimeoutSecondErr != nil {
		environmentErrors = append(environmentErrors, deliverySlackTimeoutSecondErr)
	}
	logLevel, logLevelErr := loadLogLevel(
		env.GetEnvString("HETEROLOG_DELIVERY_SLACK_LOG_LEVEL", logconfig.DefaultDeliverySlackLogLevel.String()),
		"HETEROLOG_DELIVERY_SLACK_LOG_LEVEL",
		logconfig.DefaultDeliverySlackLogLevel,
	)
	if logLevelErr != nil {
		environmentErrors = append(environmentErrors, logLevelErr)
	}
	return logconfig.DeliverySlackConfig{
		LogLevel: logLevel,
		LogBodyConfig: logconfig.LogBodyConfig{
			LogLineDateFormat:        logconfig.LogLineDateFormat,
			ErrorTraceDepth:          logconfig.DefaultErrorTraceDepth,
			ErrorTraceDepthDebugMode: logconfig.DefaultErrorTraceDepthDebugMode,
		},
		Timeout:          deliverySlackTimeoutSecond,
		Token:            env.GetEnvString("HETEROLOG_DELIVERY_SLACK_API_KEY", ""),
		Channel:          env.GetEnvString("HETEROLOG_DELIVERY_SLACK_NORMAL_CHANNEL", ""),
		ErrorChannel:     env.GetEnvString("HETEROLOG_DELIVERY_SLACK_ERROR_CHANNEL", env.GetEnvString("HETEROLOG_DELIVERY_SLACK_NORMAL_CHANNEL", "")), // エラーチャネルが未定義の場合には通常のチャネルを使用
		WarnChannel:      env.GetEnvString("HETEROLOG_DELIVERY_SLACK_WARN_CHANNEL", env.GetEnvString("HETEROLOG_DELIVERY_SLACK_NORMAL_CHANNEL", "")),
		InfoChannel:      env.GetEnvString("HETEROLOG_DELIVERY_SLACK_INFO_CHANNEL", env.GetEnvString("HETEROLOG_DELIVERY_SLACK_NORMAL_CHANNEL", "")),
		DebugChannel:     env.GetEnvString("HETEROLOG_DELIVERY_SLACK_DEBUG_CHANNEL", env.GetEnvString("HETEROLOG_DELIVERY_SLACK_NORMAL_CHANNEL", "")),
		MaxMessageLength: pkgslack.MaxMessageLength,
		HandlerConfig: internalerror.RoutingConfig{
			OnErrorConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_SLACK_LOGGER_ERROR_HANDLER", nil),
			OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_SLACK_LOGGER_WARN_HANDLER", nil),
			OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_SLACK_LOGGER_INFO_HANDLER", nil),
			OnDebugConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_SLACK_LOGGER_DEBUG_HANDLER", nil),
		},
	}, errors.Join(environmentErrors...)
}

// OpenSearchロガー設定を読み込む
func loadOpenSearchConfig(env pkgenv.Lookup) (logconfig.OpenSearchLoggerBaseConfig, error) {
	var environmentErrors []error
	opensearchFlushIntervals, opensearchFlushIntervalsErr := env.GetEnvDuration("HETEROLOG_OPENSEARCH_FLUSH_INTERVALS", logconfig.DefaultOpenSearchFlushInterval)
	if opensearchFlushIntervalsErr != nil {
		environmentErrors = append(environmentErrors, opensearchFlushIntervalsErr)
	}
	maxBulkDocumentOpensearchFlush, maxBulkDocumentOpensearchFlushErr := env.GetEnvInt("HETEROLOG_MAX_BULK_DOCUMENT_OPENSEARCH_FLUSH", logconfig.DefaultOpenSearchMaxBulkDocuments)
	if maxBulkDocumentOpensearchFlushErr != nil {
		environmentErrors = append(environmentErrors, maxBulkDocumentOpensearchFlushErr)
	}
	opensearchBufferFlushBytes, opensearchBufferFlushBytesErr := env.GetEnvInt("HETEROLOG_OPENSEARCH_BUFFER_FLUSH_BYTES", logconfig.DefaultOpenSearchBufferFlushBytes)
	if opensearchBufferFlushBytesErr != nil {
		environmentErrors = append(environmentErrors, opensearchBufferFlushBytesErr)
	}
	maxOpensearchBulkRequestBytes, maxOpensearchBulkRequestBytesErr := env.GetEnvInt("HETEROLOG_MAX_OPENSEARCH_BULK_REQUEST_BYTES", logconfig.DefaultOpenSearchMaxBulkRequestBytes)
	if maxOpensearchBulkRequestBytesErr != nil {
		environmentErrors = append(environmentErrors, maxOpensearchBulkRequestBytesErr)
	}
	enableOpensearchNoBulkFlushMode, enableOpensearchNoBulkFlushModeErr := env.GetEnvBool("HETEROLOG_ENABLE_OPENSEARCH_NO_BULK_FLUSH_MODE", logconfig.DefaultNoBulkFlushModeEnabled)
	if enableOpensearchNoBulkFlushModeErr != nil {
		environmentErrors = append(environmentErrors, enableOpensearchNoBulkFlushModeErr)
	}
	enableLogOpensearchSampling, enableLogOpensearchSamplingErr := env.GetEnvBool("HETEROLOG_ENABLE_LOG_OPENSEARCH_SAMPLING", logconfig.DefaultIsLogOpenSearchSamplingEnabled)
	if enableLogOpensearchSamplingErr != nil {
		environmentErrors = append(environmentErrors, enableLogOpensearchSamplingErr)
	}
	enableLoggingOpensearchByRequestId, enableLoggingOpensearchByRequestIdErr := env.GetEnvBool("HETEROLOG_ENABLE_LOGGING_OPENSEARCH_BY_REQUEST_ID", logconfig.DefaultEnableOpenSearchLoggingByRequestID)
	if enableLoggingOpensearchByRequestIdErr != nil {
		environmentErrors = append(environmentErrors, enableLoggingOpensearchByRequestIdErr)
	}
	logOpensearchRequestIdRate, logOpensearchRequestIdRateErr := env.GetEnvInt("HETEROLOG_LOG_OPENSEARCH_REQUEST_ID_RATE", logconfig.DefaultOpenSearchLoggingByRequestIDRate)
	if logOpensearchRequestIdRateErr != nil {
		environmentErrors = append(environmentErrors, logOpensearchRequestIdRateErr)
	}
	logOpensearchDebugRate, logOpensearchDebugRateErr := env.GetEnvInt("HETEROLOG_LOG_OPENSEARCH_DEBUG_RATE", logconfig.DefaultLogOpenSearchSamplingDebugRate)
	if logOpensearchDebugRateErr != nil {
		environmentErrors = append(environmentErrors, logOpensearchDebugRateErr)
	}
	logOpensearchInfoRate, logOpensearchInfoRateErr := env.GetEnvInt("HETEROLOG_LOG_OPENSEARCH_INFO_RATE", logconfig.DefaultLogOpenSearchSamplingInfoRate)
	if logOpensearchInfoRateErr != nil {
		environmentErrors = append(environmentErrors, logOpensearchInfoRateErr)
	}
	logOpensearchWarnRate, logOpensearchWarnRateErr := env.GetEnvInt("HETEROLOG_LOG_OPENSEARCH_WARN_RATE", logconfig.DefaultLogOpenSearchSamplingWarnRate)
	if logOpensearchWarnRateErr != nil {
		environmentErrors = append(environmentErrors, logOpensearchWarnRateErr)
	}
	logOpensearchErrorRate, logOpensearchErrorRateErr := env.GetEnvInt("HETEROLOG_LOG_OPENSEARCH_ERROR_RATE", logconfig.DefaultLogOpenSearchSamplingErrorRate)
	if logOpensearchErrorRateErr != nil {
		environmentErrors = append(environmentErrors, logOpensearchErrorRateErr)
	}
	enableIndexOpensearchLatency, enableIndexOpensearchLatencyErr := env.GetEnvBool("HETEROLOG_ENABLE_INDEX_OPENSEARCH_LATENCY", logconfig.DefaultIndexLatencyEnabled)
	if enableIndexOpensearchLatencyErr != nil {
		environmentErrors = append(environmentErrors, enableIndexOpensearchLatencyErr)
	}
	clockConfig, clockErr := loadClockConfig(env)
	logLevel, logLevelErr := loadLogLevel(
		env.GetEnvString("HETEROLOG_OPENSEARCH_LOG_LEVEL", logconfig.DefaultOpenSearchLogLevel.String()),
		"HETEROLOG_OPENSEARCH_LOG_LEVEL",
		logconfig.DefaultOpenSearchLogLevel,
	)
	environmentErrors = append(environmentErrors, clockErr, logLevelErr)
	return logconfig.OpenSearchLoggerBaseConfig{
		ClockConfig: clockConfig,
		LogLevel:    logLevel,
		LogBodyConfig: logconfig.LogBodyConfig{
			LogLineDateFormat:        logconfig.LogLineDateFormat,
			ErrorTraceDepth:          logconfig.DefaultErrorTraceDepth,
			ErrorTraceDepthDebugMode: logconfig.DefaultErrorTraceDepthDebugMode,
		},
		FlushInterval:          opensearchFlushIntervals,
		MaxBulkDocument:        maxBulkDocumentOpensearchFlush,
		BufferFlushBytes:       opensearchBufferFlushBytes,
		MaxBulkRequestBytes:    maxOpensearchBulkRequestBytes,
		NoBulkFlushModeEnabled: enableOpensearchNoBulkFlushMode,
		SamplingConfig: logconfig.SamplingConfig{
			IsSamplingEnabled:        enableLogOpensearchSampling,
			EnableLoggingByRequestID: enableLoggingOpensearchByRequestId,
			LoggingByRequestIDRate:   logOpensearchRequestIdRate,
			SamplingDebugRate:        logOpensearchDebugRate,
			SamplingInfoRate:         logOpensearchInfoRate,
			SamplingWarnRate:         logOpensearchWarnRate,
			SamplingErrorRate:        logOpensearchErrorRate,
		},
		PostWarnThresholdDuration:  logconfig.PostLogWarnThresholdDuration,
		PostErrorThresholdDuration: logconfig.PostLogErrorThresholdDuration,
		IndexLatencyEnabled:        enableIndexOpensearchLatency,
		MaxItemRetries:             logconfig.MaxOpenSearchLoggerItemRetries,
		RetryWaitTime:              logconfig.OpenSearchRetryWaitTime,
		HandlerConfig: internalerror.RoutingConfig{
			OnErrorConfig: env.GetEnvSliceString("HETEROLOG_OPENSEARCH_LOGGER_ERROR_HANDLER", nil),
			OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_OPENSEARCH_LOGGER_WARN_HANDLER", nil),
			OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_OPENSEARCH_LOGGER_INFO_HANDLER", nil),
			OnDebugConfig: env.GetEnvSliceString("HETEROLOG_OPENSEARCH_LOGGER_DEBUG_HANDLER", nil),
		},
	}, errors.Join(environmentErrors...)
}

// 非同期OpenSearchロガー設定を読み込む
func loadAsyncOpenSearchConfig(env pkgenv.Lookup) (logconfig.OpenSearchLoggerBaseConfig, error) {
	var environmentErrors []error
	asyncOpensearchFlushIntervals, asyncOpensearchFlushIntervalsErr := env.GetEnvDuration("HETEROLOG_ASYNC_OPENSEARCH_FLUSH_INTERVALS", logconfig.DefaultOpenSearchFlushInterval)
	if asyncOpensearchFlushIntervalsErr != nil {
		environmentErrors = append(environmentErrors, asyncOpensearchFlushIntervalsErr)
	}
	asyncMaxBulkDocumentOpensearchFlush, asyncMaxBulkDocumentOpensearchFlushErr := env.GetEnvInt("HETEROLOG_ASYNC_MAX_BULK_DOCUMENT_OPENSEARCH_FLUSH", logconfig.DefaultOpenSearchMaxBulkDocuments)
	if asyncMaxBulkDocumentOpensearchFlushErr != nil {
		environmentErrors = append(environmentErrors, asyncMaxBulkDocumentOpensearchFlushErr)
	}
	asyncOpensearchBufferFlushBytes, asyncOpensearchBufferFlushBytesErr := env.GetEnvInt("HETEROLOG_ASYNC_OPENSEARCH_BUFFER_FLUSH_BYTES", logconfig.DefaultOpenSearchBufferFlushBytes)
	if asyncOpensearchBufferFlushBytesErr != nil {
		environmentErrors = append(environmentErrors, asyncOpensearchBufferFlushBytesErr)
	}
	maxAsyncOpensearchBulkRequestBytes, maxAsyncOpensearchBulkRequestBytesErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_OPENSEARCH_BULK_REQUEST_BYTES", logconfig.DefaultOpenSearchMaxBulkRequestBytes)
	if maxAsyncOpensearchBulkRequestBytesErr != nil {
		environmentErrors = append(environmentErrors, maxAsyncOpensearchBulkRequestBytesErr)
	}
	enableAsyncLogOpensearchSampling, enableAsyncLogOpensearchSamplingErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_LOG_OPENSEARCH_SAMPLING", logconfig.DefaultIsLogOpenSearchSamplingEnabled)
	if enableAsyncLogOpensearchSamplingErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncLogOpensearchSamplingErr)
	}
	enableAsyncLoggingOpensearchByRequestId, enableAsyncLoggingOpensearchByRequestIdErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_LOGGING_OPENSEARCH_BY_REQUEST_ID", logconfig.DefaultEnableOpenSearchLoggingByRequestID)
	if enableAsyncLoggingOpensearchByRequestIdErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncLoggingOpensearchByRequestIdErr)
	}
	asyncLogOpensearchRequestIdRate, asyncLogOpensearchRequestIdRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_OPENSEARCH_REQUEST_ID_RATE", logconfig.DefaultOpenSearchLoggingByRequestIDRate)
	if asyncLogOpensearchRequestIdRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogOpensearchRequestIdRateErr)
	}
	asyncLogOpensearchDebugRate, asyncLogOpensearchDebugRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_OPENSEARCH_DEBUG_RATE", logconfig.DefaultLogOpenSearchSamplingDebugRate)
	if asyncLogOpensearchDebugRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogOpensearchDebugRateErr)
	}
	asyncLogOpensearchInfoRate, asyncLogOpensearchInfoRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_OPENSEARCH_INFO_RATE", logconfig.DefaultLogOpenSearchSamplingInfoRate)
	if asyncLogOpensearchInfoRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogOpensearchInfoRateErr)
	}
	asyncLogOpensearchWarnRate, asyncLogOpensearchWarnRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_OPENSEARCH_WARN_RATE", logconfig.DefaultLogOpenSearchSamplingWarnRate)
	if asyncLogOpensearchWarnRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogOpensearchWarnRateErr)
	}
	asyncLogOpensearchErrorRate, asyncLogOpensearchErrorRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_OPENSEARCH_ERROR_RATE", logconfig.DefaultLogOpenSearchSamplingErrorRate)
	if asyncLogOpensearchErrorRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogOpensearchErrorRateErr)
	}
	enableAsyncIndexOpensearchLatency, enableAsyncIndexOpensearchLatencyErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_INDEX_OPENSEARCH_LATENCY", logconfig.DefaultIndexLatencyEnabled)
	if enableAsyncIndexOpensearchLatencyErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncIndexOpensearchLatencyErr)
	}
	clockConfig, clockErr := loadClockConfig(env)
	logLevel, logLevelErr := loadLogLevel(
		env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_LOG_LEVEL", logconfig.DefaultOpenSearchLogLevel.String()),
		"HETEROLOG_ASYNC_OPENSEARCH_LOG_LEVEL",
		logconfig.DefaultOpenSearchLogLevel,
	)
	environmentErrors = append(environmentErrors, clockErr, logLevelErr)
	return logconfig.OpenSearchLoggerBaseConfig{
		ClockConfig: clockConfig,
		LogLevel:    logLevel,
		LogBodyConfig: logconfig.LogBodyConfig{
			LogLineDateFormat:        logconfig.LogLineDateFormat,
			ErrorTraceDepth:          logconfig.DefaultErrorTraceDepth,
			ErrorTraceDepthDebugMode: logconfig.DefaultErrorTraceDepthDebugMode,
		},
		FlushInterval:       asyncOpensearchFlushIntervals,
		MaxBulkDocument:     asyncMaxBulkDocumentOpensearchFlush,
		BufferFlushBytes:    asyncOpensearchBufferFlushBytes,
		MaxBulkRequestBytes: maxAsyncOpensearchBulkRequestBytes,
		SamplingConfig: logconfig.SamplingConfig{
			IsSamplingEnabled:        enableAsyncLogOpensearchSampling,
			EnableLoggingByRequestID: enableAsyncLoggingOpensearchByRequestId,
			LoggingByRequestIDRate:   asyncLogOpensearchRequestIdRate,
			SamplingDebugRate:        asyncLogOpensearchDebugRate,
			SamplingInfoRate:         asyncLogOpensearchInfoRate,
			SamplingWarnRate:         asyncLogOpensearchWarnRate,
			SamplingErrorRate:        asyncLogOpensearchErrorRate,
		},
		PostWarnThresholdDuration:  logconfig.PostLogWarnThresholdDuration,
		PostErrorThresholdDuration: logconfig.PostLogErrorThresholdDuration,
		IndexLatencyEnabled:        enableAsyncIndexOpensearchLatency,
		MaxItemRetries:             logconfig.MaxOpenSearchLoggerItemRetries,
		RetryWaitTime:              logconfig.OpenSearchRetryWaitTime,
		HandlerConfig: internalerror.RoutingConfig{
			OnErrorConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_OPENSEARCH_LOGGER_ERROR_HANDLER", nil),
			OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_OPENSEARCH_LOGGER_WARN_HANDLER", nil),
			OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_OPENSEARCH_LOGGER_INFO_HANDLER", nil),
			OnDebugConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_OPENSEARCH_LOGGER_DEBUG_HANDLER", nil),
		},
	}, errors.Join(environmentErrors...)
}

// Delivery OpenSearchロガー設定を読み込む
func loadDeliveryOpenSearchConfig(env pkgenv.Lookup) (logconfig.DeliveryOpenSearchLoggerBaseConfig, error) {
	var environmentErrors []error
	clockConfig, clockErr := loadClockConfig(env)
	environmentErrors = append(environmentErrors, clockErr)
	return logconfig.DeliveryOpenSearchLoggerBaseConfig{
		ClockConfig: clockConfig,
		LogBodyConfig: logconfig.LogBodyConfig{
			LogLineDateFormat:        logconfig.LogLineDateFormat,
			ErrorTraceDepth:          logconfig.DefaultErrorTraceDepth,
			ErrorTraceDepthDebugMode: logconfig.DefaultErrorTraceDepthDebugMode,
		},
		HandlerConfig: internalerror.RoutingConfig{
			OnErrorConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_OPENSEARCH_LOGGER_ERROR_HANDLER", nil),
			OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_OPENSEARCH_LOGGER_WARN_HANDLER", nil),
			OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_OPENSEARCH_LOGGER_INFO_HANDLER", nil),
			OnDebugConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_OPENSEARCH_LOGGER_DEBUG_HANDLER", nil),
		},
	}, errors.Join(environmentErrors...)
}

func loadLokiConfig(env pkgenv.Lookup) (logconfig.LokiLoggerBaseConfig, error) {
	var environmentErrors []error
	lokiFlushIntervalSecond, lokiFlushIntervalSecondErr := env.GetEnvDuration("HETEROLOG_LOKI_FLUSH_INTERVAL_SECOND", logconfig.DefaultLokiFlushInterval)
	if lokiFlushIntervalSecondErr != nil {
		environmentErrors = append(environmentErrors, lokiFlushIntervalSecondErr)
	}
	maxLokiPushEntries, maxLokiPushEntriesErr := env.GetEnvInt("HETEROLOG_MAX_LOKI_PUSH_ENTRIES", logconfig.DefaultLokiMaxEntries)
	if maxLokiPushEntriesErr != nil {
		environmentErrors = append(environmentErrors, maxLokiPushEntriesErr)
	}
	lokiBufferFlushBytes, lokiBufferFlushBytesErr := env.GetEnvInt("HETEROLOG_LOKI_BUFFER_FLUSH_BYTES", logconfig.DefaultLokiBufferFlushBytes)
	if lokiBufferFlushBytesErr != nil {
		environmentErrors = append(environmentErrors, lokiBufferFlushBytesErr)
	}
	maxLokiFallbackQueueEntries, maxLokiFallbackQueueEntriesErr := env.GetEnvInt("HETEROLOG_MAX_LOKI_FALLBACK_QUEUE_ENTRIES", logconfig.DefaultLokiFallbackQueueMaxEntries)
	if maxLokiFallbackQueueEntriesErr != nil {
		environmentErrors = append(environmentErrors, maxLokiFallbackQueueEntriesErr)
	}
	maxLokiLineBytes, maxLokiLineBytesErr := env.GetEnvInt("HETEROLOG_MAX_LOKI_LINE_BYTES", logconfig.DefaultLokiMaxLineBytes)
	if maxLokiLineBytesErr != nil {
		environmentErrors = append(environmentErrors, maxLokiLineBytesErr)
	}
	maxLokiPushRequestBytes, maxLokiPushRequestBytesErr := env.GetEnvInt("HETEROLOG_MAX_LOKI_PUSH_REQUEST_BYTES", logconfig.DefaultLokiMaxPushRequestBytes)
	if maxLokiPushRequestBytesErr != nil {
		environmentErrors = append(environmentErrors, maxLokiPushRequestBytesErr)
	}
	enableLokiNoBulkFlushMode, enableLokiNoBulkFlushModeErr := env.GetEnvBool("HETEROLOG_ENABLE_LOKI_NO_BULK_FLUSH_MODE", logconfig.DefaultLokiNoBulkFlushModeEnabled)
	if enableLokiNoBulkFlushModeErr != nil {
		environmentErrors = append(environmentErrors, enableLokiNoBulkFlushModeErr)
	}
	enableLogLokiSampling, enableLogLokiSamplingErr := env.GetEnvBool("HETEROLOG_ENABLE_LOG_LOKI_SAMPLING", logconfig.DefaultIsLogLokiSamplingEnabled)
	if enableLogLokiSamplingErr != nil {
		environmentErrors = append(environmentErrors, enableLogLokiSamplingErr)
	}
	enableLoggingLokiByRequestId, enableLoggingLokiByRequestIdErr := env.GetEnvBool("HETEROLOG_ENABLE_LOGGING_LOKI_BY_REQUEST_ID", logconfig.DefaultEnableLokiLoggingByRequestID)
	if enableLoggingLokiByRequestIdErr != nil {
		environmentErrors = append(environmentErrors, enableLoggingLokiByRequestIdErr)
	}
	logLokiRequestIdRate, logLokiRequestIdRateErr := env.GetEnvInt("HETEROLOG_LOG_LOKI_REQUEST_ID_RATE", logconfig.DefaultLokiLoggingByRequestIDRate)
	if logLokiRequestIdRateErr != nil {
		environmentErrors = append(environmentErrors, logLokiRequestIdRateErr)
	}
	logLokiDebugRate, logLokiDebugRateErr := env.GetEnvInt("HETEROLOG_LOG_LOKI_DEBUG_RATE", logconfig.DefaultLogLokiSamplingDebugRate)
	if logLokiDebugRateErr != nil {
		environmentErrors = append(environmentErrors, logLokiDebugRateErr)
	}
	logLokiInfoRate, logLokiInfoRateErr := env.GetEnvInt("HETEROLOG_LOG_LOKI_INFO_RATE", logconfig.DefaultLogLokiSamplingInfoRate)
	if logLokiInfoRateErr != nil {
		environmentErrors = append(environmentErrors, logLokiInfoRateErr)
	}
	logLokiWarnRate, logLokiWarnRateErr := env.GetEnvInt("HETEROLOG_LOG_LOKI_WARN_RATE", logconfig.DefaultLogLokiSamplingWarnRate)
	if logLokiWarnRateErr != nil {
		environmentErrors = append(environmentErrors, logLokiWarnRateErr)
	}
	logLokiErrorRate, logLokiErrorRateErr := env.GetEnvInt("HETEROLOG_LOG_LOKI_ERROR_RATE", logconfig.DefaultLogLokiSamplingErrorRate)
	if logLokiErrorRateErr != nil {
		environmentErrors = append(environmentErrors, logLokiErrorRateErr)
	}
	enableLokiPushLatency, enableLokiPushLatencyErr := env.GetEnvBool("HETEROLOG_ENABLE_LOKI_PUSH_LATENCY", logconfig.DefaultLokiPushLatencyEnabled)
	if enableLokiPushLatencyErr != nil {
		environmentErrors = append(environmentErrors, enableLokiPushLatencyErr)
	}
	clockConfig, clockErr := loadClockConfig(env)
	logLevel, logLevelErr := loadLogLevel(
		env.GetEnvString("HETEROLOG_LOKI_LOG_LEVEL", logconfig.DefaultLokiLogLevel.String()),
		"HETEROLOG_LOKI_LOG_LEVEL",
		logconfig.DefaultLokiLogLevel,
	)
	environmentErrors = append(environmentErrors, clockErr, logLevelErr)
	return logconfig.LokiLoggerBaseConfig{
		ClockConfig: clockConfig,
		LogLevel:    logLevel,
		LogBodyConfig: logconfig.LogBodyConfig{
			LogLineDateFormat:        logconfig.LogLineDateFormat,
			ErrorTraceDepth:          logconfig.DefaultErrorTraceDepth,
			ErrorTraceDepthDebugMode: logconfig.DefaultErrorTraceDepthDebugMode,
		},
		FlushInterval:           lokiFlushIntervalSecond,
		MaxEntries:              maxLokiPushEntries,
		BufferFlushBytes:        lokiBufferFlushBytes,
		FallbackQueueMaxEntries: maxLokiFallbackQueueEntries,
		MaxLineBytes:            maxLokiLineBytes,
		MaxPushRequestBytes:     maxLokiPushRequestBytes,
		NoBulkFlushModeEnabled:  enableLokiNoBulkFlushMode,
		SamplingConfig: logconfig.SamplingConfig{
			IsSamplingEnabled:        enableLogLokiSampling,
			EnableLoggingByRequestID: enableLoggingLokiByRequestId,
			LoggingByRequestIDRate:   logLokiRequestIdRate,
			SamplingDebugRate:        logLokiDebugRate,
			SamplingInfoRate:         logLokiInfoRate,
			SamplingWarnRate:         logLokiWarnRate,
			SamplingErrorRate:        logLokiErrorRate,
		},
		PostWarnThresholdDuration:  logconfig.PostLogWarnThresholdDuration,
		PostErrorThresholdDuration: logconfig.PostLogErrorThresholdDuration,
		PushLatencyEnabled:         enableLokiPushLatency,
		HandlerConfig: internalerror.RoutingConfig{
			OnErrorConfig: env.GetEnvSliceString("HETEROLOG_LOKI_LOGGER_ERROR_HANDLER", nil),
			OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_LOKI_LOGGER_WARN_HANDLER", nil),
			OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_LOKI_LOGGER_INFO_HANDLER", nil),
			OnDebugConfig: env.GetEnvSliceString("HETEROLOG_LOKI_LOGGER_DEBUG_HANDLER", nil),
		},
	}, errors.Join(environmentErrors...)
}

func loadAsyncLokiConfig(env pkgenv.Lookup) (logconfig.LokiLoggerBaseConfig, error) {
	var environmentErrors []error
	asyncLokiFlushIntervalSecond, asyncLokiFlushIntervalSecondErr := env.GetEnvDuration("HETEROLOG_ASYNC_LOKI_FLUSH_INTERVAL_SECOND", logconfig.DefaultLokiFlushInterval)
	if asyncLokiFlushIntervalSecondErr != nil {
		environmentErrors = append(environmentErrors, asyncLokiFlushIntervalSecondErr)
	}
	maxAsyncLokiPushEntries, maxAsyncLokiPushEntriesErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_LOKI_PUSH_ENTRIES", logconfig.DefaultLokiMaxEntries)
	if maxAsyncLokiPushEntriesErr != nil {
		environmentErrors = append(environmentErrors, maxAsyncLokiPushEntriesErr)
	}
	asyncLokiBufferFlushBytes, asyncLokiBufferFlushBytesErr := env.GetEnvInt("HETEROLOG_ASYNC_LOKI_BUFFER_FLUSH_BYTES", logconfig.DefaultLokiBufferFlushBytes)
	if asyncLokiBufferFlushBytesErr != nil {
		environmentErrors = append(environmentErrors, asyncLokiBufferFlushBytesErr)
	}
	maxAsyncLokiFallbackQueueEntries, maxAsyncLokiFallbackQueueEntriesErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_LOKI_FALLBACK_QUEUE_ENTRIES", logconfig.DefaultLokiFallbackQueueMaxEntries)
	if maxAsyncLokiFallbackQueueEntriesErr != nil {
		environmentErrors = append(environmentErrors, maxAsyncLokiFallbackQueueEntriesErr)
	}
	maxAsyncLokiLineBytes, maxAsyncLokiLineBytesErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_LOKI_LINE_BYTES", logconfig.DefaultLokiMaxLineBytes)
	if maxAsyncLokiLineBytesErr != nil {
		environmentErrors = append(environmentErrors, maxAsyncLokiLineBytesErr)
	}
	maxAsyncLokiPushRequestBytes, maxAsyncLokiPushRequestBytesErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_LOKI_PUSH_REQUEST_BYTES", logconfig.DefaultLokiMaxPushRequestBytes)
	if maxAsyncLokiPushRequestBytesErr != nil {
		environmentErrors = append(environmentErrors, maxAsyncLokiPushRequestBytesErr)
	}
	enableAsyncLogLokiSampling, enableAsyncLogLokiSamplingErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_LOG_LOKI_SAMPLING", logconfig.DefaultIsLogLokiSamplingEnabled)
	if enableAsyncLogLokiSamplingErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncLogLokiSamplingErr)
	}
	enableAsyncLoggingLokiByRequestId, enableAsyncLoggingLokiByRequestIdErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_LOGGING_LOKI_BY_REQUEST_ID", logconfig.DefaultEnableLokiLoggingByRequestID)
	if enableAsyncLoggingLokiByRequestIdErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncLoggingLokiByRequestIdErr)
	}
	asyncLogLokiRequestIdRate, asyncLogLokiRequestIdRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_LOKI_REQUEST_ID_RATE", logconfig.DefaultLokiLoggingByRequestIDRate)
	if asyncLogLokiRequestIdRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogLokiRequestIdRateErr)
	}
	asyncLogLokiDebugRate, asyncLogLokiDebugRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_LOKI_DEBUG_RATE", logconfig.DefaultLogLokiSamplingDebugRate)
	if asyncLogLokiDebugRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogLokiDebugRateErr)
	}
	asyncLogLokiInfoRate, asyncLogLokiInfoRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_LOKI_INFO_RATE", logconfig.DefaultLogLokiSamplingInfoRate)
	if asyncLogLokiInfoRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogLokiInfoRateErr)
	}
	asyncLogLokiWarnRate, asyncLogLokiWarnRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_LOKI_WARN_RATE", logconfig.DefaultLogLokiSamplingWarnRate)
	if asyncLogLokiWarnRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogLokiWarnRateErr)
	}
	asyncLogLokiErrorRate, asyncLogLokiErrorRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_LOKI_ERROR_RATE", logconfig.DefaultLogLokiSamplingErrorRate)
	if asyncLogLokiErrorRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogLokiErrorRateErr)
	}
	enableAsyncLokiPushLatency, enableAsyncLokiPushLatencyErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_LOKI_PUSH_LATENCY", logconfig.DefaultLokiPushLatencyEnabled)
	if enableAsyncLokiPushLatencyErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncLokiPushLatencyErr)
	}
	clockConfig, clockErr := loadClockConfig(env)
	logLevel, logLevelErr := loadLogLevel(
		env.GetEnvString("HETEROLOG_ASYNC_LOKI_LOG_LEVEL", logconfig.DefaultLokiLogLevel.String()),
		"HETEROLOG_ASYNC_LOKI_LOG_LEVEL",
		logconfig.DefaultLokiLogLevel,
	)
	environmentErrors = append(environmentErrors, clockErr, logLevelErr)
	return logconfig.LokiLoggerBaseConfig{
		ClockConfig: clockConfig,
		LogLevel:    logLevel,
		LogBodyConfig: logconfig.LogBodyConfig{
			LogLineDateFormat:        logconfig.LogLineDateFormat,
			ErrorTraceDepth:          logconfig.DefaultErrorTraceDepth,
			ErrorTraceDepthDebugMode: logconfig.DefaultErrorTraceDepthDebugMode,
		},
		FlushInterval:           asyncLokiFlushIntervalSecond,
		MaxEntries:              maxAsyncLokiPushEntries,
		BufferFlushBytes:        asyncLokiBufferFlushBytes,
		FallbackQueueMaxEntries: maxAsyncLokiFallbackQueueEntries,
		MaxLineBytes:            maxAsyncLokiLineBytes,
		MaxPushRequestBytes:     maxAsyncLokiPushRequestBytes,
		NoBulkFlushModeEnabled:  false,
		SamplingConfig: logconfig.SamplingConfig{
			IsSamplingEnabled:        enableAsyncLogLokiSampling,
			EnableLoggingByRequestID: enableAsyncLoggingLokiByRequestId,
			LoggingByRequestIDRate:   asyncLogLokiRequestIdRate,
			SamplingDebugRate:        asyncLogLokiDebugRate,
			SamplingInfoRate:         asyncLogLokiInfoRate,
			SamplingWarnRate:         asyncLogLokiWarnRate,
			SamplingErrorRate:        asyncLogLokiErrorRate,
		},
		PostWarnThresholdDuration:  logconfig.PostLogWarnThresholdDuration,
		PostErrorThresholdDuration: logconfig.PostLogErrorThresholdDuration,
		PushLatencyEnabled:         enableAsyncLokiPushLatency,
		HandlerConfig: internalerror.RoutingConfig{
			OnErrorConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_LOKI_LOGGER_ERROR_HANDLER", nil),
			OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_LOKI_LOGGER_WARN_HANDLER", nil),
			OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_LOKI_LOGGER_INFO_HANDLER", nil),
			OnDebugConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_LOKI_LOGGER_DEBUG_HANDLER", nil),
		},
	}, errors.Join(environmentErrors...)
}

func loadDeliveryLokiConfig(env pkgenv.Lookup) (logconfig.DeliveryLokiLoggerBaseConfig, error) {
	var environmentErrors []error
	maxDeliveryLokiLineBytes, maxDeliveryLokiLineBytesErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_LOKI_LINE_BYTES", logconfig.DefaultLokiMaxLineBytes)
	if maxDeliveryLokiLineBytesErr != nil {
		environmentErrors = append(environmentErrors, maxDeliveryLokiLineBytesErr)
	}
	maxDeliveryLokiPushRequestBytes, maxDeliveryLokiPushRequestBytesErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_LOKI_PUSH_REQUEST_BYTES", logconfig.DefaultLokiMaxPushRequestBytes)
	if maxDeliveryLokiPushRequestBytesErr != nil {
		environmentErrors = append(environmentErrors, maxDeliveryLokiPushRequestBytesErr)
	}
	clockConfig, clockErr := loadClockConfig(env)
	environmentErrors = append(environmentErrors, clockErr)
	return logconfig.DeliveryLokiLoggerBaseConfig{
		ClockConfig: clockConfig,
		LogBodyConfig: logconfig.LogBodyConfig{
			LogLineDateFormat:        logconfig.LogLineDateFormat,
			ErrorTraceDepth:          logconfig.DefaultErrorTraceDepth,
			ErrorTraceDepthDebugMode: logconfig.DefaultErrorTraceDepthDebugMode,
		},
		MaxLineBytes:        maxDeliveryLokiLineBytes,
		MaxPushRequestBytes: maxDeliveryLokiPushRequestBytes,
		HandlerConfig: internalerror.RoutingConfig{
			OnErrorConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_LOKI_LOGGER_ERROR_HANDLER", nil),
			OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_LOKI_LOGGER_WARN_HANDLER", nil),
			OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_LOKI_LOGGER_INFO_HANDLER", nil),
			OnDebugConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_LOKI_LOGGER_DEBUG_HANDLER", nil),
		},
	}, errors.Join(environmentErrors...)
}

func loadCloudWatchLogsConfig(env pkgenv.Lookup) (logconfig.CloudWatchLogsLoggerBaseConfig, error) {
	var environmentErrors []error
	cloudwatchFlushIntervalSecond, cloudwatchFlushIntervalSecondErr := env.GetEnvDuration("HETEROLOG_CLOUDWATCH_FLUSH_INTERVAL_SECOND", logconfig.DefaultCloudWatchLogsFlushInterval)
	if cloudwatchFlushIntervalSecondErr != nil {
		environmentErrors = append(environmentErrors, cloudwatchFlushIntervalSecondErr)
	}
	cloudwatchBufferFlushMaxEvents, cloudwatchBufferFlushMaxEventsErr := env.GetEnvInt("HETEROLOG_CLOUDWATCH_BUFFER_FLUSH_MAX_EVENTS", logconfig.DefaultCloudWatchLogsBufferFlushMaxEvents)
	if cloudwatchBufferFlushMaxEventsErr != nil {
		environmentErrors = append(environmentErrors, cloudwatchBufferFlushMaxEventsErr)
	}
	cloudwatchBufferFlushBytes, cloudwatchBufferFlushBytesErr := env.GetEnvInt("HETEROLOG_CLOUDWATCH_BUFFER_FLUSH_BYTES", logconfig.DefaultCloudWatchLogsBufferFlushBytes)
	if cloudwatchBufferFlushBytesErr != nil {
		environmentErrors = append(environmentErrors, cloudwatchBufferFlushBytesErr)
	}
	maxCloudwatchFallbackQueueEvents, maxCloudwatchFallbackQueueEventsErr := env.GetEnvInt("HETEROLOG_MAX_CLOUDWATCH_FALLBACK_QUEUE_EVENTS", logconfig.DefaultCloudWatchLogsFallbackQueueMaxEvents)
	if maxCloudwatchFallbackQueueEventsErr != nil {
		environmentErrors = append(environmentErrors, maxCloudwatchFallbackQueueEventsErr)
	}
	enableCloudwatchNoBulkFlushMode, enableCloudwatchNoBulkFlushModeErr := env.GetEnvBool("HETEROLOG_ENABLE_CLOUDWATCH_NO_BULK_FLUSH_MODE", logconfig.DefaultCloudWatchLogsNoBulkFlushModeEnabled)
	if enableCloudwatchNoBulkFlushModeErr != nil {
		environmentErrors = append(environmentErrors, enableCloudwatchNoBulkFlushModeErr)
	}
	enableLogCloudwatchSampling, enableLogCloudwatchSamplingErr := env.GetEnvBool("HETEROLOG_ENABLE_LOG_CLOUDWATCH_SAMPLING", logconfig.DefaultIsLogCloudWatchLogsSamplingEnabled)
	if enableLogCloudwatchSamplingErr != nil {
		environmentErrors = append(environmentErrors, enableLogCloudwatchSamplingErr)
	}
	enableLoggingCloudwatchByRequestId, enableLoggingCloudwatchByRequestIdErr := env.GetEnvBool("HETEROLOG_ENABLE_LOGGING_CLOUDWATCH_BY_REQUEST_ID", logconfig.DefaultEnableCloudWatchLogsLoggingByRequestID)
	if enableLoggingCloudwatchByRequestIdErr != nil {
		environmentErrors = append(environmentErrors, enableLoggingCloudwatchByRequestIdErr)
	}
	logCloudwatchRequestIdRate, logCloudwatchRequestIdRateErr := env.GetEnvInt("HETEROLOG_LOG_CLOUDWATCH_REQUEST_ID_RATE", logconfig.DefaultCloudWatchLogsLoggingByRequestIDRate)
	if logCloudwatchRequestIdRateErr != nil {
		environmentErrors = append(environmentErrors, logCloudwatchRequestIdRateErr)
	}
	logCloudwatchDebugRate, logCloudwatchDebugRateErr := env.GetEnvInt("HETEROLOG_LOG_CLOUDWATCH_DEBUG_RATE", logconfig.DefaultLogCloudWatchLogsSamplingDebugRate)
	if logCloudwatchDebugRateErr != nil {
		environmentErrors = append(environmentErrors, logCloudwatchDebugRateErr)
	}
	logCloudwatchInfoRate, logCloudwatchInfoRateErr := env.GetEnvInt("HETEROLOG_LOG_CLOUDWATCH_INFO_RATE", logconfig.DefaultLogCloudWatchLogsSamplingInfoRate)
	if logCloudwatchInfoRateErr != nil {
		environmentErrors = append(environmentErrors, logCloudwatchInfoRateErr)
	}
	logCloudwatchWarnRate, logCloudwatchWarnRateErr := env.GetEnvInt("HETEROLOG_LOG_CLOUDWATCH_WARN_RATE", logconfig.DefaultLogCloudWatchLogsSamplingWarnRate)
	if logCloudwatchWarnRateErr != nil {
		environmentErrors = append(environmentErrors, logCloudwatchWarnRateErr)
	}
	logCloudwatchErrorRate, logCloudwatchErrorRateErr := env.GetEnvInt("HETEROLOG_LOG_CLOUDWATCH_ERROR_RATE", logconfig.DefaultLogCloudWatchLogsSamplingErrorRate)
	if logCloudwatchErrorRateErr != nil {
		environmentErrors = append(environmentErrors, logCloudwatchErrorRateErr)
	}
	enableCloudwatchPutLatency, enableCloudwatchPutLatencyErr := env.GetEnvBool("HETEROLOG_ENABLE_CLOUDWATCH_PUT_LATENCY", logconfig.DefaultCloudWatchLogsPutLatencyEnabled)
	if enableCloudwatchPutLatencyErr != nil {
		environmentErrors = append(environmentErrors, enableCloudwatchPutLatencyErr)
	}
	clockConfig, clockErr := loadClockConfig(env)
	logLevel, logLevelErr := loadLogLevel(
		env.GetEnvString("HETEROLOG_CLOUDWATCH_LOG_LEVEL", logconfig.DefaultCloudWatchLogsLogLevel.String()),
		"HETEROLOG_CLOUDWATCH_LOG_LEVEL",
		logconfig.DefaultCloudWatchLogsLogLevel,
	)
	environmentErrors = append(environmentErrors, clockErr, logLevelErr)
	return logconfig.CloudWatchLogsLoggerBaseConfig{
		ClockConfig: clockConfig,
		LogLevel:    logLevel,
		LogBodyConfig: logconfig.LogBodyConfig{
			LogLineDateFormat:        logconfig.LogLineDateFormat,
			ErrorTraceDepth:          logconfig.DefaultErrorTraceDepth,
			ErrorTraceDepthDebugMode: logconfig.DefaultErrorTraceDepthDebugMode,
		},
		FlushInterval:          cloudwatchFlushIntervalSecond,
		BufferFlushMaxEvents:   cloudwatchBufferFlushMaxEvents,
		BufferFlushBytes:       cloudwatchBufferFlushBytes,
		FallbackQueueMaxEvents: maxCloudwatchFallbackQueueEvents,
		NoBulkFlushModeEnabled: enableCloudwatchNoBulkFlushMode,
		SamplingConfig: logconfig.SamplingConfig{
			IsSamplingEnabled:        enableLogCloudwatchSampling,
			EnableLoggingByRequestID: enableLoggingCloudwatchByRequestId,
			LoggingByRequestIDRate:   logCloudwatchRequestIdRate,
			SamplingDebugRate:        logCloudwatchDebugRate,
			SamplingInfoRate:         logCloudwatchInfoRate,
			SamplingWarnRate:         logCloudwatchWarnRate,
			SamplingErrorRate:        logCloudwatchErrorRate,
		},
		PostWarnThresholdDuration:  logconfig.PostLogWarnThresholdDuration,
		PostErrorThresholdDuration: logconfig.PostLogErrorThresholdDuration,
		PutLatencyEnabled:          enableCloudwatchPutLatency,
		HandlerConfig: internalerror.RoutingConfig{
			OnErrorConfig: env.GetEnvSliceString("HETEROLOG_CLOUDWATCH_LOGGER_ERROR_HANDLER", nil),
			OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_CLOUDWATCH_LOGGER_WARN_HANDLER", nil),
			OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_CLOUDWATCH_LOGGER_INFO_HANDLER", nil),
			OnDebugConfig: env.GetEnvSliceString("HETEROLOG_CLOUDWATCH_LOGGER_DEBUG_HANDLER", nil),
		},
	}, errors.Join(environmentErrors...)
}

func loadAsyncCloudWatchLogsConfig(env pkgenv.Lookup) (logconfig.CloudWatchLogsLoggerBaseConfig, error) {
	var environmentErrors []error
	asyncCloudwatchFlushIntervalSecond, asyncCloudwatchFlushIntervalSecondErr := env.GetEnvDuration("HETEROLOG_ASYNC_CLOUDWATCH_FLUSH_INTERVAL_SECOND", logconfig.DefaultCloudWatchLogsFlushInterval)
	if asyncCloudwatchFlushIntervalSecondErr != nil {
		environmentErrors = append(environmentErrors, asyncCloudwatchFlushIntervalSecondErr)
	}
	asyncCloudwatchBufferFlushMaxEvents, asyncCloudwatchBufferFlushMaxEventsErr := env.GetEnvInt("HETEROLOG_ASYNC_CLOUDWATCH_BUFFER_FLUSH_MAX_EVENTS", logconfig.DefaultCloudWatchLogsBufferFlushMaxEvents)
	if asyncCloudwatchBufferFlushMaxEventsErr != nil {
		environmentErrors = append(environmentErrors, asyncCloudwatchBufferFlushMaxEventsErr)
	}
	asyncCloudwatchBufferFlushBytes, asyncCloudwatchBufferFlushBytesErr := env.GetEnvInt("HETEROLOG_ASYNC_CLOUDWATCH_BUFFER_FLUSH_BYTES", logconfig.DefaultCloudWatchLogsBufferFlushBytes)
	if asyncCloudwatchBufferFlushBytesErr != nil {
		environmentErrors = append(environmentErrors, asyncCloudwatchBufferFlushBytesErr)
	}
	maxAsyncCloudwatchFallbackQueueEvents, maxAsyncCloudwatchFallbackQueueEventsErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_CLOUDWATCH_FALLBACK_QUEUE_EVENTS", logconfig.DefaultCloudWatchLogsFallbackQueueMaxEvents)
	if maxAsyncCloudwatchFallbackQueueEventsErr != nil {
		environmentErrors = append(environmentErrors, maxAsyncCloudwatchFallbackQueueEventsErr)
	}
	enableAsyncLogCloudwatchSampling, enableAsyncLogCloudwatchSamplingErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_LOG_CLOUDWATCH_SAMPLING", logconfig.DefaultIsLogCloudWatchLogsSamplingEnabled)
	if enableAsyncLogCloudwatchSamplingErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncLogCloudwatchSamplingErr)
	}
	enableAsyncLoggingCloudwatchByRequestId, enableAsyncLoggingCloudwatchByRequestIdErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_LOGGING_CLOUDWATCH_BY_REQUEST_ID", logconfig.DefaultEnableCloudWatchLogsLoggingByRequestID)
	if enableAsyncLoggingCloudwatchByRequestIdErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncLoggingCloudwatchByRequestIdErr)
	}
	asyncLogCloudwatchRequestIdRate, asyncLogCloudwatchRequestIdRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_CLOUDWATCH_REQUEST_ID_RATE", logconfig.DefaultCloudWatchLogsLoggingByRequestIDRate)
	if asyncLogCloudwatchRequestIdRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogCloudwatchRequestIdRateErr)
	}
	asyncLogCloudwatchDebugRate, asyncLogCloudwatchDebugRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_CLOUDWATCH_DEBUG_RATE", logconfig.DefaultLogCloudWatchLogsSamplingDebugRate)
	if asyncLogCloudwatchDebugRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogCloudwatchDebugRateErr)
	}
	asyncLogCloudwatchInfoRate, asyncLogCloudwatchInfoRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_CLOUDWATCH_INFO_RATE", logconfig.DefaultLogCloudWatchLogsSamplingInfoRate)
	if asyncLogCloudwatchInfoRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogCloudwatchInfoRateErr)
	}
	asyncLogCloudwatchWarnRate, asyncLogCloudwatchWarnRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_CLOUDWATCH_WARN_RATE", logconfig.DefaultLogCloudWatchLogsSamplingWarnRate)
	if asyncLogCloudwatchWarnRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogCloudwatchWarnRateErr)
	}
	asyncLogCloudwatchErrorRate, asyncLogCloudwatchErrorRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_CLOUDWATCH_ERROR_RATE", logconfig.DefaultLogCloudWatchLogsSamplingErrorRate)
	if asyncLogCloudwatchErrorRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogCloudwatchErrorRateErr)
	}
	enableAsyncCloudwatchPutLatency, enableAsyncCloudwatchPutLatencyErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_CLOUDWATCH_PUT_LATENCY", logconfig.DefaultCloudWatchLogsPutLatencyEnabled)
	if enableAsyncCloudwatchPutLatencyErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncCloudwatchPutLatencyErr)
	}
	clockConfig, clockErr := loadClockConfig(env)
	logLevel, logLevelErr := loadLogLevel(
		env.GetEnvString("HETEROLOG_ASYNC_CLOUDWATCH_LOG_LEVEL", logconfig.DefaultCloudWatchLogsLogLevel.String()),
		"HETEROLOG_ASYNC_CLOUDWATCH_LOG_LEVEL",
		logconfig.DefaultCloudWatchLogsLogLevel,
	)
	environmentErrors = append(environmentErrors, clockErr, logLevelErr)
	return logconfig.CloudWatchLogsLoggerBaseConfig{
		ClockConfig: clockConfig,
		LogLevel:    logLevel,
		LogBodyConfig: logconfig.LogBodyConfig{
			LogLineDateFormat:        logconfig.LogLineDateFormat,
			ErrorTraceDepth:          logconfig.DefaultErrorTraceDepth,
			ErrorTraceDepthDebugMode: logconfig.DefaultErrorTraceDepthDebugMode,
		},
		FlushInterval:          asyncCloudwatchFlushIntervalSecond,
		BufferFlushMaxEvents:   asyncCloudwatchBufferFlushMaxEvents,
		BufferFlushBytes:       asyncCloudwatchBufferFlushBytes,
		FallbackQueueMaxEvents: maxAsyncCloudwatchFallbackQueueEvents,
		NoBulkFlushModeEnabled: false,
		SamplingConfig: logconfig.SamplingConfig{
			IsSamplingEnabled:        enableAsyncLogCloudwatchSampling,
			EnableLoggingByRequestID: enableAsyncLoggingCloudwatchByRequestId,
			LoggingByRequestIDRate:   asyncLogCloudwatchRequestIdRate,
			SamplingDebugRate:        asyncLogCloudwatchDebugRate,
			SamplingInfoRate:         asyncLogCloudwatchInfoRate,
			SamplingWarnRate:         asyncLogCloudwatchWarnRate,
			SamplingErrorRate:        asyncLogCloudwatchErrorRate,
		},
		PostWarnThresholdDuration:  logconfig.PostLogWarnThresholdDuration,
		PostErrorThresholdDuration: logconfig.PostLogErrorThresholdDuration,
		PutLatencyEnabled:          enableAsyncCloudwatchPutLatency,
		HandlerConfig: internalerror.RoutingConfig{
			OnErrorConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_CLOUDWATCH_LOGGER_ERROR_HANDLER", nil),
			OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_CLOUDWATCH_LOGGER_WARN_HANDLER", nil),
			OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_CLOUDWATCH_LOGGER_INFO_HANDLER", nil),
			OnDebugConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_CLOUDWATCH_LOGGER_DEBUG_HANDLER", nil),
		},
	}, errors.Join(environmentErrors...)
}

func loadDeliveryCloudWatchLogsConfig(env pkgenv.Lookup) (logconfig.DeliveryCloudWatchLogsLoggerBaseConfig, error) {
	clockConfig, err := loadClockConfig(env)
	return logconfig.DeliveryCloudWatchLogsLoggerBaseConfig{
		ClockConfig: clockConfig,
		LogBodyConfig: logconfig.LogBodyConfig{
			LogLineDateFormat:        logconfig.LogLineDateFormat,
			ErrorTraceDepth:          logconfig.DefaultErrorTraceDepth,
			ErrorTraceDepthDebugMode: logconfig.DefaultErrorTraceDepthDebugMode,
		},
		HandlerConfig: internalerror.RoutingConfig{
			OnErrorConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_CLOUDWATCH_LOGGER_ERROR_HANDLER", nil),
			OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_CLOUDWATCH_LOGGER_WARN_HANDLER", nil),
			OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_CLOUDWATCH_LOGGER_INFO_HANDLER", nil),
			OnDebugConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_CLOUDWATCH_LOGGER_DEBUG_HANDLER", nil),
		},
	}, err
}

func loadClickHouseConfig(env pkgenv.Lookup) (logconfig.ClickHouseLoggerBaseConfig, error) {
	var environmentErrors []error
	clickhouseFlushIntervalSecond, clickhouseFlushIntervalSecondErr := env.GetEnvDuration("HETEROLOG_CLICKHOUSE_FLUSH_INTERVAL_SECOND", logconfig.DefaultClickHouseFlushInterval)
	if clickhouseFlushIntervalSecondErr != nil {
		environmentErrors = append(environmentErrors, clickhouseFlushIntervalSecondErr)
	}
	maxClickhouseInsertEntries, maxClickhouseInsertEntriesErr := env.GetEnvInt("HETEROLOG_MAX_CLICKHOUSE_INSERT_ENTRIES", logconfig.DefaultClickHouseMaxEntries)
	if maxClickhouseInsertEntriesErr != nil {
		environmentErrors = append(environmentErrors, maxClickhouseInsertEntriesErr)
	}
	clickhouseBufferFlushBytes, clickhouseBufferFlushBytesErr := env.GetEnvInt("HETEROLOG_CLICKHOUSE_BUFFER_FLUSH_BYTES", logconfig.DefaultClickHouseBufferFlushBytes)
	if clickhouseBufferFlushBytesErr != nil {
		environmentErrors = append(environmentErrors, clickhouseBufferFlushBytesErr)
	}
	clickhouseFallbackQueueMaxEntries, clickhouseFallbackQueueMaxEntriesErr := env.GetEnvInt("HETEROLOG_MAX_CLICKHOUSE_FALLBACK_QUEUE_ENTRIES", logconfig.DefaultClickHouseFallbackQueueMaxEntries)
	if clickhouseFallbackQueueMaxEntriesErr != nil {
		environmentErrors = append(environmentErrors, clickhouseFallbackQueueMaxEntriesErr)
	}
	maxClickhouseInsertRequestBytes, maxClickhouseInsertRequestBytesErr := env.GetEnvInt("HETEROLOG_MAX_CLICKHOUSE_INSERT_REQUEST_BYTES", logconfig.DefaultClickHouseMaxInsertRequestBytes)
	if maxClickhouseInsertRequestBytesErr != nil {
		environmentErrors = append(environmentErrors, maxClickhouseInsertRequestBytesErr)
	}
	enableLogClickhouseSampling, enableLogClickhouseSamplingErr := env.GetEnvBool("HETEROLOG_ENABLE_LOG_CLICKHOUSE_SAMPLING", logconfig.DefaultIsLogClickHouseSamplingEnabled)
	if enableLogClickhouseSamplingErr != nil {
		environmentErrors = append(environmentErrors, enableLogClickhouseSamplingErr)
	}
	enableLoggingClickhouseByRequestId, enableLoggingClickhouseByRequestIdErr := env.GetEnvBool("HETEROLOG_ENABLE_LOGGING_CLICKHOUSE_BY_REQUEST_ID", logconfig.DefaultEnableClickHouseLoggingByRequestID)
	if enableLoggingClickhouseByRequestIdErr != nil {
		environmentErrors = append(environmentErrors, enableLoggingClickhouseByRequestIdErr)
	}
	logClickhouseRequestIdRate, logClickhouseRequestIdRateErr := env.GetEnvInt("HETEROLOG_LOG_CLICKHOUSE_REQUEST_ID_RATE", logconfig.DefaultClickHouseLoggingByRequestIDRate)
	if logClickhouseRequestIdRateErr != nil {
		environmentErrors = append(environmentErrors, logClickhouseRequestIdRateErr)
	}
	logClickhouseDebugRate, logClickhouseDebugRateErr := env.GetEnvInt("HETEROLOG_LOG_CLICKHOUSE_DEBUG_RATE", logconfig.DefaultLogClickHouseSamplingDebugRate)
	if logClickhouseDebugRateErr != nil {
		environmentErrors = append(environmentErrors, logClickhouseDebugRateErr)
	}
	logClickhouseInfoRate, logClickhouseInfoRateErr := env.GetEnvInt("HETEROLOG_LOG_CLICKHOUSE_INFO_RATE", logconfig.DefaultLogClickHouseSamplingInfoRate)
	if logClickhouseInfoRateErr != nil {
		environmentErrors = append(environmentErrors, logClickhouseInfoRateErr)
	}
	logClickhouseWarnRate, logClickhouseWarnRateErr := env.GetEnvInt("HETEROLOG_LOG_CLICKHOUSE_WARN_RATE", logconfig.DefaultLogClickHouseSamplingWarnRate)
	if logClickhouseWarnRateErr != nil {
		environmentErrors = append(environmentErrors, logClickhouseWarnRateErr)
	}
	logClickhouseErrorRate, logClickhouseErrorRateErr := env.GetEnvInt("HETEROLOG_LOG_CLICKHOUSE_ERROR_RATE", logconfig.DefaultLogClickHouseSamplingErrorRate)
	if logClickhouseErrorRateErr != nil {
		environmentErrors = append(environmentErrors, logClickhouseErrorRateErr)
	}
	enableClickhouseInsertLatency, enableClickhouseInsertLatencyErr := env.GetEnvBool("HETEROLOG_ENABLE_CLICKHOUSE_INSERT_LATENCY", logconfig.DefaultClickHouseInsertLatencyEnabled)
	if enableClickhouseInsertLatencyErr != nil {
		environmentErrors = append(environmentErrors, enableClickhouseInsertLatencyErr)
	}
	clockConfig, clockErr := loadClockConfig(env)
	logLevel, logLevelErr := loadLogLevel(
		env.GetEnvString("HETEROLOG_CLICKHOUSE_LOG_LEVEL", logconfig.DefaultClickHouseLogLevel.String()),
		"HETEROLOG_CLICKHOUSE_LOG_LEVEL",
		logconfig.DefaultClickHouseLogLevel,
	)
	environmentErrors = append(environmentErrors, clockErr, logLevelErr)
	return logconfig.ClickHouseLoggerBaseConfig{
		ClockConfig: clockConfig,
		LogLevel:    logLevel,
		LogBodyConfig: logconfig.LogBodyConfig{
			LogLineDateFormat:        logconfig.LogLineDateFormat,
			ErrorTraceDepth:          logconfig.DefaultErrorTraceDepth,
			ErrorTraceDepthDebugMode: logconfig.DefaultErrorTraceDepthDebugMode,
		},
		FlushInterval:           clickhouseFlushIntervalSecond,
		MaxEntries:              maxClickhouseInsertEntries,
		BufferFlushBytes:        clickhouseBufferFlushBytes,
		FallbackQueueMaxEntries: clickhouseFallbackQueueMaxEntries,
		MaxInsertRequestBytes:   maxClickhouseInsertRequestBytes,
		SamplingConfig: logconfig.SamplingConfig{
			IsSamplingEnabled:        enableLogClickhouseSampling,
			EnableLoggingByRequestID: enableLoggingClickhouseByRequestId,
			LoggingByRequestIDRate:   logClickhouseRequestIdRate,
			SamplingDebugRate:        logClickhouseDebugRate,
			SamplingInfoRate:         logClickhouseInfoRate,
			SamplingWarnRate:         logClickhouseWarnRate,
			SamplingErrorRate:        logClickhouseErrorRate,
		},
		PostWarnThresholdDuration:  logconfig.PostLogWarnThresholdDuration,
		PostErrorThresholdDuration: logconfig.PostLogErrorThresholdDuration,
		InsertLatencyEnabled:       enableClickhouseInsertLatency,
		HandlerConfig: internalerror.RoutingConfig{
			OnErrorConfig: env.GetEnvSliceString("HETEROLOG_CLICKHOUSE_LOGGER_ERROR_HANDLER", nil),
			OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_CLICKHOUSE_LOGGER_WARN_HANDLER", nil),
			OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_CLICKHOUSE_LOGGER_INFO_HANDLER", nil),
			OnDebugConfig: env.GetEnvSliceString("HETEROLOG_CLICKHOUSE_LOGGER_DEBUG_HANDLER", nil),
		},
	}, errors.Join(environmentErrors...)
}

func loadAsyncClickHouseConfig(env pkgenv.Lookup) (logconfig.ClickHouseLoggerBaseConfig, error) {
	var environmentErrors []error
	asyncClickhouseFlushIntervalSecond, asyncClickhouseFlushIntervalSecondErr := env.GetEnvDuration("HETEROLOG_ASYNC_CLICKHOUSE_FLUSH_INTERVAL_SECOND", logconfig.DefaultClickHouseFlushInterval)
	if asyncClickhouseFlushIntervalSecondErr != nil {
		environmentErrors = append(environmentErrors, asyncClickhouseFlushIntervalSecondErr)
	}
	maxAsyncClickhouseInsertEntries, maxAsyncClickhouseInsertEntriesErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_CLICKHOUSE_INSERT_ENTRIES", logconfig.DefaultClickHouseMaxEntries)
	if maxAsyncClickhouseInsertEntriesErr != nil {
		environmentErrors = append(environmentErrors, maxAsyncClickhouseInsertEntriesErr)
	}
	asyncClickhouseBufferFlushBytes, asyncClickhouseBufferFlushBytesErr := env.GetEnvInt("HETEROLOG_ASYNC_CLICKHOUSE_BUFFER_FLUSH_BYTES", logconfig.DefaultClickHouseBufferFlushBytes)
	if asyncClickhouseBufferFlushBytesErr != nil {
		environmentErrors = append(environmentErrors, asyncClickhouseBufferFlushBytesErr)
	}
	asyncClickhouseFallbackQueueMaxEntries, asyncClickhouseFallbackQueueMaxEntriesErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_CLICKHOUSE_FALLBACK_QUEUE_ENTRIES", logconfig.DefaultClickHouseFallbackQueueMaxEntries)
	if asyncClickhouseFallbackQueueMaxEntriesErr != nil {
		environmentErrors = append(environmentErrors, asyncClickhouseFallbackQueueMaxEntriesErr)
	}
	maxAsyncClickhouseInsertRequestBytes, maxAsyncClickhouseInsertRequestBytesErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_CLICKHOUSE_INSERT_REQUEST_BYTES", logconfig.DefaultClickHouseMaxInsertRequestBytes)
	if maxAsyncClickhouseInsertRequestBytesErr != nil {
		environmentErrors = append(environmentErrors, maxAsyncClickhouseInsertRequestBytesErr)
	}
	enableAsyncLogClickhouseSampling, enableAsyncLogClickhouseSamplingErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_LOG_CLICKHOUSE_SAMPLING", logconfig.DefaultIsLogClickHouseSamplingEnabled)
	if enableAsyncLogClickhouseSamplingErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncLogClickhouseSamplingErr)
	}
	enableAsyncLoggingClickhouseByRequestId, enableAsyncLoggingClickhouseByRequestIdErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_LOGGING_CLICKHOUSE_BY_REQUEST_ID", logconfig.DefaultEnableClickHouseLoggingByRequestID)
	if enableAsyncLoggingClickhouseByRequestIdErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncLoggingClickhouseByRequestIdErr)
	}
	asyncLogClickhouseRequestIdRate, asyncLogClickhouseRequestIdRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_CLICKHOUSE_REQUEST_ID_RATE", logconfig.DefaultClickHouseLoggingByRequestIDRate)
	if asyncLogClickhouseRequestIdRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogClickhouseRequestIdRateErr)
	}
	asyncLogClickhouseDebugRate, asyncLogClickhouseDebugRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_CLICKHOUSE_DEBUG_RATE", logconfig.DefaultLogClickHouseSamplingDebugRate)
	if asyncLogClickhouseDebugRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogClickhouseDebugRateErr)
	}
	asyncLogClickhouseInfoRate, asyncLogClickhouseInfoRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_CLICKHOUSE_INFO_RATE", logconfig.DefaultLogClickHouseSamplingInfoRate)
	if asyncLogClickhouseInfoRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogClickhouseInfoRateErr)
	}
	asyncLogClickhouseWarnRate, asyncLogClickhouseWarnRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_CLICKHOUSE_WARN_RATE", logconfig.DefaultLogClickHouseSamplingWarnRate)
	if asyncLogClickhouseWarnRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogClickhouseWarnRateErr)
	}
	asyncLogClickhouseErrorRate, asyncLogClickhouseErrorRateErr := env.GetEnvInt("HETEROLOG_ASYNC_LOG_CLICKHOUSE_ERROR_RATE", logconfig.DefaultLogClickHouseSamplingErrorRate)
	if asyncLogClickhouseErrorRateErr != nil {
		environmentErrors = append(environmentErrors, asyncLogClickhouseErrorRateErr)
	}
	enableAsyncClickhouseInsertLatency, enableAsyncClickhouseInsertLatencyErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_CLICKHOUSE_INSERT_LATENCY", logconfig.DefaultClickHouseInsertLatencyEnabled)
	if enableAsyncClickhouseInsertLatencyErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncClickhouseInsertLatencyErr)
	}
	clockConfig, clockErr := loadClockConfig(env)
	logLevel, logLevelErr := loadLogLevel(
		env.GetEnvString("HETEROLOG_ASYNC_CLICKHOUSE_LOG_LEVEL", logconfig.DefaultClickHouseLogLevel.String()),
		"HETEROLOG_ASYNC_CLICKHOUSE_LOG_LEVEL",
		logconfig.DefaultClickHouseLogLevel,
	)
	environmentErrors = append(environmentErrors, clockErr, logLevelErr)
	return logconfig.ClickHouseLoggerBaseConfig{
		ClockConfig: clockConfig,
		LogLevel:    logLevel,
		LogBodyConfig: logconfig.LogBodyConfig{
			LogLineDateFormat:        logconfig.LogLineDateFormat,
			ErrorTraceDepth:          logconfig.DefaultErrorTraceDepth,
			ErrorTraceDepthDebugMode: logconfig.DefaultErrorTraceDepthDebugMode,
		},
		FlushInterval:           asyncClickhouseFlushIntervalSecond,
		MaxEntries:              maxAsyncClickhouseInsertEntries,
		BufferFlushBytes:        asyncClickhouseBufferFlushBytes,
		FallbackQueueMaxEntries: asyncClickhouseFallbackQueueMaxEntries,
		MaxInsertRequestBytes:   maxAsyncClickhouseInsertRequestBytes,
		SamplingConfig: logconfig.SamplingConfig{
			IsSamplingEnabled:        enableAsyncLogClickhouseSampling,
			EnableLoggingByRequestID: enableAsyncLoggingClickhouseByRequestId,
			LoggingByRequestIDRate:   asyncLogClickhouseRequestIdRate,
			SamplingDebugRate:        asyncLogClickhouseDebugRate,
			SamplingInfoRate:         asyncLogClickhouseInfoRate,
			SamplingWarnRate:         asyncLogClickhouseWarnRate,
			SamplingErrorRate:        asyncLogClickhouseErrorRate,
		},
		PostWarnThresholdDuration:  logconfig.PostLogWarnThresholdDuration,
		PostErrorThresholdDuration: logconfig.PostLogErrorThresholdDuration,
		InsertLatencyEnabled:       enableAsyncClickhouseInsertLatency,
		HandlerConfig: internalerror.RoutingConfig{
			OnErrorConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_CLICKHOUSE_LOGGER_ERROR_HANDLER", nil),
			OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_CLICKHOUSE_LOGGER_WARN_HANDLER", nil),
			OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_CLICKHOUSE_LOGGER_INFO_HANDLER", nil),
			OnDebugConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_CLICKHOUSE_LOGGER_DEBUG_HANDLER", nil),
		},
	}, errors.Join(environmentErrors...)
}

func loadDeliveryClickHouseConfig(env pkgenv.Lookup) (logconfig.DeliveryClickHouseLoggerBaseConfig, error) {
	var environmentErrors []error
	deliveryClickhouseFlushIntervalSecond, deliveryClickhouseFlushIntervalSecondErr := env.GetEnvDuration("HETEROLOG_DELIVERY_CLICKHOUSE_FLUSH_INTERVAL_SECOND", logconfig.DefaultClickHouseFlushInterval)
	if deliveryClickhouseFlushIntervalSecondErr != nil {
		environmentErrors = append(environmentErrors, deliveryClickhouseFlushIntervalSecondErr)
	}
	maxDeliveryClickhouseInsertEntries, maxDeliveryClickhouseInsertEntriesErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_CLICKHOUSE_INSERT_ENTRIES", logconfig.DefaultClickHouseMaxEntries)
	if maxDeliveryClickhouseInsertEntriesErr != nil {
		environmentErrors = append(environmentErrors, maxDeliveryClickhouseInsertEntriesErr)
	}
	deliveryClickhouseBufferFlushBytes, deliveryClickhouseBufferFlushBytesErr := env.GetEnvInt("HETEROLOG_DELIVERY_CLICKHOUSE_BUFFER_FLUSH_BYTES", logconfig.DefaultClickHouseBufferFlushBytes)
	if deliveryClickhouseBufferFlushBytesErr != nil {
		environmentErrors = append(environmentErrors, deliveryClickhouseBufferFlushBytesErr)
	}
	deliveryClickhouseFallbackQueueMaxEntries, deliveryClickhouseFallbackQueueMaxEntriesErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_CLICKHOUSE_FALLBACK_QUEUE_ENTRIES", logconfig.DefaultClickHouseFallbackQueueMaxEntries)
	if deliveryClickhouseFallbackQueueMaxEntriesErr != nil {
		environmentErrors = append(environmentErrors, deliveryClickhouseFallbackQueueMaxEntriesErr)
	}
	maxDeliveryClickhouseInsertRequestBytes, maxDeliveryClickhouseInsertRequestBytesErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_CLICKHOUSE_INSERT_REQUEST_BYTES", logconfig.DefaultClickHouseMaxInsertRequestBytes)
	if maxDeliveryClickhouseInsertRequestBytesErr != nil {
		environmentErrors = append(environmentErrors, maxDeliveryClickhouseInsertRequestBytesErr)
	}
	enableDeliveryClickhouseInsertLatency, enableDeliveryClickhouseInsertLatencyErr := env.GetEnvBool("HETEROLOG_ENABLE_DELIVERY_CLICKHOUSE_INSERT_LATENCY", logconfig.DefaultClickHouseInsertLatencyEnabled)
	if enableDeliveryClickhouseInsertLatencyErr != nil {
		environmentErrors = append(environmentErrors, enableDeliveryClickhouseInsertLatencyErr)
	}
	clockConfig, clockErr := loadClockConfig(env)
	environmentErrors = append(environmentErrors, clockErr)
	return logconfig.DeliveryClickHouseLoggerBaseConfig{
		ClockConfig: clockConfig,
		LogBodyConfig: logconfig.LogBodyConfig{
			LogLineDateFormat:        logconfig.LogLineDateFormat,
			ErrorTraceDepth:          logconfig.DefaultErrorTraceDepth,
			ErrorTraceDepthDebugMode: logconfig.DefaultErrorTraceDepthDebugMode,
		},
		FlushInterval:              deliveryClickhouseFlushIntervalSecond,
		MaxEntries:                 maxDeliveryClickhouseInsertEntries,
		BufferFlushBytes:           deliveryClickhouseBufferFlushBytes,
		FallbackQueueMaxEntries:    deliveryClickhouseFallbackQueueMaxEntries,
		MaxInsertRequestBytes:      maxDeliveryClickhouseInsertRequestBytes,
		PostWarnThresholdDuration:  logconfig.PostLogWarnThresholdDuration,
		PostErrorThresholdDuration: logconfig.PostLogErrorThresholdDuration,
		InsertLatencyEnabled:       enableDeliveryClickhouseInsertLatency,
		HandlerConfig: internalerror.RoutingConfig{
			OnErrorConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_CLICKHOUSE_LOGGER_ERROR_HANDLER", nil),
			OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_CLICKHOUSE_LOGGER_WARN_HANDLER", nil),
			OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_CLICKHOUSE_LOGGER_INFO_HANDLER", nil),
			OnDebugConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_CLICKHOUSE_LOGGER_DEBUG_HANDLER", nil),
		},
	}, errors.Join(environmentErrors...)
}

func loadOpenSearchFlushManagerConfig(env pkgenv.Lookup) (logconfig.OpenSearchFlushManagerConfig, error) {
	var environmentErrors []error
	enableOpensearchOtelFormatLog, enableOpensearchOtelFormatLogErr := env.GetEnvBool("HETEROLOG_ENABLE_OPENSEARCH_OTEL_FORMAT_LOG", logconfig.DefaultOTelFormatEnabledForStdout)
	if enableOpensearchOtelFormatLogErr != nil {
		environmentErrors = append(environmentErrors, enableOpensearchOtelFormatLogErr)
	}
	failureHandlerConfig, failureHandlerErr := loadOpenSearchFailureHandlerConfig(env)
	return logconfig.OpenSearchFlushManagerConfig{
		Index:                      env.GetEnvString("HETEROLOG_OPENSEARCH_INDEX", opensearch.DefaultOpenSearchIndex),
		OpenTelemetryFormatEnabled: enableOpensearchOtelFormatLog,
		FailureHandlerConfig:       failureHandlerConfig,
	}, errors.Join(errors.Join(environmentErrors...), failureHandlerErr)
}

func loadOpenSearchFailureHandlerConfig(env pkgenv.Lookup) (failure.FailureHandlerConfig, error) {
	cfg, selectionErr := loadFailureHandlerSelection(
		env,
		"HETEROLOG_ENABLE_OPENSEARCH_DEGRADE", failure.DefaultEnableDegrade,
		"HETEROLOG_MAX_OPENSEARCH_FAILURE_HANDLER_CONCURRENT", failure.DefaultMaxConcurrent,
		"HETEROLOG_OPENSEARCH_DURABLE_TYPES", failure.DefaultDurableTypes,
		"HETEROLOG_OPENSEARCH_DEGRADE_TYPES", failure.DefaultSpillTypes,
	)
	fileArchiveConfig, fileArchiveErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeFsstore),
		func() (failure.FileArchiveConfig, error) {
			var environmentErrors []error
			maxOpensearchDurableFileArchiveSizeMb, maxOpensearchDurableFileArchiveSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_OPENSEARCH_DURABLE_FILE_ARCHIVE_SIZE_MB", failure.DefaultFileArchiveMaxSizeMB, BytesPerMegabyte)
			if maxOpensearchDurableFileArchiveSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxOpensearchDurableFileArchiveSizeMbErr)
			}
			opensearchEnableDurableFileArchiveRotate, opensearchEnableDurableFileArchiveRotateErr := env.GetEnvBool("HETEROLOG_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE", failure.DefaultFileArchiveRotateEnabled)
			if opensearchEnableDurableFileArchiveRotateErr != nil {
				environmentErrors = append(environmentErrors, opensearchEnableDurableFileArchiveRotateErr)
			}
			maxOpensearchEnableDurableFileArchiveBackups, maxOpensearchEnableDurableFileArchiveBackupsErr := env.GetEnvInt("HETEROLOG_MAX_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS", failure.DefaultFileArchiveMaxBackups)
			if maxOpensearchEnableDurableFileArchiveBackupsErr != nil {
				environmentErrors = append(environmentErrors, maxOpensearchEnableDurableFileArchiveBackupsErr)
			}
			maxOpensearchEnableDurableFileArchiveAge, maxOpensearchEnableDurableFileArchiveAgeErr := env.GetEnvInt("HETEROLOG_MAX_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_AGE", failure.DefaultFileArchiveMaxAge)
			if maxOpensearchEnableDurableFileArchiveAgeErr != nil {
				environmentErrors = append(environmentErrors, maxOpensearchEnableDurableFileArchiveAgeErr)
			}
			maxOpensearchEnableDurableFileArchiveDirSizeMb, maxOpensearchEnableDurableFileArchiveDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB", failure.DefaultFileArchiveMaxDirSizeMB, BytesPerMegabyte)
			if maxOpensearchEnableDurableFileArchiveDirSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxOpensearchEnableDurableFileArchiveDirSizeMbErr)
			}
			enableOpensearchDurableFileArchiveCompress, enableOpensearchDurableFileArchiveCompressErr := env.GetEnvBool("HETEROLOG_ENABLE_OPENSEARCH_DURABLE_FILE_ARCHIVE_COMPRESS", failure.DefaultFileArchiveCompressEnabled)
			if enableOpensearchDurableFileArchiveCompressErr != nil {
				environmentErrors = append(environmentErrors, enableOpensearchDurableFileArchiveCompressErr)
			}
			compressFormat, compressFormatErr := loadLogCompressFormat(
				env.GetEnvString("HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT", string(failure.DefaultFileArchiveCompressFormat)),
				"HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT",
				failure.DefaultFileArchiveCompressFormat,
			)
			if compressFormatErr != nil {
				environmentErrors = append(environmentErrors, compressFormatErr)
			}
			objectStorageUploadConfig, objectStorageUploadConfigErr := loadOpenSearchFailureArchiveObjectStorageUploadConfig(env)
			if objectStorageUploadConfigErr != nil {
				environmentErrors = append(environmentErrors, objectStorageUploadConfigErr)
			}
			return failure.FileArchiveConfig{
				MaxFileSize:                maxOpensearchDurableFileArchiveSizeMb,
				LifecycleMode:              logconfig.ParseLogFileLifecycleMode(env.GetEnvString("HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE", string(failure.DefaultFileArchiveLifecycleMode)), failure.DefaultFileArchiveLifecycleMode),
				Dir:                        env.GetEnvString("HETEROLOG_OPENSEARCH_FAILURE_ARCHIVE_DIR", opensearch.DefaultFailureArchiveDir),
				DirCaseRetryable:           env.GetEnvString("HETEROLOG_OPENSEARCH_DURABLE_RETRYABLE_DIR", failure.DirCaseRetryable),
				DirCasePermanent:           env.GetEnvString("HETEROLOG_OPENSEARCH_DURABLE_PERMANENT_DIR", failure.DirCasePermanent),
				EnableRotate:               opensearchEnableDurableFileArchiveRotate,
				MaxFileBackups:             maxOpensearchEnableDurableFileArchiveBackups,
				MaxFileAge:                 maxOpensearchEnableDurableFileArchiveAge,
				MaxDirSize:                 maxOpensearchEnableDurableFileArchiveDirSizeMb,
				EnableFileCompress:         enableOpensearchDurableFileArchiveCompress,
				CompressFormat:             compressFormat,
				FileRotation:               loadLogRotation(env.GetEnvString("HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_ROTATION", string(failure.DefaultFileArchiveRotation)), failure.DefaultFileArchiveRotation),
				FileRotationDayOfWeek:      failure.DefaultFileArchiveRotationDayOfWeek,
				MaxWriteRetries:            fsstoreconfig.MaxFileOperationRetries,
				FileOperationRetryWaitTime: fsstoreconfig.FileOperationRetryWaitTime,
				ObjectStorageUploadConfig:  objectStorageUploadConfig,
				OverloadPolicyConfig: failure.FileArchiveOverloadPolicyConfig{
					OverloadPolicy:   failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY", string(failure.DefaultSpoolOverloadPolicy)), failure.DefaultSpoolOverloadPolicy),
					SpillDestination: failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION", string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
				},
				HandlerConfig: internalerror.RoutingConfig{
					OnErrorConfig: env.GetEnvSliceString("HETEROLOG_OPENSEARCH_DURABLE_FILE_ERROR_HANDLER", nil),
					OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_OPENSEARCH_DURABLE_FILE_WARN_HANDLER", nil),
					OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_OPENSEARCH_DURABLE_FILE_INFO_HANDLER", nil),
					OnDebugConfig: env.GetEnvSliceString("HETEROLOG_OPENSEARCH_DURABLE_FILE_DEBUG_HANDLER", nil),
				},
			}, errors.Join(environmentErrors...)
		},
	)
	spoolConfig, spoolErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeSpool),
		func() (failure.SpoolConfig, error) {
			var environmentErrors []error
			maxOpensearchSpoolScannedCacheUsageCount, maxOpensearchSpoolScannedCacheUsageCountErr := env.GetEnvUint64("HETEROLOG_MAX_OPENSEARCH_SPOOL_SCANNED_CACHE_USAGE_COUNT", failure.DefaultMaxSpoolScannedCacheUsageCount)
			if maxOpensearchSpoolScannedCacheUsageCountErr != nil {
				environmentErrors = append(environmentErrors, maxOpensearchSpoolScannedCacheUsageCountErr)
			}
			opensearchSpoolCacheLifetimeSecond, opensearchSpoolCacheLifetimeSecondErr := env.GetEnvDuration("HETEROLOG_OPENSEARCH_SPOOL_CACHE_LIFETIME_SECOND", failure.DefaultSpoolCacheLifetime)
			if opensearchSpoolCacheLifetimeSecondErr != nil {
				environmentErrors = append(environmentErrors, opensearchSpoolCacheLifetimeSecondErr)
			}
			maxOpensearchSpoolFileActiveDirSizeMb, maxOpensearchSpoolFileActiveDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_OPENSEARCH_SPOOL_FILE_ACTIVE_DIR_SIZE_MB", failure.DefaultSpoolActiveMaxDirSizeMB, BytesPerMegabyte)
			if maxOpensearchSpoolFileActiveDirSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxOpensearchSpoolFileActiveDirSizeMbErr)
			}
			opensearchActiveSpoolBacklogWarnAfterDays, opensearchActiveSpoolBacklogWarnAfterDaysErr := env.GetEnvInt("HETEROLOG_OPENSEARCH_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS", failure.DefaultSpoolActiveBacklogWarnAfterDays)
			if opensearchActiveSpoolBacklogWarnAfterDaysErr != nil {
				environmentErrors = append(environmentErrors, opensearchActiveSpoolBacklogWarnAfterDaysErr)
			}
			useMemoryPressureOpensearchSpoolOverload, useMemoryPressureOpensearchSpoolOverloadErr := env.GetEnvBool("HETEROLOG_USE_MEMORY_PRESSURE_OPENSEARCH_SPOOL_OVERLOAD", failure.DefaultUseMemoryPressureForOverload)
			if useMemoryPressureOpensearchSpoolOverloadErr != nil {
				environmentErrors = append(environmentErrors, useMemoryPressureOpensearchSpoolOverloadErr)
			}
			return failure.SpoolConfig{
				Dir: env.GetEnvString("HETEROLOG_OPENSEARCH_FAILURE_SPOOL_DIR", opensearch.DefaultFailureSpoolDir),
				OverloadPolicyConfig: failure.SpoolOverloadPolicyConfig{
					OverloadPolicy:               failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_OPENSEARCH_SPOOL_OVERLOAD_POLICY", string(failure.DefaultSpoolOverloadPolicy)), failure.DefaultSpoolOverloadPolicy),
					SpillDestination:             failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_OPENSEARCH_SPOOL_SECONDARY_DESTINATION", string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
					MaxCallsSinceScan:            maxOpensearchSpoolScannedCacheUsageCount,
					StatsScanInterval:            opensearchSpoolCacheLifetimeSecond,
					MaxDirSize:                   maxOpensearchSpoolFileActiveDirSizeMb,
					ActiveBacklogWarnAfterDays:   opensearchActiveSpoolBacklogWarnAfterDays,
					UseMemoryPressureForOverload: useMemoryPressureOpensearchSpoolOverload,
				},
				HandlerConfig: internalerror.RoutingConfig{
					OnErrorConfig: env.GetEnvSliceString("HETEROLOG_OPENSEARCH_SPOOL_FILE_ERROR_HANDLER", nil),
					OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_OPENSEARCH_SPOOL_FILE_WARN_HANDLER", nil),
					OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_OPENSEARCH_SPOOL_FILE_INFO_HANDLER", nil),
					OnDebugConfig: env.GetEnvSliceString("HETEROLOG_OPENSEARCH_SPOOL_FILE_DEBUG_HANDLER", nil),
				},
			}, errors.Join(environmentErrors...)
		},
	)
	cfg.Fsstore = failure.FailureFileSystemStoreConfig{
		FileArchiveConfig: fileArchiveConfig,
		SpoolConfig:       spoolConfig,
	}
	return cfg, errors.Join(selectionErr, fileArchiveErr, spoolErr)
}

func loadAsyncOpenSearchFlushManagerConfig(env pkgenv.Lookup) (logconfig.OpenSearchFlushManagerConfig, error) {
	var environmentErrors []error
	enableAsyncOpensearchOtelFormatLog, enableAsyncOpensearchOtelFormatLogErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_OPENSEARCH_OTEL_FORMAT_LOG", logconfig.DefaultOTelFormatEnabledForStdout)
	if enableAsyncOpensearchOtelFormatLogErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncOpensearchOtelFormatLogErr)
	}
	failureHandlerConfig, failureHandlerErr := loadAsyncOpenSearchFailureHandlerConfig(env)
	return logconfig.OpenSearchFlushManagerConfig{
		Index:                      env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_INDEX", opensearch.DefaultOpenSearchIndex),
		OpenTelemetryFormatEnabled: enableAsyncOpensearchOtelFormatLog,
		FailureHandlerConfig:       failureHandlerConfig,
	}, errors.Join(errors.Join(environmentErrors...), failureHandlerErr)
}

func loadAsyncOpenSearchFailureHandlerConfig(env pkgenv.Lookup) (failure.FailureHandlerConfig, error) {
	cfg, selectionErr := loadFailureHandlerSelection(
		env,
		"HETEROLOG_ENABLE_ASYNC_OPENSEARCH_DEGRADE", failure.DefaultAsyncEnableDegrade,
		"HETEROLOG_MAX_ASYNC_OPENSEARCH_FAILURE_HANDLER_CONCURRENT", failure.DefaultMaxAsyncConcurrent,
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_TYPES", failure.DefaultAsyncDurableTypes,
		"HETEROLOG_ASYNC_OPENSEARCH_DEGRADE_TYPES", failure.DefaultAsyncSpillTypes,
	)
	fileArchiveConfig, fileArchiveErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeFsstore),
		func() (failure.FileArchiveConfig, error) {
			var environmentErrors []error
			maxAsyncOpensearchDurableFileArchiveSizeMb, maxAsyncOpensearchDurableFileArchiveSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_SIZE_MB", failure.DefaultFileArchiveMaxSizeMB, BytesPerMegabyte)
			if maxAsyncOpensearchDurableFileArchiveSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncOpensearchDurableFileArchiveSizeMbErr)
			}
			asyncOpensearchEnableDurableFileArchiveRotate, asyncOpensearchEnableDurableFileArchiveRotateErr := env.GetEnvBool("HETEROLOG_ASYNC_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE", failure.DefaultAsyncFileArchiveRotateEnabled)
			if asyncOpensearchEnableDurableFileArchiveRotateErr != nil {
				environmentErrors = append(environmentErrors, asyncOpensearchEnableDurableFileArchiveRotateErr)
			}
			maxAsyncOpensearchEnableDurableFileArchiveBackups, maxAsyncOpensearchEnableDurableFileArchiveBackupsErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS", failure.DefaultAsyncFileArchiveMaxBackups)
			if maxAsyncOpensearchEnableDurableFileArchiveBackupsErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncOpensearchEnableDurableFileArchiveBackupsErr)
			}
			maxAsyncOpensearchEnableDurableFileArchiveAge, maxAsyncOpensearchEnableDurableFileArchiveAgeErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_AGE", failure.DefaultAsyncFileArchiveMaxAge)
			if maxAsyncOpensearchEnableDurableFileArchiveAgeErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncOpensearchEnableDurableFileArchiveAgeErr)
			}
			maxAsyncOpensearchEnableDurableFileArchiveDirSizeMb, maxAsyncOpensearchEnableDurableFileArchiveDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_ASYNC_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB", failure.DefaultAsyncFileArchiveMaxDirSizeMB, BytesPerMegabyte)
			if maxAsyncOpensearchEnableDurableFileArchiveDirSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncOpensearchEnableDurableFileArchiveDirSizeMbErr)
			}
			enableAsyncOpensearchDurableFileArchiveCompress, enableAsyncOpensearchDurableFileArchiveCompressErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_COMPRESS", failure.DefaultAsyncFileArchiveCompressEnabled)
			if enableAsyncOpensearchDurableFileArchiveCompressErr != nil {
				environmentErrors = append(environmentErrors, enableAsyncOpensearchDurableFileArchiveCompressErr)
			}
			compressFormat, compressFormatErr := loadLogCompressFormat(
				env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT", string(failure.DefaultAsyncFileArchiveCompressFormat)),
				"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT",
				failure.DefaultAsyncFileArchiveCompressFormat,
			)
			if compressFormatErr != nil {
				environmentErrors = append(environmentErrors, compressFormatErr)
			}
			objectStorageUploadConfig, objectStorageUploadConfigErr := loadAsyncOpenSearchFailureArchiveObjectStorageUploadConfig(env)
			if objectStorageUploadConfigErr != nil {
				environmentErrors = append(environmentErrors, objectStorageUploadConfigErr)
			}
			return failure.FileArchiveConfig{
				MaxFileSize:                maxAsyncOpensearchDurableFileArchiveSizeMb,
				LifecycleMode:              logconfig.ParseLogFileLifecycleMode(env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE", string(failure.DefaultFileArchiveLifecycleMode)), failure.DefaultFileArchiveLifecycleMode),
				Dir:                        env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_FAILURE_ARCHIVE_DIR", opensearch.DefaultAsyncFailureArchiveDir),
				DirCaseRetryable:           env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_RETRYABLE_DIR", failure.DirCaseRetryable),
				DirCasePermanent:           env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_PERMANENT_DIR", failure.DirCasePermanent),
				EnableRotate:               asyncOpensearchEnableDurableFileArchiveRotate,
				MaxFileBackups:             maxAsyncOpensearchEnableDurableFileArchiveBackups,
				MaxFileAge:                 maxAsyncOpensearchEnableDurableFileArchiveAge,
				MaxDirSize:                 maxAsyncOpensearchEnableDurableFileArchiveDirSizeMb,
				EnableFileCompress:         enableAsyncOpensearchDurableFileArchiveCompress,
				CompressFormat:             compressFormat,
				FileRotation:               loadLogRotation(env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_ROTATION", string(failure.DefaultAsyncFileArchiveRotation)), failure.DefaultAsyncFileArchiveRotation),
				FileRotationDayOfWeek:      failure.DefaultAsyncFileArchiveRotationDayOfWeek,
				MaxWriteRetries:            fsstoreconfig.MaxFileOperationRetries,
				FileOperationRetryWaitTime: fsstoreconfig.FileOperationRetryWaitTime,
				ObjectStorageUploadConfig:  objectStorageUploadConfig,
				OverloadPolicyConfig: failure.FileArchiveOverloadPolicyConfig{
					OverloadPolicy:   failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY", string(failure.DefaultAsyncSpoolOverloadPolicy)), failure.DefaultAsyncSpoolOverloadPolicy),
					SpillDestination: failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION", string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
				},
				HandlerConfig: internalerror.RoutingConfig{
					OnErrorConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ERROR_HANDLER", nil),
					OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_WARN_HANDLER", nil),
					OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_INFO_HANDLER", nil),
					OnDebugConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_DEBUG_HANDLER", nil),
				},
			}, errors.Join(environmentErrors...)
		},
	)
	spoolConfig, spoolErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeSpool),
		func() (failure.SpoolConfig, error) {
			var environmentErrors []error
			maxAsyncOpensearchSpoolScannedCacheUsageCount, maxAsyncOpensearchSpoolScannedCacheUsageCountErr := env.GetEnvUint64("HETEROLOG_MAX_ASYNC_OPENSEARCH_SPOOL_SCANNED_CACHE_USAGE_COUNT", failure.DefaultMaxSpoolScannedCacheUsageCount)
			if maxAsyncOpensearchSpoolScannedCacheUsageCountErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncOpensearchSpoolScannedCacheUsageCountErr)
			}
			asyncOpensearchSpoolCacheLifetimeSecond, asyncOpensearchSpoolCacheLifetimeSecondErr := env.GetEnvDuration("HETEROLOG_ASYNC_OPENSEARCH_SPOOL_CACHE_LIFETIME_SECOND", failure.DefaultSpoolCacheLifetime)
			if asyncOpensearchSpoolCacheLifetimeSecondErr != nil {
				environmentErrors = append(environmentErrors, asyncOpensearchSpoolCacheLifetimeSecondErr)
			}
			maxAsyncOpensearchSpoolFileActiveDirSizeMb, maxAsyncOpensearchSpoolFileActiveDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_ASYNC_OPENSEARCH_SPOOL_FILE_ACTIVE_DIR_SIZE_MB", failure.DefaultAsyncSpoolActiveMaxDirSizeMB, BytesPerMegabyte)
			if maxAsyncOpensearchSpoolFileActiveDirSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncOpensearchSpoolFileActiveDirSizeMbErr)
			}
			asyncOpensearchActiveSpoolBacklogWarnAfterDays, asyncOpensearchActiveSpoolBacklogWarnAfterDaysErr := env.GetEnvInt("HETEROLOG_ASYNC_OPENSEARCH_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS", failure.DefaultAsyncSpoolActiveBacklogWarnAfterDays)
			if asyncOpensearchActiveSpoolBacklogWarnAfterDaysErr != nil {
				environmentErrors = append(environmentErrors, asyncOpensearchActiveSpoolBacklogWarnAfterDaysErr)
			}
			useMemoryPressureAsyncOpensearchSpoolOverload, useMemoryPressureAsyncOpensearchSpoolOverloadErr := env.GetEnvBool("HETEROLOG_USE_MEMORY_PRESSURE_ASYNC_OPENSEARCH_SPOOL_OVERLOAD", failure.DefaultUseMemoryPressureForOverload)
			if useMemoryPressureAsyncOpensearchSpoolOverloadErr != nil {
				environmentErrors = append(environmentErrors, useMemoryPressureAsyncOpensearchSpoolOverloadErr)
			}
			return failure.SpoolConfig{
				Dir: env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_FAILURE_SPOOL_DIR", opensearch.DefaultAsyncFailureSpoolDir),
				OverloadPolicyConfig: failure.SpoolOverloadPolicyConfig{
					OverloadPolicy:               failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_SPOOL_OVERLOAD_POLICY", string(failure.DefaultAsyncSpoolOverloadPolicy)), failure.DefaultAsyncSpoolOverloadPolicy),
					SpillDestination:             failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_SPOOL_SECONDARY_DESTINATION", string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
					MaxCallsSinceScan:            maxAsyncOpensearchSpoolScannedCacheUsageCount,
					StatsScanInterval:            asyncOpensearchSpoolCacheLifetimeSecond,
					MaxDirSize:                   maxAsyncOpensearchSpoolFileActiveDirSizeMb,
					ActiveBacklogWarnAfterDays:   asyncOpensearchActiveSpoolBacklogWarnAfterDays,
					UseMemoryPressureForOverload: useMemoryPressureAsyncOpensearchSpoolOverload,
				},
				HandlerConfig: internalerror.RoutingConfig{
					OnErrorConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_OPENSEARCH_SPOOL_FILE_ERROR_HANDLER", nil),
					OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_OPENSEARCH_SPOOL_FILE_WARN_HANDLER", nil),
					OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_OPENSEARCH_SPOOL_FILE_INFO_HANDLER", nil),
					OnDebugConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_OPENSEARCH_SPOOL_FILE_DEBUG_HANDLER", nil),
				},
			}, errors.Join(environmentErrors...)
		},
	)
	cfg.Fsstore = failure.FailureFileSystemStoreConfig{
		FileArchiveConfig: fileArchiveConfig,
		SpoolConfig:       spoolConfig,
	}
	return cfg, errors.Join(selectionErr, fileArchiveErr, spoolErr)
}

func loadDeliveryOpenSearchFlushManagerConfig(env pkgenv.Lookup) (logconfig.DeliveryOpenSearchFlushManagerConfig, error) {
	var environmentErrors []error
	enableDeliveryOpensearchOtelFormatLog, enableDeliveryOpensearchOtelFormatLogErr := env.GetEnvBool("HETEROLOG_ENABLE_DELIVERY_OPENSEARCH_OTEL_FORMAT_LOG", logconfig.DefaultOTelFormatEnabledForStdout)
	if enableDeliveryOpensearchOtelFormatLogErr != nil {
		environmentErrors = append(environmentErrors, enableDeliveryOpensearchOtelFormatLogErr)
	}
	enableDeliveryIndexOpensearchLatency, enableDeliveryIndexOpensearchLatencyErr := env.GetEnvBool("HETEROLOG_ENABLE_DELIVERY_INDEX_OPENSEARCH_LATENCY", logconfig.DefaultIndexLatencyEnabled)
	if enableDeliveryIndexOpensearchLatencyErr != nil {
		environmentErrors = append(environmentErrors, enableDeliveryIndexOpensearchLatencyErr)
	}
	failureHandlerConfig, failureHandlerErr := loadDeliveryOpenSearchFailureHandlerConfig(env)
	return logconfig.DeliveryOpenSearchFlushManagerConfig{
		Index:                      env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_INDEX", opensearch.DefaultOpenSearchIndex),
		OpenTelemetryFormatEnabled: enableDeliveryOpensearchOtelFormatLog,
		FailureHandlerConfig:       failureHandlerConfig,
		PostWarnThresholdDuration:  logconfig.PostLogWarnThresholdDuration,
		PostErrorThresholdDuration: logconfig.PostLogErrorThresholdDuration,
		IndexLatencyEnabled:        enableDeliveryIndexOpensearchLatency,
	}, errors.Join(errors.Join(environmentErrors...), failureHandlerErr)
}

func loadDeliveryOpenSearchFailureHandlerConfig(env pkgenv.Lookup) (failure.DeliveryFailureHandlerConfig, error) {
	cfg, selectionErr := loadDeliveryFailureHandlerSelection(
		env,
		"HETEROLOG_ENABLE_DELIVERY_OPENSEARCH_DEGRADE", failure.DefaultEnableDegrade,
		"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_TYPES", failure.DefaultDeliveryDurableTypes,
		"HETEROLOG_DELIVERY_OPENSEARCH_DEGRADE_TYPES", failure.DefaultSpillTypes,
	)
	fileArchiveConfig, fileArchiveErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeFsstore),
		func() (failure.DeliveryFileArchiveConfig, error) {
			var environmentErrors []error
			maxDeliveryOpensearchDurableFileArchiveSizeMb, maxDeliveryOpensearchDurableFileArchiveSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_SIZE_MB", failure.DefaultFileArchiveMaxSizeMB, BytesPerMegabyte)
			if maxDeliveryOpensearchDurableFileArchiveSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxDeliveryOpensearchDurableFileArchiveSizeMbErr)
			}
			deliveryOpensearchEnableDurableFileArchiveRotate, deliveryOpensearchEnableDurableFileArchiveRotateErr := env.GetEnvBool("HETEROLOG_DELIVERY_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE", failure.DefaultFileArchiveRotateEnabled)
			if deliveryOpensearchEnableDurableFileArchiveRotateErr != nil {
				environmentErrors = append(environmentErrors, deliveryOpensearchEnableDurableFileArchiveRotateErr)
			}
			maxDeliveryOpensearchEnableDurableFileArchiveBackups, maxDeliveryOpensearchEnableDurableFileArchiveBackupsErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS", failure.DefaultFileArchiveMaxBackups)
			if maxDeliveryOpensearchEnableDurableFileArchiveBackupsErr != nil {
				environmentErrors = append(environmentErrors, maxDeliveryOpensearchEnableDurableFileArchiveBackupsErr)
			}
			maxDeliveryOpensearchEnableDurableFileArchiveAge, maxDeliveryOpensearchEnableDurableFileArchiveAgeErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_AGE", failure.DefaultFileArchiveMaxAge)
			if maxDeliveryOpensearchEnableDurableFileArchiveAgeErr != nil {
				environmentErrors = append(environmentErrors, maxDeliveryOpensearchEnableDurableFileArchiveAgeErr)
			}
			maxDeliveryOpensearchEnableDurableFileArchiveDirSizeMb, maxDeliveryOpensearchEnableDurableFileArchiveDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_DELIVERY_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB", failure.DefaultFileArchiveMaxDirSizeMB, BytesPerMegabyte)
			if maxDeliveryOpensearchEnableDurableFileArchiveDirSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxDeliveryOpensearchEnableDurableFileArchiveDirSizeMbErr)
			}
			enableDeliveryOpensearchDurableFileArchiveCompress, enableDeliveryOpensearchDurableFileArchiveCompressErr := env.GetEnvBool("HETEROLOG_ENABLE_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_COMPRESS", failure.DefaultFileArchiveCompressEnabled)
			if enableDeliveryOpensearchDurableFileArchiveCompressErr != nil {
				environmentErrors = append(environmentErrors, enableDeliveryOpensearchDurableFileArchiveCompressErr)
			}
			compressFormat, compressFormatErr := loadLogCompressFormat(
				env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT", string(failure.DefaultFileArchiveCompressFormat)),
				"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT",
				failure.DefaultFileArchiveCompressFormat,
			)
			if compressFormatErr != nil {
				environmentErrors = append(environmentErrors, compressFormatErr)
			}
			objectStorageUploadConfig, objectStorageUploadConfigErr := loadDeliveryOpenSearchFailureArchiveObjectStorageUploadConfig(env)
			if objectStorageUploadConfigErr != nil {
				environmentErrors = append(environmentErrors, objectStorageUploadConfigErr)
			}
			return failure.DeliveryFileArchiveConfig{
				MaxFileSize:                maxDeliveryOpensearchDurableFileArchiveSizeMb,
				LifecycleMode:              logconfig.ParseLogFileLifecycleMode(env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE", string(failure.DefaultFileArchiveLifecycleMode)), failure.DefaultFileArchiveLifecycleMode),
				Dir:                        env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_FAILURE_ARCHIVE_DIR", opensearch.DefaultDeliveryFailureArchiveDir),
				DirCaseRetryable:           env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_RETRYABLE_DIR", failure.DirCaseRetryable),
				DirCasePermanent:           env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_PERMANENT_DIR", failure.DirCasePermanent),
				EnableRotate:               deliveryOpensearchEnableDurableFileArchiveRotate,
				MaxFileBackups:             maxDeliveryOpensearchEnableDurableFileArchiveBackups,
				MaxFileAge:                 maxDeliveryOpensearchEnableDurableFileArchiveAge,
				MaxDirSize:                 maxDeliveryOpensearchEnableDurableFileArchiveDirSizeMb,
				EnableFileCompress:         enableDeliveryOpensearchDurableFileArchiveCompress,
				CompressFormat:             compressFormat,
				FileRotation:               loadLogRotation(env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_ROTATION", string(failure.DefaultFileArchiveRotation)), failure.DefaultFileArchiveRotation),
				FileRotationDayOfWeek:      failure.DefaultFileArchiveRotationDayOfWeek,
				MaxWriteRetries:            fsstoreconfig.MaxFileOperationRetries,
				FileOperationRetryWaitTime: fsstoreconfig.FileOperationRetryWaitTime,
				ObjectStorageUploadConfig:  objectStorageUploadConfig,
				OverloadPolicyConfig: failure.FileArchiveOverloadPolicyConfig{
					OverloadPolicy:   failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY", string(failure.OverloadPolicyDrop)), failure.OverloadPolicyDrop),
					SpillDestination: failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION", string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
				},
				HandlerConfig: internalerror.RoutingConfig{
					OnErrorConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ERROR_HANDLER", nil),
					OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_WARN_HANDLER", nil),
					OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_INFO_HANDLER", nil),
					OnDebugConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_DEBUG_HANDLER", nil),
				},
			}, errors.Join(environmentErrors...)
		},
	)
	cfg.FileArchiveConfig = fileArchiveConfig
	return cfg, errors.Join(selectionErr, fileArchiveErr)
}

func loadOpenSearchFailureArchiveObjectStorageUploadConfig(env pkgenv.Lookup) (objectstorage.UploadTargetConfig, error) {
	return loadFailureArchiveObjectStorageUploadConfig(
		env,
		"HETEROLOG_ENABLE_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD",
		"HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE",
		"HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET",
		"HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX",
		"HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_DESTINATIONS",
	)
}

func loadAsyncOpenSearchFailureArchiveObjectStorageUploadConfig(env pkgenv.Lookup) (objectstorage.UploadTargetConfig, error) {
	return loadFailureArchiveObjectStorageUploadConfig(
		env,
		"HETEROLOG_ENABLE_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD",
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE",
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET",
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX",
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_DESTINATIONS",
	)
}

func loadDeliveryOpenSearchFailureArchiveObjectStorageUploadConfig(env pkgenv.Lookup) (objectstorage.UploadTargetConfig, error) {
	return loadFailureArchiveObjectStorageUploadConfig(
		env,
		"HETEROLOG_ENABLE_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD",
		"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE",
		"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET",
		"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX",
		"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_DESTINATIONS",
	)
}

func loadFailureArchiveObjectStorageUploadConfig(
	env pkgenv.Lookup,
	enabledKey string,
	targetsKey string,
	bucketKey string,
	prefixKey string,
	destinationsKey string,
) (objectstorage.UploadTargetConfig, error) {
	enabled, enabledErr := env.GetEnvBool(enabledKey, objectstorages3cfg.DefaultUploadEnabled)
	var targets []objectstorage.UploadTarget
	var s3Cfg objectstorage.S3Config
	var s3Err error
	if enabled {
		targets = objectStorageUploadTargets(env.GetEnvSliceString(targetsKey, nil))
		s3Cfg, s3Err = loadCommonObjectStorageS3Config(env)
	}

	s3Cfg.Bucket = env.GetEnvString(bucketKey, objectstorages3cfg.DefaultBucket)
	s3Cfg.Prefix = env.GetEnvString(prefixKey, objectstorages3cfg.DefaultPrefix)
	cfg := objectstorage.UploadTargetConfig{
		Enabled: enabled,
		Targets: targets,
		S3:      s3Cfg,
	}
	var destinationsErr error
	if enabled && cfg.Has(objectstorage.UploadTargetS3) {
		cfg.S3Destinations, destinationsErr = parseS3Destinations(env.GetEnvString(destinationsKey, ""), s3Cfg)
		if destinationsErr != nil {
			// 一覧には認証情報が含まれ得るため、Valueには値を保持しません。
			destinationsErr = newInvalidEnvironmentValueError(
				destinationsKey,
				"",
				destinationsErr,
				"%s: %v",
				destinationsKey,
				destinationsErr,
			)
		}
	}
	return cfg, errors.Join(enabledErr, s3Err, destinationsErr)
}

func loadLokiFlushManagerConfig(env pkgenv.Lookup) (logconfig.LokiFlushManagerConfig, error) {
	failureHandlerConfig, err := loadLokiFailureHandlerConfig(env)
	return logconfig.LokiFlushManagerConfig{
		FailureHandlerConfig: failureHandlerConfig,
	}, err
}

func loadLokiFailureHandlerConfig(env pkgenv.Lookup) (failure.FailureHandlerConfig, error) {
	cfg, selectionErr := loadFailureHandlerSelection(
		env,
		"HETEROLOG_ENABLE_LOKI_DEGRADE", failure.DefaultEnableDegrade,
		"HETEROLOG_MAX_LOKI_FAILURE_HANDLER_CONCURRENT", failure.DefaultMaxConcurrent,
		"HETEROLOG_LOKI_DURABLE_TYPES", failure.DefaultDurableTypes,
		"HETEROLOG_LOKI_DEGRADE_TYPES", failure.DefaultSpillTypes,
	)
	fileArchiveConfig, fileArchiveErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeFsstore),
		func() (failure.FileArchiveConfig, error) {
			var environmentErrors []error
			maxLokiDurableFileArchiveSizeMb, maxLokiDurableFileArchiveSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_LOKI_DURABLE_FILE_ARCHIVE_SIZE_MB", failure.DefaultFileArchiveMaxSizeMB, BytesPerMegabyte)
			if maxLokiDurableFileArchiveSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxLokiDurableFileArchiveSizeMbErr)
			}
			lokiEnableDurableFileArchiveRotate, lokiEnableDurableFileArchiveRotateErr := env.GetEnvBool("HETEROLOG_LOKI_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE", failure.DefaultFileArchiveRotateEnabled)
			if lokiEnableDurableFileArchiveRotateErr != nil {
				environmentErrors = append(environmentErrors, lokiEnableDurableFileArchiveRotateErr)
			}
			maxLokiEnableDurableFileArchiveBackups, maxLokiEnableDurableFileArchiveBackupsErr := env.GetEnvInt("HETEROLOG_MAX_LOKI_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS", failure.DefaultFileArchiveMaxBackups)
			if maxLokiEnableDurableFileArchiveBackupsErr != nil {
				environmentErrors = append(environmentErrors, maxLokiEnableDurableFileArchiveBackupsErr)
			}
			maxLokiEnableDurableFileArchiveAge, maxLokiEnableDurableFileArchiveAgeErr := env.GetEnvInt("HETEROLOG_MAX_LOKI_ENABLE_DURABLE_FILE_ARCHIVE_AGE", failure.DefaultFileArchiveMaxAge)
			if maxLokiEnableDurableFileArchiveAgeErr != nil {
				environmentErrors = append(environmentErrors, maxLokiEnableDurableFileArchiveAgeErr)
			}
			maxLokiEnableDurableFileArchiveDirSizeMb, maxLokiEnableDurableFileArchiveDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_LOKI_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB", failure.DefaultFileArchiveMaxDirSizeMB, BytesPerMegabyte)
			if maxLokiEnableDurableFileArchiveDirSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxLokiEnableDurableFileArchiveDirSizeMbErr)
			}
			enableLokiDurableFileArchiveCompress, enableLokiDurableFileArchiveCompressErr := env.GetEnvBool("HETEROLOG_ENABLE_LOKI_DURABLE_FILE_ARCHIVE_COMPRESS", failure.DefaultFileArchiveCompressEnabled)
			if enableLokiDurableFileArchiveCompressErr != nil {
				environmentErrors = append(environmentErrors, enableLokiDurableFileArchiveCompressErr)
			}
			compressFormat, compressFormatErr := loadLogCompressFormat(
				env.GetEnvString("HETEROLOG_LOKI_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT", string(failure.DefaultFileArchiveCompressFormat)),
				"HETEROLOG_LOKI_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT",
				failure.DefaultFileArchiveCompressFormat,
			)
			if compressFormatErr != nil {
				environmentErrors = append(environmentErrors, compressFormatErr)
			}
			objectStorageUploadConfig, objectStorageUploadConfigErr := loadLokiFailureArchiveObjectStorageUploadConfig(env)
			if objectStorageUploadConfigErr != nil {
				environmentErrors = append(environmentErrors, objectStorageUploadConfigErr)
			}
			return failure.FileArchiveConfig{
				MaxFileSize:                maxLokiDurableFileArchiveSizeMb,
				LifecycleMode:              logconfig.ParseLogFileLifecycleMode(env.GetEnvString("HETEROLOG_LOKI_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE", string(failure.DefaultFileArchiveLifecycleMode)), failure.DefaultFileArchiveLifecycleMode),
				Dir:                        env.GetEnvString("HETEROLOG_LOKI_FAILURE_ARCHIVE_DIR", lokicfg.DefaultLokiFailureArchiveDir),
				DirCaseRetryable:           env.GetEnvString("HETEROLOG_LOKI_DURABLE_RETRYABLE_DIR", failure.DirCaseRetryable),
				DirCasePermanent:           env.GetEnvString("HETEROLOG_LOKI_DURABLE_PERMANENT_DIR", failure.DirCasePermanent),
				EnableRotate:               lokiEnableDurableFileArchiveRotate,
				MaxFileBackups:             maxLokiEnableDurableFileArchiveBackups,
				MaxFileAge:                 maxLokiEnableDurableFileArchiveAge,
				MaxDirSize:                 maxLokiEnableDurableFileArchiveDirSizeMb,
				EnableFileCompress:         enableLokiDurableFileArchiveCompress,
				CompressFormat:             compressFormat,
				FileRotation:               loadLogRotation(env.GetEnvString("HETEROLOG_LOKI_DURABLE_FILE_ARCHIVE_ROTATION", string(failure.DefaultFileArchiveRotation)), failure.DefaultFileArchiveRotation),
				FileRotationDayOfWeek:      failure.DefaultFileArchiveRotationDayOfWeek,
				MaxWriteRetries:            fsstoreconfig.MaxFileOperationRetries,
				FileOperationRetryWaitTime: fsstoreconfig.FileOperationRetryWaitTime,
				ObjectStorageUploadConfig:  objectStorageUploadConfig,
				OverloadPolicyConfig: failure.FileArchiveOverloadPolicyConfig{
					OverloadPolicy:   failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_LOKI_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY", string(failure.OverloadPolicyDrop)), failure.OverloadPolicyDrop),
					SpillDestination: failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_LOKI_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION", string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
				},
				HandlerConfig: internalerror.RoutingConfig{
					OnErrorConfig: env.GetEnvSliceString("HETEROLOG_LOKI_DURABLE_FILE_ERROR_HANDLER", nil),
					OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_LOKI_DURABLE_FILE_WARN_HANDLER", nil),
					OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_LOKI_DURABLE_FILE_INFO_HANDLER", nil),
					OnDebugConfig: env.GetEnvSliceString("HETEROLOG_LOKI_DURABLE_FILE_DEBUG_HANDLER", nil),
				},
			}, errors.Join(environmentErrors...)
		},
	)
	spoolConfig, spoolErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeSpool),
		func() (failure.SpoolConfig, error) {
			var environmentErrors []error
			maxLokiSpoolScannedCacheUsageCount, maxLokiSpoolScannedCacheUsageCountErr := env.GetEnvUint64("HETEROLOG_MAX_LOKI_SPOOL_SCANNED_CACHE_USAGE_COUNT", failure.DefaultMaxSpoolScannedCacheUsageCount)
			if maxLokiSpoolScannedCacheUsageCountErr != nil {
				environmentErrors = append(environmentErrors, maxLokiSpoolScannedCacheUsageCountErr)
			}
			lokiSpoolCacheLifetimeSecond, lokiSpoolCacheLifetimeSecondErr := env.GetEnvDuration("HETEROLOG_LOKI_SPOOL_CACHE_LIFETIME_SECOND", failure.DefaultSpoolCacheLifetime)
			if lokiSpoolCacheLifetimeSecondErr != nil {
				environmentErrors = append(environmentErrors, lokiSpoolCacheLifetimeSecondErr)
			}
			maxLokiSpoolFileActiveDirSizeMb, maxLokiSpoolFileActiveDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_LOKI_SPOOL_FILE_ACTIVE_DIR_SIZE_MB", failure.DefaultSpoolActiveMaxDirSizeMB, BytesPerMegabyte)
			if maxLokiSpoolFileActiveDirSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxLokiSpoolFileActiveDirSizeMbErr)
			}
			lokiActiveSpoolBacklogWarnAfterDays, lokiActiveSpoolBacklogWarnAfterDaysErr := env.GetEnvInt("HETEROLOG_LOKI_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS", failure.DefaultSpoolActiveBacklogWarnAfterDays)
			if lokiActiveSpoolBacklogWarnAfterDaysErr != nil {
				environmentErrors = append(environmentErrors, lokiActiveSpoolBacklogWarnAfterDaysErr)
			}
			useMemoryPressureLokiSpoolOverload, useMemoryPressureLokiSpoolOverloadErr := env.GetEnvBool("HETEROLOG_USE_MEMORY_PRESSURE_LOKI_SPOOL_OVERLOAD", failure.DefaultUseMemoryPressureForOverload)
			if useMemoryPressureLokiSpoolOverloadErr != nil {
				environmentErrors = append(environmentErrors, useMemoryPressureLokiSpoolOverloadErr)
			}
			return failure.SpoolConfig{
				Dir: env.GetEnvString("HETEROLOG_LOKI_FAILURE_SPOOL_DIR", lokicfg.DefaultLokiFailureSpoolDir),
				OverloadPolicyConfig: failure.SpoolOverloadPolicyConfig{
					OverloadPolicy:               failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_LOKI_SPOOL_OVERLOAD_POLICY", string(failure.DefaultSpoolOverloadPolicy)), failure.DefaultSpoolOverloadPolicy),
					SpillDestination:             failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_LOKI_SPOOL_SECONDARY_DESTINATION", string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
					MaxCallsSinceScan:            maxLokiSpoolScannedCacheUsageCount,
					StatsScanInterval:            lokiSpoolCacheLifetimeSecond,
					MaxDirSize:                   maxLokiSpoolFileActiveDirSizeMb,
					ActiveBacklogWarnAfterDays:   lokiActiveSpoolBacklogWarnAfterDays,
					UseMemoryPressureForOverload: useMemoryPressureLokiSpoolOverload,
				},
				HandlerConfig: internalerror.RoutingConfig{
					OnErrorConfig: env.GetEnvSliceString("HETEROLOG_LOKI_SPOOL_FILE_ERROR_HANDLER", nil),
					OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_LOKI_SPOOL_FILE_WARN_HANDLER", nil),
					OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_LOKI_SPOOL_FILE_INFO_HANDLER", nil),
					OnDebugConfig: env.GetEnvSliceString("HETEROLOG_LOKI_SPOOL_FILE_DEBUG_HANDLER", nil),
				},
			}, errors.Join(environmentErrors...)
		},
	)
	cfg.Fsstore = failure.FailureFileSystemStoreConfig{
		FileArchiveConfig: fileArchiveConfig,
		SpoolConfig:       spoolConfig,
	}
	return cfg, errors.Join(selectionErr, fileArchiveErr, spoolErr)
}

func loadAsyncLokiFlushManagerConfig(env pkgenv.Lookup) (logconfig.LokiFlushManagerConfig, error) {
	failureHandlerConfig, err := loadAsyncLokiFailureHandlerConfig(env)
	return logconfig.LokiFlushManagerConfig{
		FailureHandlerConfig: failureHandlerConfig,
	}, err
}

func loadAsyncLokiFailureHandlerConfig(env pkgenv.Lookup) (failure.FailureHandlerConfig, error) {
	cfg, selectionErr := loadFailureHandlerSelection(
		env,
		"HETEROLOG_ENABLE_ASYNC_LOKI_DEGRADE", failure.DefaultAsyncEnableDegrade,
		"HETEROLOG_MAX_ASYNC_LOKI_FAILURE_HANDLER_CONCURRENT", failure.DefaultMaxConcurrent,
		"HETEROLOG_ASYNC_LOKI_DURABLE_TYPES", failure.DefaultDurableTypes,
		"HETEROLOG_ASYNC_LOKI_DEGRADE_TYPES", failure.DefaultAsyncSpillTypes,
	)
	fileArchiveConfig, fileArchiveErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeFsstore),
		func() (failure.FileArchiveConfig, error) {
			var environmentErrors []error
			maxAsyncLokiDurableFileArchiveSizeMb, maxAsyncLokiDurableFileArchiveSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_ASYNC_LOKI_DURABLE_FILE_ARCHIVE_SIZE_MB", failure.DefaultFileArchiveMaxSizeMB, BytesPerMegabyte)
			if maxAsyncLokiDurableFileArchiveSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncLokiDurableFileArchiveSizeMbErr)
			}
			asyncLokiEnableDurableFileArchiveRotate, asyncLokiEnableDurableFileArchiveRotateErr := env.GetEnvBool("HETEROLOG_ASYNC_LOKI_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE", failure.DefaultFileArchiveRotateEnabled)
			if asyncLokiEnableDurableFileArchiveRotateErr != nil {
				environmentErrors = append(environmentErrors, asyncLokiEnableDurableFileArchiveRotateErr)
			}
			maxAsyncLokiEnableDurableFileArchiveBackups, maxAsyncLokiEnableDurableFileArchiveBackupsErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_LOKI_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS", failure.DefaultFileArchiveMaxBackups)
			if maxAsyncLokiEnableDurableFileArchiveBackupsErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncLokiEnableDurableFileArchiveBackupsErr)
			}
			maxAsyncLokiEnableDurableFileArchiveAge, maxAsyncLokiEnableDurableFileArchiveAgeErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_LOKI_ENABLE_DURABLE_FILE_ARCHIVE_AGE", failure.DefaultFileArchiveMaxAge)
			if maxAsyncLokiEnableDurableFileArchiveAgeErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncLokiEnableDurableFileArchiveAgeErr)
			}
			maxAsyncLokiEnableDurableFileArchiveDirSizeMb, maxAsyncLokiEnableDurableFileArchiveDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_ASYNC_LOKI_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB", failure.DefaultFileArchiveMaxDirSizeMB, BytesPerMegabyte)
			if maxAsyncLokiEnableDurableFileArchiveDirSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncLokiEnableDurableFileArchiveDirSizeMbErr)
			}
			enableAsyncLokiDurableFileArchiveCompress, enableAsyncLokiDurableFileArchiveCompressErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_LOKI_DURABLE_FILE_ARCHIVE_COMPRESS", failure.DefaultFileArchiveCompressEnabled)
			if enableAsyncLokiDurableFileArchiveCompressErr != nil {
				environmentErrors = append(environmentErrors, enableAsyncLokiDurableFileArchiveCompressErr)
			}
			compressFormat, compressFormatErr := loadLogCompressFormat(
				env.GetEnvString("HETEROLOG_ASYNC_LOKI_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT", string(failure.DefaultFileArchiveCompressFormat)),
				"HETEROLOG_ASYNC_LOKI_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT",
				failure.DefaultFileArchiveCompressFormat,
			)
			if compressFormatErr != nil {
				environmentErrors = append(environmentErrors, compressFormatErr)
			}
			objectStorageUploadConfig, objectStorageUploadConfigErr := loadAsyncLokiFailureArchiveObjectStorageUploadConfig(env)
			if objectStorageUploadConfigErr != nil {
				environmentErrors = append(environmentErrors, objectStorageUploadConfigErr)
			}
			return failure.FileArchiveConfig{
				MaxFileSize:                maxAsyncLokiDurableFileArchiveSizeMb,
				LifecycleMode:              logconfig.ParseLogFileLifecycleMode(env.GetEnvString("HETEROLOG_ASYNC_LOKI_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE", string(failure.DefaultFileArchiveLifecycleMode)), failure.DefaultFileArchiveLifecycleMode),
				Dir:                        env.GetEnvString("HETEROLOG_ASYNC_LOKI_FAILURE_ARCHIVE_DIR", lokicfg.DefaultAsyncLokiFailureArchiveDir),
				DirCaseRetryable:           env.GetEnvString("HETEROLOG_ASYNC_LOKI_DURABLE_RETRYABLE_DIR", failure.DirCaseRetryable),
				DirCasePermanent:           env.GetEnvString("HETEROLOG_ASYNC_LOKI_DURABLE_PERMANENT_DIR", failure.DirCasePermanent),
				EnableRotate:               asyncLokiEnableDurableFileArchiveRotate,
				MaxFileBackups:             maxAsyncLokiEnableDurableFileArchiveBackups,
				MaxFileAge:                 maxAsyncLokiEnableDurableFileArchiveAge,
				MaxDirSize:                 maxAsyncLokiEnableDurableFileArchiveDirSizeMb,
				EnableFileCompress:         enableAsyncLokiDurableFileArchiveCompress,
				CompressFormat:             compressFormat,
				FileRotation:               loadLogRotation(env.GetEnvString("HETEROLOG_ASYNC_LOKI_DURABLE_FILE_ARCHIVE_ROTATION", string(failure.DefaultFileArchiveRotation)), failure.DefaultFileArchiveRotation),
				FileRotationDayOfWeek:      failure.DefaultFileArchiveRotationDayOfWeek,
				MaxWriteRetries:            fsstoreconfig.MaxFileOperationRetries,
				FileOperationRetryWaitTime: fsstoreconfig.FileOperationRetryWaitTime,
				ObjectStorageUploadConfig:  objectStorageUploadConfig,
				OverloadPolicyConfig: failure.FileArchiveOverloadPolicyConfig{
					OverloadPolicy:   failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_ASYNC_LOKI_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY", string(failure.OverloadPolicyDrop)), failure.OverloadPolicyDrop),
					SpillDestination: failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_ASYNC_LOKI_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION", string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
				},
				HandlerConfig: internalerror.RoutingConfig{
					OnErrorConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_LOKI_DURABLE_FILE_ERROR_HANDLER", nil),
					OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_LOKI_DURABLE_FILE_WARN_HANDLER", nil),
					OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_LOKI_DURABLE_FILE_INFO_HANDLER", nil),
					OnDebugConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_LOKI_DURABLE_FILE_DEBUG_HANDLER", nil),
				},
			}, errors.Join(environmentErrors...)
		},
	)
	spoolConfig, spoolErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeSpool),
		func() (failure.SpoolConfig, error) {
			var environmentErrors []error
			maxAsyncLokiSpoolScannedCacheUsageCount, maxAsyncLokiSpoolScannedCacheUsageCountErr := env.GetEnvUint64("HETEROLOG_MAX_ASYNC_LOKI_SPOOL_SCANNED_CACHE_USAGE_COUNT", failure.DefaultMaxSpoolScannedCacheUsageCount)
			if maxAsyncLokiSpoolScannedCacheUsageCountErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncLokiSpoolScannedCacheUsageCountErr)
			}
			asyncLokiSpoolCacheLifetimeSecond, asyncLokiSpoolCacheLifetimeSecondErr := env.GetEnvDuration("HETEROLOG_ASYNC_LOKI_SPOOL_CACHE_LIFETIME_SECOND", failure.DefaultSpoolCacheLifetime)
			if asyncLokiSpoolCacheLifetimeSecondErr != nil {
				environmentErrors = append(environmentErrors, asyncLokiSpoolCacheLifetimeSecondErr)
			}
			maxAsyncLokiSpoolFileActiveDirSizeMb, maxAsyncLokiSpoolFileActiveDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_ASYNC_LOKI_SPOOL_FILE_ACTIVE_DIR_SIZE_MB", failure.DefaultSpoolActiveMaxDirSizeMB, BytesPerMegabyte)
			if maxAsyncLokiSpoolFileActiveDirSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncLokiSpoolFileActiveDirSizeMbErr)
			}
			asyncLokiActiveSpoolBacklogWarnAfterDays, asyncLokiActiveSpoolBacklogWarnAfterDaysErr := env.GetEnvInt("HETEROLOG_ASYNC_LOKI_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS", failure.DefaultAsyncSpoolActiveBacklogWarnAfterDays)
			if asyncLokiActiveSpoolBacklogWarnAfterDaysErr != nil {
				environmentErrors = append(environmentErrors, asyncLokiActiveSpoolBacklogWarnAfterDaysErr)
			}
			useMemoryPressureAsyncLokiSpoolOverload, useMemoryPressureAsyncLokiSpoolOverloadErr := env.GetEnvBool("HETEROLOG_USE_MEMORY_PRESSURE_ASYNC_LOKI_SPOOL_OVERLOAD", failure.DefaultUseMemoryPressureForOverload)
			if useMemoryPressureAsyncLokiSpoolOverloadErr != nil {
				environmentErrors = append(environmentErrors, useMemoryPressureAsyncLokiSpoolOverloadErr)
			}
			return failure.SpoolConfig{
				Dir: env.GetEnvString("HETEROLOG_ASYNC_LOKI_FAILURE_SPOOL_DIR", lokicfg.DefaultAsyncLokiFailureSpoolDir),
				OverloadPolicyConfig: failure.SpoolOverloadPolicyConfig{
					OverloadPolicy:               failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_ASYNC_LOKI_SPOOL_OVERLOAD_POLICY", string(failure.DefaultSpoolOverloadPolicy)), failure.DefaultSpoolOverloadPolicy),
					SpillDestination:             failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_ASYNC_LOKI_SPOOL_SECONDARY_DESTINATION", string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
					MaxCallsSinceScan:            maxAsyncLokiSpoolScannedCacheUsageCount,
					StatsScanInterval:            asyncLokiSpoolCacheLifetimeSecond,
					MaxDirSize:                   maxAsyncLokiSpoolFileActiveDirSizeMb,
					ActiveBacklogWarnAfterDays:   asyncLokiActiveSpoolBacklogWarnAfterDays,
					UseMemoryPressureForOverload: useMemoryPressureAsyncLokiSpoolOverload,
				},
				HandlerConfig: internalerror.RoutingConfig{
					OnErrorConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_LOKI_SPOOL_FILE_ERROR_HANDLER", nil),
					OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_LOKI_SPOOL_FILE_WARN_HANDLER", nil),
					OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_LOKI_SPOOL_FILE_INFO_HANDLER", nil),
					OnDebugConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_LOKI_SPOOL_FILE_DEBUG_HANDLER", nil),
				},
			}, errors.Join(environmentErrors...)
		},
	)
	cfg.Fsstore = failure.FailureFileSystemStoreConfig{
		FileArchiveConfig: fileArchiveConfig,
		SpoolConfig:       spoolConfig,
	}
	return cfg, errors.Join(selectionErr, fileArchiveErr, spoolErr)
}

func loadDeliveryLokiFlushManagerConfig(env pkgenv.Lookup) (logconfig.DeliveryLokiFlushManagerConfig, error) {
	var environmentErrors []error
	enableDeliveryLokiPushLatency, enableDeliveryLokiPushLatencyErr := env.GetEnvBool("HETEROLOG_ENABLE_DELIVERY_LOKI_PUSH_LATENCY", logconfig.DefaultLokiPushLatencyEnabled)
	if enableDeliveryLokiPushLatencyErr != nil {
		environmentErrors = append(environmentErrors, enableDeliveryLokiPushLatencyErr)
	}
	failureHandlerConfig, failureHandlerErr := loadDeliveryLokiFailureHandlerConfig(env)
	return logconfig.DeliveryLokiFlushManagerConfig{
		FailureHandlerConfig:       failureHandlerConfig,
		PostWarnThresholdDuration:  logconfig.PostLogWarnThresholdDuration,
		PostErrorThresholdDuration: logconfig.PostLogErrorThresholdDuration,
		PushLatencyEnabled:         enableDeliveryLokiPushLatency,
	}, errors.Join(errors.Join(environmentErrors...), failureHandlerErr)
}

func loadDeliveryLokiFailureHandlerConfig(env pkgenv.Lookup) (failure.DeliveryFailureHandlerConfig, error) {
	cfg, selectionErr := loadDeliveryFailureHandlerSelection(
		env,
		"HETEROLOG_ENABLE_DELIVERY_LOKI_DEGRADE", failure.DefaultEnableDegrade,
		"HETEROLOG_DELIVERY_LOKI_DURABLE_TYPES", failure.DefaultDeliveryDurableTypes,
		"HETEROLOG_DELIVERY_LOKI_DEGRADE_TYPES", failure.DefaultSpillTypes,
	)
	fileArchiveConfig, fileArchiveErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeFsstore),
		func() (failure.DeliveryFileArchiveConfig, error) {
			var environmentErrors []error
			maxDeliveryLokiDurableFileArchiveSizeMb, maxDeliveryLokiDurableFileArchiveSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_DELIVERY_LOKI_DURABLE_FILE_ARCHIVE_SIZE_MB", failure.DefaultFileArchiveMaxSizeMB, BytesPerMegabyte)
			if maxDeliveryLokiDurableFileArchiveSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxDeliveryLokiDurableFileArchiveSizeMbErr)
			}
			deliveryLokiEnableDurableFileArchiveRotate, deliveryLokiEnableDurableFileArchiveRotateErr := env.GetEnvBool("HETEROLOG_DELIVERY_LOKI_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE", failure.DefaultFileArchiveRotateEnabled)
			if deliveryLokiEnableDurableFileArchiveRotateErr != nil {
				environmentErrors = append(environmentErrors, deliveryLokiEnableDurableFileArchiveRotateErr)
			}
			maxDeliveryLokiEnableDurableFileArchiveBackups, maxDeliveryLokiEnableDurableFileArchiveBackupsErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_LOKI_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS", failure.DefaultFileArchiveMaxBackups)
			if maxDeliveryLokiEnableDurableFileArchiveBackupsErr != nil {
				environmentErrors = append(environmentErrors, maxDeliveryLokiEnableDurableFileArchiveBackupsErr)
			}
			maxDeliveryLokiEnableDurableFileArchiveAge, maxDeliveryLokiEnableDurableFileArchiveAgeErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_LOKI_ENABLE_DURABLE_FILE_ARCHIVE_AGE", failure.DefaultFileArchiveMaxAge)
			if maxDeliveryLokiEnableDurableFileArchiveAgeErr != nil {
				environmentErrors = append(environmentErrors, maxDeliveryLokiEnableDurableFileArchiveAgeErr)
			}
			maxDeliveryLokiEnableDurableFileArchiveDirSizeMb, maxDeliveryLokiEnableDurableFileArchiveDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_DELIVERY_LOKI_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB", failure.DefaultFileArchiveMaxDirSizeMB, BytesPerMegabyte)
			if maxDeliveryLokiEnableDurableFileArchiveDirSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxDeliveryLokiEnableDurableFileArchiveDirSizeMbErr)
			}
			enableDeliveryLokiDurableFileArchiveCompress, enableDeliveryLokiDurableFileArchiveCompressErr := env.GetEnvBool("HETEROLOG_ENABLE_DELIVERY_LOKI_DURABLE_FILE_ARCHIVE_COMPRESS", failure.DefaultFileArchiveCompressEnabled)
			if enableDeliveryLokiDurableFileArchiveCompressErr != nil {
				environmentErrors = append(environmentErrors, enableDeliveryLokiDurableFileArchiveCompressErr)
			}
			compressFormat, compressFormatErr := loadLogCompressFormat(
				env.GetEnvString("HETEROLOG_DELIVERY_LOKI_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT", string(failure.DefaultFileArchiveCompressFormat)),
				"HETEROLOG_DELIVERY_LOKI_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT",
				failure.DefaultFileArchiveCompressFormat,
			)
			if compressFormatErr != nil {
				environmentErrors = append(environmentErrors, compressFormatErr)
			}
			objectStorageUploadConfig, objectStorageUploadConfigErr := loadDeliveryLokiFailureArchiveObjectStorageUploadConfig(env)
			if objectStorageUploadConfigErr != nil {
				environmentErrors = append(environmentErrors, objectStorageUploadConfigErr)
			}
			return failure.DeliveryFileArchiveConfig{
				MaxFileSize:                maxDeliveryLokiDurableFileArchiveSizeMb,
				LifecycleMode:              logconfig.ParseLogFileLifecycleMode(env.GetEnvString("HETEROLOG_DELIVERY_LOKI_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE", string(failure.DefaultFileArchiveLifecycleMode)), failure.DefaultFileArchiveLifecycleMode),
				Dir:                        env.GetEnvString("HETEROLOG_DELIVERY_LOKI_FAILURE_ARCHIVE_DIR", lokicfg.DefaultDeliveryLokiFailureArchiveDir),
				DirCaseRetryable:           env.GetEnvString("HETEROLOG_DELIVERY_LOKI_DURABLE_RETRYABLE_DIR", failure.DirCaseRetryable),
				DirCasePermanent:           env.GetEnvString("HETEROLOG_DELIVERY_LOKI_DURABLE_PERMANENT_DIR", failure.DirCasePermanent),
				EnableRotate:               deliveryLokiEnableDurableFileArchiveRotate,
				MaxFileBackups:             maxDeliveryLokiEnableDurableFileArchiveBackups,
				MaxFileAge:                 maxDeliveryLokiEnableDurableFileArchiveAge,
				MaxDirSize:                 maxDeliveryLokiEnableDurableFileArchiveDirSizeMb,
				EnableFileCompress:         enableDeliveryLokiDurableFileArchiveCompress,
				CompressFormat:             compressFormat,
				FileRotation:               loadLogRotation(env.GetEnvString("HETEROLOG_DELIVERY_LOKI_DURABLE_FILE_ARCHIVE_ROTATION", string(failure.DefaultFileArchiveRotation)), failure.DefaultFileArchiveRotation),
				FileRotationDayOfWeek:      failure.DefaultFileArchiveRotationDayOfWeek,
				MaxWriteRetries:            fsstoreconfig.MaxFileOperationRetries,
				FileOperationRetryWaitTime: fsstoreconfig.FileOperationRetryWaitTime,
				ObjectStorageUploadConfig:  objectStorageUploadConfig,
				OverloadPolicyConfig: failure.FileArchiveOverloadPolicyConfig{
					OverloadPolicy:   failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_DELIVERY_LOKI_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY", string(failure.OverloadPolicyDrop)), failure.OverloadPolicyDrop),
					SpillDestination: failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_DELIVERY_LOKI_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION", string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
				},
				HandlerConfig: internalerror.RoutingConfig{
					OnErrorConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_LOKI_DURABLE_FILE_ERROR_HANDLER", nil),
					OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_LOKI_DURABLE_FILE_WARN_HANDLER", nil),
					OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_LOKI_DURABLE_FILE_INFO_HANDLER", nil),
					OnDebugConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_LOKI_DURABLE_FILE_DEBUG_HANDLER", nil),
				},
			}, errors.Join(environmentErrors...)
		},
	)
	cfg.FileArchiveConfig = fileArchiveConfig
	return cfg, errors.Join(selectionErr, fileArchiveErr)
}

func loadLokiFailureArchiveObjectStorageUploadConfig(env pkgenv.Lookup) (objectstorage.UploadTargetConfig, error) {
	return loadFailureArchiveObjectStorageUploadConfig(
		env,
		"HETEROLOG_ENABLE_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD",
		"HETEROLOG_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE",
		"HETEROLOG_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET",
		"HETEROLOG_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX",
		"HETEROLOG_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_DESTINATIONS",
	)
}

func loadAsyncLokiFailureArchiveObjectStorageUploadConfig(env pkgenv.Lookup) (objectstorage.UploadTargetConfig, error) {
	return loadFailureArchiveObjectStorageUploadConfig(
		env,
		"HETEROLOG_ENABLE_ASYNC_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD",
		"HETEROLOG_ASYNC_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE",
		"HETEROLOG_ASYNC_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET",
		"HETEROLOG_ASYNC_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX",
		"HETEROLOG_ASYNC_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_DESTINATIONS",
	)
}

func loadDeliveryLokiFailureArchiveObjectStorageUploadConfig(env pkgenv.Lookup) (objectstorage.UploadTargetConfig, error) {
	return loadFailureArchiveObjectStorageUploadConfig(
		env,
		"HETEROLOG_ENABLE_DELIVERY_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD",
		"HETEROLOG_DELIVERY_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE",
		"HETEROLOG_DELIVERY_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET",
		"HETEROLOG_DELIVERY_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX",
		"HETEROLOG_DELIVERY_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_DESTINATIONS",
	)
}

func loadCloudWatchLogsFlushManagerConfig(env pkgenv.Lookup) (logconfig.CloudWatchLogsFlushManagerConfig, error) {
	failureHandlerConfig, err := loadCloudWatchLogsFailureHandlerConfig(env)
	return logconfig.CloudWatchLogsFlushManagerConfig{
		LogGroupName:                env.GetEnvString("HETEROLOG_CLOUDWATCH_LOG_GROUP_NAME", cloudwatchlogscfg.DefaultLogGroupName),
		LogStreamName:               env.GetEnvString("HETEROLOG_CLOUDWATCH_LOG_STREAM_NAME", cloudwatchlogscfg.DefaultLogStreamName),
		LogStreamRoutingGranularity: loadCloudWatchLogsStreamRoutingGranularity(env.GetEnvString("HETEROLOG_CLOUDWATCH_LOG_STREAM_ROTATION", string(cloudwatchlogscfg.DefaultStreamRoutingGranularity))),
		FailureHandlerConfig:        failureHandlerConfig,
	}, err
}

func loadAsyncCloudWatchLogsFlushManagerConfig(env pkgenv.Lookup) (logconfig.CloudWatchLogsFlushManagerConfig, error) {
	failureHandlerConfig, err := loadAsyncCloudWatchLogsFailureHandlerConfig(env)
	return logconfig.CloudWatchLogsFlushManagerConfig{
		LogGroupName:                env.GetEnvString("HETEROLOG_ASYNC_CLOUDWATCH_LOG_GROUP_NAME", cloudwatchlogscfg.DefaultLogGroupName),
		LogStreamName:               env.GetEnvString("HETEROLOG_ASYNC_CLOUDWATCH_LOG_STREAM_NAME", cloudwatchlogscfg.DefaultLogStreamName),
		LogStreamRoutingGranularity: loadCloudWatchLogsStreamRoutingGranularity(env.GetEnvString("HETEROLOG_ASYNC_CLOUDWATCH_LOG_STREAM_ROTATION", string(cloudwatchlogscfg.DefaultStreamRoutingGranularity))),
		FailureHandlerConfig:        failureHandlerConfig,
	}, err
}

func loadDeliveryCloudWatchLogsFlushManagerConfig(env pkgenv.Lookup) (logconfig.DeliveryCloudWatchLogsFlushManagerConfig, error) {
	var environmentErrors []error
	enableDeliveryCloudwatchPutLatency, enableDeliveryCloudwatchPutLatencyErr := env.GetEnvBool("HETEROLOG_ENABLE_DELIVERY_CLOUDWATCH_PUT_LATENCY", logconfig.DefaultCloudWatchLogsPutLatencyEnabled)
	if enableDeliveryCloudwatchPutLatencyErr != nil {
		environmentErrors = append(environmentErrors, enableDeliveryCloudwatchPutLatencyErr)
	}
	failureHandlerConfig, failureHandlerErr := loadDeliveryCloudWatchLogsFailureHandlerConfig(env)
	return logconfig.DeliveryCloudWatchLogsFlushManagerConfig{
		LogGroupName:                env.GetEnvString("HETEROLOG_DELIVERY_CLOUDWATCH_LOG_GROUP_NAME", cloudwatchlogscfg.DefaultLogGroupName),
		LogStreamName:               env.GetEnvString("HETEROLOG_DELIVERY_CLOUDWATCH_LOG_STREAM_NAME", cloudwatchlogscfg.DefaultLogStreamName),
		LogStreamRoutingGranularity: loadCloudWatchLogsStreamRoutingGranularity(env.GetEnvString("HETEROLOG_DELIVERY_CLOUDWATCH_LOG_STREAM_ROTATION", string(cloudwatchlogscfg.DefaultStreamRoutingGranularity))),
		PostWarnThresholdDuration:   logconfig.PostLogWarnThresholdDuration,
		PostErrorThresholdDuration:  logconfig.PostLogErrorThresholdDuration,
		PutLatencyEnabled:           enableDeliveryCloudwatchPutLatency,
		FailureHandlerConfig:        failureHandlerConfig,
	}, errors.Join(errors.Join(environmentErrors...), failureHandlerErr)
}

// loadCloudWatchLogsStreamRoutingGranularityは読み込み済みの値の前後空白と英字の大小を
// 正規化します。対応値かどうかはmapping後のvalidationで検証します。
func loadCloudWatchLogsStreamRoutingGranularity(value string) pkgcloudwatchlogs.StreamRouting {
	return pkgcloudwatchlogs.StreamRouting(value).Normalize()
}

func loadCloudWatchLogsFailureHandlerConfig(env pkgenv.Lookup) (failure.FailureHandlerConfig, error) {
	cfg, selectionErr := loadFailureHandlerSelection(
		env,
		"HETEROLOG_ENABLE_CLOUDWATCH_DEGRADE", failure.DefaultEnableDegrade,
		"HETEROLOG_MAX_CLOUDWATCH_FAILURE_HANDLER_CONCURRENT", failure.DefaultMaxConcurrent,
		"HETEROLOG_CLOUDWATCH_DURABLE_TYPES", failure.DefaultDurableTypes,
		"HETEROLOG_CLOUDWATCH_DEGRADE_TYPES", failure.DefaultSpillTypes,
	)
	fileArchiveConfig, fileArchiveErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeFsstore),
		func() (failure.FileArchiveConfig, error) {
			var environmentErrors []error
			maxCloudwatchDurableFileArchiveSizeMb, maxCloudwatchDurableFileArchiveSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_CLOUDWATCH_DURABLE_FILE_ARCHIVE_SIZE_MB", failure.DefaultFileArchiveMaxSizeMB, BytesPerMegabyte)
			if maxCloudwatchDurableFileArchiveSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxCloudwatchDurableFileArchiveSizeMbErr)
			}
			cloudwatchEnableDurableFileArchiveRotate, cloudwatchEnableDurableFileArchiveRotateErr := env.GetEnvBool("HETEROLOG_CLOUDWATCH_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE", failure.DefaultFileArchiveRotateEnabled)
			if cloudwatchEnableDurableFileArchiveRotateErr != nil {
				environmentErrors = append(environmentErrors, cloudwatchEnableDurableFileArchiveRotateErr)
			}
			maxCloudwatchEnableDurableFileArchiveBackups, maxCloudwatchEnableDurableFileArchiveBackupsErr := env.GetEnvInt("HETEROLOG_MAX_CLOUDWATCH_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS", failure.DefaultFileArchiveMaxBackups)
			if maxCloudwatchEnableDurableFileArchiveBackupsErr != nil {
				environmentErrors = append(environmentErrors, maxCloudwatchEnableDurableFileArchiveBackupsErr)
			}
			maxCloudwatchEnableDurableFileArchiveAge, maxCloudwatchEnableDurableFileArchiveAgeErr := env.GetEnvInt("HETEROLOG_MAX_CLOUDWATCH_ENABLE_DURABLE_FILE_ARCHIVE_AGE", failure.DefaultFileArchiveMaxAge)
			if maxCloudwatchEnableDurableFileArchiveAgeErr != nil {
				environmentErrors = append(environmentErrors, maxCloudwatchEnableDurableFileArchiveAgeErr)
			}
			maxCloudwatchEnableDurableFileArchiveDirSizeMb, maxCloudwatchEnableDurableFileArchiveDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_CLOUDWATCH_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB", failure.DefaultFileArchiveMaxDirSizeMB, BytesPerMegabyte)
			if maxCloudwatchEnableDurableFileArchiveDirSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxCloudwatchEnableDurableFileArchiveDirSizeMbErr)
			}
			enableCloudwatchDurableFileArchiveCompress, enableCloudwatchDurableFileArchiveCompressErr := env.GetEnvBool("HETEROLOG_ENABLE_CLOUDWATCH_DURABLE_FILE_ARCHIVE_COMPRESS", failure.DefaultFileArchiveCompressEnabled)
			if enableCloudwatchDurableFileArchiveCompressErr != nil {
				environmentErrors = append(environmentErrors, enableCloudwatchDurableFileArchiveCompressErr)
			}
			compressFormat, compressFormatErr := loadLogCompressFormat(
				env.GetEnvString("HETEROLOG_CLOUDWATCH_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT", string(failure.DefaultFileArchiveCompressFormat)),
				"HETEROLOG_CLOUDWATCH_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT",
				failure.DefaultFileArchiveCompressFormat,
			)
			if compressFormatErr != nil {
				environmentErrors = append(environmentErrors, compressFormatErr)
			}
			objectStorageUploadConfig, objectStorageUploadConfigErr := loadCloudWatchLogsFailureArchiveObjectStorageUploadConfig(env)
			if objectStorageUploadConfigErr != nil {
				environmentErrors = append(environmentErrors, objectStorageUploadConfigErr)
			}
			return failure.FileArchiveConfig{
				MaxFileSize:                maxCloudwatchDurableFileArchiveSizeMb,
				LifecycleMode:              logconfig.ParseLogFileLifecycleMode(env.GetEnvString("HETEROLOG_CLOUDWATCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE", string(failure.DefaultFileArchiveLifecycleMode)), failure.DefaultFileArchiveLifecycleMode),
				Dir:                        env.GetEnvString("HETEROLOG_CLOUDWATCH_FAILURE_ARCHIVE_DIR", cloudwatchlogscfg.DefaultCloudWatchLogsFailureArchiveDir),
				DirCaseRetryable:           env.GetEnvString("HETEROLOG_CLOUDWATCH_DURABLE_RETRYABLE_DIR", failure.DirCaseRetryable),
				DirCasePermanent:           env.GetEnvString("HETEROLOG_CLOUDWATCH_DURABLE_PERMANENT_DIR", failure.DirCasePermanent),
				EnableRotate:               cloudwatchEnableDurableFileArchiveRotate,
				MaxFileBackups:             maxCloudwatchEnableDurableFileArchiveBackups,
				MaxFileAge:                 maxCloudwatchEnableDurableFileArchiveAge,
				MaxDirSize:                 maxCloudwatchEnableDurableFileArchiveDirSizeMb,
				EnableFileCompress:         enableCloudwatchDurableFileArchiveCompress,
				CompressFormat:             compressFormat,
				FileRotation:               loadLogRotation(env.GetEnvString("HETEROLOG_CLOUDWATCH_DURABLE_FILE_ARCHIVE_ROTATION", string(failure.DefaultFileArchiveRotation)), failure.DefaultFileArchiveRotation),
				FileRotationDayOfWeek:      failure.DefaultFileArchiveRotationDayOfWeek,
				MaxWriteRetries:            fsstoreconfig.MaxFileOperationRetries,
				FileOperationRetryWaitTime: fsstoreconfig.FileOperationRetryWaitTime,
				ObjectStorageUploadConfig:  objectStorageUploadConfig,
				OverloadPolicyConfig: failure.FileArchiveOverloadPolicyConfig{
					OverloadPolicy:   failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY", string(failure.OverloadPolicyDrop)), failure.OverloadPolicyDrop),
					SpillDestination: failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_CLOUDWATCH_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION", string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
				},
				HandlerConfig: internalerror.RoutingConfig{
					OnErrorConfig: env.GetEnvSliceString("HETEROLOG_CLOUDWATCH_DURABLE_FILE_ERROR_HANDLER", nil),
					OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_CLOUDWATCH_DURABLE_FILE_WARN_HANDLER", nil),
					OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_CLOUDWATCH_DURABLE_FILE_INFO_HANDLER", nil),
					OnDebugConfig: env.GetEnvSliceString("HETEROLOG_CLOUDWATCH_DURABLE_FILE_DEBUG_HANDLER", nil),
				},
			}, errors.Join(environmentErrors...)
		},
	)
	spoolConfig, spoolErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeSpool),
		func() (failure.SpoolConfig, error) {
			var environmentErrors []error
			maxCloudwatchSpoolScannedCacheUsageCount, maxCloudwatchSpoolScannedCacheUsageCountErr := env.GetEnvUint64("HETEROLOG_MAX_CLOUDWATCH_SPOOL_SCANNED_CACHE_USAGE_COUNT", failure.DefaultMaxSpoolScannedCacheUsageCount)
			if maxCloudwatchSpoolScannedCacheUsageCountErr != nil {
				environmentErrors = append(environmentErrors, maxCloudwatchSpoolScannedCacheUsageCountErr)
			}
			cloudwatchSpoolCacheLifetimeSecond, cloudwatchSpoolCacheLifetimeSecondErr := env.GetEnvDuration("HETEROLOG_CLOUDWATCH_SPOOL_CACHE_LIFETIME_SECOND", failure.DefaultSpoolCacheLifetime)
			if cloudwatchSpoolCacheLifetimeSecondErr != nil {
				environmentErrors = append(environmentErrors, cloudwatchSpoolCacheLifetimeSecondErr)
			}
			maxCloudwatchSpoolFileActiveDirSizeMb, maxCloudwatchSpoolFileActiveDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_CLOUDWATCH_SPOOL_FILE_ACTIVE_DIR_SIZE_MB", failure.DefaultSpoolActiveMaxDirSizeMB, BytesPerMegabyte)
			if maxCloudwatchSpoolFileActiveDirSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxCloudwatchSpoolFileActiveDirSizeMbErr)
			}
			cloudwatchActiveSpoolBacklogWarnAfterDays, cloudwatchActiveSpoolBacklogWarnAfterDaysErr := env.GetEnvInt("HETEROLOG_CLOUDWATCH_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS", failure.DefaultSpoolActiveBacklogWarnAfterDays)
			if cloudwatchActiveSpoolBacklogWarnAfterDaysErr != nil {
				environmentErrors = append(environmentErrors, cloudwatchActiveSpoolBacklogWarnAfterDaysErr)
			}
			useMemoryPressureCloudwatchSpoolOverload, useMemoryPressureCloudwatchSpoolOverloadErr := env.GetEnvBool("HETEROLOG_USE_MEMORY_PRESSURE_CLOUDWATCH_SPOOL_OVERLOAD", failure.DefaultUseMemoryPressureForOverload)
			if useMemoryPressureCloudwatchSpoolOverloadErr != nil {
				environmentErrors = append(environmentErrors, useMemoryPressureCloudwatchSpoolOverloadErr)
			}
			return failure.SpoolConfig{
				Dir: env.GetEnvString("HETEROLOG_CLOUDWATCH_FAILURE_SPOOL_DIR", cloudwatchlogscfg.DefaultCloudWatchLogsFailureSpoolDir),
				OverloadPolicyConfig: failure.SpoolOverloadPolicyConfig{
					OverloadPolicy:               failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_CLOUDWATCH_SPOOL_OVERLOAD_POLICY", string(failure.DefaultSpoolOverloadPolicy)), failure.DefaultSpoolOverloadPolicy),
					SpillDestination:             failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_CLOUDWATCH_SPOOL_SECONDARY_DESTINATION", string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
					MaxCallsSinceScan:            maxCloudwatchSpoolScannedCacheUsageCount,
					StatsScanInterval:            cloudwatchSpoolCacheLifetimeSecond,
					MaxDirSize:                   maxCloudwatchSpoolFileActiveDirSizeMb,
					ActiveBacklogWarnAfterDays:   cloudwatchActiveSpoolBacklogWarnAfterDays,
					UseMemoryPressureForOverload: useMemoryPressureCloudwatchSpoolOverload,
				},
				HandlerConfig: internalerror.RoutingConfig{
					OnErrorConfig: env.GetEnvSliceString("HETEROLOG_CLOUDWATCH_SPOOL_FILE_ERROR_HANDLER", nil),
					OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_CLOUDWATCH_SPOOL_FILE_WARN_HANDLER", nil),
					OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_CLOUDWATCH_SPOOL_FILE_INFO_HANDLER", nil),
					OnDebugConfig: env.GetEnvSliceString("HETEROLOG_CLOUDWATCH_SPOOL_FILE_DEBUG_HANDLER", nil),
				},
			}, errors.Join(environmentErrors...)
		},
	)
	cfg.Fsstore = failure.FailureFileSystemStoreConfig{
		FileArchiveConfig: fileArchiveConfig,
		SpoolConfig:       spoolConfig,
	}
	return cfg, errors.Join(selectionErr, fileArchiveErr, spoolErr)
}

func loadAsyncCloudWatchLogsFailureHandlerConfig(env pkgenv.Lookup) (failure.FailureHandlerConfig, error) {
	cfg, selectionErr := loadFailureHandlerSelection(
		env,
		"HETEROLOG_ENABLE_ASYNC_CLOUDWATCH_DEGRADE", failure.DefaultAsyncEnableDegrade,
		"HETEROLOG_MAX_ASYNC_CLOUDWATCH_FAILURE_HANDLER_CONCURRENT", failure.DefaultMaxConcurrent,
		"HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_TYPES", failure.DefaultDurableTypes,
		"HETEROLOG_ASYNC_CLOUDWATCH_DEGRADE_TYPES", failure.DefaultAsyncSpillTypes,
	)
	fileArchiveConfig, fileArchiveErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeFsstore),
		func() (failure.FileArchiveConfig, error) {
			var environmentErrors []error
			maxAsyncCloudwatchDurableFileArchiveSizeMb, maxAsyncCloudwatchDurableFileArchiveSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE_SIZE_MB", failure.DefaultFileArchiveMaxSizeMB, BytesPerMegabyte)
			if maxAsyncCloudwatchDurableFileArchiveSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncCloudwatchDurableFileArchiveSizeMbErr)
			}
			asyncCloudwatchEnableDurableFileArchiveRotate, asyncCloudwatchEnableDurableFileArchiveRotateErr := env.GetEnvBool("HETEROLOG_ASYNC_CLOUDWATCH_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE", failure.DefaultFileArchiveRotateEnabled)
			if asyncCloudwatchEnableDurableFileArchiveRotateErr != nil {
				environmentErrors = append(environmentErrors, asyncCloudwatchEnableDurableFileArchiveRotateErr)
			}
			maxAsyncCloudwatchEnableDurableFileArchiveBackups, maxAsyncCloudwatchEnableDurableFileArchiveBackupsErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_CLOUDWATCH_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS", failure.DefaultFileArchiveMaxBackups)
			if maxAsyncCloudwatchEnableDurableFileArchiveBackupsErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncCloudwatchEnableDurableFileArchiveBackupsErr)
			}
			maxAsyncCloudwatchEnableDurableFileArchiveAge, maxAsyncCloudwatchEnableDurableFileArchiveAgeErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_CLOUDWATCH_ENABLE_DURABLE_FILE_ARCHIVE_AGE", failure.DefaultFileArchiveMaxAge)
			if maxAsyncCloudwatchEnableDurableFileArchiveAgeErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncCloudwatchEnableDurableFileArchiveAgeErr)
			}
			maxAsyncCloudwatchEnableDurableFileArchiveDirSizeMb, maxAsyncCloudwatchEnableDurableFileArchiveDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_ASYNC_CLOUDWATCH_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB", failure.DefaultFileArchiveMaxDirSizeMB, BytesPerMegabyte)
			if maxAsyncCloudwatchEnableDurableFileArchiveDirSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncCloudwatchEnableDurableFileArchiveDirSizeMbErr)
			}
			enableAsyncCloudwatchDurableFileArchiveCompress, enableAsyncCloudwatchDurableFileArchiveCompressErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE_COMPRESS", failure.DefaultFileArchiveCompressEnabled)
			if enableAsyncCloudwatchDurableFileArchiveCompressErr != nil {
				environmentErrors = append(environmentErrors, enableAsyncCloudwatchDurableFileArchiveCompressErr)
			}
			compressFormat, compressFormatErr := loadLogCompressFormat(
				env.GetEnvString("HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT", string(failure.DefaultFileArchiveCompressFormat)),
				"HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT",
				failure.DefaultFileArchiveCompressFormat,
			)
			if compressFormatErr != nil {
				environmentErrors = append(environmentErrors, compressFormatErr)
			}
			objectStorageUploadConfig, objectStorageUploadConfigErr := loadAsyncCloudWatchLogsFailureArchiveObjectStorageUploadConfig(env)
			if objectStorageUploadConfigErr != nil {
				environmentErrors = append(environmentErrors, objectStorageUploadConfigErr)
			}
			return failure.FileArchiveConfig{
				MaxFileSize:                maxAsyncCloudwatchDurableFileArchiveSizeMb,
				LifecycleMode:              logconfig.ParseLogFileLifecycleMode(env.GetEnvString("HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE", string(failure.DefaultFileArchiveLifecycleMode)), failure.DefaultFileArchiveLifecycleMode),
				Dir:                        env.GetEnvString("HETEROLOG_ASYNC_CLOUDWATCH_FAILURE_ARCHIVE_DIR", cloudwatchlogscfg.DefaultAsyncCloudWatchLogsFailureArchiveDir),
				DirCaseRetryable:           env.GetEnvString("HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_RETRYABLE_DIR", failure.DirCaseRetryable),
				DirCasePermanent:           env.GetEnvString("HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_PERMANENT_DIR", failure.DirCasePermanent),
				EnableRotate:               asyncCloudwatchEnableDurableFileArchiveRotate,
				MaxFileBackups:             maxAsyncCloudwatchEnableDurableFileArchiveBackups,
				MaxFileAge:                 maxAsyncCloudwatchEnableDurableFileArchiveAge,
				MaxDirSize:                 maxAsyncCloudwatchEnableDurableFileArchiveDirSizeMb,
				EnableFileCompress:         enableAsyncCloudwatchDurableFileArchiveCompress,
				CompressFormat:             compressFormat,
				FileRotation:               loadLogRotation(env.GetEnvString("HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE_ROTATION", string(failure.DefaultFileArchiveRotation)), failure.DefaultFileArchiveRotation),
				FileRotationDayOfWeek:      failure.DefaultFileArchiveRotationDayOfWeek,
				MaxWriteRetries:            fsstoreconfig.MaxFileOperationRetries,
				FileOperationRetryWaitTime: fsstoreconfig.FileOperationRetryWaitTime,
				ObjectStorageUploadConfig:  objectStorageUploadConfig,
				OverloadPolicyConfig: failure.FileArchiveOverloadPolicyConfig{
					OverloadPolicy:   failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY", string(failure.OverloadPolicyDrop)), failure.OverloadPolicyDrop),
					SpillDestination: failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION", string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
				},
				HandlerConfig: internalerror.RoutingConfig{
					OnErrorConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_FILE_ERROR_HANDLER", nil),
					OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_FILE_WARN_HANDLER", nil),
					OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_FILE_INFO_HANDLER", nil),
					OnDebugConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_FILE_DEBUG_HANDLER", nil),
				},
			}, errors.Join(environmentErrors...)
		},
	)
	spoolConfig, spoolErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeSpool),
		func() (failure.SpoolConfig, error) {
			var environmentErrors []error
			maxAsyncCloudwatchSpoolScannedCacheUsageCount, maxAsyncCloudwatchSpoolScannedCacheUsageCountErr := env.GetEnvUint64("HETEROLOG_MAX_ASYNC_CLOUDWATCH_SPOOL_SCANNED_CACHE_USAGE_COUNT", failure.DefaultMaxSpoolScannedCacheUsageCount)
			if maxAsyncCloudwatchSpoolScannedCacheUsageCountErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncCloudwatchSpoolScannedCacheUsageCountErr)
			}
			asyncCloudwatchSpoolCacheLifetimeSecond, asyncCloudwatchSpoolCacheLifetimeSecondErr := env.GetEnvDuration("HETEROLOG_ASYNC_CLOUDWATCH_SPOOL_CACHE_LIFETIME_SECOND", failure.DefaultSpoolCacheLifetime)
			if asyncCloudwatchSpoolCacheLifetimeSecondErr != nil {
				environmentErrors = append(environmentErrors, asyncCloudwatchSpoolCacheLifetimeSecondErr)
			}
			maxAsyncCloudwatchSpoolFileActiveDirSizeMb, maxAsyncCloudwatchSpoolFileActiveDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_ASYNC_CLOUDWATCH_SPOOL_FILE_ACTIVE_DIR_SIZE_MB", failure.DefaultSpoolActiveMaxDirSizeMB, BytesPerMegabyte)
			if maxAsyncCloudwatchSpoolFileActiveDirSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncCloudwatchSpoolFileActiveDirSizeMbErr)
			}
			asyncCloudwatchActiveSpoolBacklogWarnAfterDays, asyncCloudwatchActiveSpoolBacklogWarnAfterDaysErr := env.GetEnvInt("HETEROLOG_ASYNC_CLOUDWATCH_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS", failure.DefaultAsyncSpoolActiveBacklogWarnAfterDays)
			if asyncCloudwatchActiveSpoolBacklogWarnAfterDaysErr != nil {
				environmentErrors = append(environmentErrors, asyncCloudwatchActiveSpoolBacklogWarnAfterDaysErr)
			}
			useMemoryPressureAsyncCloudwatchSpoolOverload, useMemoryPressureAsyncCloudwatchSpoolOverloadErr := env.GetEnvBool("HETEROLOG_USE_MEMORY_PRESSURE_ASYNC_CLOUDWATCH_SPOOL_OVERLOAD", failure.DefaultUseMemoryPressureForOverload)
			if useMemoryPressureAsyncCloudwatchSpoolOverloadErr != nil {
				environmentErrors = append(environmentErrors, useMemoryPressureAsyncCloudwatchSpoolOverloadErr)
			}
			return failure.SpoolConfig{
				Dir: env.GetEnvString("HETEROLOG_ASYNC_CLOUDWATCH_FAILURE_SPOOL_DIR", cloudwatchlogscfg.DefaultAsyncCloudWatchLogsFailureSpoolDir),
				OverloadPolicyConfig: failure.SpoolOverloadPolicyConfig{
					OverloadPolicy:               failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_ASYNC_CLOUDWATCH_SPOOL_OVERLOAD_POLICY", string(failure.DefaultSpoolOverloadPolicy)), failure.DefaultSpoolOverloadPolicy),
					SpillDestination:             failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_ASYNC_CLOUDWATCH_SPOOL_SECONDARY_DESTINATION", string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
					MaxCallsSinceScan:            maxAsyncCloudwatchSpoolScannedCacheUsageCount,
					StatsScanInterval:            asyncCloudwatchSpoolCacheLifetimeSecond,
					MaxDirSize:                   maxAsyncCloudwatchSpoolFileActiveDirSizeMb,
					ActiveBacklogWarnAfterDays:   asyncCloudwatchActiveSpoolBacklogWarnAfterDays,
					UseMemoryPressureForOverload: useMemoryPressureAsyncCloudwatchSpoolOverload,
				},
				HandlerConfig: internalerror.RoutingConfig{
					OnErrorConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_CLOUDWATCH_SPOOL_FILE_ERROR_HANDLER", nil),
					OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_CLOUDWATCH_SPOOL_FILE_WARN_HANDLER", nil),
					OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_CLOUDWATCH_SPOOL_FILE_INFO_HANDLER", nil),
					OnDebugConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_CLOUDWATCH_SPOOL_FILE_DEBUG_HANDLER", nil),
				},
			}, errors.Join(environmentErrors...)
		},
	)
	cfg.Fsstore = failure.FailureFileSystemStoreConfig{
		FileArchiveConfig: fileArchiveConfig,
		SpoolConfig:       spoolConfig,
	}
	return cfg, errors.Join(selectionErr, fileArchiveErr, spoolErr)
}

func loadDeliveryCloudWatchLogsFailureHandlerConfig(env pkgenv.Lookup) (failure.DeliveryFailureHandlerConfig, error) {
	cfg, selectionErr := loadDeliveryFailureHandlerSelection(
		env,
		"HETEROLOG_ENABLE_DELIVERY_CLOUDWATCH_DEGRADE", failure.DefaultEnableDegrade,
		"HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_TYPES", failure.DefaultDeliveryDurableTypes,
		"HETEROLOG_DELIVERY_CLOUDWATCH_DEGRADE_TYPES", failure.DefaultSpillTypes,
	)
	fileArchiveConfig, fileArchiveErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeFsstore),
		func() (failure.DeliveryFileArchiveConfig, error) {
			var environmentErrors []error
			maxDeliveryCloudwatchDurableFileArchiveSizeMb, maxDeliveryCloudwatchDurableFileArchiveSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE_SIZE_MB", failure.DefaultFileArchiveMaxSizeMB, BytesPerMegabyte)
			if maxDeliveryCloudwatchDurableFileArchiveSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxDeliveryCloudwatchDurableFileArchiveSizeMbErr)
			}
			deliveryCloudwatchEnableDurableFileArchiveRotate, deliveryCloudwatchEnableDurableFileArchiveRotateErr := env.GetEnvBool("HETEROLOG_DELIVERY_CLOUDWATCH_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE", failure.DefaultFileArchiveRotateEnabled)
			if deliveryCloudwatchEnableDurableFileArchiveRotateErr != nil {
				environmentErrors = append(environmentErrors, deliveryCloudwatchEnableDurableFileArchiveRotateErr)
			}
			maxDeliveryCloudwatchEnableDurableFileArchiveBackups, maxDeliveryCloudwatchEnableDurableFileArchiveBackupsErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_CLOUDWATCH_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS", failure.DefaultFileArchiveMaxBackups)
			if maxDeliveryCloudwatchEnableDurableFileArchiveBackupsErr != nil {
				environmentErrors = append(environmentErrors, maxDeliveryCloudwatchEnableDurableFileArchiveBackupsErr)
			}
			maxDeliveryCloudwatchEnableDurableFileArchiveAge, maxDeliveryCloudwatchEnableDurableFileArchiveAgeErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_CLOUDWATCH_ENABLE_DURABLE_FILE_ARCHIVE_AGE", failure.DefaultFileArchiveMaxAge)
			if maxDeliveryCloudwatchEnableDurableFileArchiveAgeErr != nil {
				environmentErrors = append(environmentErrors, maxDeliveryCloudwatchEnableDurableFileArchiveAgeErr)
			}
			maxDeliveryCloudwatchEnableDurableFileArchiveDirSizeMb, maxDeliveryCloudwatchEnableDurableFileArchiveDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_DELIVERY_CLOUDWATCH_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB", failure.DefaultFileArchiveMaxDirSizeMB, BytesPerMegabyte)
			if maxDeliveryCloudwatchEnableDurableFileArchiveDirSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxDeliveryCloudwatchEnableDurableFileArchiveDirSizeMbErr)
			}
			enableDeliveryCloudwatchDurableFileArchiveCompress, enableDeliveryCloudwatchDurableFileArchiveCompressErr := env.GetEnvBool("HETEROLOG_ENABLE_DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE_COMPRESS", failure.DefaultFileArchiveCompressEnabled)
			if enableDeliveryCloudwatchDurableFileArchiveCompressErr != nil {
				environmentErrors = append(environmentErrors, enableDeliveryCloudwatchDurableFileArchiveCompressErr)
			}
			compressFormat, compressFormatErr := loadLogCompressFormat(
				env.GetEnvString("HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT", string(failure.DefaultFileArchiveCompressFormat)),
				"HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT",
				failure.DefaultFileArchiveCompressFormat,
			)
			if compressFormatErr != nil {
				environmentErrors = append(environmentErrors, compressFormatErr)
			}
			objectStorageUploadConfig, objectStorageUploadConfigErr := loadDeliveryCloudWatchLogsFailureArchiveObjectStorageUploadConfig(env)
			if objectStorageUploadConfigErr != nil {
				environmentErrors = append(environmentErrors, objectStorageUploadConfigErr)
			}
			return failure.DeliveryFileArchiveConfig{
				MaxFileSize:                maxDeliveryCloudwatchDurableFileArchiveSizeMb,
				LifecycleMode:              logconfig.ParseLogFileLifecycleMode(env.GetEnvString("HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE", string(failure.DefaultFileArchiveLifecycleMode)), failure.DefaultFileArchiveLifecycleMode),
				Dir:                        env.GetEnvString("HETEROLOG_DELIVERY_CLOUDWATCH_FAILURE_ARCHIVE_DIR", cloudwatchlogscfg.DefaultDeliveryCloudWatchLogsFailureArchiveDir),
				DirCaseRetryable:           env.GetEnvString("HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_RETRYABLE_DIR", failure.DirCaseRetryable),
				DirCasePermanent:           env.GetEnvString("HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_PERMANENT_DIR", failure.DirCasePermanent),
				EnableRotate:               deliveryCloudwatchEnableDurableFileArchiveRotate,
				MaxFileBackups:             maxDeliveryCloudwatchEnableDurableFileArchiveBackups,
				MaxFileAge:                 maxDeliveryCloudwatchEnableDurableFileArchiveAge,
				MaxDirSize:                 maxDeliveryCloudwatchEnableDurableFileArchiveDirSizeMb,
				EnableFileCompress:         enableDeliveryCloudwatchDurableFileArchiveCompress,
				CompressFormat:             compressFormat,
				FileRotation:               loadLogRotation(env.GetEnvString("HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE_ROTATION", string(failure.DefaultFileArchiveRotation)), failure.DefaultFileArchiveRotation),
				FileRotationDayOfWeek:      failure.DefaultFileArchiveRotationDayOfWeek,
				MaxWriteRetries:            fsstoreconfig.MaxFileOperationRetries,
				FileOperationRetryWaitTime: fsstoreconfig.FileOperationRetryWaitTime,
				ObjectStorageUploadConfig:  objectStorageUploadConfig,
				OverloadPolicyConfig: failure.FileArchiveOverloadPolicyConfig{
					OverloadPolicy:   failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY", string(failure.OverloadPolicyDrop)), failure.OverloadPolicyDrop),
					SpillDestination: failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION", string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
				},
				HandlerConfig: internalerror.RoutingConfig{
					OnErrorConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_FILE_ERROR_HANDLER", nil),
					OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_FILE_WARN_HANDLER", nil),
					OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_FILE_INFO_HANDLER", nil),
					OnDebugConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_FILE_DEBUG_HANDLER", nil),
				},
			}, errors.Join(environmentErrors...)
		},
	)
	cfg.FileArchiveConfig = fileArchiveConfig
	return cfg, errors.Join(selectionErr, fileArchiveErr)
}

func loadCloudWatchLogsFailureArchiveObjectStorageUploadConfig(env pkgenv.Lookup) (objectstorage.UploadTargetConfig, error) {
	return loadFailureArchiveObjectStorageUploadConfig(
		env,
		"HETEROLOG_ENABLE_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD",
		"HETEROLOG_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE",
		"HETEROLOG_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET",
		"HETEROLOG_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX",
		"HETEROLOG_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_DESTINATIONS",
	)
}

func loadAsyncCloudWatchLogsFailureArchiveObjectStorageUploadConfig(env pkgenv.Lookup) (objectstorage.UploadTargetConfig, error) {
	return loadFailureArchiveObjectStorageUploadConfig(
		env,
		"HETEROLOG_ENABLE_ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD",
		"HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE",
		"HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET",
		"HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX",
		"HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_DESTINATIONS",
	)
}

func loadDeliveryCloudWatchLogsFailureArchiveObjectStorageUploadConfig(env pkgenv.Lookup) (objectstorage.UploadTargetConfig, error) {
	return loadFailureArchiveObjectStorageUploadConfig(
		env,
		"HETEROLOG_ENABLE_DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD",
		"HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE",
		"HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET",
		"HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX",
		"HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_DESTINATIONS",
	)
}

func loadClickHouseFlushManagerConfig(env pkgenv.Lookup) (logconfig.ClickHouseFlushManagerConfig, error) {
	failureHandlerConfig, err := loadClickHouseFailureHandlerConfig(env)
	return logconfig.ClickHouseFlushManagerConfig{
		FailureHandlerConfig: failureHandlerConfig,
	}, err
}

func loadAsyncClickHouseFlushManagerConfig(env pkgenv.Lookup) (logconfig.ClickHouseFlushManagerConfig, error) {
	failureHandlerConfig, err := loadAsyncClickHouseFailureHandlerConfig(env)
	return logconfig.ClickHouseFlushManagerConfig{
		FailureHandlerConfig: failureHandlerConfig,
	}, err
}

func loadDeliveryClickHouseFlushManagerConfig(env pkgenv.Lookup) (logconfig.DeliveryClickHouseFlushManagerConfig, error) {
	failureHandlerConfig, err := loadDeliveryClickHouseFailureHandlerConfig(env)
	return logconfig.DeliveryClickHouseFlushManagerConfig{
		FailureHandlerConfig: failureHandlerConfig,
	}, err
}

func loadClickHouseFailureHandlerConfig(env pkgenv.Lookup) (failure.FailureHandlerConfig, error) {
	cfg, selectionErr := loadFailureHandlerSelection(
		env,
		"HETEROLOG_ENABLE_CLICKHOUSE_DEGRADE", failure.DefaultEnableDegrade,
		"HETEROLOG_MAX_CLICKHOUSE_FAILURE_HANDLER_CONCURRENT", failure.DefaultMaxConcurrent,
		"HETEROLOG_CLICKHOUSE_DURABLE_TYPES", failure.DefaultDurableTypes,
		"HETEROLOG_CLICKHOUSE_DEGRADE_TYPES", failure.DefaultSpillTypes,
	)
	fileArchiveConfig, fileArchiveErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeFsstore),
		func() (failure.FileArchiveConfig, error) {
			var environmentErrors []error
			maxClickhouseDurableFileArchiveSizeMb, maxClickhouseDurableFileArchiveSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_CLICKHOUSE_DURABLE_FILE_ARCHIVE_SIZE_MB", failure.DefaultFileArchiveMaxSizeMB, BytesPerMegabyte)
			if maxClickhouseDurableFileArchiveSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxClickhouseDurableFileArchiveSizeMbErr)
			}
			clickhouseEnableDurableFileArchiveRotate, clickhouseEnableDurableFileArchiveRotateErr := env.GetEnvBool("HETEROLOG_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE", failure.DefaultFileArchiveRotateEnabled)
			if clickhouseEnableDurableFileArchiveRotateErr != nil {
				environmentErrors = append(environmentErrors, clickhouseEnableDurableFileArchiveRotateErr)
			}
			maxClickhouseEnableDurableFileArchiveBackups, maxClickhouseEnableDurableFileArchiveBackupsErr := env.GetEnvInt("HETEROLOG_MAX_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS", failure.DefaultFileArchiveMaxBackups)
			if maxClickhouseEnableDurableFileArchiveBackupsErr != nil {
				environmentErrors = append(environmentErrors, maxClickhouseEnableDurableFileArchiveBackupsErr)
			}
			maxClickhouseEnableDurableFileArchiveAge, maxClickhouseEnableDurableFileArchiveAgeErr := env.GetEnvInt("HETEROLOG_MAX_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_AGE", failure.DefaultFileArchiveMaxAge)
			if maxClickhouseEnableDurableFileArchiveAgeErr != nil {
				environmentErrors = append(environmentErrors, maxClickhouseEnableDurableFileArchiveAgeErr)
			}
			maxClickhouseEnableDurableFileArchiveDirSizeMb, maxClickhouseEnableDurableFileArchiveDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB", failure.DefaultFileArchiveMaxDirSizeMB, BytesPerMegabyte)
			if maxClickhouseEnableDurableFileArchiveDirSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxClickhouseEnableDurableFileArchiveDirSizeMbErr)
			}
			enableClickhouseDurableFileArchiveCompress, enableClickhouseDurableFileArchiveCompressErr := env.GetEnvBool("HETEROLOG_ENABLE_CLICKHOUSE_DURABLE_FILE_ARCHIVE_COMPRESS", failure.DefaultFileArchiveCompressEnabled)
			if enableClickhouseDurableFileArchiveCompressErr != nil {
				environmentErrors = append(environmentErrors, enableClickhouseDurableFileArchiveCompressErr)
			}
			compressFormat, compressFormatErr := loadLogCompressFormat(
				env.GetEnvString("HETEROLOG_CLICKHOUSE_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT", string(failure.DefaultFileArchiveCompressFormat)),
				"HETEROLOG_CLICKHOUSE_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT",
				failure.DefaultFileArchiveCompressFormat,
			)
			if compressFormatErr != nil {
				environmentErrors = append(environmentErrors, compressFormatErr)
			}
			objectStorageUploadConfig, objectStorageUploadConfigErr := loadClickHouseFailureArchiveObjectStorageUploadConfig(env)
			if objectStorageUploadConfigErr != nil {
				environmentErrors = append(environmentErrors, objectStorageUploadConfigErr)
			}
			return failure.FileArchiveConfig{
				MaxFileSize:                maxClickhouseDurableFileArchiveSizeMb,
				LifecycleMode:              logconfig.ParseLogFileLifecycleMode(env.GetEnvString("HETEROLOG_CLICKHOUSE_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE", string(failure.DefaultFileArchiveLifecycleMode)), failure.DefaultFileArchiveLifecycleMode),
				Dir:                        env.GetEnvString("HETEROLOG_CLICKHOUSE_FAILURE_ARCHIVE_DIR", clickhousecfg.DefaultClickHouseFailureArchiveDir),
				DirCaseRetryable:           env.GetEnvString("HETEROLOG_CLICKHOUSE_DURABLE_RETRYABLE_DIR", failure.DirCaseRetryable),
				DirCasePermanent:           env.GetEnvString("HETEROLOG_CLICKHOUSE_DURABLE_PERMANENT_DIR", failure.DirCasePermanent),
				EnableRotate:               clickhouseEnableDurableFileArchiveRotate,
				MaxFileBackups:             maxClickhouseEnableDurableFileArchiveBackups,
				MaxFileAge:                 maxClickhouseEnableDurableFileArchiveAge,
				MaxDirSize:                 maxClickhouseEnableDurableFileArchiveDirSizeMb,
				EnableFileCompress:         enableClickhouseDurableFileArchiveCompress,
				CompressFormat:             compressFormat,
				FileRotation:               loadLogRotation(env.GetEnvString("HETEROLOG_CLICKHOUSE_DURABLE_FILE_ARCHIVE_ROTATION", string(failure.DefaultFileArchiveRotation)), failure.DefaultFileArchiveRotation),
				FileRotationDayOfWeek:      failure.DefaultFileArchiveRotationDayOfWeek,
				MaxWriteRetries:            fsstoreconfig.MaxFileOperationRetries,
				FileOperationRetryWaitTime: fsstoreconfig.FileOperationRetryWaitTime,
				ObjectStorageUploadConfig:  objectStorageUploadConfig,
				OverloadPolicyConfig: failure.FileArchiveOverloadPolicyConfig{
					OverloadPolicy:   failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY", string(failure.OverloadPolicyDrop)), failure.OverloadPolicyDrop),
					SpillDestination: failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_CLICKHOUSE_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION", string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
				},
				HandlerConfig: internalerror.RoutingConfig{
					OnErrorConfig: env.GetEnvSliceString("HETEROLOG_CLICKHOUSE_DURABLE_FILE_ERROR_HANDLER", nil),
					OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_CLICKHOUSE_DURABLE_FILE_WARN_HANDLER", nil),
					OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_CLICKHOUSE_DURABLE_FILE_INFO_HANDLER", nil),
					OnDebugConfig: env.GetEnvSliceString("HETEROLOG_CLICKHOUSE_DURABLE_FILE_DEBUG_HANDLER", nil),
				},
			}, errors.Join(environmentErrors...)
		},
	)
	spoolConfig, spoolErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeSpool),
		func() (failure.SpoolConfig, error) {
			var environmentErrors []error
			maxClickhouseSpoolScannedCacheUsageCount, maxClickhouseSpoolScannedCacheUsageCountErr := env.GetEnvUint64("HETEROLOG_MAX_CLICKHOUSE_SPOOL_SCANNED_CACHE_USAGE_COUNT", failure.DefaultMaxSpoolScannedCacheUsageCount)
			if maxClickhouseSpoolScannedCacheUsageCountErr != nil {
				environmentErrors = append(environmentErrors, maxClickhouseSpoolScannedCacheUsageCountErr)
			}
			clickhouseSpoolCacheLifetimeSecond, clickhouseSpoolCacheLifetimeSecondErr := env.GetEnvDuration("HETEROLOG_CLICKHOUSE_SPOOL_CACHE_LIFETIME_SECOND", failure.DefaultSpoolCacheLifetime)
			if clickhouseSpoolCacheLifetimeSecondErr != nil {
				environmentErrors = append(environmentErrors, clickhouseSpoolCacheLifetimeSecondErr)
			}
			maxClickhouseSpoolFileActiveDirSizeMb, maxClickhouseSpoolFileActiveDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_CLICKHOUSE_SPOOL_FILE_ACTIVE_DIR_SIZE_MB", failure.DefaultSpoolActiveMaxDirSizeMB, BytesPerMegabyte)
			if maxClickhouseSpoolFileActiveDirSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxClickhouseSpoolFileActiveDirSizeMbErr)
			}
			clickhouseActiveSpoolBacklogWarnAfterDays, clickhouseActiveSpoolBacklogWarnAfterDaysErr := env.GetEnvInt("HETEROLOG_CLICKHOUSE_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS", failure.DefaultSpoolActiveBacklogWarnAfterDays)
			if clickhouseActiveSpoolBacklogWarnAfterDaysErr != nil {
				environmentErrors = append(environmentErrors, clickhouseActiveSpoolBacklogWarnAfterDaysErr)
			}
			useMemoryPressureClickhouseSpoolOverload, useMemoryPressureClickhouseSpoolOverloadErr := env.GetEnvBool("HETEROLOG_USE_MEMORY_PRESSURE_CLICKHOUSE_SPOOL_OVERLOAD", failure.DefaultUseMemoryPressureForOverload)
			if useMemoryPressureClickhouseSpoolOverloadErr != nil {
				environmentErrors = append(environmentErrors, useMemoryPressureClickhouseSpoolOverloadErr)
			}
			return failure.SpoolConfig{
				Dir: env.GetEnvString("HETEROLOG_CLICKHOUSE_FAILURE_SPOOL_DIR", clickhousecfg.DefaultClickHouseFailureSpoolDir),
				OverloadPolicyConfig: failure.SpoolOverloadPolicyConfig{
					OverloadPolicy:               failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_CLICKHOUSE_SPOOL_OVERLOAD_POLICY", string(failure.DefaultSpoolOverloadPolicy)), failure.DefaultSpoolOverloadPolicy),
					SpillDestination:             failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_CLICKHOUSE_SPOOL_SECONDARY_DESTINATION", string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
					MaxCallsSinceScan:            maxClickhouseSpoolScannedCacheUsageCount,
					StatsScanInterval:            clickhouseSpoolCacheLifetimeSecond,
					MaxDirSize:                   maxClickhouseSpoolFileActiveDirSizeMb,
					ActiveBacklogWarnAfterDays:   clickhouseActiveSpoolBacklogWarnAfterDays,
					UseMemoryPressureForOverload: useMemoryPressureClickhouseSpoolOverload,
				},
				HandlerConfig: internalerror.RoutingConfig{
					OnErrorConfig: env.GetEnvSliceString("HETEROLOG_CLICKHOUSE_SPOOL_FILE_ERROR_HANDLER", nil),
					OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_CLICKHOUSE_SPOOL_FILE_WARN_HANDLER", nil),
					OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_CLICKHOUSE_SPOOL_FILE_INFO_HANDLER", nil),
					OnDebugConfig: env.GetEnvSliceString("HETEROLOG_CLICKHOUSE_SPOOL_FILE_DEBUG_HANDLER", nil),
				},
			}, errors.Join(environmentErrors...)
		},
	)
	cfg.Fsstore = failure.FailureFileSystemStoreConfig{
		FileArchiveConfig: fileArchiveConfig,
		SpoolConfig:       spoolConfig,
	}
	return cfg, errors.Join(selectionErr, fileArchiveErr, spoolErr)
}

func loadAsyncClickHouseFailureHandlerConfig(env pkgenv.Lookup) (failure.FailureHandlerConfig, error) {
	cfg, selectionErr := loadFailureHandlerSelection(
		env,
		"HETEROLOG_ENABLE_ASYNC_CLICKHOUSE_DEGRADE", failure.DefaultAsyncEnableDegrade,
		"HETEROLOG_MAX_ASYNC_CLICKHOUSE_FAILURE_HANDLER_CONCURRENT", failure.DefaultMaxConcurrent,
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_TYPES", failure.DefaultDurableTypes,
		"HETEROLOG_ASYNC_CLICKHOUSE_DEGRADE_TYPES", failure.DefaultAsyncSpillTypes,
	)
	fileArchiveConfig, fileArchiveErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeFsstore),
		func() (failure.FileArchiveConfig, error) {
			var environmentErrors []error
			maxAsyncClickhouseDurableFileArchiveSizeMb, maxAsyncClickhouseDurableFileArchiveSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_SIZE_MB", failure.DefaultFileArchiveMaxSizeMB, BytesPerMegabyte)
			if maxAsyncClickhouseDurableFileArchiveSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncClickhouseDurableFileArchiveSizeMbErr)
			}
			asyncClickhouseEnableDurableFileArchiveRotate, asyncClickhouseEnableDurableFileArchiveRotateErr := env.GetEnvBool("HETEROLOG_ASYNC_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE", failure.DefaultFileArchiveRotateEnabled)
			if asyncClickhouseEnableDurableFileArchiveRotateErr != nil {
				environmentErrors = append(environmentErrors, asyncClickhouseEnableDurableFileArchiveRotateErr)
			}
			maxAsyncClickhouseEnableDurableFileArchiveBackups, maxAsyncClickhouseEnableDurableFileArchiveBackupsErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS", failure.DefaultFileArchiveMaxBackups)
			if maxAsyncClickhouseEnableDurableFileArchiveBackupsErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncClickhouseEnableDurableFileArchiveBackupsErr)
			}
			maxAsyncClickhouseEnableDurableFileArchiveAge, maxAsyncClickhouseEnableDurableFileArchiveAgeErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_AGE", failure.DefaultFileArchiveMaxAge)
			if maxAsyncClickhouseEnableDurableFileArchiveAgeErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncClickhouseEnableDurableFileArchiveAgeErr)
			}
			maxAsyncClickhouseEnableDurableFileArchiveDirSizeMb, maxAsyncClickhouseEnableDurableFileArchiveDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_ASYNC_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB", failure.DefaultFileArchiveMaxDirSizeMB, BytesPerMegabyte)
			if maxAsyncClickhouseEnableDurableFileArchiveDirSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncClickhouseEnableDurableFileArchiveDirSizeMbErr)
			}
			enableAsyncClickhouseDurableFileArchiveCompress, enableAsyncClickhouseDurableFileArchiveCompressErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_COMPRESS", failure.DefaultFileArchiveCompressEnabled)
			if enableAsyncClickhouseDurableFileArchiveCompressErr != nil {
				environmentErrors = append(environmentErrors, enableAsyncClickhouseDurableFileArchiveCompressErr)
			}
			compressFormat, compressFormatErr := loadLogCompressFormat(
				env.GetEnvString("HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT", string(failure.DefaultFileArchiveCompressFormat)),
				"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT",
				failure.DefaultFileArchiveCompressFormat,
			)
			if compressFormatErr != nil {
				environmentErrors = append(environmentErrors, compressFormatErr)
			}
			objectStorageUploadConfig, objectStorageUploadConfigErr := loadAsyncClickHouseFailureArchiveObjectStorageUploadConfig(env)
			if objectStorageUploadConfigErr != nil {
				environmentErrors = append(environmentErrors, objectStorageUploadConfigErr)
			}
			return failure.FileArchiveConfig{
				MaxFileSize:                maxAsyncClickhouseDurableFileArchiveSizeMb,
				LifecycleMode:              logconfig.ParseLogFileLifecycleMode(env.GetEnvString("HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE", string(failure.DefaultFileArchiveLifecycleMode)), failure.DefaultFileArchiveLifecycleMode),
				Dir:                        env.GetEnvString("HETEROLOG_ASYNC_CLICKHOUSE_FAILURE_ARCHIVE_DIR", clickhousecfg.DefaultAsyncClickHouseFailureArchiveDir),
				DirCaseRetryable:           env.GetEnvString("HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_RETRYABLE_DIR", failure.DirCaseRetryable),
				DirCasePermanent:           env.GetEnvString("HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_PERMANENT_DIR", failure.DirCasePermanent),
				EnableRotate:               asyncClickhouseEnableDurableFileArchiveRotate,
				MaxFileBackups:             maxAsyncClickhouseEnableDurableFileArchiveBackups,
				MaxFileAge:                 maxAsyncClickhouseEnableDurableFileArchiveAge,
				MaxDirSize:                 maxAsyncClickhouseEnableDurableFileArchiveDirSizeMb,
				EnableFileCompress:         enableAsyncClickhouseDurableFileArchiveCompress,
				CompressFormat:             compressFormat,
				FileRotation:               loadLogRotation(env.GetEnvString("HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_ROTATION", string(failure.DefaultFileArchiveRotation)), failure.DefaultFileArchiveRotation),
				FileRotationDayOfWeek:      failure.DefaultFileArchiveRotationDayOfWeek,
				MaxWriteRetries:            fsstoreconfig.MaxFileOperationRetries,
				FileOperationRetryWaitTime: fsstoreconfig.FileOperationRetryWaitTime,
				ObjectStorageUploadConfig:  objectStorageUploadConfig,
				OverloadPolicyConfig: failure.FileArchiveOverloadPolicyConfig{
					OverloadPolicy:   failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY", string(failure.OverloadPolicyDrop)), failure.OverloadPolicyDrop),
					SpillDestination: failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION", string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
				},
				HandlerConfig: internalerror.RoutingConfig{
					OnErrorConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ERROR_HANDLER", nil),
					OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_WARN_HANDLER", nil),
					OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_INFO_HANDLER", nil),
					OnDebugConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_DEBUG_HANDLER", nil),
				},
			}, errors.Join(environmentErrors...)
		},
	)
	spoolConfig, spoolErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeSpool),
		func() (failure.SpoolConfig, error) {
			var environmentErrors []error
			maxAsyncClickhouseSpoolScannedCacheUsageCount, maxAsyncClickhouseSpoolScannedCacheUsageCountErr := env.GetEnvUint64("HETEROLOG_MAX_ASYNC_CLICKHOUSE_SPOOL_SCANNED_CACHE_USAGE_COUNT", failure.DefaultMaxSpoolScannedCacheUsageCount)
			if maxAsyncClickhouseSpoolScannedCacheUsageCountErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncClickhouseSpoolScannedCacheUsageCountErr)
			}
			asyncClickhouseSpoolCacheLifetimeSecond, asyncClickhouseSpoolCacheLifetimeSecondErr := env.GetEnvDuration("HETEROLOG_ASYNC_CLICKHOUSE_SPOOL_CACHE_LIFETIME_SECOND", failure.DefaultSpoolCacheLifetime)
			if asyncClickhouseSpoolCacheLifetimeSecondErr != nil {
				environmentErrors = append(environmentErrors, asyncClickhouseSpoolCacheLifetimeSecondErr)
			}
			maxAsyncClickhouseSpoolFileActiveDirSizeMb, maxAsyncClickhouseSpoolFileActiveDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_ASYNC_CLICKHOUSE_SPOOL_FILE_ACTIVE_DIR_SIZE_MB", failure.DefaultSpoolActiveMaxDirSizeMB, BytesPerMegabyte)
			if maxAsyncClickhouseSpoolFileActiveDirSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxAsyncClickhouseSpoolFileActiveDirSizeMbErr)
			}
			asyncClickhouseActiveSpoolBacklogWarnAfterDays, asyncClickhouseActiveSpoolBacklogWarnAfterDaysErr := env.GetEnvInt("HETEROLOG_ASYNC_CLICKHOUSE_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS", failure.DefaultAsyncSpoolActiveBacklogWarnAfterDays)
			if asyncClickhouseActiveSpoolBacklogWarnAfterDaysErr != nil {
				environmentErrors = append(environmentErrors, asyncClickhouseActiveSpoolBacklogWarnAfterDaysErr)
			}
			useMemoryPressureAsyncClickhouseSpoolOverload, useMemoryPressureAsyncClickhouseSpoolOverloadErr := env.GetEnvBool("HETEROLOG_USE_MEMORY_PRESSURE_ASYNC_CLICKHOUSE_SPOOL_OVERLOAD", failure.DefaultUseMemoryPressureForOverload)
			if useMemoryPressureAsyncClickhouseSpoolOverloadErr != nil {
				environmentErrors = append(environmentErrors, useMemoryPressureAsyncClickhouseSpoolOverloadErr)
			}
			return failure.SpoolConfig{
				Dir: env.GetEnvString("HETEROLOG_ASYNC_CLICKHOUSE_FAILURE_SPOOL_DIR", clickhousecfg.DefaultAsyncClickHouseFailureSpoolDir),
				OverloadPolicyConfig: failure.SpoolOverloadPolicyConfig{
					OverloadPolicy:               failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_ASYNC_CLICKHOUSE_SPOOL_OVERLOAD_POLICY", string(failure.DefaultSpoolOverloadPolicy)), failure.DefaultSpoolOverloadPolicy),
					SpillDestination:             failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_ASYNC_CLICKHOUSE_SPOOL_SECONDARY_DESTINATION", string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
					MaxCallsSinceScan:            maxAsyncClickhouseSpoolScannedCacheUsageCount,
					StatsScanInterval:            asyncClickhouseSpoolCacheLifetimeSecond,
					MaxDirSize:                   maxAsyncClickhouseSpoolFileActiveDirSizeMb,
					ActiveBacklogWarnAfterDays:   asyncClickhouseActiveSpoolBacklogWarnAfterDays,
					UseMemoryPressureForOverload: useMemoryPressureAsyncClickhouseSpoolOverload,
				},
				HandlerConfig: internalerror.RoutingConfig{
					OnErrorConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_CLICKHOUSE_SPOOL_FILE_ERROR_HANDLER", nil),
					OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_CLICKHOUSE_SPOOL_FILE_WARN_HANDLER", nil),
					OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_ASYNC_CLICKHOUSE_SPOOL_FILE_INFO_HANDLER", nil),
					OnDebugConfig: env.GetEnvSliceString("HETEROLOG_ASYNC_CLICKHOUSE_SPOOL_FILE_DEBUG_HANDLER", nil),
				},
			}, errors.Join(environmentErrors...)
		},
	)
	cfg.Fsstore = failure.FailureFileSystemStoreConfig{
		FileArchiveConfig: fileArchiveConfig,
		SpoolConfig:       spoolConfig,
	}
	return cfg, errors.Join(selectionErr, fileArchiveErr, spoolErr)
}

func loadDeliveryClickHouseFailureHandlerConfig(env pkgenv.Lookup) (failure.DeliveryFailureHandlerConfig, error) {
	cfg, selectionErr := loadDeliveryFailureHandlerSelection(
		env,
		"HETEROLOG_ENABLE_DELIVERY_CLICKHOUSE_DEGRADE", failure.DefaultEnableDegrade,
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_TYPES", failure.DefaultDeliveryDurableTypes,
		"HETEROLOG_DELIVERY_CLICKHOUSE_DEGRADE_TYPES", failure.DefaultSpillTypes,
	)
	fileArchiveConfig, fileArchiveErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeFsstore),
		func() (failure.DeliveryFileArchiveConfig, error) {
			var environmentErrors []error
			maxDeliveryClickhouseDurableFileArchiveSizeMb, maxDeliveryClickhouseDurableFileArchiveSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_SIZE_MB", failure.DefaultFileArchiveMaxSizeMB, BytesPerMegabyte)
			if maxDeliveryClickhouseDurableFileArchiveSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxDeliveryClickhouseDurableFileArchiveSizeMbErr)
			}
			deliveryClickhouseEnableDurableFileArchiveRotate, deliveryClickhouseEnableDurableFileArchiveRotateErr := env.GetEnvBool("HETEROLOG_DELIVERY_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE", failure.DefaultFileArchiveRotateEnabled)
			if deliveryClickhouseEnableDurableFileArchiveRotateErr != nil {
				environmentErrors = append(environmentErrors, deliveryClickhouseEnableDurableFileArchiveRotateErr)
			}
			maxDeliveryClickhouseEnableDurableFileArchiveBackups, maxDeliveryClickhouseEnableDurableFileArchiveBackupsErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS", failure.DefaultFileArchiveMaxBackups)
			if maxDeliveryClickhouseEnableDurableFileArchiveBackupsErr != nil {
				environmentErrors = append(environmentErrors, maxDeliveryClickhouseEnableDurableFileArchiveBackupsErr)
			}
			maxDeliveryClickhouseEnableDurableFileArchiveAge, maxDeliveryClickhouseEnableDurableFileArchiveAgeErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_AGE", failure.DefaultFileArchiveMaxAge)
			if maxDeliveryClickhouseEnableDurableFileArchiveAgeErr != nil {
				environmentErrors = append(environmentErrors, maxDeliveryClickhouseEnableDurableFileArchiveAgeErr)
			}
			maxDeliveryClickhouseEnableDurableFileArchiveDirSizeMb, maxDeliveryClickhouseEnableDurableFileArchiveDirSizeMbErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_DELIVERY_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB", failure.DefaultFileArchiveMaxDirSizeMB, BytesPerMegabyte)
			if maxDeliveryClickhouseEnableDurableFileArchiveDirSizeMbErr != nil {
				environmentErrors = append(environmentErrors, maxDeliveryClickhouseEnableDurableFileArchiveDirSizeMbErr)
			}
			enableDeliveryClickhouseDurableFileArchiveCompress, enableDeliveryClickhouseDurableFileArchiveCompressErr := env.GetEnvBool("HETEROLOG_ENABLE_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_COMPRESS", failure.DefaultFileArchiveCompressEnabled)
			if enableDeliveryClickhouseDurableFileArchiveCompressErr != nil {
				environmentErrors = append(environmentErrors, enableDeliveryClickhouseDurableFileArchiveCompressErr)
			}
			compressFormat, compressFormatErr := loadLogCompressFormat(
				env.GetEnvString("HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT", string(failure.DefaultFileArchiveCompressFormat)),
				"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT",
				failure.DefaultFileArchiveCompressFormat,
			)
			if compressFormatErr != nil {
				environmentErrors = append(environmentErrors, compressFormatErr)
			}
			objectStorageUploadConfig, objectStorageUploadConfigErr := loadDeliveryClickHouseFailureArchiveObjectStorageUploadConfig(env)
			if objectStorageUploadConfigErr != nil {
				environmentErrors = append(environmentErrors, objectStorageUploadConfigErr)
			}
			return failure.DeliveryFileArchiveConfig{
				MaxFileSize:                maxDeliveryClickhouseDurableFileArchiveSizeMb,
				LifecycleMode:              logconfig.ParseLogFileLifecycleMode(env.GetEnvString("HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE", string(failure.DefaultFileArchiveLifecycleMode)), failure.DefaultFileArchiveLifecycleMode),
				Dir:                        env.GetEnvString("HETEROLOG_DELIVERY_CLICKHOUSE_FAILURE_ARCHIVE_DIR", clickhousecfg.DefaultDeliveryClickHouseFailureArchiveDir),
				DirCaseRetryable:           env.GetEnvString("HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_RETRYABLE_DIR", failure.DirCaseRetryable),
				DirCasePermanent:           env.GetEnvString("HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_PERMANENT_DIR", failure.DirCasePermanent),
				EnableRotate:               deliveryClickhouseEnableDurableFileArchiveRotate,
				MaxFileBackups:             maxDeliveryClickhouseEnableDurableFileArchiveBackups,
				MaxFileAge:                 maxDeliveryClickhouseEnableDurableFileArchiveAge,
				MaxDirSize:                 maxDeliveryClickhouseEnableDurableFileArchiveDirSizeMb,
				EnableFileCompress:         enableDeliveryClickhouseDurableFileArchiveCompress,
				CompressFormat:             compressFormat,
				FileRotation:               loadLogRotation(env.GetEnvString("HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_ROTATION", string(failure.DefaultFileArchiveRotation)), failure.DefaultFileArchiveRotation),
				FileRotationDayOfWeek:      failure.DefaultFileArchiveRotationDayOfWeek,
				MaxWriteRetries:            fsstoreconfig.MaxFileOperationRetries,
				FileOperationRetryWaitTime: fsstoreconfig.FileOperationRetryWaitTime,
				ObjectStorageUploadConfig:  objectStorageUploadConfig,
				OverloadPolicyConfig: failure.FileArchiveOverloadPolicyConfig{
					OverloadPolicy:   failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY", string(failure.OverloadPolicyDrop)), failure.OverloadPolicyDrop),
					SpillDestination: failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION", string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
				},
				HandlerConfig: internalerror.RoutingConfig{
					OnErrorConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ERROR_HANDLER", nil),
					OnWarnConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_WARN_HANDLER", nil),
					OnInfoConfig:  env.GetEnvSliceString("HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_INFO_HANDLER", nil),
					OnDebugConfig: env.GetEnvSliceString("HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_DEBUG_HANDLER", nil),
				},
			}, errors.Join(environmentErrors...)
		},
	)
	cfg.FileArchiveConfig = fileArchiveConfig
	return cfg, errors.Join(selectionErr, fileArchiveErr)
}

func loadClickHouseFailureArchiveObjectStorageUploadConfig(env pkgenv.Lookup) (objectstorage.UploadTargetConfig, error) {
	return loadFailureArchiveObjectStorageUploadConfig(
		env,
		"HETEROLOG_ENABLE_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD",
		"HETEROLOG_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE",
		"HETEROLOG_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET",
		"HETEROLOG_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX",
		"HETEROLOG_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_DESTINATIONS",
	)
}

func loadAsyncClickHouseFailureArchiveObjectStorageUploadConfig(env pkgenv.Lookup) (objectstorage.UploadTargetConfig, error) {
	return loadFailureArchiveObjectStorageUploadConfig(
		env,
		"HETEROLOG_ENABLE_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_DESTINATIONS",
	)
}

func loadDeliveryClickHouseFailureArchiveObjectStorageUploadConfig(env pkgenv.Lookup) (objectstorage.UploadTargetConfig, error) {
	return loadFailureArchiveObjectStorageUploadConfig(
		env,
		"HETEROLOG_ENABLE_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_DESTINATIONS",
	)
}

// retry client modeはretry／circuit breaker構成を単一値で選択します。固定ID createの
// 409成功化はOpenSearch backendの結果分類が担当するため、このmodeには含めません。
// circuit breakerを含むmodeではopen中の接続を抑制し、cooldown後の1件だけをprobeとして通します。
func loadOpenSearchRetryableClientConfig(env pkgenv.Lookup) (logconfig.OpenSearchRetryAbleClientConfig, error) {
	mode, modeErr := loadRetryClientMode(
		env.GetEnvString("HETEROLOG_OPENSEARCH_RETRY_CLIENT_MODE", string(opensearch.DefaultRetryClientMode)),
		"HETEROLOG_OPENSEARCH_RETRY_CLIENT_MODE",
		pkglogger.RetryClientMode(opensearch.DefaultRetryClientMode),
	)
	maxRetries, maxRetriesErr := env.GetEnvInt("HETEROLOG_MAX_OPENSEARCH_RETRYABLE_CLIENT_RETRIES", opensearch.DefaultMaxRetries)
	baseDelay, baseDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_OPENSEARCH_RETRYABLE_CLIENT_MILLISECOND", opensearch.DefaultBaseDelay)
	maxDelay, maxDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_MAX_OPENSEARCH_RETRYABLE_CLIENT_DELAY_MILLISECOND", opensearch.DefaultMaxDelay)
	return logconfig.OpenSearchRetryAbleClientConfig{
		RetryClientMode: mode,
		MaxRetries:      maxRetries,
		BaseDelay:       baseDelay,
		MaxDelay:        maxDelay,
	}, errors.Join(modeErr, maxRetriesErr, baseDelayErr, maxDelayErr)
}

func loadAsyncOpenSearchRetryableClientConfig(env pkgenv.Lookup) (logconfig.OpenSearchRetryAbleClientConfig, error) {
	mode, modeErr := loadRetryClientMode(
		env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_RETRY_CLIENT_MODE", string(opensearch.DefaultRetryClientMode)),
		"HETEROLOG_ASYNC_OPENSEARCH_RETRY_CLIENT_MODE",
		pkglogger.RetryClientMode(opensearch.DefaultRetryClientMode),
	)
	maxRetries, maxRetriesErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_OPENSEARCH_RETRYABLE_CLIENT_RETRIES", opensearch.DefaultMaxRetries)
	baseDelay, baseDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_ASYNC_OPENSEARCH_RETRYABLE_CLIENT_MILLISECOND", opensearch.DefaultBaseDelay)
	maxDelay, maxDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_MAX_ASYNC_OPENSEARCH_RETRYABLE_CLIENT_DELAY_MILLISECOND", opensearch.DefaultMaxDelay)
	return logconfig.OpenSearchRetryAbleClientConfig{
		RetryClientMode: mode,
		MaxRetries:      maxRetries,
		BaseDelay:       baseDelay,
		MaxDelay:        maxDelay,
	}, errors.Join(modeErr, maxRetriesErr, baseDelayErr, maxDelayErr)
}

func loadDeliveryOpenSearchRetryableClientConfig(env pkgenv.Lookup) (logconfig.OpenSearchRetryAbleClientConfig, error) {
	mode, modeErr := loadRetryClientMode(
		env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_RETRY_CLIENT_MODE", string(opensearch.DefaultRetryClientMode)),
		"HETEROLOG_DELIVERY_OPENSEARCH_RETRY_CLIENT_MODE",
		pkglogger.RetryClientMode(opensearch.DefaultRetryClientMode),
	)
	maxRetries, maxRetriesErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_OPENSEARCH_RETRYABLE_CLIENT_RETRIES", opensearch.DefaultMaxRetries)
	baseDelay, baseDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_DELIVERY_OPENSEARCH_RETRYABLE_CLIENT_MILLISECOND", opensearch.DefaultBaseDelay)
	maxDelay, maxDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_MAX_DELIVERY_OPENSEARCH_RETRYABLE_CLIENT_DELAY_MILLISECOND", opensearch.DefaultMaxDelay)
	return logconfig.OpenSearchRetryAbleClientConfig{
		RetryClientMode: mode,
		MaxRetries:      maxRetries,
		BaseDelay:       baseDelay,
		MaxDelay:        maxDelay,
	}, errors.Join(modeErr, maxRetriesErr, baseDelayErr, maxDelayErr)
}

func loadLokiRetryableClientConfig(env pkgenv.Lookup) (logconfig.LokiRetryAbleClientConfig, error) {
	mode, modeErr := loadRetryClientMode(
		env.GetEnvString("HETEROLOG_LOKI_RETRY_CLIENT_MODE", string(lokicfg.DefaultRetryClientMode)),
		"HETEROLOG_LOKI_RETRY_CLIENT_MODE",
		pkglogger.RetryClientMode(lokicfg.DefaultRetryClientMode),
	)
	maxRetries, maxRetriesErr := env.GetEnvInt("HETEROLOG_MAX_LOKI_RETRYABLE_CLIENT_RETRIES", lokicfg.DefaultMaxRetries)
	baseDelay, baseDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_LOKI_RETRYABLE_CLIENT_MILLISECOND", lokicfg.DefaultBaseDelay)
	maxDelay, maxDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_MAX_LOKI_RETRYABLE_CLIENT_DELAY_MILLISECOND", lokicfg.DefaultMaxDelay)
	return logconfig.LokiRetryAbleClientConfig{
		RetryClientMode: mode,
		MaxRetries:      maxRetries,
		BaseDelay:       baseDelay,
		MaxDelay:        maxDelay,
	}, errors.Join(modeErr, maxRetriesErr, baseDelayErr, maxDelayErr)
}

func loadAsyncLokiRetryableClientConfig(env pkgenv.Lookup) (logconfig.LokiRetryAbleClientConfig, error) {
	mode, modeErr := loadRetryClientMode(
		env.GetEnvString("HETEROLOG_ASYNC_LOKI_RETRY_CLIENT_MODE", string(lokicfg.DefaultRetryClientMode)),
		"HETEROLOG_ASYNC_LOKI_RETRY_CLIENT_MODE",
		pkglogger.RetryClientMode(lokicfg.DefaultRetryClientMode),
	)
	maxRetries, maxRetriesErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_LOKI_RETRYABLE_CLIENT_RETRIES", lokicfg.DefaultMaxRetries)
	baseDelay, baseDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_ASYNC_LOKI_RETRYABLE_CLIENT_MILLISECOND", lokicfg.DefaultBaseDelay)
	maxDelay, maxDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_MAX_ASYNC_LOKI_RETRYABLE_CLIENT_DELAY_MILLISECOND", lokicfg.DefaultMaxDelay)
	return logconfig.LokiRetryAbleClientConfig{
		RetryClientMode: mode,
		MaxRetries:      maxRetries,
		BaseDelay:       baseDelay,
		MaxDelay:        maxDelay,
	}, errors.Join(modeErr, maxRetriesErr, baseDelayErr, maxDelayErr)
}

func loadDeliveryLokiRetryableClientConfig(env pkgenv.Lookup) (logconfig.LokiRetryAbleClientConfig, error) {
	mode, modeErr := loadRetryClientMode(
		env.GetEnvString("HETEROLOG_DELIVERY_LOKI_RETRY_CLIENT_MODE", string(lokicfg.DefaultRetryClientMode)),
		"HETEROLOG_DELIVERY_LOKI_RETRY_CLIENT_MODE",
		pkglogger.RetryClientMode(lokicfg.DefaultRetryClientMode),
	)
	maxRetries, maxRetriesErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_LOKI_RETRYABLE_CLIENT_RETRIES", lokicfg.DefaultMaxRetries)
	baseDelay, baseDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_DELIVERY_LOKI_RETRYABLE_CLIENT_MILLISECOND", lokicfg.DefaultBaseDelay)
	maxDelay, maxDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_MAX_DELIVERY_LOKI_RETRYABLE_CLIENT_DELAY_MILLISECOND", lokicfg.DefaultMaxDelay)
	return logconfig.LokiRetryAbleClientConfig{
		RetryClientMode: mode,
		MaxRetries:      maxRetries,
		BaseDelay:       baseDelay,
		MaxDelay:        maxDelay,
	}, errors.Join(modeErr, maxRetriesErr, baseDelayErr, maxDelayErr)
}

func loadCloudWatchLogsRetryableClientConfig(env pkgenv.Lookup) (logconfig.CloudWatchLogsRetryAbleClientConfig, error) {
	mode, modeErr := loadRetryClientMode(
		env.GetEnvString("HETEROLOG_CLOUDWATCH_RETRY_CLIENT_MODE", string(cloudwatchlogscfg.DefaultRetryClientMode)),
		"HETEROLOG_CLOUDWATCH_RETRY_CLIENT_MODE",
		pkglogger.RetryClientMode(cloudwatchlogscfg.DefaultRetryClientMode),
	)
	maxRetries, maxRetriesErr := env.GetEnvInt("HETEROLOG_MAX_CLOUDWATCH_RETRYABLE_CLIENT_RETRIES", cloudwatchlogscfg.DefaultMaxRetries)
	baseDelay, baseDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_CLOUDWATCH_RETRYABLE_CLIENT_MILLISECOND", cloudwatchlogscfg.DefaultBaseDelay)
	maxDelay, maxDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_MAX_CLOUDWATCH_RETRYABLE_CLIENT_DELAY_MILLISECOND", cloudwatchlogscfg.DefaultMaxDelay)
	return logconfig.CloudWatchLogsRetryAbleClientConfig{
		RetryClientMode: mode,
		MaxRetries:      maxRetries,
		BaseDelay:       baseDelay,
		MaxDelay:        maxDelay,
	}, errors.Join(modeErr, maxRetriesErr, baseDelayErr, maxDelayErr)
}

func loadAsyncCloudWatchLogsRetryableClientConfig(env pkgenv.Lookup) (logconfig.CloudWatchLogsRetryAbleClientConfig, error) {
	mode, modeErr := loadRetryClientMode(
		env.GetEnvString("HETEROLOG_ASYNC_CLOUDWATCH_RETRY_CLIENT_MODE", string(cloudwatchlogscfg.DefaultRetryClientMode)),
		"HETEROLOG_ASYNC_CLOUDWATCH_RETRY_CLIENT_MODE",
		pkglogger.RetryClientMode(cloudwatchlogscfg.DefaultRetryClientMode),
	)
	maxRetries, maxRetriesErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_CLOUDWATCH_RETRYABLE_CLIENT_RETRIES", cloudwatchlogscfg.DefaultMaxRetries)
	baseDelay, baseDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_ASYNC_CLOUDWATCH_RETRYABLE_CLIENT_MILLISECOND", cloudwatchlogscfg.DefaultBaseDelay)
	maxDelay, maxDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_MAX_ASYNC_CLOUDWATCH_RETRYABLE_CLIENT_DELAY_MILLISECOND", cloudwatchlogscfg.DefaultMaxDelay)
	return logconfig.CloudWatchLogsRetryAbleClientConfig{
		RetryClientMode: mode,
		MaxRetries:      maxRetries,
		BaseDelay:       baseDelay,
		MaxDelay:        maxDelay,
	}, errors.Join(modeErr, maxRetriesErr, baseDelayErr, maxDelayErr)
}

func loadDeliveryCloudWatchLogsRetryableClientConfig(env pkgenv.Lookup) (logconfig.CloudWatchLogsRetryAbleClientConfig, error) {
	mode, modeErr := loadRetryClientMode(
		env.GetEnvString("HETEROLOG_DELIVERY_CLOUDWATCH_RETRY_CLIENT_MODE", string(cloudwatchlogscfg.DefaultRetryClientMode)),
		"HETEROLOG_DELIVERY_CLOUDWATCH_RETRY_CLIENT_MODE",
		pkglogger.RetryClientMode(cloudwatchlogscfg.DefaultRetryClientMode),
	)
	maxRetries, maxRetriesErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_CLOUDWATCH_RETRYABLE_CLIENT_RETRIES", cloudwatchlogscfg.DefaultMaxRetries)
	baseDelay, baseDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_DELIVERY_CLOUDWATCH_RETRYABLE_CLIENT_MILLISECOND", cloudwatchlogscfg.DefaultBaseDelay)
	maxDelay, maxDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_MAX_DELIVERY_CLOUDWATCH_RETRYABLE_CLIENT_DELAY_MILLISECOND", cloudwatchlogscfg.DefaultMaxDelay)
	return logconfig.CloudWatchLogsRetryAbleClientConfig{
		RetryClientMode: mode,
		MaxRetries:      maxRetries,
		BaseDelay:       baseDelay,
		MaxDelay:        maxDelay,
	}, errors.Join(modeErr, maxRetriesErr, baseDelayErr, maxDelayErr)
}

func loadClickHouseRetryableClientConfig(env pkgenv.Lookup) (logconfig.ClickHouseRetryAbleClientConfig, error) {
	mode, modeErr := loadRetryClientMode(
		env.GetEnvString("HETEROLOG_CLICKHOUSE_RETRY_CLIENT_MODE", string(clickhousecfg.DefaultRetryClientMode)),
		"HETEROLOG_CLICKHOUSE_RETRY_CLIENT_MODE",
		pkglogger.RetryClientMode(clickhousecfg.DefaultRetryClientMode),
	)
	maxRetries, maxRetriesErr := env.GetEnvInt("HETEROLOG_MAX_CLICKHOUSE_RETRYABLE_CLIENT_RETRIES", clickhousecfg.DefaultMaxRetries)
	baseDelay, baseDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_CLICKHOUSE_RETRYABLE_CLIENT_MILLISECOND", clickhousecfg.DefaultBaseDelay)
	maxDelay, maxDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_MAX_CLICKHOUSE_RETRYABLE_CLIENT_DELAY_MILLISECOND", clickhousecfg.DefaultMaxDelay)
	return logconfig.ClickHouseRetryAbleClientConfig{
		RetryClientMode: mode,
		MaxRetries:      maxRetries,
		BaseDelay:       baseDelay,
		MaxDelay:        maxDelay,
	}, errors.Join(modeErr, maxRetriesErr, baseDelayErr, maxDelayErr)
}

func loadAsyncClickHouseRetryableClientConfig(env pkgenv.Lookup) (logconfig.ClickHouseRetryAbleClientConfig, error) {
	mode, modeErr := loadRetryClientMode(
		env.GetEnvString("HETEROLOG_ASYNC_CLICKHOUSE_RETRY_CLIENT_MODE", string(clickhousecfg.DefaultRetryClientMode)),
		"HETEROLOG_ASYNC_CLICKHOUSE_RETRY_CLIENT_MODE",
		pkglogger.RetryClientMode(clickhousecfg.DefaultRetryClientMode),
	)
	maxRetries, maxRetriesErr := env.GetEnvInt("HETEROLOG_MAX_ASYNC_CLICKHOUSE_RETRYABLE_CLIENT_RETRIES", clickhousecfg.DefaultMaxRetries)
	baseDelay, baseDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_ASYNC_CLICKHOUSE_RETRYABLE_CLIENT_MILLISECOND", clickhousecfg.DefaultBaseDelay)
	maxDelay, maxDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_MAX_ASYNC_CLICKHOUSE_RETRYABLE_CLIENT_DELAY_MILLISECOND", clickhousecfg.DefaultMaxDelay)
	return logconfig.ClickHouseRetryAbleClientConfig{
		RetryClientMode: mode,
		MaxRetries:      maxRetries,
		BaseDelay:       baseDelay,
		MaxDelay:        maxDelay,
	}, errors.Join(modeErr, maxRetriesErr, baseDelayErr, maxDelayErr)
}

func loadDeliveryClickHouseRetryableClientConfig(env pkgenv.Lookup) (logconfig.ClickHouseRetryAbleClientConfig, error) {
	mode, modeErr := loadRetryClientMode(
		env.GetEnvString("HETEROLOG_DELIVERY_CLICKHOUSE_RETRY_CLIENT_MODE", string(clickhousecfg.DefaultRetryClientMode)),
		"HETEROLOG_DELIVERY_CLICKHOUSE_RETRY_CLIENT_MODE",
		pkglogger.RetryClientMode(clickhousecfg.DefaultRetryClientMode),
	)
	maxRetries, maxRetriesErr := env.GetEnvInt("HETEROLOG_MAX_DELIVERY_CLICKHOUSE_RETRYABLE_CLIENT_RETRIES", clickhousecfg.DefaultMaxRetries)
	baseDelay, baseDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_DELIVERY_CLICKHOUSE_RETRYABLE_CLIENT_MILLISECOND", clickhousecfg.DefaultBaseDelay)
	maxDelay, maxDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_MAX_DELIVERY_CLICKHOUSE_RETRYABLE_CLIENT_DELAY_MILLISECOND", clickhousecfg.DefaultMaxDelay)
	return logconfig.ClickHouseRetryAbleClientConfig{
		RetryClientMode: mode,
		MaxRetries:      maxRetries,
		BaseDelay:       baseDelay,
		MaxDelay:        maxDelay,
	}, errors.Join(modeErr, maxRetriesErr, baseDelayErr, maxDelayErr)
}

// Slack設定を読み込む
func loadSlackAppConfig() slack.SlackConfig {
	return slack.SlackConfig{
		SlackPostWarnThresholdDuration:  slack.SlackPostWarnThresholdDuration,
		SlackPostErrorThresholdDuration: slack.SlackPostErrorThresholdDuration,
	}
}

// OpenSearch設定を読み込む
func loadOpenSearchClientConfig(env pkgenv.Lookup) (opensearch.OpenSearchClientConfig, error) {
	var environmentErrors []error
	opensearchTimeoutSecond, opensearchTimeoutSecondErr := env.GetEnvDuration("HETEROLOG_OPENSEARCH_TIMEOUT_SECOND", opensearch.DefaultOpenSearchTimeout)
	if opensearchTimeoutSecondErr != nil {
		environmentErrors = append(environmentErrors, opensearchTimeoutSecondErr)
	}
	opensearchSkipVerifySsl, opensearchSkipVerifySslErr := env.GetEnvBool("HETEROLOG_OPENSEARCH_SKIP_VERIFY_SSL", opensearch.DefaultOpenSearchSkipVerify)
	if opensearchSkipVerifySslErr != nil {
		environmentErrors = append(environmentErrors, opensearchSkipVerifySslErr)
	}
	enableOpensearchCompressRequest, enableOpensearchCompressRequestErr := env.GetEnvBool("HETEROLOG_ENABLE_OPENSEARCH_COMPRESS_REQUEST", opensearch.DefaultCompressRequest)
	if enableOpensearchCompressRequestErr != nil {
		environmentErrors = append(environmentErrors, enableOpensearchCompressRequestErr)
	}
	return opensearch.OpenSearchClientConfig{
		Endpoint:        env.GetEnvString("HETEROLOG_OPENSEARCH_ENDPOINT", opensearch.DefaultOpenSearchEndpoint),
		Timeout:         opensearchTimeoutSecond,
		SkipVerifySSL:   opensearchSkipVerifySsl,
		Auth:            buildOpenSearchAuthConfig(env),
		CompressRequest: enableOpensearchCompressRequest,
	}, errors.Join(environmentErrors...)
}

// 非同期OpenSearch設定を読み込む
func loadAsyncOpenSearchClientConfig(env pkgenv.Lookup) (opensearch.OpenSearchClientConfig, error) {
	var environmentErrors []error
	asyncOpensearchTimeoutSecond, asyncOpensearchTimeoutSecondErr := env.GetEnvDuration("HETEROLOG_ASYNC_OPENSEARCH_TIMEOUT_SECOND", opensearch.DefaultOpenSearchTimeout)
	if asyncOpensearchTimeoutSecondErr != nil {
		environmentErrors = append(environmentErrors, asyncOpensearchTimeoutSecondErr)
	}
	asyncOpensearchSkipVerifySsl, asyncOpensearchSkipVerifySslErr := env.GetEnvBool("HETEROLOG_ASYNC_OPENSEARCH_SKIP_VERIFY_SSL", opensearch.DefaultOpenSearchSkipVerify)
	if asyncOpensearchSkipVerifySslErr != nil {
		environmentErrors = append(environmentErrors, asyncOpensearchSkipVerifySslErr)
	}
	enableAsyncOpensearchCompressRequest, enableAsyncOpensearchCompressRequestErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_OPENSEARCH_COMPRESS_REQUEST", opensearch.DefaultCompressRequest)
	if enableAsyncOpensearchCompressRequestErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncOpensearchCompressRequestErr)
	}
	return opensearch.OpenSearchClientConfig{
		Endpoint:        env.GetEnvString("HETEROLOG_ASYNC_OPENSEARCH_ENDPOINT", opensearch.DefaultOpenSearchEndpoint),
		Timeout:         asyncOpensearchTimeoutSecond,
		SkipVerifySSL:   asyncOpensearchSkipVerifySsl,
		Auth:            buildAsyncOpenSearchAuthConfig(env),
		CompressRequest: enableAsyncOpensearchCompressRequest,
	}, errors.Join(environmentErrors...)
}

func loadDeliveryOpenSearchClientConfig(env pkgenv.Lookup) (opensearch.OpenSearchClientConfig, error) {
	var environmentErrors []error
	deliveryOpensearchTimeoutSecond, deliveryOpensearchTimeoutSecondErr := env.GetEnvDuration("HETEROLOG_DELIVERY_OPENSEARCH_TIMEOUT_SECOND", opensearch.DefaultOpenSearchTimeout)
	if deliveryOpensearchTimeoutSecondErr != nil {
		environmentErrors = append(environmentErrors, deliveryOpensearchTimeoutSecondErr)
	}
	deliveryOpensearchSkipVerifySsl, deliveryOpensearchSkipVerifySslErr := env.GetEnvBool("HETEROLOG_DELIVERY_OPENSEARCH_SKIP_VERIFY_SSL", opensearch.DefaultOpenSearchSkipVerify)
	if deliveryOpensearchSkipVerifySslErr != nil {
		environmentErrors = append(environmentErrors, deliveryOpensearchSkipVerifySslErr)
	}
	enableDeliveryOpensearchCompressRequest, enableDeliveryOpensearchCompressRequestErr := env.GetEnvBool("HETEROLOG_ENABLE_DELIVERY_OPENSEARCH_COMPRESS_REQUEST", opensearch.DefaultCompressRequest)
	if enableDeliveryOpensearchCompressRequestErr != nil {
		environmentErrors = append(environmentErrors, enableDeliveryOpensearchCompressRequestErr)
	}
	return opensearch.OpenSearchClientConfig{
		Endpoint:        env.GetEnvString("HETEROLOG_DELIVERY_OPENSEARCH_ENDPOINT", opensearch.DefaultOpenSearchEndpoint),
		Timeout:         deliveryOpensearchTimeoutSecond,
		SkipVerifySSL:   deliveryOpensearchSkipVerifySsl,
		Auth:            buildDeliveryOpenSearchAuthConfig(env),
		CompressRequest: enableDeliveryOpensearchCompressRequest,
	}, errors.Join(environmentErrors...)
}

func loadLokiClientConfig(env pkgenv.Lookup) (lokicfg.LokiClientConfig, error) {
	var environmentErrors []error
	username := env.GetEnvString("HETEROLOG_LOKI_USERNAME", lokicfg.DefaultUsername)
	password := env.GetEnvString("HETEROLOG_LOKI_PASSWORD", lokicfg.DefaultPassword)
	var basicAuth *lokicfg.BasicAuthConfig
	if username != "" || password != "" {
		basicAuth = &lokicfg.BasicAuthConfig{
			Username: username,
			Password: password,
		}
	}

	lokiTimeoutSecond, lokiTimeoutSecondErr := env.GetEnvDuration("HETEROLOG_LOKI_TIMEOUT_SECOND", lokicfg.DefaultTimeout)
	if lokiTimeoutSecondErr != nil {
		environmentErrors = append(environmentErrors, lokiTimeoutSecondErr)
	}
	lokiHeaders, lokiHeadersErr := loadHeaderEnv(env, "HETEROLOG_LOKI_HEADERS", nil)
	if lokiHeadersErr != nil {
		environmentErrors = append(environmentErrors, lokiHeadersErr)
	}
	enableLokiCompressRequest, enableLokiCompressRequestErr := env.GetEnvBool("HETEROLOG_ENABLE_LOKI_COMPRESS_REQUEST", lokicfg.DefaultCompressRequest)
	if enableLokiCompressRequestErr != nil {
		environmentErrors = append(environmentErrors, enableLokiCompressRequestErr)
	}
	return lokicfg.LokiClientConfig{
		Endpoint:        env.GetEnvString("HETEROLOG_LOKI_ENDPOINT", lokicfg.DefaultLokiEndpoint),
		Timeout:         lokiTimeoutSecond,
		TenantID:        env.GetEnvString("HETEROLOG_LOKI_TENANT_ID", lokicfg.DefaultLokiTenantID),
		BasicAuth:       basicAuth,
		BearerToken:     env.GetEnvString("HETEROLOG_LOKI_BEARER_TOKEN", lokicfg.DefaultBearerToken),
		Headers:         lokiHeaders,
		CompressRequest: enableLokiCompressRequest,
	}, errors.Join(environmentErrors...)
}

func loadAsyncLokiClientConfig(env pkgenv.Lookup) (lokicfg.LokiClientConfig, error) {
	var environmentErrors []error
	username := env.GetEnvString("HETEROLOG_ASYNC_LOKI_USERNAME", lokicfg.DefaultUsername)
	password := env.GetEnvString("HETEROLOG_ASYNC_LOKI_PASSWORD", lokicfg.DefaultPassword)
	var basicAuth *lokicfg.BasicAuthConfig
	if username != "" || password != "" {
		basicAuth = &lokicfg.BasicAuthConfig{
			Username: username,
			Password: password,
		}
	}

	asyncLokiTimeoutSecond, asyncLokiTimeoutSecondErr := env.GetEnvDuration("HETEROLOG_ASYNC_LOKI_TIMEOUT_SECOND", lokicfg.DefaultTimeout)
	if asyncLokiTimeoutSecondErr != nil {
		environmentErrors = append(environmentErrors, asyncLokiTimeoutSecondErr)
	}
	asyncLokiHeaders, asyncLokiHeadersErr := loadHeaderEnv(env, "HETEROLOG_ASYNC_LOKI_HEADERS", nil)
	if asyncLokiHeadersErr != nil {
		environmentErrors = append(environmentErrors, asyncLokiHeadersErr)
	}
	enableAsyncLokiCompressRequest, enableAsyncLokiCompressRequestErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_LOKI_COMPRESS_REQUEST", lokicfg.DefaultCompressRequest)
	if enableAsyncLokiCompressRequestErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncLokiCompressRequestErr)
	}
	return lokicfg.LokiClientConfig{
		Endpoint:        env.GetEnvString("HETEROLOG_ASYNC_LOKI_ENDPOINT", lokicfg.DefaultLokiEndpoint),
		Timeout:         asyncLokiTimeoutSecond,
		TenantID:        env.GetEnvString("HETEROLOG_ASYNC_LOKI_TENANT_ID", lokicfg.DefaultLokiTenantID),
		BasicAuth:       basicAuth,
		BearerToken:     env.GetEnvString("HETEROLOG_ASYNC_LOKI_BEARER_TOKEN", lokicfg.DefaultBearerToken),
		Headers:         asyncLokiHeaders,
		CompressRequest: enableAsyncLokiCompressRequest,
	}, errors.Join(environmentErrors...)
}

func loadDeliveryLokiClientConfig(env pkgenv.Lookup) (lokicfg.LokiClientConfig, error) {
	var environmentErrors []error
	username := env.GetEnvString("HETEROLOG_DELIVERY_LOKI_USERNAME", lokicfg.DefaultUsername)
	password := env.GetEnvString("HETEROLOG_DELIVERY_LOKI_PASSWORD", lokicfg.DefaultPassword)
	var basicAuth *lokicfg.BasicAuthConfig
	if username != "" || password != "" {
		basicAuth = &lokicfg.BasicAuthConfig{
			Username: username,
			Password: password,
		}
	}

	deliveryLokiTimeoutSecond, deliveryLokiTimeoutSecondErr := env.GetEnvDuration("HETEROLOG_DELIVERY_LOKI_TIMEOUT_SECOND", lokicfg.DefaultTimeout)
	if deliveryLokiTimeoutSecondErr != nil {
		environmentErrors = append(environmentErrors, deliveryLokiTimeoutSecondErr)
	}
	deliveryLokiHeaders, deliveryLokiHeadersErr := loadHeaderEnv(env, "HETEROLOG_DELIVERY_LOKI_HEADERS", nil)
	if deliveryLokiHeadersErr != nil {
		environmentErrors = append(environmentErrors, deliveryLokiHeadersErr)
	}
	enableDeliveryLokiCompressRequest, enableDeliveryLokiCompressRequestErr := env.GetEnvBool("HETEROLOG_ENABLE_DELIVERY_LOKI_COMPRESS_REQUEST", lokicfg.DefaultCompressRequest)
	if enableDeliveryLokiCompressRequestErr != nil {
		environmentErrors = append(environmentErrors, enableDeliveryLokiCompressRequestErr)
	}
	return lokicfg.LokiClientConfig{
		Endpoint:        env.GetEnvString("HETEROLOG_DELIVERY_LOKI_ENDPOINT", lokicfg.DefaultLokiEndpoint),
		Timeout:         deliveryLokiTimeoutSecond,
		TenantID:        env.GetEnvString("HETEROLOG_DELIVERY_LOKI_TENANT_ID", lokicfg.DefaultLokiTenantID),
		BasicAuth:       basicAuth,
		BearerToken:     env.GetEnvString("HETEROLOG_DELIVERY_LOKI_BEARER_TOKEN", lokicfg.DefaultBearerToken),
		Headers:         deliveryLokiHeaders,
		CompressRequest: enableDeliveryLokiCompressRequest,
	}, errors.Join(environmentErrors...)
}

func loadCloudWatchLogsClientConfig(env pkgenv.Lookup) (cloudwatchlogscfg.CloudWatchLogsClientConfig, error) {
	var environmentErrors []error
	cloudwatchTimeoutSecond, cloudwatchTimeoutSecondErr := env.GetEnvDuration("HETEROLOG_CLOUDWATCH_TIMEOUT_SECOND", cloudwatchlogscfg.DefaultTimeout)
	if cloudwatchTimeoutSecondErr != nil {
		environmentErrors = append(environmentErrors, cloudwatchTimeoutSecondErr)
	}
	cloudwatchHeaders, cloudwatchHeadersErr := loadHeaderEnv(env, "HETEROLOG_CLOUDWATCH_HEADERS", nil)
	if cloudwatchHeadersErr != nil {
		environmentErrors = append(environmentErrors, cloudwatchHeadersErr)
	}
	enableCloudwatchAutoCreateLogStream, enableCloudwatchAutoCreateLogStreamErr := env.GetEnvBool("HETEROLOG_ENABLE_CLOUDWATCH_AUTO_CREATE_LOG_STREAM", cloudwatchlogscfg.DefaultAutoCreateLogStream)
	if enableCloudwatchAutoCreateLogStreamErr != nil {
		environmentErrors = append(environmentErrors, enableCloudwatchAutoCreateLogStreamErr)
	}
	enableCloudwatchCompressRequest, enableCloudwatchCompressRequestErr := env.GetEnvBool("HETEROLOG_ENABLE_CLOUDWATCH_COMPRESS_REQUEST", cloudwatchlogscfg.DefaultCompressRequest)
	if enableCloudwatchCompressRequestErr != nil {
		environmentErrors = append(environmentErrors, enableCloudwatchCompressRequestErr)
	}
	return cloudwatchlogscfg.CloudWatchLogsClientConfig{
		Region:              env.GetEnvString("HETEROLOG_CLOUDWATCH_REGION", env.GetEnvString("AWS_REGION", cloudwatchlogscfg.DefaultRegion)),
		Endpoint:            env.GetEnvString("HETEROLOG_CLOUDWATCH_ENDPOINT", cloudwatchlogscfg.DefaultEndpoint),
		LogGroupName:        env.GetEnvString("HETEROLOG_CLOUDWATCH_LOG_GROUP_NAME", cloudwatchlogscfg.DefaultLogGroupName),
		Timeout:             cloudwatchTimeoutSecond,
		AccessKeyID:         env.GetEnvString("HETEROLOG_CLOUDWATCH_AWS_ACCESS_KEY_ID", env.GetEnvString("AWS_ACCESS_KEY_ID", "")),
		SecretAccessKey:     env.GetEnvString("HETEROLOG_CLOUDWATCH_AWS_SECRET_ACCESS_KEY", env.GetEnvString("AWS_SECRET_ACCESS_KEY", "")),
		SessionToken:        env.GetEnvString("HETEROLOG_CLOUDWATCH_AWS_SESSION_TOKEN", env.GetEnvString("AWS_SESSION_TOKEN", "")),
		Headers:             cloudwatchHeaders,
		AutoCreateLogStream: enableCloudwatchAutoCreateLogStream,
		CompressRequest:     enableCloudwatchCompressRequest,
	}, errors.Join(environmentErrors...)
}

func loadAsyncCloudWatchLogsClientConfig(env pkgenv.Lookup) (cloudwatchlogscfg.CloudWatchLogsClientConfig, error) {
	var environmentErrors []error
	asyncCloudwatchTimeoutSecond, asyncCloudwatchTimeoutSecondErr := env.GetEnvDuration("HETEROLOG_ASYNC_CLOUDWATCH_TIMEOUT_SECOND", cloudwatchlogscfg.DefaultTimeout)
	if asyncCloudwatchTimeoutSecondErr != nil {
		environmentErrors = append(environmentErrors, asyncCloudwatchTimeoutSecondErr)
	}
	asyncCloudwatchHeaders, asyncCloudwatchHeadersErr := loadHeaderEnv(env, "HETEROLOG_ASYNC_CLOUDWATCH_HEADERS", nil)
	if asyncCloudwatchHeadersErr != nil {
		environmentErrors = append(environmentErrors, asyncCloudwatchHeadersErr)
	}
	enableAsyncCloudwatchAutoCreateLogStream, enableAsyncCloudwatchAutoCreateLogStreamErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_CLOUDWATCH_AUTO_CREATE_LOG_STREAM", cloudwatchlogscfg.DefaultAutoCreateLogStream)
	if enableAsyncCloudwatchAutoCreateLogStreamErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncCloudwatchAutoCreateLogStreamErr)
	}
	enableAsyncCloudwatchCompressRequest, enableAsyncCloudwatchCompressRequestErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_CLOUDWATCH_COMPRESS_REQUEST", cloudwatchlogscfg.DefaultCompressRequest)
	if enableAsyncCloudwatchCompressRequestErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncCloudwatchCompressRequestErr)
	}
	return cloudwatchlogscfg.CloudWatchLogsClientConfig{
		Region:              env.GetEnvString("HETEROLOG_ASYNC_CLOUDWATCH_REGION", env.GetEnvString("AWS_REGION", cloudwatchlogscfg.DefaultRegion)),
		Endpoint:            env.GetEnvString("HETEROLOG_ASYNC_CLOUDWATCH_ENDPOINT", cloudwatchlogscfg.DefaultEndpoint),
		LogGroupName:        env.GetEnvString("HETEROLOG_ASYNC_CLOUDWATCH_LOG_GROUP_NAME", cloudwatchlogscfg.DefaultLogGroupName),
		Timeout:             asyncCloudwatchTimeoutSecond,
		AccessKeyID:         env.GetEnvString("HETEROLOG_ASYNC_CLOUDWATCH_AWS_ACCESS_KEY_ID", env.GetEnvString("AWS_ACCESS_KEY_ID", "")),
		SecretAccessKey:     env.GetEnvString("HETEROLOG_ASYNC_CLOUDWATCH_AWS_SECRET_ACCESS_KEY", env.GetEnvString("AWS_SECRET_ACCESS_KEY", "")),
		SessionToken:        env.GetEnvString("HETEROLOG_ASYNC_CLOUDWATCH_AWS_SESSION_TOKEN", env.GetEnvString("AWS_SESSION_TOKEN", "")),
		Headers:             asyncCloudwatchHeaders,
		AutoCreateLogStream: enableAsyncCloudwatchAutoCreateLogStream,
		CompressRequest:     enableAsyncCloudwatchCompressRequest,
	}, errors.Join(environmentErrors...)
}

func loadDeliveryCloudWatchLogsClientConfig(env pkgenv.Lookup) (cloudwatchlogscfg.CloudWatchLogsClientConfig, error) {
	var environmentErrors []error
	deliveryCloudwatchTimeoutSecond, deliveryCloudwatchTimeoutSecondErr := env.GetEnvDuration("HETEROLOG_DELIVERY_CLOUDWATCH_TIMEOUT_SECOND", cloudwatchlogscfg.DefaultTimeout)
	if deliveryCloudwatchTimeoutSecondErr != nil {
		environmentErrors = append(environmentErrors, deliveryCloudwatchTimeoutSecondErr)
	}
	deliveryCloudwatchHeaders, deliveryCloudwatchHeadersErr := loadHeaderEnv(env, "HETEROLOG_DELIVERY_CLOUDWATCH_HEADERS", nil)
	if deliveryCloudwatchHeadersErr != nil {
		environmentErrors = append(environmentErrors, deliveryCloudwatchHeadersErr)
	}
	enableDeliveryCloudwatchAutoCreateLogStream, enableDeliveryCloudwatchAutoCreateLogStreamErr := env.GetEnvBool("HETEROLOG_ENABLE_DELIVERY_CLOUDWATCH_AUTO_CREATE_LOG_STREAM", cloudwatchlogscfg.DefaultAutoCreateLogStream)
	if enableDeliveryCloudwatchAutoCreateLogStreamErr != nil {
		environmentErrors = append(environmentErrors, enableDeliveryCloudwatchAutoCreateLogStreamErr)
	}
	enableDeliveryCloudwatchCompressRequest, enableDeliveryCloudwatchCompressRequestErr := env.GetEnvBool("HETEROLOG_ENABLE_DELIVERY_CLOUDWATCH_COMPRESS_REQUEST", cloudwatchlogscfg.DefaultCompressRequest)
	if enableDeliveryCloudwatchCompressRequestErr != nil {
		environmentErrors = append(environmentErrors, enableDeliveryCloudwatchCompressRequestErr)
	}
	return cloudwatchlogscfg.CloudWatchLogsClientConfig{
		Region:              env.GetEnvString("HETEROLOG_DELIVERY_CLOUDWATCH_REGION", env.GetEnvString("AWS_REGION", cloudwatchlogscfg.DefaultRegion)),
		Endpoint:            env.GetEnvString("HETEROLOG_DELIVERY_CLOUDWATCH_ENDPOINT", cloudwatchlogscfg.DefaultEndpoint),
		LogGroupName:        env.GetEnvString("HETEROLOG_DELIVERY_CLOUDWATCH_LOG_GROUP_NAME", cloudwatchlogscfg.DefaultLogGroupName),
		Timeout:             deliveryCloudwatchTimeoutSecond,
		AccessKeyID:         env.GetEnvString("HETEROLOG_DELIVERY_CLOUDWATCH_AWS_ACCESS_KEY_ID", env.GetEnvString("AWS_ACCESS_KEY_ID", "")),
		SecretAccessKey:     env.GetEnvString("HETEROLOG_DELIVERY_CLOUDWATCH_AWS_SECRET_ACCESS_KEY", env.GetEnvString("AWS_SECRET_ACCESS_KEY", "")),
		SessionToken:        env.GetEnvString("HETEROLOG_DELIVERY_CLOUDWATCH_AWS_SESSION_TOKEN", env.GetEnvString("AWS_SESSION_TOKEN", "")),
		Headers:             deliveryCloudwatchHeaders,
		AutoCreateLogStream: enableDeliveryCloudwatchAutoCreateLogStream,
		CompressRequest:     enableDeliveryCloudwatchCompressRequest,
	}, errors.Join(environmentErrors...)
}

func loadClickHouseClientConfig(env pkgenv.Lookup) (clickhousecfg.ClientConfig, error) {
	var environmentErrors []error
	clickhouseTimeoutSecond, clickhouseTimeoutSecondErr := env.GetEnvDuration("HETEROLOG_CLICKHOUSE_TIMEOUT_SECOND", clickhousecfg.DefaultTimeout)
	if clickhouseTimeoutSecondErr != nil {
		environmentErrors = append(environmentErrors, clickhouseTimeoutSecondErr)
	}
	clickhouseHeaders, clickhouseHeadersErr := loadHeaderEnv(env, "HETEROLOG_CLICKHOUSE_HEADERS", nil)
	if clickhouseHeadersErr != nil {
		environmentErrors = append(environmentErrors, clickhouseHeadersErr)
	}
	enableClickhouseCompressRequest, enableClickhouseCompressRequestErr := env.GetEnvBool("HETEROLOG_ENABLE_CLICKHOUSE_COMPRESS_REQUEST", clickhousecfg.DefaultCompressRequest)
	if enableClickhouseCompressRequestErr != nil {
		environmentErrors = append(environmentErrors, enableClickhouseCompressRequestErr)
	}
	return clickhousecfg.ClientConfig{
		Endpoint:        env.GetEnvString("HETEROLOG_CLICKHOUSE_ENDPOINT", clickhousecfg.DefaultEndpoint),
		Database:        env.GetEnvString("HETEROLOG_CLICKHOUSE_DATABASE", clickhousecfg.DefaultDatabase),
		Table:           env.GetEnvString("HETEROLOG_CLICKHOUSE_TABLE", clickhousecfg.DefaultTable),
		Timeout:         clickhouseTimeoutSecond,
		Username:        env.GetEnvString("HETEROLOG_CLICKHOUSE_USERNAME", clickhousecfg.DefaultUsername),
		Password:        env.GetEnvString("HETEROLOG_CLICKHOUSE_PASSWORD", clickhousecfg.DefaultPassword),
		Headers:         clickhouseHeaders,
		CompressRequest: enableClickhouseCompressRequest,
	}, errors.Join(environmentErrors...)
}

func loadAsyncClickHouseClientConfig(env pkgenv.Lookup) (clickhousecfg.ClientConfig, error) {
	var environmentErrors []error
	asyncClickhouseTimeoutSecond, asyncClickhouseTimeoutSecondErr := env.GetEnvDuration("HETEROLOG_ASYNC_CLICKHOUSE_TIMEOUT_SECOND", clickhousecfg.DefaultTimeout)
	if asyncClickhouseTimeoutSecondErr != nil {
		environmentErrors = append(environmentErrors, asyncClickhouseTimeoutSecondErr)
	}
	asyncClickhouseHeaders, asyncClickhouseHeadersErr := loadHeaderEnv(env, "HETEROLOG_ASYNC_CLICKHOUSE_HEADERS", nil)
	if asyncClickhouseHeadersErr != nil {
		environmentErrors = append(environmentErrors, asyncClickhouseHeadersErr)
	}
	enableAsyncClickhouseCompressRequest, enableAsyncClickhouseCompressRequestErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_CLICKHOUSE_COMPRESS_REQUEST", clickhousecfg.DefaultCompressRequest)
	if enableAsyncClickhouseCompressRequestErr != nil {
		environmentErrors = append(environmentErrors, enableAsyncClickhouseCompressRequestErr)
	}
	return clickhousecfg.ClientConfig{
		Endpoint:        env.GetEnvString("HETEROLOG_ASYNC_CLICKHOUSE_ENDPOINT", clickhousecfg.DefaultEndpoint),
		Database:        env.GetEnvString("HETEROLOG_ASYNC_CLICKHOUSE_DATABASE", clickhousecfg.DefaultDatabase),
		Table:           env.GetEnvString("HETEROLOG_ASYNC_CLICKHOUSE_TABLE", clickhousecfg.DefaultTable),
		Timeout:         asyncClickhouseTimeoutSecond,
		Username:        env.GetEnvString("HETEROLOG_ASYNC_CLICKHOUSE_USERNAME", clickhousecfg.DefaultUsername),
		Password:        env.GetEnvString("HETEROLOG_ASYNC_CLICKHOUSE_PASSWORD", clickhousecfg.DefaultPassword),
		Headers:         asyncClickhouseHeaders,
		CompressRequest: enableAsyncClickhouseCompressRequest,
	}, errors.Join(environmentErrors...)
}

func loadDeliveryClickHouseClientConfig(env pkgenv.Lookup) (clickhousecfg.ClientConfig, error) {
	var environmentErrors []error
	deliveryClickhouseTimeoutSecond, deliveryClickhouseTimeoutSecondErr := env.GetEnvDuration("HETEROLOG_DELIVERY_CLICKHOUSE_TIMEOUT_SECOND", clickhousecfg.DefaultTimeout)
	if deliveryClickhouseTimeoutSecondErr != nil {
		environmentErrors = append(environmentErrors, deliveryClickhouseTimeoutSecondErr)
	}
	deliveryClickhouseHeaders, deliveryClickhouseHeadersErr := loadHeaderEnv(env, "HETEROLOG_DELIVERY_CLICKHOUSE_HEADERS", nil)
	if deliveryClickhouseHeadersErr != nil {
		environmentErrors = append(environmentErrors, deliveryClickhouseHeadersErr)
	}
	enableDeliveryClickhouseCompressRequest, enableDeliveryClickhouseCompressRequestErr := env.GetEnvBool("HETEROLOG_ENABLE_DELIVERY_CLICKHOUSE_COMPRESS_REQUEST", clickhousecfg.DefaultCompressRequest)
	if enableDeliveryClickhouseCompressRequestErr != nil {
		environmentErrors = append(environmentErrors, enableDeliveryClickhouseCompressRequestErr)
	}
	return clickhousecfg.ClientConfig{
		Endpoint:        env.GetEnvString("HETEROLOG_DELIVERY_CLICKHOUSE_ENDPOINT", clickhousecfg.DefaultEndpoint),
		Database:        env.GetEnvString("HETEROLOG_DELIVERY_CLICKHOUSE_DATABASE", clickhousecfg.DefaultDatabase),
		Table:           env.GetEnvString("HETEROLOG_DELIVERY_CLICKHOUSE_TABLE", clickhousecfg.DefaultTable),
		Timeout:         deliveryClickhouseTimeoutSecond,
		Username:        env.GetEnvString("HETEROLOG_DELIVERY_CLICKHOUSE_USERNAME", clickhousecfg.DefaultUsername),
		Password:        env.GetEnvString("HETEROLOG_DELIVERY_CLICKHOUSE_PASSWORD", clickhousecfg.DefaultPassword),
		Headers:         deliveryClickhouseHeaders,
		CompressRequest: enableDeliveryClickhouseCompressRequest,
	}, errors.Join(environmentErrors...)
}

// LoadConfigは、process environmentと、HETEROLOG_ENV_PATHが指す環境設定fileから
// アプリケーション全体の設定を読み込みます。HETEROLOG_ENV_PATHを明示した場合、そのfileを
// 読み込めない状態は設定ミスとして返します。省略した場合は現在のdirectoryの.envを読み、
// 存在しなければprocess environmentだけで読み込みます。process environmentは変更しません。
func LoadConfig() (AppConfig, error) {
	path, explicit := os.LookupEnv("HETEROLOG_ENV_PATH")
	if path == "" {
		path, explicit = ".env", false
	}
	source, err := newEnvSource(path, explicit)
	if err != nil {
		return AppConfig{}, err
	}
	return loadConfig(source)
}

// LoadConfigFileは、process environmentと、pathの環境設定fileからアプリケーション全体の
// 設定を読み込みます。pathが空ならprocess environmentだけを読み、HETEROLOG_ENV_PATHは
// 参照しません。fileを読み込めない場合はEnvFileErrorを返します。同じkeyがprocess
// environmentにもある場合は、process environmentの値を使います。process environmentは
// 変更しません。
func LoadConfigFile(path string) (AppConfig, error) {
	source, err := newEnvSource(path, true)
	if err != nil {
		return AppConfig{}, err
	}
	return loadConfig(source)
}

// loadConfigは、sourceからアプリケーション全体の設定を読み込みます。
func loadConfig(source envSource) (AppConfig, error) {
	env := pkgenv.Lookup(source.lookup)
	var errs []error
	// 綴りを修正した旧keyは別名として読まず、新しいkey名を示す設定エラーにします。
	errs = append(errs, validateRenamedEnvironmentKeys(source.keys()))
	runtimeConfig, runtimeErr := loadRuntimeConfig(env)
	fsstoreConfig, fsstoreErr := loadFsstoreConfig(env)
	appendLogRunnerConfig, appendLogRunnerErr := loadAppendLogRunnerConfig(env)
	archiveCheckpointAutoRecovery, archiveCheckpointAutoRecoveryErr := env.GetEnvBool(
		"HETEROLOG_ENABLE_ARCHIVE_CHECKPOINT_AUTO_RECOVERY",
		true,
	)
	errs = append(errs, runtimeErr, fsstoreErr, appendLogRunnerErr, archiveCheckpointAutoRecoveryErr)

	appCfg := AppConfig{
		RuntimeEnv:                    LoadRuntimeEnv(env),
		RuntimeConfig:                 runtimeConfig,
		AppConfig:                     loadAppConfig(env),
		MaxLogEntryBytes:              pkglogger.DefaultMaxLogEntryBytes,
		FsstoreConfig:                 fsstoreConfig,
		AppendLogRunnerConfig:         appendLogRunnerConfig,
		ArchiveCheckpointAutoRecovery: archiveCheckpointAutoRecovery,
		// Slackの遅延閾値は出力先の選択とは無関係なapplication共通設定なので、
		// Slack loggerが選択されているかにかかわらず初期化します。
		Slack:       loadSlackAppConfig(),
		AdminServer: loadAdminServerConfig(env),
	}

	// 明示的に必要な構成だけ読み込む。
	enableLogging, enableLoggingErr := env.GetEnvBool("HETEROLOG_ENABLE_LOGGING", true)
	enableAsyncLogging, enableAsyncLoggingErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING", true)
	enableDeliveryLogging, enableDeliveryLoggingErr := env.GetEnvBool("HETEROLOG_ENABLE_DELIVERY_LOGGING", true)
	appCfg.LoggerEnabled = enableLogging
	appCfg.AsyncLoggerEnabled = enableAsyncLogging
	appCfg.DeliveryLoggerEnabled = enableDeliveryLogging
	errs = append(
		errs,
		enableLoggingErr,
		enableAsyncLoggingErr,
		enableDeliveryLoggingErr,
	)
	if enableLogging || enableAsyncLogging || enableDeliveryLogging || appCfg.AdminServer.Addr != "" {
		maxLogEntryBytes, maxLogEntryBytesErr := env.GetEnvInt(
			"HETEROLOG_MAX_LOG_ENTRY_BYTES",
			pkglogger.DefaultMaxLogEntryBytes,
		)
		appCfg.MaxLogEntryBytes = maxLogEntryBytes
		errs = append(errs, maxLogEntryBytesErr)
	}
	if enableLogging {
		var err error
		appCfg.Logger, err = loadLoggerConfig(env)
		errs = append(errs, err)
	}

	if enableAsyncLogging {
		var err error
		appCfg.AsyncBestEffortMultiLoggerConfig, err = loadAsyncBestEffortMultiLoggerConfig(env)
		errs = append(errs, err)
	}

	if enableDeliveryLogging {
		var err error
		appCfg.DeliveryLogger, err = loadDeliveryLoggerConfig(env)
		errs = append(errs, err)
	}

	// 管理用APIの監査用loggerは、Delivery loggerを無効にしても、管理用サーバーを有効に
	// すれば使います。
	if appCfg.AdminServer.Addr != "" {
		var err error
		appCfg.AdminAudit, err = loadAdminAuditConfig(env)
		errs = append(errs, err)
	}

	var tracesErr error
	appCfg.Traces, tracesErr = loadTracesConfig(env)
	errs = append(errs, tracesErr)

	var objectStorageCompressedLogFileUploadFailureStoreErr error
	appCfg.CompressedLogFileUploadFailureStore, objectStorageCompressedLogFileUploadFailureStoreErr =
		loadObjectStorageCompressedLogFileUploadFailureStoreConfig(env, appCfg)
	errs = append(errs, objectStorageCompressedLogFileUploadFailureStoreErr)

	var spoolErr error
	appCfg.SpoolConfig, spoolErr = loadSpoolConfig(env, appCfg)
	errs = append(errs, spoolErr)

	enableOpenSearch, enableOpenSearchErr := env.GetEnvBool("HETEROLOG_ENABLE_OPENSEARCH", false)
	errs = append(errs, enableOpenSearchErr)
	if enableOpenSearch || requiresLoggerTargetConfig(
		appCfg.Logger.LogOutputDestinations,
		appCfg.Logger.HandlerTargetDestinations,
		logconfig.LogOutputOpenSearch,
	) {
		var err error
		appCfg.OpenSearchClient, err = loadOpenSearchClientConfig(env)
		errs = append(errs, err)
	}

	enableAsyncOpenSearch, enableAsyncOpenSearchErr := env.GetEnvBool("HETEROLOG_ENABLE_ASYNC_OPENSEARCH", false)
	errs = append(errs, enableAsyncOpenSearchErr)
	if enableAsyncOpenSearch || requiresLoggerTargetConfig(
		appCfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations,
		appCfg.AsyncBestEffortMultiLoggerConfig.HandlerTargetDestinations,
		logconfig.LogOutputOpenSearch,
	) {
		var err error
		appCfg.AsyncOpenSearchClient, err = loadAsyncOpenSearchClientConfig(env)
		errs = append(errs, err)
	}

	if requiresLoggerTargetConfig(
		appCfg.DeliveryLogger.LogOutputDestinations,
		appCfg.DeliveryLogger.HandlerTargetDestinations,
		logconfig.LogOutputOpenSearch,
	) || appCfg.AdminAudit.requires(logconfig.LogOutputOpenSearch) {
		var err error
		appCfg.DeliveryOpenSearchClient, err = loadDeliveryOpenSearchClientConfig(env)
		errs = append(errs, err)
	}

	if requiresLoggerTargetConfig(
		appCfg.Logger.LogOutputDestinations,
		appCfg.Logger.HandlerTargetDestinations,
		logconfig.LogOutputLoki,
	) {
		var err error
		appCfg.LokiClient, err = loadLokiClientConfig(env)
		errs = append(errs, err)
	}
	if requiresLoggerTargetConfig(
		appCfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations,
		appCfg.AsyncBestEffortMultiLoggerConfig.HandlerTargetDestinations,
		logconfig.LogOutputLoki,
	) {
		var err error
		appCfg.AsyncLokiClient, err = loadAsyncLokiClientConfig(env)
		errs = append(errs, err)
	}
	if requiresLoggerTargetConfig(
		appCfg.DeliveryLogger.LogOutputDestinations,
		appCfg.DeliveryLogger.HandlerTargetDestinations,
		logconfig.LogOutputLoki,
	) || appCfg.AdminAudit.requires(logconfig.LogOutputLoki) {
		var err error
		appCfg.DeliveryLokiClient, err = loadDeliveryLokiClientConfig(env)
		errs = append(errs, err)
	}

	if requiresLoggerTargetConfig(
		appCfg.Logger.LogOutputDestinations,
		appCfg.Logger.HandlerTargetDestinations,
		logconfig.LogOutputCloudWatchLogs,
	) {
		var err error
		appCfg.CloudWatchLogsClient, err = loadCloudWatchLogsClientConfig(env)
		errs = append(errs, err)
	}
	if requiresLoggerTargetConfig(
		appCfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations,
		appCfg.AsyncBestEffortMultiLoggerConfig.HandlerTargetDestinations,
		logconfig.LogOutputCloudWatchLogs,
	) {
		var err error
		appCfg.AsyncCloudWatchLogsClient, err = loadAsyncCloudWatchLogsClientConfig(env)
		errs = append(errs, err)
	}
	if requiresLoggerTargetConfig(
		appCfg.DeliveryLogger.LogOutputDestinations,
		appCfg.DeliveryLogger.HandlerTargetDestinations,
		logconfig.LogOutputCloudWatchLogs,
	) || appCfg.AdminAudit.requires(logconfig.LogOutputCloudWatchLogs) {
		var err error
		appCfg.DeliveryCloudWatchLogsClient, err = loadDeliveryCloudWatchLogsClientConfig(env)
		errs = append(errs, err)
	}

	if requiresLoggerTargetConfig(
		appCfg.Logger.LogOutputDestinations,
		appCfg.Logger.HandlerTargetDestinations,
		logconfig.LogOutputClickHouse,
	) {
		var err error
		appCfg.ClickHouseClient, err = loadClickHouseClientConfig(env)
		errs = append(errs, err)
	}
	if requiresLoggerTargetConfig(
		appCfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations,
		appCfg.AsyncBestEffortMultiLoggerConfig.HandlerTargetDestinations,
		logconfig.LogOutputClickHouse,
	) {
		var err error
		appCfg.AsyncClickHouseClient, err = loadAsyncClickHouseClientConfig(env)
		errs = append(errs, err)
	}
	if requiresLoggerTargetConfig(
		appCfg.DeliveryLogger.LogOutputDestinations,
		appCfg.DeliveryLogger.HandlerTargetDestinations,
		logconfig.LogOutputClickHouse,
	) || appCfg.AdminAudit.requires(logconfig.LogOutputClickHouse) {
		var err error
		appCfg.DeliveryClickHouseClient, err = loadDeliveryClickHouseClientConfig(env)
		errs = append(errs, err)
	}

	return appCfg, errors.Join(errs...)
}
