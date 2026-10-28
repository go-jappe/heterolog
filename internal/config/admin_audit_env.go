package config

import (
	"errors"
	"fmt"
	"maps"

	"github.com/go-jappe/heterolog/internal/config/failure"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	pkgenv "github.com/go-jappe/heterolog/pkg/env"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// adminAuditLoggerLabelは、監査用loggerの設定のerrorと、保存先の所有者の名前に使います。
const adminAuditLoggerLabel = "admin_audit_logger"

// defaultAdminAuditOutputsは、監査logの出力先の既定値です。監査logは欠けてはいけない
// ため、ほかのloggerと違い、標準出力に加えて手元のfileにも残します。
var defaultAdminAuditOutputs = []string{pkglogger.LogOutputStdout.String(), pkglogger.LogOutputFile.String()}

// defaultAdminAuditFileLogDirは、監査logのfileを置くdirectoryの既定値です。ほかの
// loggerのfileと同じく、HETEROLOG_SYSTEM_LOG_DIRとHETEROLOG_TARGETを基準に解決します。
const defaultAdminAuditFileLogDir = "/audit_logs"

// ErrAdminAuditNotEnabledは、管理用サーバーを有効にしていないため、監査用loggerの設定が
// ないことを表します。
var ErrAdminAuditNotEnabled = errors.New("the admin API audit logger is not configured because HETEROLOG_ADMIN_ADDR is empty")

// AdminAuditConfigは、管理用APIの監査logを記録する送達確認loggerの設定です。接続先・
// 認証・timeoutなどはDelivery loggerの設定（HETEROLOG_DELIVERY_*）を引き継ぎ、出力先
// （HETEROLOG_ADMIN_AUDIT_LOG_TYPE）と、出力先ごとの記録の場所だけを変えます。
type AdminAuditConfig struct {
	// Enabledは、管理用サーバーを有効にしたため、この設定を読み込んだことを表します。
	Enabled bool
	// DeliveryLoggerは、監査logの出力先で読み込み、記録の場所と、Delivery loggerと共有
	// できない保存先を監査用へ変えたDelivery loggerの設定です。
	DeliveryLogger logconfig.DeliveryLoggerConfig
	// Identityは、出力先ごとの記録の場所です。
	Identity AdminAuditIdentity
}

// AdminAuditIdentityは、監査logの出力先ごとの記録の場所です。空の項目（Lokiのlabelは
// nil）はDelivery loggerの設定を引き継ぎます。
type AdminAuditIdentity struct {
	FileLogDir                  string
	SlackChannel                string
	OpenSearchIndex             string
	LokiLabels                  map[string]string
	CloudWatchLogsLogGroupName  string
	CloudWatchLogsLogStreamName string
	ClickHouseDatabase          string
	ClickHouseTable             string
}

// loadAdminAuditConfigは、監査用loggerの設定を読み込みます。出力先の設定は、
// HETEROLOG_DELIVERY_*の同じ項目から読み込みます。
func loadAdminAuditConfig(env pkgenv.Lookup) (AdminAuditConfig, error) {
	destinations, err := loadLogOutputDestinationsWithDefault(env, "HETEROLOG_ADMIN_AUDIT_LOG_TYPE", defaultAdminAuditOutputs)
	if err != nil {
		return AdminAuditConfig{}, err
	}
	delivery, deliveryErr := loadDeliveryLoggerConfigWithDestinations(env, destinations)
	identity, identityErr := loadAdminAuditIdentity(
		env,
		requiresLoggerTargetConfig(delivery.LogOutputDestinations, delivery.HandlerTargetDestinations, logconfig.LogOutputLoki),
	)
	cfg := AdminAuditConfig{Enabled: true, DeliveryLogger: delivery, Identity: identity}
	cfg.applyIdentity()
	return cfg, errors.Join(deliveryErr, identityErr)
}

// loadAdminAuditIdentityは、出力先ごとの記録の場所を読み込みます。Lokiを使わない場合は
// labelを解析しません。
func loadAdminAuditIdentity(env pkgenv.Lookup, loadLokiLabels bool) (AdminAuditIdentity, error) {
	var lokiLabels map[string]string
	var err error
	if loadLokiLabels {
		lokiLabels, err = pkgenv.GetEnvJSONFrom[map[string]string](
			env,
			"HETEROLOG_ADMIN_AUDIT_LOKI_LABELS",
			map[string]string(nil),
			false,
		)
	}
	return AdminAuditIdentity{
		FileLogDir:                  env.GetEnvString("HETEROLOG_ADMIN_AUDIT_FILE_LOG_DIR", defaultAdminAuditFileLogDir),
		SlackChannel:                env.GetEnvString("HETEROLOG_ADMIN_AUDIT_SLACK_CHANNEL", ""),
		OpenSearchIndex:             env.GetEnvString("HETEROLOG_ADMIN_AUDIT_OPENSEARCH_INDEX", ""),
		LokiLabels:                  lokiLabels,
		CloudWatchLogsLogGroupName:  env.GetEnvString("HETEROLOG_ADMIN_AUDIT_CLOUDWATCH_LOG_GROUP_NAME", ""),
		CloudWatchLogsLogStreamName: env.GetEnvString("HETEROLOG_ADMIN_AUDIT_CLOUDWATCH_LOG_STREAM_NAME", ""),
		ClickHouseDatabase:          env.GetEnvString("HETEROLOG_ADMIN_AUDIT_CLICKHOUSE_DATABASE", ""),
		ClickHouseTable:             env.GetEnvString("HETEROLOG_ADMIN_AUDIT_CLICKHOUSE_TABLE", ""),
	}, err
}

// applyIdentityは、Delivery loggerの設定の、記録の場所と、Delivery loggerと共有できない
// 保存先を監査用へ変えます。clientの設定とLokiのlabelは、adminAuditAppConfigと
// MapAdminAuditLoggerConfigが変えます。
func (c *AdminAuditConfig) applyIdentity() {
	d := &c.DeliveryLogger
	// 内部logの専用の保存先は、Delivery loggerのdirectoryなどを使うため引き継ぎません。
	d.InternalLogDestinationConfig = logconfig.InternalLogDestinationConfig{}

	file := &d.DeliveryFileConfig.FileSystemStoreConfig
	dir := c.Identity.FileLogDir
	file.LogDir, file.ErrorLogDir, file.DebugLogDir = dir, dir, dir
	file.IsSeparateDebugLogDirEnabled = false
	// 一つの要求の記録が別々のfileへ分かれないよう、levelでfileを分けません。
	file.IsSeparateErrorLogsEnabled = false
	file.IsSeparateDebugLogsEnabled = false
	// 監査logは保守で消さず、圧縮もuploadもしません。
	file.LifecycleMode = pkgfsstore.FileLifecycleModeDisable
	file.EnableFileCompress = false
	file.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{}

	if channel := c.Identity.SlackChannel; channel != "" {
		slack := &d.DeliverySlackConfig
		slack.Channel, slack.ErrorChannel, slack.WarnChannel, slack.InfoChannel, slack.DebugChannel =
			channel, channel, channel, channel, channel
	}
	if index := c.Identity.OpenSearchIndex; index != "" {
		d.DeliveryOpenSearchConfig.DeliveryOpenSearchFlushManagerConfig.Index = index
	}
	if group := c.Identity.CloudWatchLogsLogGroupName; group != "" {
		d.DeliveryCloudWatchLogsConfig.DeliveryCloudWatchLogsFlushManagerConfig.LogGroupName = group
	}
	if stream := c.Identity.CloudWatchLogsLogStreamName; stream != "" {
		d.DeliveryCloudWatchLogsConfig.DeliveryCloudWatchLogsFlushManagerConfig.LogStreamName = stream
	}

	// failure archiveは、Delivery loggerのものと重ならないよう、backendごとに監査用の
	// directoryを使い、uploadもしません。
	for destination, store := range c.failureStores() {
		store.FileArchiveConfig.Dir = fmt.Sprintf("%s_%s_failure_archives", adminAuditLoggerLabel, destination)
		store.FileArchiveConfig.ObjectStorageUploadConfig = objectstorage.UploadTargetConfig{}
	}
}

// failureStoresは、failure storeを持つbackendごとの、監査用loggerのfailure handlerの
// 設定です。
func (c *AdminAuditConfig) failureStores() map[pkglogger.LogOutputDestination]*failure.DeliveryFailureHandlerConfig {
	d := &c.DeliveryLogger
	return map[pkglogger.LogOutputDestination]*failure.DeliveryFailureHandlerConfig{
		pkglogger.LogOutputOpenSearch:     &d.DeliveryOpenSearchConfig.DeliveryOpenSearchFlushManagerConfig.FailureHandlerConfig,
		pkglogger.LogOutputLoki:           &d.DeliveryLokiConfig.DeliveryLokiFlushManagerConfig.FailureHandlerConfig,
		pkglogger.LogOutputCloudWatchLogs: &d.DeliveryCloudWatchLogsConfig.DeliveryCloudWatchLogsFlushManagerConfig.FailureHandlerConfig,
		pkglogger.LogOutputClickHouse:     &d.DeliveryClickHouseConfig.DeliveryClickHouseFlushManagerConfig.FailureHandlerConfig,
	}
}

// requiresは、監査用loggerが、出力先またはinternal logの委譲先としてdestinationを使うかを
// 返します。
func (c AdminAuditConfig) requires(destination pkglogger.LogOutputDestination) bool {
	return c.Enabled && requiresLoggerTargetConfig(c.DeliveryLogger.LogOutputDestinations, c.DeliveryLogger.HandlerTargetDestinations, destination)
}

// adminAuditAppConfigは、Delivery loggerの設定を監査用loggerの設定へ置き換えたAppConfigを
// 返します。clientの設定は、記録の場所だけを監査用へ変えた複製です。
func (a AppConfig) adminAuditAppConfig() AppConfig {
	audit := a
	audit.DeliveryLogger = a.AdminAudit.DeliveryLogger
	identity := a.AdminAudit.Identity
	if group := identity.CloudWatchLogsLogGroupName; group != "" {
		audit.DeliveryCloudWatchLogsClient.LogGroupName = group
	}
	if database := identity.ClickHouseDatabase; database != "" {
		audit.DeliveryClickHouseClient.Database = database
	}
	if table := identity.ClickHouseTable; table != "" {
		audit.DeliveryClickHouseClient.Table = table
	}
	return audit
}

// ToAdminAuditLoggerConfigは、監査用loggerの設定のfield変換だけを行います。管理用
// サーバーを有効にしていない場合はゼロ値を返します。application runtimeへ渡す設定には、
// 検証を含むMapAdminAuditLoggerConfigを使用します。
func ToAdminAuditLoggerConfig(cfg AppConfig) pkglogger.DeliveryLoggerConfig {
	if !cfg.AdminAudit.Enabled {
		return pkglogger.DeliveryLoggerConfig{}
	}
	mapped := ToDeliveryLoggerConfig(cfg.adminAuditAppConfig())
	if labels := cfg.AdminAudit.Identity.LokiLabels; labels != nil && mapped.Loki != nil {
		mapped.Loki.StaticLabels = maps.Clone(labels)
	}
	return mapped
}

// MapAdminAuditLoggerConfigは、管理用APIの監査logを記録する送達確認loggerの設定へ変換し、
// MapDeliveryLoggerConfigと同じ検証を行います。管理用サーバーを有効にしていない場合は
// ErrAdminAuditNotEnabledを返します。
func MapAdminAuditLoggerConfig(cfg AppConfig) (pkglogger.DeliveryLoggerConfig, error) {
	if !cfg.AdminAudit.Enabled {
		return pkglogger.DeliveryLoggerConfig{}, ErrAdminAuditNotEnabled
	}
	validated, err := validateMappedDeliveryLoggerConfig(cfg.adminAuditAppConfig(), ToAdminAuditLoggerConfig(cfg))
	if err != nil {
		return pkglogger.DeliveryLoggerConfig{}, fmt.Errorf("%s: %w", adminAuditLoggerLabel, err)
	}
	return validated, nil
}
