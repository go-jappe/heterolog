// Package envconfigは、HETEROLOG_の環境変数とenv fileから届く設定の入力窓口です。
// 読み込んだ設定を検証し、loggerのconstructorへ渡す設定とoptionと、管理用APIのserverの
// 設定にします。loggerは、internal/bootstrapがこの設定から作ります。
package envconfig

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	appconfig "github.com/go-jappe/heterolog/internal/config"
	"github.com/go-jappe/heterolog/internal/presentation/http/admin"
	pkgadmin "github.com/go-jappe/heterolog/pkg/admin"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
)

// ErrLoggerDisabledは、環境設定がHETEROLOG_ENABLE_LOGGING、
// HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING、HETEROLOG_ENABLE_DELIVERY_LOGGINGを
// falseにして無効にしたloggerを、EnvConfigに求めたことを表します。
var ErrLoggerDisabled = errors.New("logger is disabled by the environment")

// ErrAdminAuditNotEnabledは、HETEROLOG_ADMIN_ADDRが空で管理用serverを有効にしていない
// ため、監査用loggerの設定がないことを表します。
var ErrAdminAuditNotEnabled = appconfig.ErrAdminAuditNotEnabled

// EnvConfigは、LoadがHETEROLOG_の環境変数から読んだ設定を持ちます。Config・AsyncConfig・
// DeliveryConfig・AdminAuditConfigは、loggerのconstructorへ渡す設定とoptionを返します。
//
// 環境設定から作ったloggerは、計装を持ちません。TelemetryConfigの設定から作ったtelemetryの
// Tracerなどを、WithObserversでoptionとして渡します。
// HETEROLOG_USE_MEMORY_PRESSURE_*で有効にしたspoolは、WithMemoryPressureMonitorで監視を
// 渡さなければ、HETEROLOG_MEMORY_CRITICAL_RATIO・HETEROLOG_MIN_AVAILABLE_MEMORY_BYTES_MB・
// HETEROLOG_MEMORY_CRITICAL_CONSECUTIVE_HITSでmemory逼迫を判定します。
type EnvConfig struct {
	app appconfig.AppConfig
}

// Loadは、process environmentと、pathが空でなければpathのenv fileから、HETEROLOG_の
// 設定を読み込みます。process environmentにある変数は、fileの値より優先します。
// process environmentは変更せず、ほかのenv fileも読みません。HETEROLOG_ENV_PATHと、
// pathで指定していない.env fileは無視します。AWS SDKのAWS_ACCESS_KEY_ID・
// AWS_SECRET_ACCESS_KEY・AWS_SESSION_TOKEN・AWS_REGIONは、S3とCloudWatch Logsの
// 設定の既定値になります。
//
// 設定していない項目は.env.exampleに記載した既定値を使うため、空の環境ではstdoutへ
// 書くloggerになります。有効な各loggerの設定と、二つの保存先が同じdirectoryを使う
// といったlogger同士の組み合わせを確かめ、見つけた問題をerrors.Joinでまとめて
// 返します。型の誤りなど一部の問題は後の検査を止めるため、それを直すと別の問題が
// 見つかることがあります。設定を特定するには、errors.AsでEnvFileError・
// InvalidEnvironmentValueError・RenamedEnvironmentKeyError、またはpkg/envの
// VariableErrorを取り出します。
func Load(path string) (*EnvConfig, error) {
	app, err := appconfig.LoadConfigFile(path)
	if err != nil {
		return nil, err
	}
	errs := []error{appconfig.ValidateApplicationConfig(app)}
	if adminCfg, ok := newAdminServerConfig(app.AdminServer); ok {
		if err := admin.ValidateServerConfig(adminCfg); err != nil {
			errs = append(errs, fmt.Errorf("HETEROLOG_ADMIN_*: %w", err))
		}
	}
	if app.LoggerEnabled {
		_, err := appconfig.MapLoggerConfig(app)
		errs = append(errs, err)
	}
	if app.AsyncLoggerEnabled {
		_, err := appconfig.MapAsyncLoggerConfig(app)
		errs = append(errs, err)
	}
	if app.DeliveryLoggerEnabled {
		_, err := appconfig.MapDeliveryLoggerConfig(app)
		errs = append(errs, err)
	}
	if app.AdminAudit.Enabled {
		_, err := appconfig.MapAdminAuditLoggerConfig(app)
		errs = append(errs, err)
	}
	if app.Traces.Enabled {
		if err := appconfig.MapTelemetryConfig(app).Validate(); err != nil {
			errs = append(errs, fmt.Errorf("HETEROLOG_TRACES_*: %w", err))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return &EnvConfig{app: app}, nil
}

// Configは、同期loggerの設定と、環境設定から読んだspoolの再送設定とCloudWatch Logsの
// stream分割のoptionを返します。どちらも同期loggerのconstructorへ渡します。呼び出しごとに新しい設定を
// 返しますが、LogOutputDestinationsなどのsliceはEnvConfigと配列を共有することがあるため、
// 要素を書き換えずにsliceごと置き換えてください。HETEROLOG_ENABLE_LOGGINGがfalseの
// 場合は、ErrLoggerDisabledを包んだerrorを返します。
func (c *EnvConfig) Config() (pkglogger.LoggerConfig, []loggerruntime.Option, error) {
	if !c.app.LoggerEnabled {
		return pkglogger.LoggerConfig{}, nil, fmt.Errorf("%w: HETEROLOG_ENABLE_LOGGING is false", ErrLoggerDisabled)
	}
	cfg, err := appconfig.MapLoggerConfig(c.app)
	if err != nil {
		return pkglogger.LoggerConfig{}, nil, err
	}
	return cfg, []loggerruntime.Option{
		loggerruntime.WithSpool(c.app.SpoolConfig),
		loggerruntime.WithCloudWatchLogsStreamRouting(c.app.Logger.CloudWatchLogsConfig.LogStreamRoutingGranularity),
	}, nil
}

// AsyncConfigは、Configと同じく、非同期loggerの設定とconstructorのoptionを返します。
// HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGINGがfalseの場合は、ErrLoggerDisabledを包んだ
// errorを返します。
func (c *EnvConfig) AsyncConfig() (pkglogger.AsyncBestEffortMultiLoggerConfig, []loggerruntime.Option, error) {
	if !c.app.AsyncLoggerEnabled {
		return pkglogger.AsyncBestEffortMultiLoggerConfig{}, nil, fmt.Errorf("%w: HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING is false", ErrLoggerDisabled)
	}
	cfg, err := appconfig.MapAsyncLoggerConfig(c.app)
	if err != nil {
		return pkglogger.AsyncBestEffortMultiLoggerConfig{}, nil, err
	}
	return cfg, []loggerruntime.Option{
		loggerruntime.WithSpool(c.app.SpoolConfig),
		loggerruntime.WithCloudWatchLogsStreamRouting(c.app.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.LogStreamRoutingGranularity),
	}, nil
}

// DeliveryConfigは、Delivery loggerの設定とconstructorのoptionを返します。Delivery
// loggerはspoolを再送しないため、optionはCloudWatch Logsのstream分割だけです。
// HETEROLOG_ENABLE_DELIVERY_LOGGINGがfalseの場合は、ErrLoggerDisabledを包んだerrorを
// 返します。
func (c *EnvConfig) DeliveryConfig() (pkglogger.DeliveryLoggerConfig, []loggerruntime.Option, error) {
	if !c.app.DeliveryLoggerEnabled {
		return pkglogger.DeliveryLoggerConfig{}, nil, fmt.Errorf("%w: HETEROLOG_ENABLE_DELIVERY_LOGGING is false", ErrLoggerDisabled)
	}
	cfg, err := appconfig.MapDeliveryLoggerConfig(c.app)
	if err != nil {
		return pkglogger.DeliveryLoggerConfig{}, nil, err
	}
	return cfg, []loggerruntime.Option{
		loggerruntime.WithCloudWatchLogsStreamRouting(c.app.DeliveryLogger.DeliveryCloudWatchLogsConfig.LogStreamRoutingGranularity),
	}, nil
}

// AdminServerConfigは、HETEROLOG_ADMIN_ADDR・HETEROLOG_ADMIN_TOKEN・
// HETEROLOG_ADMIN_TLS_CERT_FILE・HETEROLOG_ADMIN_TLS_KEY_FILEから読んだ管理用serverの
// 設定と、HETEROLOG_ADMIN_ADDRでserverを有効にしたかを返します。Loadで確認済みです。
// 設定は、AdminAuditConfigの設定から作る監査用loggerとともに、管理用serverの起動へ渡します。
func (c *EnvConfig) AdminServerConfig() (pkgadmin.ServerConfig, bool) {
	return newAdminServerConfig(c.app.AdminServer)
}

// AdminAuditConfigは、管理用APIの監査logを記録する送達確認loggerの設定と、constructorの
// optionを返します。出力先はHETEROLOG_ADMIN_AUDIT_LOG_TYPE、出力先ごとの記録の場所は
// HETEROLOG_ADMIN_AUDIT_*で、接続先・認証・timeoutなどはHETEROLOG_DELIVERY_*の設定を
// 引き継ぎます。HETEROLOG_ADMIN_ADDRが空の場合は、ErrAdminAuditNotEnabledを返します。
func (c *EnvConfig) AdminAuditConfig() (pkglogger.DeliveryLoggerConfig, []loggerruntime.Option, error) {
	cfg, err := appconfig.MapAdminAuditLoggerConfig(c.app)
	if err != nil {
		return pkglogger.DeliveryLoggerConfig{}, nil, err
	}
	return cfg, []loggerruntime.Option{
		loggerruntime.WithCloudWatchLogsStreamRouting(c.app.AdminAudit.DeliveryLogger.DeliveryCloudWatchLogsConfig.LogStreamRoutingGranularity),
	}, nil
}

// TelemetryConfigは、NewTelemetryへ渡すtelemetryの設定を返します。traceの設定は
// HETEROLOG_ENABLE_TRACESとHETEROLOG_TRACES_*から、Resourceはloggerと同じ項目
// （HETEROLOG_APP_NAMEなど）から読みます。HETEROLOG_ENABLE_TRACESがfalseなら、送り先の
// ない設定を返します。Loadで確認済みです。
func (c *EnvConfig) TelemetryConfig() pkgtelemetry.Config {
	return appconfig.MapTelemetryConfig(c.app)
}

func newAdminServerConfig(cfg appconfig.AdminServerConfig) (pkgadmin.ServerConfig, bool) {
	if cfg.Addr == "" {
		return pkgadmin.ServerConfig{}, false
	}
	return pkgadmin.ServerConfig{
		Addr:        cfg.Addr,
		Token:       cfg.Token,
		TLSCertFile: cfg.TLSCertFile,
		TLSKeyFile:  cfg.TLSKeyFile,
	}, true
}
