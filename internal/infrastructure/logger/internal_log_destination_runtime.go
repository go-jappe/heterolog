package logger

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/file"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/stdout"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
)

// internalLogDestinationSourceは、runtimeの構築で通知先を作れない場合のNotificationTargetErrorの
// Sourceです。通常は、resourceを作る前の検証（validateLoggerNotificationTargets）がloggerの
// 種類をSourceとして同じ不備を先に報告します。
const internalLogDestinationSource = "internal log destination"

var (
	// ErrInternalLogFileLoggerSetCreationは、sync/async loggerのinternal log専用file保存先の
	// LoggerSetを作成できないことを表します。原因はerrors.Is/Asで判定できます。
	ErrInternalLogFileLoggerSetCreation = errors.New("create internal log file logger set")
	// ErrDeliveryInternalLogFileLoggerSetCreationは、Delivery loggerのinternal log専用file
	// 保存先のLoggerSetを作成できないことを表します。原因はerrors.Is/Asで判定できます。
	ErrDeliveryInternalLogFileLoggerSetCreation = errors.New("create delivery internal log file logger set")
)

// internalLogDestinationRuntimeは、applicationから渡された通常ログではなく、
// backend送信失敗、failure store、spool、appendlog maintenanceなど、loggerの
// 内部処理を記録するinternal logの委譲先設定とresourceを管理します。
//
// 一つのLoggerContractKindについて、通常ログと同じ保存先を使用するbackendはbase設定を
// 継承し、InternalLogDestinationsで別の保存先が指定されたbackendだけを専用設定へ
// 置き換えます。例えばOpenSearchならbase設定のindexとは別のindex、Fileならbase設定の
// directoryとは別のdirectoryをinternal logの保存先として使用できます。
//
// この型自体はlogを記録しません。internal log handlerが使用する実効設定と、
// File LoggerSetの参照およびClose責任だけを保持します。
//
// File LoggerSetの扱いはClose責任に応じた次の二通りで、二つのfieldが同時に設定される
// ことはありません。
//
//   - internal log専用file保存先がない場合は、top-level loggerがbase File設定から作った
//     LoggerSetをparentOwnedFileLoggerSetから借りて使用し、このruntimeではCloseしません。
//     このLoggerSetは通常のfile出力にも使われる場合と、通常出力ではfileを選ばず
//     internal logのhandler targetとしてだけ使われる場合があります。
//   - internal log専用file保存先がある場合は、runtimeOwnedFileLoggerSetを新しく作成して
//     使用し、このruntimeがCloseします。
type internalLogDestinationRuntime struct {
	// effectiveConfigは、LoggerContractKindのbase設定へinternal log専用の保存先設定を反映した、
	// internal log handlerが実際に使用する設定です。
	effectiveConfig pkglogger.LoggerConfig
	// cloudWatchLogsStreamRoutingGranularityは、このLoggerContractKindの通常出力と
	// internal log出力が共通して使用するlog stream分割粒度です。保存先のgroup・
	// streamを専用設定へ差し替えても、粒度は同じ契約設定を継承します。
	cloudWatchLogsStreamRoutingGranularity streamrouting.Granularity
	// parentOwnedFileLoggerSetは、base File設定からtop-level loggerが作成・Closeする
	// LoggerSetを借りて保持します。通常のfile出力が無効でも、internal logのhandler
	// targetとしてだけfileを使う場合は、このfieldに設定されます。
	parentOwnedFileLoggerSet *file.LoggerSet
	// runtimeOwnedFileLoggerSetは、InternalLogDestinations.Fileに通常設定とは異なる
	// file保存先がある場合だけ、このruntimeが作成・使用・CloseするLoggerSetです。
	runtimeOwnedFileLoggerSet *file.LoggerSet
	// stdoutNotificationTargetは、構築時に作ったstdoutの通知先です。stdoutの設定がない場合と、
	// 通知先に選ばれていないstdoutの設定が通知先を作れない値の場合はnilです。
	stdoutNotificationTarget *stdout.NotificationTarget
}

func newInternalLogDestinationRuntime(
	baseConfig pkglogger.LoggerConfig,
	cloudWatchLogsStreamRoutingGranularity streamrouting.Granularity,
	baseFileLoggerSet *file.LoggerSet,
	fileDirectoryScopeLabel directorylease.ScopeLabel,
	tracer observer.Tracer,
	meter observer.Meter,
	spoolOwners ...*compressedlog.RootRuntimeBuilder,
) (*internalLogDestinationRuntime, error) {
	// dedicatedConfigには、base設定とは異なる保存先が明示されたbackendだけが入ります。
	effectiveConfig, dedicatedConfig := effectiveInternalLogConfig(baseConfig)
	runtime := &internalLogDestinationRuntime{
		effectiveConfig:                        effectiveConfig,
		cloudWatchLogsStreamRoutingGranularity: cloudWatchLogsStreamRoutingGranularity,
		parentOwnedFileLoggerSet:               baseFileLoggerSet,
	}
	// stdoutの通知先は構築時に一度だけ作り、通知の時点では作り直しません。通知先に選ばれた
	// stdoutの設定が不正なら、構築を設定errorで終えます。
	stdoutTarget, err := newStdoutNotificationTarget(effectiveConfig)
	if err != nil && pkglogger.LoggerConfigSelectsHandlerTarget(baseConfig, pkglogger.LogOutputStdout) {
		return nil, &NotificationTargetError{Source: internalLogDestinationSource, Backend: pkglogger.LogOutputStdout, Err: err}
	}
	if err == nil {
		runtime.stdoutNotificationTarget = stdoutTarget
	}
	if dedicatedConfig.File == nil {
		return runtime, nil
	}
	loggerSet, err := newFileLoggerSetWithDirectoryScopeLabel(
		fileDirectoryScopeLabel,
		dedicatedConfig.File,
		tracer,
		meter,
		spoolOwners...,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInternalLogFileLoggerSetCreation, err)
	}
	runtime.parentOwnedFileLoggerSet = nil
	runtime.runtimeOwnedFileLoggerSet = loggerSet
	return runtime, nil
}

// applyDedicatedInternalLogDestinationsは、internal log専用の保存先が設定された
// backendだけをeffectiveConfigへ反映します。専用設定がnilのbackendは変更しません。
func applyDedicatedInternalLogDestinations(
	effectiveConfig *pkglogger.LoggerConfig,
	dedicatedConfig pkglogger.LoggerConfig,
) {
	if dedicatedConfig.File != nil {
		effectiveConfig.File = dedicatedConfig.File
	}
	if dedicatedConfig.Slack != nil {
		effectiveConfig.Slack = dedicatedConfig.Slack
	}
	if dedicatedConfig.OpenSearch != nil {
		effectiveConfig.OpenSearch = dedicatedConfig.OpenSearch
		effectiveConfig.OpenSearchClient = dedicatedConfig.OpenSearchClient
	}
	if dedicatedConfig.Loki != nil {
		effectiveConfig.Loki = dedicatedConfig.Loki
		effectiveConfig.LokiClient = dedicatedConfig.LokiClient
	}
	if dedicatedConfig.CloudWatchLogs != nil {
		effectiveConfig.CloudWatchLogs = dedicatedConfig.CloudWatchLogs
		effectiveConfig.CloudWatchLogsClient = dedicatedConfig.CloudWatchLogsClient
	}
	if dedicatedConfig.ClickHouse != nil {
		effectiveConfig.ClickHouse = dedicatedConfig.ClickHouse
		effectiveConfig.ClickHouseClient = dedicatedConfig.ClickHouseClient
	}
}

// fileLoggerSetForHandlersは、internal logのfile handlerが使用するLoggerSetを返します。
// 専用保存先がなければ通常設定側から借りたLoggerSet、あれば専用LoggerSetを返します。
func (r *internalLogDestinationRuntime) fileLoggerSetForHandlers() *file.LoggerSet {
	if r.runtimeOwnedFileLoggerSet != nil {
		return r.runtimeOwnedFileLoggerSet
	}
	return r.parentOwnedFileLoggerSet
}

func (r *internalLogDestinationRuntime) newHandlers() handlerStruct {
	if r == nil {
		return handlerStruct{}
	}
	return getHandler(
		r.effectiveConfig,
		r.cloudWatchLogsStreamRoutingGranularity,
		r.fileLoggerSetForHandlers(),
		r.stdoutNotificationTarget,
	)
}

func (r *internalLogDestinationRuntime) dedicatedFileConfig() *pkglogger.FileLoggerConfig {
	if r == nil || r.runtimeOwnedFileLoggerSet == nil {
		return nil
	}
	return r.effectiveConfig.File
}

func (r *internalLogDestinationRuntime) Close() error {
	if r == nil || r.runtimeOwnedFileLoggerSet == nil {
		return nil
	}
	if err := r.runtimeOwnedFileLoggerSet.Close(); err != nil {
		return fmt.Errorf("close internal log file logger set: %w", err)
	}
	return nil
}

// deliveryInternalLogDestinationRuntimeは、Delivery LoggerContractKindでloggerの内部処理を
// 記録するinternal logについて、通常のDelivery logとは別に使用する委譲先設定と
// resourceを管理します。logの記録処理自体はhandlerへ委ね、ここでは実効設定の選択と
// File LoggerSetの参照・Close責任を保持します。
type deliveryInternalLogDestinationRuntime struct {
	// effectiveConfigはDeliveryのbase設定へinternal log専用保存先を反映した設定です。
	effectiveConfig pkglogger.DeliveryLoggerConfig
	// cloudWatchLogsStreamRoutingGranularityは、Deliveryの通常出力とinternal log出力が
	// 共通して使用するlog stream分割粒度です。
	cloudWatchLogsStreamRoutingGranularity streamrouting.Granularity
	// parentOwnedFileLoggerSetはbase File設定からtop-level Delivery loggerが作成・Closeする
	// LoggerSetを借ります。通常出力がfileである必要はありません。
	parentOwnedFileLoggerSet *file.DeliveryLoggerSet
	// runtimeOwnedFileLoggerSetは専用file保存先がある場合だけ、このruntimeが作成・Closeします。
	runtimeOwnedFileLoggerSet *file.DeliveryLoggerSet
	// stdoutNotificationTargetは、構築時に作ったstdoutの通知先です。nilになる条件は
	// internalLogDestinationRuntimeと同じです。
	stdoutNotificationTarget *stdout.NotificationTarget
}

func newDeliveryInternalLogDestinationRuntime(
	baseConfig pkglogger.DeliveryLoggerConfig,
	cloudWatchLogsStreamRoutingGranularity streamrouting.Granularity,
	baseFileLoggerSet *file.DeliveryLoggerSet,
	fileDirectoryScopeLabel directorylease.ScopeLabel,
	tracer observer.Tracer,
	meter observer.Meter,
) (*deliveryInternalLogDestinationRuntime, error) {
	effectiveConfig, dedicatedConfig := effectiveDeliveryInternalLogConfig(baseConfig)
	runtime := &deliveryInternalLogDestinationRuntime{
		effectiveConfig:                        effectiveConfig,
		cloudWatchLogsStreamRoutingGranularity: cloudWatchLogsStreamRoutingGranularity,
		parentOwnedFileLoggerSet:               baseFileLoggerSet,
	}
	// stdoutの通知先は構築時に一度だけ作ります（internalLogDestinationRuntimeと同じです）。
	stdoutTarget, err := newDeliveryStdoutNotificationTarget(effectiveConfig)
	if err != nil && pkglogger.DeliveryLoggerConfigSelectsHandlerTarget(baseConfig, pkglogger.LogOutputStdout) {
		return nil, &NotificationTargetError{Source: internalLogDestinationSource, Backend: pkglogger.LogOutputStdout, Err: err}
	}
	if err == nil {
		runtime.stdoutNotificationTarget = stdoutTarget
	}
	if dedicatedConfig.File == nil {
		return runtime, nil
	}
	loggerSet, err := newDeliveryFileLoggerSetWithDirectoryScopeLabel(
		fileDirectoryScopeLabel,
		dedicatedConfig.File,
		tracer,
		meter,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDeliveryInternalLogFileLoggerSetCreation, err)
	}
	runtime.parentOwnedFileLoggerSet = nil
	runtime.runtimeOwnedFileLoggerSet = loggerSet
	return runtime, nil
}

func applyDedicatedDeliveryInternalLogDestinations(
	effectiveConfig *pkglogger.DeliveryLoggerConfig,
	dedicatedConfig pkglogger.DeliveryLoggerConfig,
) {
	if dedicatedConfig.File != nil {
		effectiveConfig.File = dedicatedConfig.File
	}
	if dedicatedConfig.Slack != nil {
		effectiveConfig.Slack = dedicatedConfig.Slack
	}
	if dedicatedConfig.OpenSearch != nil {
		effectiveConfig.OpenSearch = dedicatedConfig.OpenSearch
		effectiveConfig.OpenSearchClient = dedicatedConfig.OpenSearchClient
	}
	if dedicatedConfig.Loki != nil {
		effectiveConfig.Loki = dedicatedConfig.Loki
		effectiveConfig.LokiClient = dedicatedConfig.LokiClient
	}
	if dedicatedConfig.CloudWatchLogs != nil {
		effectiveConfig.CloudWatchLogs = dedicatedConfig.CloudWatchLogs
		effectiveConfig.CloudWatchLogsClient = dedicatedConfig.CloudWatchLogsClient
	}
	if dedicatedConfig.ClickHouse != nil {
		effectiveConfig.ClickHouse = dedicatedConfig.ClickHouse
		effectiveConfig.ClickHouseClient = dedicatedConfig.ClickHouseClient
	}
}

func (r *deliveryInternalLogDestinationRuntime) fileLoggerSetForHandlers() *file.DeliveryLoggerSet {
	if r.runtimeOwnedFileLoggerSet != nil {
		return r.runtimeOwnedFileLoggerSet
	}
	return r.parentOwnedFileLoggerSet
}

func (r *deliveryInternalLogDestinationRuntime) newHandlers() handlerStruct {
	if r == nil {
		return handlerStruct{}
	}
	return getDeliveryHandler(
		r.effectiveConfig,
		r.cloudWatchLogsStreamRoutingGranularity,
		r.fileLoggerSetForHandlers(),
		r.stdoutNotificationTarget,
	)
}

func (r *deliveryInternalLogDestinationRuntime) dedicatedFileConfig() *pkglogger.DeliveryFileLoggerConfig {
	if r == nil || r.runtimeOwnedFileLoggerSet == nil {
		return nil
	}
	return r.effectiveConfig.File
}

func (r *deliveryInternalLogDestinationRuntime) Close() error {
	if r == nil || r.runtimeOwnedFileLoggerSet == nil {
		return nil
	}
	if err := r.runtimeOwnedFileLoggerSet.Close(); err != nil {
		return fmt.Errorf("close delivery internal log file logger set: %w", err)
	}
	return nil
}

// terminalInternalLogFileConfigは、直接組み立てられたpkg configでもmapper経由と
// 同じterminal sink契約に揃えます。callerの設定を変更しないようcopyした上で、
// dedicated file自身のlifecycle eventを再び内部routingへ投入する設定だけを外します。
func terminalInternalLogFileConfig(
	cfg *pkglogger.FileLoggerConfig,
) *pkglogger.FileLoggerConfig {
	if cfg == nil {
		return nil
	}
	cloned := *cfg
	cloned.LifecycleHandlerConfig = pkgloggerhandler.HandlerConfig{}
	return &cloned
}

func terminalDeliveryInternalLogFileConfig(
	cfg *pkglogger.DeliveryFileLoggerConfig,
) *pkglogger.DeliveryFileLoggerConfig {
	if cfg == nil {
		return nil
	}
	cloned := *cfg
	cloned.LifecycleRunnerSettings.LifecycleHandlerConfig =
		pkgloggerhandler.HandlerConfig{}
	return &cloned
}
