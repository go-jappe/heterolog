package logrecovery

import (
	"fmt"
	"slices"

	"github.com/go-jappe/heterolog/internal/infrastructure/delivery/failurestore"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	metricsdelivery "github.com/go-jappe/heterolog/internal/infrastructure/metrics"
	"github.com/go-jappe/heterolog/internal/infrastructure/metrics/victoriametrics"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/storageowner"
	"github.com/go-jappe/heterolog/internal/infrastructure/traces"
	tracesclickhouse "github.com/go-jappe/heterolog/internal/infrastructure/traces/clickhouse"
	tracesopensearch "github.com/go-jappe/heterolog/internal/infrastructure/traces/opensearch"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
)

// Configsは、手動復旧が系列を選ぶための、変換済みのloggerとTelemetryの設定です。設定の
// 変換は、選んだ所有者の分だけ行います。
type Configs struct {
	// Targetは、保存先の識別子に使う設定のtargetです。
	Target string
	// Logger・Async・Deliveryは、同期・非同期・Delivery loggerの設定を作ります。
	Logger   func() (pkglogger.LoggerConfig, error)
	Async    func() (pkglogger.AsyncBestEffortMultiLoggerConfig, error)
	Delivery func() (pkglogger.DeliveryLoggerConfig, error)
	// Telemetryは、NewTelemetryへ渡すtraceとmetricsの設定を作ります。
	Telemetry func() pkgtelemetry.Config
}

type recoverySpec struct {
	name                string
	checkpointDirectory string
	spoolConfigs        []compressedlog.ConfigProvider
	newLifecycles       func(compressedlog.RootRuntimes, logfilelifecycle.RunReporter, ...logfilelifecycle.RecoveryOption) ([]*logfilelifecycle.DirectoryLifecycle, error)
}

// buildは復旧CLIが所有するBuilderから共有spoolを取得し、復旧処理へ渡します。
// 同じ保存先を使う系列はruntimeを共有し、構築・復旧を再試行しても作り直しません。
// Builderの解放はRecoverが全復旧処理の終了後に行います。
func (s recoverySpec) build(owner *compressedlog.RootRuntimeBuilder, reporter logfilelifecycle.RunReporter, options ...logfilelifecycle.RecoveryOption) ([]*logfilelifecycle.DirectoryLifecycle, error) {
	runtimes, err := owner.Build(s.spoolConfigs...)
	if err != nil {
		return nil, fmt.Errorf("build recovery spool runtimes: %w", err)
	}
	return s.newLifecycles(runtimes, reporter, options...)
}

// recoverySpecsは通常の設定mapperが作った設定から、実際に保存を所有する系列だけを選びます。
// loggerを生成しないため、書き込みや通常backend送信、周期処理は開始しません。
func recoverySpecs(configs Configs, opts Options) ([]recoverySpec, error) {
	switch opts.LoggerContract {
	case pkglogger.LoggerContractSync:
		mapped, err := configs.Logger()
		if err != nil {
			return nil, err
		}
		return logRecoverySpecs(mapped, opts.Scope), nil
	case pkglogger.LoggerContractAsyncBestEffort:
		mapped, err := configs.Async()
		if err != nil {
			return nil, err
		}
		return logRecoverySpecs(mapped.ToLoggerConfig(), opts.Scope), nil
	case pkglogger.LoggerContractDelivery:
		mapped, err := configs.Delivery()
		if err != nil {
			return nil, err
		}
		return deliveryRecoverySpecs(mapped, opts.Scope), nil
	case storageowner.Telemetry:
		return telemetryRecoverySpecs(configs.Telemetry(), opts.Scope), nil
	default:
		return nil, fmt.Errorf("invalid recovery logger contract %q", opts.LoggerContract)
	}
}

func outputEnabled(outputs pkglogger.LogOutputDestinations, backend pkglogger.LogOutputDestination) bool {
	return len(outputs) == 0 || outputs.Has(backend)
}

func logRecoverySpecs(cfg pkglogger.LoggerConfig, scope RecoveryScope) []recoverySpec {
	var specs []recoverySpec
	if scope != RecoveryScopeLogBackendFailureArchive {
		var internal *pkglogger.FileLoggerConfig
		if cfg.InternalLogDestinations != nil {
			internal = cfg.InternalLogDestinations.File
		}
		// 専用internal fileのtemplateにしか使われないbase設定は所有者ではありません。
		if cfg.File != nil && (outputEnabled(cfg.LogOutputDestinations, pkglogger.LogOutputFile) ||
			internal == nil && pkglogger.LoggerConfigRequiresBaseBackend(cfg, pkglogger.LogOutputFile)) {
			specs = append(specs, logFileSpec(string(RecoveryScopeFileLogger), cfg.File))
		}
		if internal != nil {
			specs = append(specs, logFileSpec(string(RecoveryScopeFileLogger)+"/internal", internal))
		}
	}
	if scope == RecoveryScopeFileLogger {
		return specs
	}
	add := func(backend pkglogger.LogOutputDestination, failure durable.FailureStoreConfig) {
		if outputEnabled(cfg.LogOutputDestinations, backend) && slices.Contains(failure.DurableType, durable.DurableTypeFileArchive) {
			specs = append(specs, failureArchiveSpec(logBackendFailureArchiveName(string(backend)),
				loggerdurable.NewFileArchiveConfigAdapter(failure.DurableStoreConfig.FileArchiveConfig)))
		}
	}
	if cfg.OpenSearch != nil {
		add(pkglogger.LogOutputOpenSearch, cfg.OpenSearch.FailureStoreConfig)
	}
	if cfg.Loki != nil {
		add(pkglogger.LogOutputLoki, cfg.Loki.FailureStoreConfig)
	}
	if cfg.CloudWatchLogs != nil {
		add(pkglogger.LogOutputCloudWatchLogs, cfg.CloudWatchLogs.FailureStoreConfig)
	}
	if cfg.ClickHouse != nil {
		add(pkglogger.LogOutputClickHouse, cfg.ClickHouse.FailureStoreConfig)
	}
	return specs
}

func logFileSpec(name string, cfg *pkglogger.FileLoggerConfig) recoverySpec {
	// DTOだけを組み立て、常駐側のLifecycleModeをrunnerへ変更しません。
	runnerConfig := pkgfsstore.LogFileLifecycleRunnerConfig{
		FsstoreConfig: cfg.FsstoreConfig, ObjectStorageUploadConfig: cfg.ObjectStorageUploadConfig,
		ArchiveCheckpointConfig: cfg.ArchiveCheckpointConfig, RunnerConfig: cfg.LifecycleRunnerSettings,
	}
	spec := recoverySpec{name: name, newLifecycles: func(runtimes compressedlog.RootRuntimes, reporter logfilelifecycle.RunReporter, options ...logfilelifecycle.RecoveryOption) ([]*logfilelifecycle.DirectoryLifecycle, error) {
		return logfilelifecycle.NewLogFileRecovery(logfilelifecycle.LogFileOptions{Config: runnerConfig, Clock: clock.NewUTCClock(), Reporter: reporter, SpoolRuntimes: runtimes}, options...)
	}}
	spec.spoolConfigs = compressedlog.NewConfigAdapters(cfg.ObjectStorageUploadConfig.CompressedLogFileUploadSpool,
		cfg.ObjectStorageUploadConfig.CompressedLogFileUploadSpoolServices(cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled, cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled))
	if fileUploadEnabled(cfg.ObjectStorageUploadConfig, cfg.FsstoreConfig.Layout) {
		spec.checkpointDirectory = cfg.ArchiveCheckpointConfig.Directory
	}
	return spec
}

func fileUploadEnabled(upload objectstorage.LogFileUploadConfig, layout pkgfsstore.FsstoreConfig) bool {
	return upload.Enabled && (upload.Default.Enabled || layout.IsSeparateErrorLogsEnabled && upload.Error.Enabled ||
		layout.IsSeparateDebugLogsEnabled && upload.Debug.Enabled)
}

// logBackendFailureArchiveNameは、ログbackendの送信失敗archiveの結果に表示する系列名です。
func logBackendFailureArchiveName(backend string) string {
	return string(RecoveryScopeLogBackendFailureArchive) + "/" + backend
}

// failureArchiveSpecは、nameの系列として、送信失敗archiveのretryable/permanentを復旧します。
func failureArchiveSpec(name string, cfg logfilelifecycle.FailureArchiveRunnerConfigProvider) recoverySpec {
	spec := recoverySpec{name: name,
		newLifecycles: func(runtimes compressedlog.RootRuntimes, reporter logfilelifecycle.RunReporter, options ...logfilelifecycle.RecoveryOption) ([]*logfilelifecycle.DirectoryLifecycle, error) {
			return logfilelifecycle.NewFailureArchiveRecovery(logfilelifecycle.FailureArchiveOptions{Config: cfg, Clock: clock.NewUTCClock(), Reporter: reporter, SpoolRuntimes: runtimes}, options...)
		}}
	upload := cfg.ObjectStorageUpload()
	spec.spoolConfigs = compressedlog.NewConfigAdapters(upload.CompressedLogFileUploadSpool, upload.CompressedLogFileUploadSpoolServices())
	if cfg.ObjectStorageUpload().Enabled {
		spec.checkpointDirectory = cfg.ArchiveCheckpoint().Directory
	}
	return spec
}

func deliveryRecoverySpecs(cfg pkglogger.DeliveryLoggerConfig, scope RecoveryScope) []recoverySpec {
	var specs []recoverySpec
	if scope != RecoveryScopeLogBackendFailureArchive {
		var internal *pkglogger.DeliveryFileLoggerConfig
		if cfg.InternalLogDestinations != nil {
			internal = cfg.InternalLogDestinations.File
		}
		if cfg.File != nil && (outputEnabled(cfg.LogOutputDestinations, pkglogger.LogOutputFile) ||
			internal == nil && pkglogger.DeliveryLoggerConfigRequiresBaseBackend(cfg, pkglogger.LogOutputFile)) {
			specs = append(specs, deliveryFileSpec(string(RecoveryScopeFileLogger), cfg.File))
		}
		if internal != nil {
			specs = append(specs, deliveryFileSpec(string(RecoveryScopeFileLogger)+"/internal", internal))
		}
	}
	if scope == RecoveryScopeFileLogger {
		return specs
	}
	add := func(backend pkglogger.LogOutputDestination, failure durable.DeliveryFailureStoreConfig) {
		if outputEnabled(cfg.LogOutputDestinations, backend) && slices.Contains(failure.DurableType, durable.DurableTypeFileArchive) {
			adapter := loggerdurable.NewDeliveryDurableStoreConfigAdapter(failure.FileArchiveConfig)
			specs = append(specs, failureArchiveSpec(logBackendFailureArchiveName(string(backend)), adapter.FileArchiveConfig()))
		}
	}
	if cfg.OpenSearch != nil {
		add(pkglogger.LogOutputOpenSearch, cfg.OpenSearch.FailureStoreConfig)
	}
	if cfg.Loki != nil {
		add(pkglogger.LogOutputLoki, cfg.Loki.FailureStoreConfig)
	}
	if cfg.CloudWatchLogs != nil {
		add(pkglogger.LogOutputCloudWatchLogs, cfg.CloudWatchLogs.FailureStoreConfig)
	}
	if cfg.ClickHouse != nil {
		add(pkglogger.LogOutputClickHouse, cfg.ClickHouse.FailureStoreConfig)
	}
	return specs
}

func deliveryFileSpec(name string, cfg *pkglogger.DeliveryFileLoggerConfig) recoverySpec {
	// 既存mapperからDTOを得るための値コピーです。呼び出し元の設定は変更しません。
	copy := *cfg
	copy.LifecycleMode = pkgfsstore.FileLifecycleModeRunner
	runnerConfig := *copy.LifecycleRunnerConfig()
	spec := recoverySpec{name: name, newLifecycles: func(runtimes compressedlog.RootRuntimes, reporter logfilelifecycle.RunReporter, options ...logfilelifecycle.RecoveryOption) ([]*logfilelifecycle.DirectoryLifecycle, error) {
		return logfilelifecycle.NewDeliveryFileRecovery(logfilelifecycle.DeliveryFileOptions{Config: runnerConfig, Clock: clock.NewUTCClock(), Reporter: reporter, SpoolRuntimes: runtimes}, options...)
	}}
	upload := runnerConfig.ObjectStorageUploadConfig
	layout := runnerConfig.FsstoreConfig.Layout
	spec.spoolConfigs = compressedlog.NewConfigAdapters(upload.CompressedLogFileUploadSpool,
		upload.CompressedLogFileUploadSpoolServices(layout.IsSeparateErrorLogsEnabled, layout.IsSeparateDebugLogsEnabled))
	if fileUploadEnabled(runnerConfig.ObjectStorageUploadConfig, runnerConfig.FsstoreConfig.Layout) {
		spec.checkpointDirectory = runnerConfig.ArchiveCheckpointConfig.Directory
	}
	return spec
}

// telemetryRecoverySpecsは、NewTelemetryの設定から、traceとmetricsの送り先のうち、
// failure archiveへ保存するものを選びます。通常の起動と同じく、送り先に選んでいない
// 設定と、failure storeを使わない設定は所有者ではありません。fileとstdoutの送り先は
// failure storeを持たず、fileもrotationしないため、復旧対象はありません。
func telemetryRecoverySpecs(cfg pkgtelemetry.Config, scope RecoveryScope) []recoverySpec {
	if scope != RecoveryScopeAll && scope != RecoveryScopeTelemetryFailureArchive {
		return nil
	}
	var specs []recoverySpec
	add := func(backend string, failure pkgtelemetry.FailureStoreConfig) {
		if !failure.Enabled() || !slices.Contains(failure.Store.DurableType, durable.DurableTypeFileArchive) {
			return
		}
		// 通常の起動と同じ所有者の保存先を使い、uploadできなかった圧縮fileをloggerの保存先へ
		// 混ぜません。
		store := failurestore.OwnArchiveUploadStorage(failure.Store)
		specs = append(specs, failureArchiveSpec(string(RecoveryScopeTelemetryFailureArchive)+"/"+backend,
			loggerdurable.NewFileArchiveConfigAdapter(store.DurableStoreConfig.FileArchiveConfig)))
	}
	for _, exporter := range cfg.Traces.Exporters {
		switch exporter {
		case pkgtelemetry.TraceExporterOTLP:
			add(traces.OTLPBackendName, cfg.Traces.OTLP.FailureStore)
		case pkgtelemetry.TraceExporterOpenSearch:
			add(tracesopensearch.BackendName, cfg.Traces.OpenSearch.FailureStore)
		case pkgtelemetry.TraceExporterClickHouse:
			add(tracesclickhouse.BackendName, cfg.Traces.ClickHouse.FailureStore)
		}
	}
	for _, exporter := range cfg.Metrics.Exporters {
		switch exporter {
		case pkgtelemetry.MetricExporterOTLP:
			add(metricsdelivery.OTLPBackendName, cfg.Metrics.OTLP.FailureStore)
		case pkgtelemetry.MetricExporterVictoriaMetrics:
			add(victoriametrics.BackendName, cfg.Metrics.VictoriaMetrics.FailureStore)
		}
	}
	return specs
}
