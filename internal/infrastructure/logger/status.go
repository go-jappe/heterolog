package logger

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/status"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	fsstorespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/file"
	logspool "github.com/go-jappe/heterolog/internal/infrastructure/spool/log"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

// statusの報告に載せるdirectoryのroleです。
const (
	roleLog                = "log"
	roleErrorLog           = "error_log"
	roleDebugLog           = "debug_log"
	roleInternalLogPrefix  = "internal_"
	roleFailureArchive     = "failure_archive"
	fileDirectoryRoleError = string(file.ErrorLogger)
	fileDirectoryRoleDebug = string(file.DebugLogger)
)

// loggerStatusLayoutは、設定から構築時に決まるstatusの材料です。出力先の一覧、fileと
// failure archiveのdirectory、spoolの上限を持ちます。spoolの今の様子は、報告のたびに
// pipelineから読みます。
type loggerStatusLayout struct {
	backends    []pkglogger.LogOutputDestination
	directories []status.DirectoryInspection
	spoolLimits map[pkglogger.LogOutputDestination]pkgdurable.SpoolOverloadPolicyConfig
}

// newLoggerStatusLayoutは、sync・async loggerの設定からstatusの材料を作ります。
// internalFileは、internal logだけが使う専用のfileの設定（なければnil）です。
func newLoggerStatusLayout(cfg pkglogger.LoggerConfig, internalFile *pkglogger.FileLoggerConfig) loggerStatusLayout {
	layout := loggerStatusLayout{spoolLimits: map[pkglogger.LogOutputDestination]pkgdurable.SpoolOverloadPolicyConfig{}}
	for _, backend := range []struct {
		name    pkglogger.LogOutputDestination
		present bool
	}{
		{pkglogger.LogOutputFile, cfg.File != nil},
		{pkglogger.LogOutputStdout, cfg.Stdout != nil},
		{pkglogger.LogOutputSlack, cfg.Slack != nil},
		{pkglogger.LogOutputOpenSearch, cfg.OpenSearch != nil},
		{pkglogger.LogOutputLoki, cfg.Loki != nil},
		{pkglogger.LogOutputCloudWatchLogs, cfg.CloudWatchLogs != nil},
		{pkglogger.LogOutputClickHouse, cfg.ClickHouse != nil},
	} {
		if backend.present && logOutputEnabled(cfg.LogOutputDestinations, backend.name) {
			layout.backends = append(layout.backends, backend.name)
		}
	}
	if fileOutputEnabled(cfg) {
		layout.directories = append(layout.directories, fileDirectories(cfg.File, "")...)
	}
	if internalFile != nil {
		layout.directories = append(layout.directories, fileDirectories(internalFile, roleInternalLogPrefix)...)
	}
	for _, backend := range []pkglogger.LogOutputDestination{
		pkglogger.LogOutputOpenSearch, pkglogger.LogOutputLoki, pkglogger.LogOutputCloudWatchLogs, pkglogger.LogOutputClickHouse,
	} {
		store, ok := spoolFailureStoreConfig(backend, cfg)
		if !ok || !logOutputEnabled(cfg.LogOutputDestinations, backend) {
			continue
		}
		for _, durableType := range store.DurableType {
			switch durableType {
			case pkgdurable.DurableTypeSpool:
				layout.spoolLimits[backend] = store.DurableStoreConfig.SpoolConfig.OverloadPolicyConfig
			case pkgdurable.DurableTypeFileArchive:
				archive := store.DurableStoreConfig.FileArchiveConfig
				layout.directories = append(layout.directories, status.DirectoryInspection{
					Path:         failurearchive.ResolveDirectoryPath(archive.FailureFileArchivePath, archive.Target, archive.Dir, ""),
					Role:         roleFailureArchive,
					Backend:      string(backend),
					LimitBytes:   archive.MaxDirSize,
					MinFreeBytes: archive.OverloadPolicyConfig.MinAvailableDiskSpace,
				})
			}
		}
	}
	return layout
}

// fileDirectoriesは、file loggerのdirectoryを、roleにprefixを付けて返します。
func fileDirectories(cfg *pkglogger.FileLoggerConfig, rolePrefix string) []status.DirectoryInspection {
	var directories []status.DirectoryInspection
	for _, directory := range file.Directories(file.NewConfigAdapter(*cfg)) {
		directories = append(directories, status.DirectoryInspection{
			Path:         directory.Path,
			Role:         rolePrefix + directoryRole(directory.Role),
			Backend:      string(pkglogger.LogOutputFile),
			LimitBytes:   directory.MaxDirSize,
			MinFreeBytes: cfg.FsstoreConfig.Overload.MinAvailableDiskSpace,
		})
	}
	return directories
}

func directoryRole(fileRole string) string {
	switch fileRole {
	case fileDirectoryRoleError:
		return roleErrorLog
	case fileDirectoryRoleDebug:
		return roleDebugLog
	default:
		return roleLog
	}
}

// newDeliveryStatusLayoutは、Delivery loggerの設定からstatusの材料を作ります。Delivery
// loggerはspoolを使わないため、failure archiveのdirectoryだけです。
func newDeliveryStatusLayout(cfg pkglogger.DeliveryLoggerConfig, internalFile *pkglogger.DeliveryFileLoggerConfig) loggerStatusLayout {
	var layout loggerStatusLayout
	for _, backend := range []struct {
		name    pkglogger.LogOutputDestination
		present bool
	}{
		{pkglogger.LogOutputFile, cfg.File != nil},
		{pkglogger.LogOutputStdout, cfg.Stdout != nil},
		{pkglogger.LogOutputSlack, cfg.Slack != nil},
		{pkglogger.LogOutputOpenSearch, cfg.OpenSearch != nil},
		{pkglogger.LogOutputLoki, cfg.Loki != nil},
		{pkglogger.LogOutputCloudWatchLogs, cfg.CloudWatchLogs != nil},
		{pkglogger.LogOutputClickHouse, cfg.ClickHouse != nil},
	} {
		if backend.present && logOutputEnabled(cfg.LogOutputDestinations, backend.name) {
			layout.backends = append(layout.backends, backend.name)
		}
	}
	if deliveryFileOutputEnabled(cfg) {
		layout.directories = append(layout.directories, deliveryFileDirectories(cfg.File, "")...)
	}
	if internalFile != nil {
		layout.directories = append(layout.directories, deliveryFileDirectories(internalFile, roleInternalLogPrefix)...)
	}
	for _, backend := range []struct {
		name  pkglogger.LogOutputDestination
		store *pkgdurable.DeliveryFailureStoreConfig
	}{
		{pkglogger.LogOutputOpenSearch, deliveryFailureStore(cfg.OpenSearch != nil, func() pkgdurable.DeliveryFailureStoreConfig { return cfg.OpenSearch.FailureStoreConfig })},
		{pkglogger.LogOutputLoki, deliveryFailureStore(cfg.Loki != nil, func() pkgdurable.DeliveryFailureStoreConfig { return cfg.Loki.FailureStoreConfig })},
		{pkglogger.LogOutputCloudWatchLogs, deliveryFailureStore(cfg.CloudWatchLogs != nil, func() pkgdurable.DeliveryFailureStoreConfig { return cfg.CloudWatchLogs.FailureStoreConfig })},
		{pkglogger.LogOutputClickHouse, deliveryFailureStore(cfg.ClickHouse != nil, func() pkgdurable.DeliveryFailureStoreConfig { return cfg.ClickHouse.FailureStoreConfig })},
	} {
		if backend.store == nil || !logOutputEnabled(cfg.LogOutputDestinations, backend.name) {
			continue
		}
		for _, durableType := range backend.store.DurableType {
			if durableType != pkgdurable.DurableTypeFileArchive {
				continue
			}
			archive := backend.store.FileArchiveConfig
			layout.directories = append(layout.directories, status.DirectoryInspection{
				Path:         failurearchive.ResolveDirectoryPath(archive.FailureFileArchivePath, archive.Target, archive.Dir, ""),
				Role:         roleFailureArchive,
				Backend:      string(backend.name),
				LimitBytes:   archive.MaxDirSize,
				MinFreeBytes: archive.OverloadPolicyConfig.MinAvailableDiskSpace,
			})
		}
	}
	return layout
}

func deliveryFailureStore(present bool, get func() pkgdurable.DeliveryFailureStoreConfig) *pkgdurable.DeliveryFailureStoreConfig {
	if !present {
		return nil
	}
	store := get()
	return &store
}

func deliveryFileDirectories(cfg *pkglogger.DeliveryFileLoggerConfig, rolePrefix string) []status.DirectoryInspection {
	var directories []status.DirectoryInspection
	for _, directory := range file.DeliveryDirectories(file.NewDeliveryFileLoggerConfigAdapter(*cfg), cfg.LifecycleRunnerSettings.MaxDirSize) {
		directories = append(directories, status.DirectoryInspection{
			Path:       directory.Path,
			Role:       rolePrefix + directoryRole(directory.Role),
			Backend:    string(pkglogger.LogOutputFile),
			LimitBytes: directory.MaxDirSize,
		})
	}
	return directories
}

// inspectionは、出力先ごとの報告を作ります。spoolを使う出力先は、pipelinesからspoolの
// 今の様子を読みます。
func (l loggerStatusLayout) inspection(ctx context.Context, pipelines *failureSpoolPipelineSet, contractKind pkglogger.LoggerContractKind) status.Inspection {
	inspection := status.Inspection{Directories: l.directories}
	for _, backend := range l.backends {
		entry := status.BackendInspection{Name: string(backend)}
		if pipelines != nil {
			if pipeline, ok := pipelines.byID[failureSpoolPipelineID{contractKind: contractKind, backend: backend}]; ok {
				entry.Spool = spoolInspectionOf(ctx, pipeline, l.spoolLimits[backend])
			}
		}
		inspection.Backends = append(inspection.Backends, entry)
	}
	return inspection
}

// spoolInspectionOfは、pipelineのspoolの場所と送り直しの状態を報告にします。
func spoolInspectionOf(ctx context.Context, pipeline *failureSpoolPipeline, overload pkgdurable.SpoolOverloadPolicyConfig) *status.SpoolInspection {
	root := fsstorespool.DirAt(pipeline.root)
	stateDirs := map[string]string{}
	for state, dir := range root.StateDirs() {
		stateDirs[string(state)] = dir.Path()
	}
	inspection := &status.SpoolInspection{
		Root:             pipeline.root,
		StateDirs:        stateDirs,
		ActiveLimitBytes: overload.MaxDirSize,
		MinFreeBytes:     overload.MinAvailableDiskSpace,
	}
	if pipeline.runner != nil {
		inspection.Replay = logspool.ReplayStatusOf(pipeline.runner.supervisor)
	}
	for _, writer := range pipeline.writers {
		if writer.handler != nil && writer.handler.MemoryMonitor != nil {
			if critical, err := writer.handler.MemoryMonitor.IsCritical(ctx); err == nil {
				inspection.MemoryCritical = &critical
			}
			break
		}
	}
	return inspection
}

// InspectStatusは、statusの報告に載せる出力先・spool・directoryを返します。
func (r *topLevelLoggerRuntime) InspectStatus(ctx context.Context) status.Inspection {
	return r.statusLayout.inspection(ctx, r.failureSpoolPipelines, pkglogger.LoggerContractSync)
}

// InspectStatusは、statusの報告に載せる出力先・spool・directoryを返します。
func (r *topLevelAsyncLoggerRuntime) InspectStatus(ctx context.Context) status.Inspection {
	return r.statusLayout.inspection(ctx, r.failureSpoolPipelines, pkglogger.LoggerContractAsyncBestEffort)
}

// InspectStatusは、statusの報告に載せる出力先とdirectoryを返します。
func (r *topLevelDeliveryLoggerRuntime) InspectStatus(ctx context.Context) status.Inspection {
	return r.statusLayout.inspection(ctx, nil, pkglogger.LoggerContractDelivery)
}
