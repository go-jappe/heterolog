package config

import (
	"reflect"
	"testing"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

func TestNewInternalLogDestinationsMapsSelectedOverrides(
	t *testing.T,
) {
	baseFile := &pkglogger.FileLoggerConfig{
		LifecycleMode: pkgfsstore.FileLifecycleModeInline,
		LifecycleHandlerConfig: pkgloggerhandler.HandlerConfig{
			OnErrorConfig: []string{"file"},
		},
	}
	baseFile.FsstoreConfig.Layout.LogPath = "base-path"
	baseFile.FsstoreConfig.Layout.Target = "base-target"
	baseFile.FsstoreConfig.Layout.LogDir = "base-logs"
	baseFile.ArchiveCheckpointConfig.SourcePrefix = "logger_file"

	base := pkglogger.LoggerConfig{
		File:  baseFile,
		Slack: &pkglogger.SlackLoggerConfig{Channel: "base-channel"},
		OpenSearch: &pkglogger.OpenSearchConfig{
			OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
				Index: "base-index",
			},
		},
		OpenSearchClient: &pkgopensearch.Config{},
		Loki: &pkglogger.LokiConfig{
			LokiFlushManagerConfig: pkglogger.LokiFlushManagerConfig{
				StaticLabels: map[string]string{"scope": "base"},
			},
		},
		LokiClient: &pkgloki.Config{},
		CloudWatchLogs: &pkglogger.CloudWatchLogsConfig{
			CloudWatchLogsFlushManagerConfig: pkglogger.CloudWatchLogsFlushManagerConfig{
				LogGroupName:  "base-group",
				LogStreamName: "base-stream",
			},
		},
		CloudWatchLogsClient: &pkgcloudwatchlogs.Config{
			LogGroupName: "base-group",
		},
		ClickHouse:       &pkglogger.ClickHouseConfig{},
		ClickHouseClient: &pkgclickhouse.Config{Database: "base-db", Table: "base-table"},
	}
	override := logconfig.InternalLogDestinationConfig{
		FileLogPath:                 "internal-path",
		FileTarget:                  "internal-target",
		FileLogDir:                  "internal-logs",
		SlackChannel:                "internal-channel",
		OpenSearchIndex:             "internal-index",
		LokiLabels:                  map[string]string{"scope": "internal"},
		CloudWatchLogsLogGroupName:  "internal-group",
		CloudWatchLogsLogStreamName: "internal-stream",
		ClickHouseDatabase:          "internal-db",
		ClickHouseTable:             "internal-table",
	}

	destinations := newInternalLogDestinations(
		override,
		internalLogUsesTargets(
			pkglogger.LogOutputFile,
			pkglogger.LogOutputSlack,
			pkglogger.LogOutputOpenSearch,
			pkglogger.LogOutputLoki,
			pkglogger.LogOutputCloudWatchLogs,
			pkglogger.LogOutputClickHouse,
		),
		base,
	)
	if destinations == nil {
		t.Fatal("internal-log destinations are nil")
	}
	if got := destinations.File.FsstoreConfig.Layout.LogPath; got != "internal-path" {
		t.Fatalf("internal-log file path = %q", got)
	}
	if got := destinations.File.FsstoreConfig.Layout.Target; got != "internal-target" {
		t.Fatalf("internal-log file target = %q", got)
	}
	if got := destinations.File.LifecycleMode; got != pkgfsstore.FileLifecycleModeInline {
		t.Fatalf("internal-log file lifecycle mode = %q", got)
	}
	if got := destinations.File.ArchiveCheckpointConfig.SourcePrefix; got != "logger_file_internal_log" {
		t.Fatalf("internal-log checkpoint source prefix = %q, want dedicated source", got)
	}
	if got := baseFile.ArchiveCheckpointConfig.SourcePrefix; got != "logger_file" {
		t.Fatalf("base checkpoint source prefix was mutated: %q", got)
	}
	if got := destinations.File.LifecycleHandlerConfig.OnErrorConfig; len(got) != 0 {
		t.Fatalf("internal-log file lifecycle routing = %v, want empty terminal sink", got)
	}
	if got := baseFile.LifecycleHandlerConfig.OnErrorConfig; len(got) != 1 || got[0] != "file" {
		t.Fatalf("base file lifecycle routing was mutated: %v", got)
	}
	if got := destinations.Slack.Channel; got != "internal-channel" {
		t.Fatalf("internal-log Slack channel = %q", got)
	}
	if got := destinations.OpenSearch.Index; got != "internal-index" {
		t.Fatalf("internal-log OpenSearch index = %q", got)
	}
	if destinations.OpenSearchClient != base.OpenSearchClient {
		t.Fatal("internal-log OpenSearch client was not inherited")
	}
	if got := destinations.Loki.StaticLabels["scope"]; got != "internal" {
		t.Fatalf("internal-log Loki labels = %v", destinations.Loki.StaticLabels)
	}
	if destinations.LokiClient != base.LokiClient {
		t.Fatal("internal-log Loki client was not inherited")
	}
	if got := destinations.CloudWatchLogs.LogStreamName; got != "internal-stream" {
		t.Fatalf("internal-log CloudWatch Logs stream = %q", got)
	}
	if got := base.CloudWatchLogs.LogStreamName; got != "base-stream" {
		t.Fatalf("base CloudWatch Logs stream was mutated: %q", got)
	}
	if got := destinations.ClickHouseClient.Database; got != "internal-db" {
		t.Fatalf("internal-log ClickHouse database = %q", got)
	}
	if got := base.ClickHouseClient.Database; got != "base-db" {
		t.Fatalf("base ClickHouse client was mutated: database = %q", got)
	}
}

func TestNewDeliveryInternalLogDestinationsMapsSelectedOverrides(t *testing.T) {
	baseFile := &pkglogger.DeliveryFileLoggerConfig{
		LifecycleMode: pkgfsstore.FileLifecycleModeRunner,
		LifecycleRunnerSettings: pkglogger.DeliveryFileLifecycleRunnerSettings{
			LifecycleHandlerConfig: pkgloggerhandler.HandlerConfig{
				OnInfoConfig: []string{"file"},
			},
		},
	}
	baseFile.FsstoreConfig.Layout.LogPath = "delivery-base"
	baseFile.LifecycleRunnerSettings.ArchiveCheckpointConfig.SourcePrefix = "delivery_logger_file"
	base := pkglogger.DeliveryLoggerConfig{
		File: baseFile,
		Loki: &pkglogger.DeliveryLokiConfig{
			DeliveryLokiFlushManagerConfig: pkglogger.DeliveryLokiFlushManagerConfig{
				StaticLabels: map[string]string{"scope": "base"},
			},
		},
		LokiClient: &pkgloki.Config{},
	}

	destinations := newDeliveryInternalLogDestinations(
		logconfig.InternalLogDestinationConfig{
			FileLogPath: "delivery-internal",
			LokiLabels:  map[string]string{"scope": "delivery-internal"},
		},
		internalLogUsesTargets(pkglogger.LogOutputFile, pkglogger.LogOutputLoki),
		base,
	)
	if destinations == nil {
		t.Fatal("delivery internal-log destinations are nil")
	}
	if got := destinations.File.FsstoreConfig.Layout.LogPath; got != "delivery-internal" {
		t.Fatalf("delivery internal-log file path = %q", got)
	}
	if got := destinations.File.LifecycleRunnerSettings.
		LifecycleHandlerConfig.OnInfoConfig; len(got) != 0 {
		t.Fatalf("delivery internal-log lifecycle routing = %v, want empty", got)
	}
	if got := destinations.File.LifecycleRunnerSettings.
		ArchiveCheckpointConfig.SourcePrefix; got != "delivery_logger_file_internal_log" {
		t.Fatalf("delivery internal-log checkpoint source prefix = %q, want dedicated source", got)
	}
	if got := baseFile.LifecycleRunnerSettings.
		ArchiveCheckpointConfig.SourcePrefix; got != "delivery_logger_file" {
		t.Fatalf("delivery base checkpoint source prefix was mutated: %q", got)
	}
	if got := baseFile.LifecycleRunnerSettings.
		LifecycleHandlerConfig.OnInfoConfig; len(got) != 1 || got[0] != "file" {
		t.Fatalf("delivery base lifecycle routing was mutated: %v", got)
	}
	if got := destinations.Loki.StaticLabels["scope"]; got != "delivery-internal" {
		t.Fatalf("delivery internal-log Loki labels = %v", destinations.Loki.StaticLabels)
	}
}

func TestInternalLogDestinationsRequireAnOverriddenUsedBackend(t *testing.T) {
	base := pkglogger.LoggerConfig{
		File: &pkglogger.FileLoggerConfig{},
		Loki: &pkglogger.LokiConfig{},
	}

	if got := newInternalLogDestinations(
		logconfig.InternalLogDestinationConfig{},
		internalLogUsesTargets(pkglogger.LogOutputFile),
		base,
	); got != nil {
		t.Fatalf("destinations without override = %+v, want nil", got)
	}
	if got := newInternalLogDestinations(
		logconfig.InternalLogDestinationConfig{FileLogDir: "internal"},
		internalLogUsesTargets(pkglogger.LogOutputLoki),
		base,
	); got != nil {
		t.Fatalf("destinations for unused override = %+v, want nil", got)
	}
	if got := newInternalLogDestinations(
		logconfig.InternalLogDestinationConfig{
			LokiLabels: map[string]string{"scope": "internal"},
		},
		internalLogUsesTargets(pkglogger.LogOutputLoki),
		base,
	); got != nil {
		t.Fatalf("remote destination without client = %+v, want nil", got)
	}
}

func TestInternalLogDestinationsLokiLabelInheritance(t *testing.T) {
	for _, contract := range []string{"sync_async", "delivery"} {
		for _, tt := range []struct {
			name     string
			override map[string]string
			want     map[string]string
		}{
			{name: "nil inherits base"},
			{name: "empty removes additional labels", override: map[string]string{}, want: map[string]string{}},
			{name: "nonempty replaces base labels", override: map[string]string{"scope": "internal"}, want: map[string]string{"scope": "internal"}},
		} {
			t.Run(contract+"/"+tt.name, func(t *testing.T) {
				baseLabels := map[string]string{"scope": "base", "base_only": "preserved"}
				override := logconfig.InternalLogDestinationConfig{LokiLabels: tt.override}
				usesTarget := internalLogUsesTargets(pkglogger.LogOutputLoki)
				var got, baseAfter map[string]string
				var configured bool
				if contract == "delivery" {
					base := pkglogger.DeliveryLoggerConfig{
						Loki: &pkglogger.DeliveryLokiConfig{
							DeliveryLokiFlushManagerConfig: pkglogger.DeliveryLokiFlushManagerConfig{StaticLabels: baseLabels},
						},
						LokiClient: &pkgloki.Config{},
					}
					destinations := newDeliveryInternalLogDestinations(override, usesTarget, base)
					baseAfter = base.Loki.StaticLabels
					configured = destinations != nil && destinations.Loki != nil
					if configured {
						got = destinations.Loki.StaticLabels
					}
				} else {
					base := pkglogger.LoggerConfig{
						Loki: &pkglogger.LokiConfig{
							LokiFlushManagerConfig: pkglogger.LokiFlushManagerConfig{StaticLabels: baseLabels},
						},
						LokiClient: &pkgloki.Config{},
					}
					destinations := newInternalLogDestinations(override, usesTarget, base)
					baseAfter = base.Loki.StaticLabels
					configured = destinations != nil && destinations.Loki != nil
					if configured {
						got = destinations.Loki.StaticLabels
					}
				}
				if want := tt.override != nil; configured != want {
					t.Fatalf("dedicated Loki config exists = %t, want %t", configured, want)
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Fatalf("internal-log Loki labels = %v, want %v", got, tt.want)
				}
				wantBase := map[string]string{"scope": "base", "base_only": "preserved"}
				if !reflect.DeepEqual(baseAfter, wantBase) {
					t.Fatalf("base Loki labels were mutated: got %v, want %v", baseAfter, wantBase)
				}
			})
		}
	}
}

func TestInternalLogDestinationsSlackChannelInheritance(t *testing.T) {
	baseChannels := [5]string{"base", "base-error", "base-warn", "base-info", "base-debug"}
	for _, contract := range []string{"sync_async", "delivery"} {
		for _, tt := range []struct {
			name     string
			override logconfig.InternalLogDestinationConfig
			want     [5]string
		}{
			{
				name:     "common channel preserves per-level channels",
				override: logconfig.InternalLogDestinationConfig{SlackChannel: "internal"},
				want:     [5]string{"internal", "base-error", "base-warn", "base-info", "base-debug"},
			},
			{
				name: "explicit per-level channels preserve common channel",
				override: logconfig.InternalLogDestinationConfig{
					SlackErrorChannel: "internal-error", SlackWarnChannel: "internal-warn",
					SlackInfoChannel: "internal-info", SlackDebugChannel: "internal-debug",
				},
				want: [5]string{"base", "internal-error", "internal-warn", "internal-info", "internal-debug"},
			},
		} {
			t.Run(contract+"/"+tt.name, func(t *testing.T) {
				usesTarget := internalLogUsesTargets(pkglogger.LogOutputSlack)
				var got, baseAfter [5]string
				if contract == "delivery" {
					base := &pkglogger.DeliverySlackLoggerConfig{
						Channel: baseChannels[0], ErrorChannel: baseChannels[1], WarnChannel: baseChannels[2],
						InfoChannel: baseChannels[3], DebugChannel: baseChannels[4],
					}
					destinations := newDeliveryInternalLogDestinations(tt.override, usesTarget, pkglogger.DeliveryLoggerConfig{Slack: base})
					if destinations == nil || destinations.Slack == nil {
						t.Fatal("dedicated Slack config is nil")
					}
					cfg := destinations.Slack
					got = [5]string{cfg.Channel, cfg.ErrorChannel, cfg.WarnChannel, cfg.InfoChannel, cfg.DebugChannel}
					baseAfter = [5]string{base.Channel, base.ErrorChannel, base.WarnChannel, base.InfoChannel, base.DebugChannel}
				} else {
					base := &pkglogger.SlackLoggerConfig{
						Channel: baseChannels[0], ErrorChannel: baseChannels[1], WarnChannel: baseChannels[2],
						InfoChannel: baseChannels[3], DebugChannel: baseChannels[4],
					}
					destinations := newInternalLogDestinations(tt.override, usesTarget, pkglogger.LoggerConfig{Slack: base})
					if destinations == nil || destinations.Slack == nil {
						t.Fatal("dedicated Slack config is nil")
					}
					cfg := destinations.Slack
					got = [5]string{cfg.Channel, cfg.ErrorChannel, cfg.WarnChannel, cfg.InfoChannel, cfg.DebugChannel}
					baseAfter = [5]string{base.Channel, base.ErrorChannel, base.WarnChannel, base.InfoChannel, base.DebugChannel}
				}
				if got != tt.want {
					t.Fatalf("internal-log Slack channels = %v, want %v", got, tt.want)
				}
				if baseAfter != baseChannels {
					t.Fatalf("base Slack channels were mutated: got %v, want %v", baseAfter, baseChannels)
				}
			})
		}
	}
}

func internalLogUsesTargets(
	targets ...pkglogger.LogOutputDestination,
) func(pkglogger.LogOutputDestination) bool {
	used := make(map[pkglogger.LogOutputDestination]struct{}, len(targets))
	for _, target := range targets {
		used[target] = struct{}{}
	}
	return func(target pkglogger.LogOutputDestination) bool {
		_, ok := used[target]
		return ok
	}
}
