package config

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	configruntime "github.com/go-jappe/heterolog/internal/config/runtime"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestFixtureEnvMapsArchiveCheckpointForEveryLoggerAndBackendScope(t *testing.T) {
	repoRoot := configFixtureRootForTest(t)
	sourceEnv, err := os.ReadFile(filepath.Join(repoRoot, ".env"))
	if err != nil {
		t.Fatalf("read fixture .env: %v", err)
	}

	restoreProcessEnvAfterTest(t)
	envPath := filepath.Join(t.TempDir(), ".env")
	stateBase := filepath.Join(t.TempDir(), "state")
	clearProcessEnvForTest(t)
	if err := os.WriteFile(envPath, sourceEnv, 0o600); err != nil {
		t.Fatalf("copy fixture .env: %v", err)
	}
	const target = "fixture-env-checkpoint-mapping"
	setEnvValuesForTest(t, map[string]string{
		"HETEROLOG_ENV_PATH":      envPath,
		"HETEROLOG_APP_STATE_DIR": stateBase,
		"HETEROLOG_TARGET":        target,
		// 公開例の値は既定値と同じtrueなので、falseを与えて全scopeへの伝搬を確認します。
		"HETEROLOG_ENABLE_ARCHIVE_CHECKPOINT_AUTO_RECOVERY": "false",
	})

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() from copied fixture .env: %v", err)
	}
	wantDirectory := filepath.Join(stateBase, target, "archive-checkpoints")
	configs := []struct {
		name string
		cfg  pkgfsstore.ArchiveCheckpointConfig
	}{
		{name: "sync file", cfg: NewFileLoggerConfig(&cfg).ArchiveCheckpointConfig},
		{name: "async file", cfg: NewAsyncFileLoggerConfig(&cfg).ArchiveCheckpointConfig},
		{name: "delivery file", cfg: NewDeliveryFileLoggerConfig(&cfg).LifecycleRunnerSettings.ArchiveCheckpointConfig},
		{name: "sync opensearch", cfg: NewOpenSearchFlushManagerConfig(&cfg).FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig},
		{name: "async opensearch", cfg: NewAsyncOpenSearchFlushManagerConfig(&cfg).FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig},
		{name: "delivery opensearch", cfg: NewDeliveryOpenSearchFlushManagerConfig(&cfg).FailureStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig},
		{name: "sync loki", cfg: NewLokiFlushManagerConfig(&cfg).FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig},
		{name: "async loki", cfg: NewAsyncLokiFlushManagerConfig(&cfg).FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig},
		{name: "delivery loki", cfg: NewDeliveryLokiFlushManagerConfig(&cfg).FailureStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig},
		{name: "sync cloudwatch logs", cfg: NewCloudWatchLogsFlushManagerConfig(&cfg).FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig},
		{name: "async cloudwatch logs", cfg: NewAsyncCloudWatchLogsFlushManagerConfig(&cfg).FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig},
		{name: "delivery cloudwatch logs", cfg: NewDeliveryCloudWatchLogsFlushManagerConfig(&cfg).FailureStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig},
		{name: "sync clickhouse", cfg: NewClickHouseFlushManagerConfig(&cfg).FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig},
		{name: "async clickhouse", cfg: NewAsyncClickHouseFlushManagerConfig(&cfg).FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig},
		{name: "delivery clickhouse", cfg: NewDeliveryClickHouseFlushManagerConfig(&cfg).FailureStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig},
	}

	seenSources := make(map[string]string, len(configs))
	for _, candidate := range configs {
		if candidate.cfg.Directory != wantDirectory {
			t.Fatalf("%s checkpoint directory = %q, want %q", candidate.name, candidate.cfg.Directory, wantDirectory)
		}
		if candidate.cfg.SourcePrefix == "" {
			t.Fatalf("%s checkpoint source prefix is empty", candidate.name)
		}
		if candidate.cfg.AutoRecoverMissingCheckpoint {
			t.Fatalf("%s checkpoint auto recovery = true, want overridden false", candidate.name)
		}
		if previous, exists := seenSources[candidate.cfg.SourcePrefix]; exists {
			t.Fatalf("%s checkpoint source %q duplicates %s", candidate.name, candidate.cfg.SourcePrefix, previous)
		}
		seenSources[candidate.cfg.SourcePrefix] = candidate.name
	}
}

func TestArchiveCheckpointMappingCoversEveryLoggerAndBackendScope(t *testing.T) {
	cfg := AppConfig{}
	cfg.RuntimeEnv.Cloud = &configruntime.CloudInfo{}
	cfg.FsstoreConfig.AppStatePath = filepath.Join(t.TempDir(), "state")
	cfg.RuntimeConfig.Target = "mapping-target"
	wantDirectory := filepath.Join(
		cfg.FsstoreConfig.AppStatePath,
		cfg.RuntimeConfig.Target,
		"archive-checkpoints",
	)

	type mappingTest struct {
		name       string
		wantSource archiveCheckpointSourcePrefix
		config     func() pkgfsstore.ArchiveCheckpointConfig
	}
	tests := []mappingTest{
		{
			name:       "sync file",
			wantSource: checkpointSourceLoggerFile,
			config: func() pkgfsstore.ArchiveCheckpointConfig {
				return NewFileLoggerConfig(&cfg).ArchiveCheckpointConfig
			},
		},
		{
			name:       "async file",
			wantSource: checkpointSourceAsyncLoggerFile,
			config: func() pkgfsstore.ArchiveCheckpointConfig {
				return NewAsyncFileLoggerConfig(&cfg).ArchiveCheckpointConfig
			},
		},
		{
			name:       "delivery file",
			wantSource: checkpointSourceDeliveryLoggerFile,
			config: func() pkgfsstore.ArchiveCheckpointConfig {
				return NewDeliveryFileLoggerConfig(&cfg).
					LifecycleRunnerSettings.ArchiveCheckpointConfig
			},
		},
	}

	appendFailure := func(
		name string,
		syncSource archiveCheckpointSourcePrefix,
		asyncSource archiveCheckpointSourcePrefix,
		deliverySource archiveCheckpointSourcePrefix,
		syncConfig func() pkgfsstore.ArchiveCheckpointConfig,
		asyncConfig func() pkgfsstore.ArchiveCheckpointConfig,
		deliveryConfig func() pkgfsstore.ArchiveCheckpointConfig,
	) {
		tests = append(tests,
			mappingTest{name: "sync " + name, wantSource: syncSource, config: syncConfig},
			mappingTest{name: "async " + name, wantSource: asyncSource, config: asyncConfig},
			mappingTest{name: "delivery " + name, wantSource: deliverySource, config: deliveryConfig},
		)
	}

	appendFailure(
		"opensearch failure archive",
		checkpointSourceLoggerOpenSearch,
		checkpointSourceAsyncLoggerOpenSearch,
		checkpointSourceDeliveryLoggerOpenSearch,
		func() pkgfsstore.ArchiveCheckpointConfig {
			return NewOpenSearchFlushManagerConfig(&cfg).FailureStoreConfig.
				DurableStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig
		},
		func() pkgfsstore.ArchiveCheckpointConfig {
			return NewAsyncOpenSearchFlushManagerConfig(&cfg).FailureStoreConfig.
				DurableStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig
		},
		func() pkgfsstore.ArchiveCheckpointConfig {
			return NewDeliveryOpenSearchFlushManagerConfig(&cfg).FailureStoreConfig.
				FileArchiveConfig.ArchiveCheckpointConfig
		},
	)
	appendFailure(
		"loki failure archive",
		checkpointSourceLoggerLoki,
		checkpointSourceAsyncLoggerLoki,
		checkpointSourceDeliveryLoggerLoki,
		func() pkgfsstore.ArchiveCheckpointConfig {
			return NewLokiFlushManagerConfig(&cfg).FailureStoreConfig.
				DurableStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig
		},
		func() pkgfsstore.ArchiveCheckpointConfig {
			return NewAsyncLokiFlushManagerConfig(&cfg).FailureStoreConfig.
				DurableStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig
		},
		func() pkgfsstore.ArchiveCheckpointConfig {
			return NewDeliveryLokiFlushManagerConfig(&cfg).FailureStoreConfig.
				FileArchiveConfig.ArchiveCheckpointConfig
		},
	)
	appendFailure(
		"cloudwatchlogs failure archive",
		checkpointSourceLoggerCloudWatchLogs,
		checkpointSourceAsyncLoggerCloudWatchLogs,
		checkpointSourceDeliveryLoggerCloudWatchLogs,
		func() pkgfsstore.ArchiveCheckpointConfig {
			return NewCloudWatchLogsFlushManagerConfig(&cfg).FailureStoreConfig.
				DurableStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig
		},
		func() pkgfsstore.ArchiveCheckpointConfig {
			return NewAsyncCloudWatchLogsFlushManagerConfig(&cfg).FailureStoreConfig.
				DurableStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig
		},
		func() pkgfsstore.ArchiveCheckpointConfig {
			return NewDeliveryCloudWatchLogsFlushManagerConfig(&cfg).FailureStoreConfig.
				FileArchiveConfig.ArchiveCheckpointConfig
		},
	)
	appendFailure(
		"clickhouse failure archive",
		checkpointSourceLoggerClickHouse,
		checkpointSourceAsyncLoggerClickHouse,
		checkpointSourceDeliveryLoggerClickHouse,
		func() pkgfsstore.ArchiveCheckpointConfig {
			return NewClickHouseFlushManagerConfig(&cfg).FailureStoreConfig.
				DurableStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig
		},
		func() pkgfsstore.ArchiveCheckpointConfig {
			return NewAsyncClickHouseFlushManagerConfig(&cfg).FailureStoreConfig.
				DurableStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig
		},
		func() pkgfsstore.ArchiveCheckpointConfig {
			return NewDeliveryClickHouseFlushManagerConfig(&cfg).FailureStoreConfig.
				FileArchiveConfig.ArchiveCheckpointConfig
		},
	)

	seenSources := make(map[string]string, len(tests))
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := test.config()
			if got.Directory != wantDirectory {
				t.Fatalf("checkpoint directory = %q, want %q", got.Directory, wantDirectory)
			}
			if got.SourcePrefix != string(test.wantSource) {
				t.Fatalf("checkpoint source = %q, want %q", got.SourcePrefix, test.wantSource)
			}
			if previous, duplicated := seenSources[got.SourcePrefix]; duplicated {
				t.Fatalf("checkpoint source %q duplicates %s", got.SourcePrefix, previous)
			}
			seenSources[got.SourcePrefix] = test.name
			for _, enabled := range []bool{false, true} {
				t.Run("auto_recovery="+strconv.FormatBool(enabled), func(t *testing.T) {
					cfg.ArchiveCheckpointAutoRecovery = enabled
					if got := test.config().AutoRecoverMissingCheckpoint; got != enabled {
						t.Fatalf("checkpoint auto recovery = %t, want %t", got, enabled)
					}
				})
			}
		})
	}
}
