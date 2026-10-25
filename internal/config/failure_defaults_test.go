package config

import (
	"testing"

	clickhouseconfig "github.com/go-jappe/heterolog/internal/config/clickhouse"
	cloudwatchlogsconfig "github.com/go-jappe/heterolog/internal/config/cloudwatchlogs"
	"github.com/go-jappe/heterolog/internal/config/failure"
	lokiconfig "github.com/go-jappe/heterolog/internal/config/loki"
	opensearchconfig "github.com/go-jappe/heterolog/internal/config/opensearch"
)

func TestFailureArchiveDirectoryDefaultsUseExplicitModeAndBackendNames(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{
			name: "opensearch sync",
			got:  opensearchconfig.DefaultFailureArchiveDir,
			want: "opensearch_failure_archives",
		},
		{
			name: "opensearch async",
			got:  opensearchconfig.DefaultAsyncFailureArchiveDir,
			want: "async_opensearch_failure_archives",
		},
		{
			name: "opensearch delivery",
			got:  opensearchconfig.DefaultDeliveryFailureArchiveDir,
			want: "delivery_opensearch_failure_archives",
		},
		{
			name: "loki sync",
			got:  lokiconfig.DefaultLokiFailureArchiveDir,
			want: "loki_failure_archives",
		},
		{
			name: "loki async",
			got:  lokiconfig.DefaultAsyncLokiFailureArchiveDir,
			want: "async_loki_failure_archives",
		},
		{
			name: "loki delivery",
			got:  lokiconfig.DefaultDeliveryLokiFailureArchiveDir,
			want: "delivery_loki_failure_archives",
		},
		{
			name: "cloudwatchlogs sync",
			got:  cloudwatchlogsconfig.DefaultCloudWatchLogsFailureArchiveDir,
			want: "cloudwatchlogs_failure_archives",
		},
		{
			name: "cloudwatchlogs async",
			got:  cloudwatchlogsconfig.DefaultAsyncCloudWatchLogsFailureArchiveDir,
			want: "async_cloudwatchlogs_failure_archives",
		},
		{
			name: "cloudwatchlogs delivery",
			got:  cloudwatchlogsconfig.DefaultDeliveryCloudWatchLogsFailureArchiveDir,
			want: "delivery_cloudwatchlogs_failure_archives",
		},
		{
			name: "clickhouse sync",
			got:  clickhouseconfig.DefaultClickHouseFailureArchiveDir,
			want: "clickhouse_failure_archives",
		},
		{
			name: "clickhouse async",
			got:  clickhouseconfig.DefaultAsyncClickHouseFailureArchiveDir,
			want: "async_clickhouse_failure_archives",
		},
		{
			name: "clickhouse delivery",
			got:  clickhouseconfig.DefaultDeliveryClickHouseFailureArchiveDir,
			want: "delivery_clickhouse_failure_archives",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("failure archive directory = %q, want %q", tt.got, tt.want)
			}
		})
	}
}

func TestOpenSearchFailureStoreDefaultsUseMegabyteValuesOnce(t *testing.T) {
	restoreProcessEnvAfterTest(t)
	clearProcessEnvForTest(t)
	t.Setenv("HETEROLOG_OPENSEARCH_DURABLE_TYPES", "spool,fileArchive")
	t.Setenv("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_TYPES", "spool,fileArchive")

	syncConfig := mustLoadConfig(t, fromProcessEnvironment(loadOpenSearchFlushManagerConfig))
	asyncConfig := mustLoadConfig(t, fromProcessEnvironment(loadAsyncOpenSearchFlushManagerConfig))
	deliveryConfig := mustLoadConfig(t, fromProcessEnvironment(loadDeliveryOpenSearchFlushManagerConfig))

	wantArchiveSize := uint64(failure.DefaultFileArchiveMaxSizeMB) * BytesPerMegabyte
	archives := []struct {
		name        string
		dir         string
		wantDir     string
		maxFileSize uint64
	}{
		{
			name:        "sync",
			dir:         syncConfig.FailureHandlerConfig.Fsstore.FileArchiveConfig.Dir,
			wantDir:     opensearchconfig.DefaultFailureArchiveDir,
			maxFileSize: syncConfig.FailureHandlerConfig.Fsstore.FileArchiveConfig.MaxFileSize,
		},
		{
			name:        "async",
			dir:         asyncConfig.FailureHandlerConfig.Fsstore.FileArchiveConfig.Dir,
			wantDir:     opensearchconfig.DefaultAsyncFailureArchiveDir,
			maxFileSize: asyncConfig.FailureHandlerConfig.Fsstore.FileArchiveConfig.MaxFileSize,
		},
		{
			name:        "delivery",
			dir:         deliveryConfig.FailureHandlerConfig.FileArchiveConfig.Dir,
			wantDir:     opensearchconfig.DefaultDeliveryFailureArchiveDir,
			maxFileSize: deliveryConfig.FailureHandlerConfig.FileArchiveConfig.MaxFileSize,
		},
	}
	for _, archive := range archives {
		t.Run(archive.name, func(t *testing.T) {
			if archive.dir != archive.wantDir {
				t.Fatalf("archive dir = %q, want %q", archive.dir, archive.wantDir)
			}
			if archive.maxFileSize != wantArchiveSize {
				t.Fatalf("archive max file size = %d, want %d", archive.maxFileSize, wantArchiveSize)
			}
		})
	}

	spools := []struct {
		name              string
		dir               string
		wantDir           string
		maxCallsSinceScan uint64
		maxDirSize        uint64
	}{
		{
			name:              "sync",
			dir:               syncConfig.FailureHandlerConfig.Fsstore.SpoolConfig.Dir,
			wantDir:           opensearchconfig.DefaultFailureSpoolDir,
			maxCallsSinceScan: syncConfig.FailureHandlerConfig.Fsstore.SpoolConfig.OverloadPolicyConfig.MaxCallsSinceScan,
			maxDirSize:        syncConfig.FailureHandlerConfig.Fsstore.SpoolConfig.OverloadPolicyConfig.MaxDirSize,
		},
		{
			name:              "async",
			dir:               asyncConfig.FailureHandlerConfig.Fsstore.SpoolConfig.Dir,
			wantDir:           opensearchconfig.DefaultAsyncFailureSpoolDir,
			maxCallsSinceScan: asyncConfig.FailureHandlerConfig.Fsstore.SpoolConfig.OverloadPolicyConfig.MaxCallsSinceScan,
			maxDirSize:        asyncConfig.FailureHandlerConfig.Fsstore.SpoolConfig.OverloadPolicyConfig.MaxDirSize,
		},
	}
	for _, spool := range spools {
		t.Run(spool.name+" spool", func(t *testing.T) {
			if spool.dir != spool.wantDir {
				t.Fatalf("spool dir = %q, want %q", spool.dir, spool.wantDir)
			}
			if spool.maxCallsSinceScan != failure.DefaultMaxSpoolScannedCacheUsageCount {
				t.Fatalf("MaxCallsSinceScan = %d, want %d", spool.maxCallsSinceScan, failure.DefaultMaxSpoolScannedCacheUsageCount)
			}
			wantMaxDirSize := uint64(failure.DefaultSpoolActiveMaxDirSizeMB) * BytesPerMegabyte
			if spool.maxDirSize != wantMaxDirSize {
				t.Fatalf("MaxDirSize = %d, want %d", spool.maxDirSize, wantMaxDirSize)
			}
		})
	}

	durableTypes := deliveryConfig.FailureHandlerConfig.DurableType
	if len(durableTypes) != 1 || durableTypes[0] != failure.DurableTypeFsstore {
		t.Fatalf("delivery durable types = %v, want [%s]", durableTypes, failure.DurableTypeFsstore)
	}
}

func TestBackendFailureStoreDefaultsUseOwnerSpecificDirectories(t *testing.T) {
	restoreProcessEnvAfterTest(t)
	clearProcessEnvForTest(t)
	for _, key := range []string{
		"HETEROLOG_OPENSEARCH_DURABLE_TYPES",
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_TYPES",
		"HETEROLOG_LOKI_DURABLE_TYPES",
		"HETEROLOG_ASYNC_LOKI_DURABLE_TYPES",
		"HETEROLOG_CLOUDWATCH_DURABLE_TYPES",
		"HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_TYPES",
		"HETEROLOG_CLICKHOUSE_DURABLE_TYPES",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_TYPES",
	} {
		t.Setenv(key, "spool,fileArchive")
	}

	type directories struct {
		syncArchive     string
		asyncArchive    string
		deliveryArchive string
		syncSpool       string
		asyncSpool      string
	}

	opensearchSync := mustLoadConfig(t, fromProcessEnvironment(loadOpenSearchFlushManagerConfig))
	opensearchAsync := mustLoadConfig(t, fromProcessEnvironment(loadAsyncOpenSearchFlushManagerConfig))
	opensearchDelivery := mustLoadConfig(t, fromProcessEnvironment(loadDeliveryOpenSearchFlushManagerConfig))
	lokiSync := mustLoadConfig(t, fromProcessEnvironment(loadLokiFlushManagerConfig))
	lokiAsync := mustLoadConfig(t, fromProcessEnvironment(loadAsyncLokiFlushManagerConfig))
	lokiDelivery := mustLoadConfig(t, fromProcessEnvironment(loadDeliveryLokiFlushManagerConfig))
	cloudWatchSync := mustLoadConfig(t, fromProcessEnvironment(loadCloudWatchLogsFlushManagerConfig))
	cloudWatchAsync := mustLoadConfig(t, fromProcessEnvironment(loadAsyncCloudWatchLogsFlushManagerConfig))
	cloudWatchDelivery := mustLoadConfig(t, fromProcessEnvironment(loadDeliveryCloudWatchLogsFlushManagerConfig))
	clickHouseSync := mustLoadConfig(t, fromProcessEnvironment(loadClickHouseFlushManagerConfig))
	clickHouseAsync := mustLoadConfig(t, fromProcessEnvironment(loadAsyncClickHouseFlushManagerConfig))
	clickHouseDelivery := mustLoadConfig(t, fromProcessEnvironment(loadDeliveryClickHouseFlushManagerConfig))

	tests := []struct {
		name string
		got  directories
		want directories
	}{
		{
			name: "opensearch",
			got: directories{
				syncArchive:     opensearchSync.FailureHandlerConfig.Fsstore.FileArchiveConfig.Dir,
				asyncArchive:    opensearchAsync.FailureHandlerConfig.Fsstore.FileArchiveConfig.Dir,
				deliveryArchive: opensearchDelivery.FailureHandlerConfig.FileArchiveConfig.Dir,
				syncSpool:       opensearchSync.FailureHandlerConfig.Fsstore.SpoolConfig.Dir,
				asyncSpool:      opensearchAsync.FailureHandlerConfig.Fsstore.SpoolConfig.Dir,
			},
			want: directories{
				syncArchive:     opensearchconfig.DefaultFailureArchiveDir,
				asyncArchive:    opensearchconfig.DefaultAsyncFailureArchiveDir,
				deliveryArchive: opensearchconfig.DefaultDeliveryFailureArchiveDir,
				syncSpool:       opensearchconfig.DefaultFailureSpoolDir,
				asyncSpool:      opensearchconfig.DefaultAsyncFailureSpoolDir,
			},
		},
		{
			name: "loki",
			got: directories{
				syncArchive:     lokiSync.FailureHandlerConfig.Fsstore.FileArchiveConfig.Dir,
				asyncArchive:    lokiAsync.FailureHandlerConfig.Fsstore.FileArchiveConfig.Dir,
				deliveryArchive: lokiDelivery.FailureHandlerConfig.FileArchiveConfig.Dir,
				syncSpool:       lokiSync.FailureHandlerConfig.Fsstore.SpoolConfig.Dir,
				asyncSpool:      lokiAsync.FailureHandlerConfig.Fsstore.SpoolConfig.Dir,
			},
			want: directories{
				syncArchive:     lokiconfig.DefaultLokiFailureArchiveDir,
				asyncArchive:    lokiconfig.DefaultAsyncLokiFailureArchiveDir,
				deliveryArchive: lokiconfig.DefaultDeliveryLokiFailureArchiveDir,
				syncSpool:       lokiconfig.DefaultLokiFailureSpoolDir,
				asyncSpool:      lokiconfig.DefaultAsyncLokiFailureSpoolDir,
			},
		},
		{
			name: "cloudwatchlogs",
			got: directories{
				syncArchive:     cloudWatchSync.FailureHandlerConfig.Fsstore.FileArchiveConfig.Dir,
				asyncArchive:    cloudWatchAsync.FailureHandlerConfig.Fsstore.FileArchiveConfig.Dir,
				deliveryArchive: cloudWatchDelivery.FailureHandlerConfig.FileArchiveConfig.Dir,
				syncSpool:       cloudWatchSync.FailureHandlerConfig.Fsstore.SpoolConfig.Dir,
				asyncSpool:      cloudWatchAsync.FailureHandlerConfig.Fsstore.SpoolConfig.Dir,
			},
			want: directories{
				syncArchive:     cloudwatchlogsconfig.DefaultCloudWatchLogsFailureArchiveDir,
				asyncArchive:    cloudwatchlogsconfig.DefaultAsyncCloudWatchLogsFailureArchiveDir,
				deliveryArchive: cloudwatchlogsconfig.DefaultDeliveryCloudWatchLogsFailureArchiveDir,
				syncSpool:       cloudwatchlogsconfig.DefaultCloudWatchLogsFailureSpoolDir,
				asyncSpool:      cloudwatchlogsconfig.DefaultAsyncCloudWatchLogsFailureSpoolDir,
			},
		},
		{
			name: "clickhouse",
			got: directories{
				syncArchive:     clickHouseSync.FailureHandlerConfig.Fsstore.FileArchiveConfig.Dir,
				asyncArchive:    clickHouseAsync.FailureHandlerConfig.Fsstore.FileArchiveConfig.Dir,
				deliveryArchive: clickHouseDelivery.FailureHandlerConfig.FileArchiveConfig.Dir,
				syncSpool:       clickHouseSync.FailureHandlerConfig.Fsstore.SpoolConfig.Dir,
				asyncSpool:      clickHouseAsync.FailureHandlerConfig.Fsstore.SpoolConfig.Dir,
			},
			want: directories{
				syncArchive:     clickhouseconfig.DefaultClickHouseFailureArchiveDir,
				asyncArchive:    clickhouseconfig.DefaultAsyncClickHouseFailureArchiveDir,
				deliveryArchive: clickhouseconfig.DefaultDeliveryClickHouseFailureArchiveDir,
				syncSpool:       clickhouseconfig.DefaultClickHouseFailureSpoolDir,
				asyncSpool:      clickhouseconfig.DefaultAsyncClickHouseFailureSpoolDir,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("failure store directories = %+v, want %+v", tt.got, tt.want)
			}
			if tt.got.syncArchive == tt.got.asyncArchive ||
				tt.got.syncArchive == tt.got.deliveryArchive ||
				tt.got.asyncArchive == tt.got.deliveryArchive {
				t.Fatalf("archive defaults must be owner-specific: %+v", tt.got)
			}
			if tt.got.syncSpool == tt.got.asyncSpool {
				t.Fatalf("spool defaults must be owner-specific: %+v", tt.got)
			}
		})
	}
}

// backend送信失敗のfailure archive（fileArchive）は未送信の失敗データを保持するため、
// 既定では保持世代数・保持日数・ディレクトリ上限のいずれでも削除しません（0は上限なし）。
// env未設定時のdefaultと公開例の値を、retryable・permanentを保存する全owner（sync・async・
// delivery）で0に固定します。ディスクの保護は空き容量の閾値による過負荷判定が担います。
func TestBackendFailureArchiveRetentionDefaultsKeepArchives(t *testing.T) {
	restoreProcessEnvAfterTest(t)
	clearProcessEnvForTest(t)
	for _, key := range []string{
		"HETEROLOG_OPENSEARCH_DURABLE_TYPES",
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_TYPES",
		"HETEROLOG_LOKI_DURABLE_TYPES",
		"HETEROLOG_ASYNC_LOKI_DURABLE_TYPES",
		"HETEROLOG_CLOUDWATCH_DURABLE_TYPES",
		"HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_TYPES",
		"HETEROLOG_CLICKHOUSE_DURABLE_TYPES",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_TYPES",
	} {
		t.Setenv(key, "spool,fileArchive")
	}

	type retention struct {
		maxFileBackups int
		maxFileAge     int
		maxDirSize     uint64
	}
	fromArchive := func(cfg failure.FileArchiveConfig) retention {
		return retention{maxFileBackups: cfg.MaxFileBackups, maxFileAge: cfg.MaxFileAge, maxDirSize: cfg.MaxDirSize}
	}
	fromDeliveryArchive := func(cfg failure.DeliveryFileArchiveConfig) retention {
		return retention{maxFileBackups: cfg.MaxFileBackups, maxFileAge: cfg.MaxFileAge, maxDirSize: cfg.MaxDirSize}
	}
	opensearchSync := mustLoadConfig(t, fromProcessEnvironment(loadOpenSearchFlushManagerConfig))
	opensearchAsync := mustLoadConfig(t, fromProcessEnvironment(loadAsyncOpenSearchFlushManagerConfig))
	opensearchDelivery := mustLoadConfig(t, fromProcessEnvironment(loadDeliveryOpenSearchFlushManagerConfig))
	lokiSync := mustLoadConfig(t, fromProcessEnvironment(loadLokiFlushManagerConfig))
	lokiAsync := mustLoadConfig(t, fromProcessEnvironment(loadAsyncLokiFlushManagerConfig))
	lokiDelivery := mustLoadConfig(t, fromProcessEnvironment(loadDeliveryLokiFlushManagerConfig))
	cloudWatchSync := mustLoadConfig(t, fromProcessEnvironment(loadCloudWatchLogsFlushManagerConfig))
	cloudWatchAsync := mustLoadConfig(t, fromProcessEnvironment(loadAsyncCloudWatchLogsFlushManagerConfig))
	cloudWatchDelivery := mustLoadConfig(t, fromProcessEnvironment(loadDeliveryCloudWatchLogsFlushManagerConfig))
	clickHouseSync := mustLoadConfig(t, fromProcessEnvironment(loadClickHouseFlushManagerConfig))
	clickHouseAsync := mustLoadConfig(t, fromProcessEnvironment(loadAsyncClickHouseFlushManagerConfig))
	clickHouseDelivery := mustLoadConfig(t, fromProcessEnvironment(loadDeliveryClickHouseFlushManagerConfig))
	loaded := map[string]retention{
		"OPENSEARCH":          fromArchive(opensearchSync.FailureHandlerConfig.Fsstore.FileArchiveConfig),
		"ASYNC_OPENSEARCH":    fromArchive(opensearchAsync.FailureHandlerConfig.Fsstore.FileArchiveConfig),
		"DELIVERY_OPENSEARCH": fromDeliveryArchive(opensearchDelivery.FailureHandlerConfig.FileArchiveConfig),
		"LOKI":                fromArchive(lokiSync.FailureHandlerConfig.Fsstore.FileArchiveConfig),
		"ASYNC_LOKI":          fromArchive(lokiAsync.FailureHandlerConfig.Fsstore.FileArchiveConfig),
		"DELIVERY_LOKI":       fromDeliveryArchive(lokiDelivery.FailureHandlerConfig.FileArchiveConfig),
		"CLOUDWATCH":          fromArchive(cloudWatchSync.FailureHandlerConfig.Fsstore.FileArchiveConfig),
		"ASYNC_CLOUDWATCH":    fromArchive(cloudWatchAsync.FailureHandlerConfig.Fsstore.FileArchiveConfig),
		"DELIVERY_CLOUDWATCH": fromDeliveryArchive(cloudWatchDelivery.FailureHandlerConfig.FileArchiveConfig),
		"CLICKHOUSE":          fromArchive(clickHouseSync.FailureHandlerConfig.Fsstore.FileArchiveConfig),
		"ASYNC_CLICKHOUSE":    fromArchive(clickHouseAsync.FailureHandlerConfig.Fsstore.FileArchiveConfig),
		"DELIVERY_CLICKHOUSE": fromDeliveryArchive(clickHouseDelivery.FailureHandlerConfig.FileArchiveConfig),
	}

	example := readPublicEnvExample(t)
	for scope, got := range loaded {
		t.Run(scope, func(t *testing.T) {
			if got != (retention{}) {
				t.Fatalf("default failure archive retention = %+v, want all limits 0 (no deletion)", got)
			}
			for _, key := range []string{
				"HETEROLOG_MAX_" + scope + "_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS",
				"HETEROLOG_MAX_" + scope + "_ENABLE_DURABLE_FILE_ARCHIVE_AGE",
				"HETEROLOG_MAX_" + scope + "_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB",
			} {
				value, ok := example[key]
				if !ok {
					t.Fatalf(".env.example is missing failure archive retention key %s", key)
				}
				if value != "0" {
					t.Fatalf(".env.example %s = %q, want %q (no deletion by default)", key, value, "0")
				}
			}
		})
	}
}
