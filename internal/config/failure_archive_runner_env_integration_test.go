package config

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/fsarchivehandler"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
)

// TestFixtureEnvMapsAndRunsFailureArchiveRunnerは、公開設定のfixtureを読み込み、
// config mappingで作られた設定をそのままfailure archive handlerへ渡します。
// これにより、environment key、JSONL命名契約、runner配線が個別試験ではなく
// 一つのproduction境界として接続されていることを確認します。
func TestFixtureEnvMapsAndRunsFailureArchiveRunner(t *testing.T) {
	repoRoot := configFixtureRootForTest(t)
	sourceEnvPath := filepath.Join(repoRoot, ".env")
	sourceEnv, err := os.ReadFile(sourceEnvPath)
	if err != nil {
		t.Fatalf("read fixture .env: %v", err)
	}

	restoreProcessEnvAfterTest(t)
	envPath := filepath.Join(t.TempDir(), ".env")
	stateDir := t.TempDir()
	clearProcessEnvForTest(t)

	if err := os.WriteFile(envPath, sourceEnv, 0o600); err != nil {
		t.Fatalf("copy fixture .env: %v", err)
	}
	setEnvValuesForTest(t, map[string]string{
		"HETEROLOG_ENV_PATH": envPath,

		"HETEROLOG_APP_STATE_DIR": stateDir,
		"HETEROLOG_TARGET":        "failure-archive-env-test",

		"HETEROLOG_ENABLE_LOGGING":                   "true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING": "false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING":          "false",
		"HETEROLOG_LOG_TYPE":                         "opensearch",
		// 初回scanも周期を待つため、運用向けの長いintervalへ依存しません。
		"HETEROLOG_APPENDLOG_RUNNER_INTERVAL_SECOND": "1",

		"HETEROLOG_OPENSEARCH_DURABLE_TYPES":                                     string(durable.DurableTypeFileArchive),
		"HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE":               string(pkgfsstore.FileLifecycleModeRunner),
		"HETEROLOG_ENABLE_OPENSEARCH_DURABLE_FILE_ARCHIVE_COMPRESS":              "true",
		"HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT":              string(pkgfsstore.CompressGzip),
		"HETEROLOG_ENABLE_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD": "false",

		// この試験ではrunnerの配線だけを対象とし、internal logの外部送信は起動しません。
		"HETEROLOG_OPENSEARCH_DURABLE_FILE_ERROR_HANDLER": "",
		"HETEROLOG_OPENSEARCH_DURABLE_FILE_WARN_HANDLER":  "",
		"HETEROLOG_OPENSEARCH_DURABLE_FILE_INFO_HANDLER":  "",
		"HETEROLOG_OPENSEARCH_DURABLE_FILE_DEBUG_HANDLER": "",
	})

	appCfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() from copied fixture .env: %v", err)
	}
	mapped, err := MapLoggerConfig(appCfg)
	if err != nil {
		t.Fatalf("MapLoggerConfig() from copied fixture .env: %v", err)
	}
	if mapped.OpenSearch == nil {
		t.Fatal("mapped OpenSearch config is nil")
	}

	archiveCfg := mapped.OpenSearch.
		OpenSearchFlushManagerConfig.
		FailureStoreConfig.
		DurableStoreConfig.
		FileArchiveConfig
	if archiveCfg.LifecycleMode != pkgfsstore.FileLifecycleModeRunner {
		t.Fatalf("mapped lifecycle mode = %q, want runner", archiveCfg.LifecycleMode)
	}
	if archiveCfg.FileNameFormat != pkgfsstore.FailureArchiveFileNameFormat {
		t.Fatalf(
			"mapped file name format = %q, want %q",
			archiveCfg.FileNameFormat,
			pkgfsstore.FailureArchiveFileNameFormat,
		)
	}

	// 圧縮配線の試験なので、保持期限で削除されない前日のファイルを使います。
	now := time.Date(2042, time.May, 6, 12, 0, 0, 0, time.UTC)
	interval := appCfg.AppendLogRunnerConfig.Interval
	if interval != time.Second {
		t.Fatalf("HETEROLOG_APPENDLOG_RUNNER_INTERVAL_SECOND loaded interval = %s, want 1s", interval)
	}
	// runnerの周期は偽の時刻で進め、実時間の待機やpollingで処理の完了を判定しません。
	synctestWithHangWatchdog(t, func(t *testing.T) {
		pipeline, err := fsarchivehandler.NewFailureArchivePipeline(
			context.Background(),
			clock.NewFixedClock(now),
			loggerdurable.NewFileArchiveConfigAdapter(archiveCfg),
			observer.NopTracer{},
			observer.NopMeter{},
			fsarchivehandler.InternalLogHandlers{},
		)
		if err != nil {
			t.Fatalf("NewFailureArchivePipeline() from mapped config: %v", err)
		}
		// 判定が失敗してもbubble内のgoroutineを止められるよう、終了処理を登録します。
		t.Cleanup(func() {
			if err := pipeline.Shutdown(context.Background()); err != nil {
				t.Errorf("cleanup failure archive pipeline: %v", err)
			}
		})

		activePath, err := pipeline.ActiveArchivePath(failurearchive.FailureCategoryRetryable)
		if err != nil {
			t.Fatalf("ActiveArchivePath(retryable): %v", err)
		}
		inactivePath := filepath.Join(
			filepath.Dir(activePath),
			archiveCfg.Target+"_"+now.AddDate(0, 0, -1).Format(pkgfsstore.FileDateFormat)+"_1."+failurearchive.FileExtension,
		)
		if err := os.WriteFile(inactivePath, []byte("{\"failure\":\"retryable\"}\n"), 0o600); err != nil {
			t.Fatalf("write inactive failure archive: %v", err)
		}
		// 初回scanも周期を待つため、一周期だけ時刻を進め、runnerがその周期の処理を終えて
		// 次の周期を待つまで待ちます。周期の前には圧縮しないことも確認します。
		if _, err := os.Stat(inactivePath); err != nil {
			t.Fatalf("inactive JSONL before the first runner period: %v", err)
		}
		time.Sleep(interval)
		synctest.Wait()

		// Shutdownは新しい周期を開始しないため、runner自身の周期処理による圧縮を
		// 観測してから停止します。
		if _, err := os.Stat(inactivePath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("inactive JSONL stat error after one runner period = %v, want source removed after compression", err)
		}
		if _, err := os.Stat(inactivePath + pkgfsstore.ExtensionCompressGzip); err != nil {
			t.Fatalf("compressed JSONL is missing after one runner period: %v", err)
		}
		if err := pipeline.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown() after runner compression: %v", err)
		}
		if _, err := os.Stat(inactivePath + pkgfsstore.ExtensionCompressGzip); err != nil {
			t.Fatalf("compressed JSONL is missing after shutdown: %v", err)
		}
		if _, err := os.Stat(activePath); err != nil {
			t.Fatalf("shutdown-time active JSONL must remain uncompressed: %v", err)
		}
	})
}

func setEnvValuesForTest(t *testing.T, values map[string]string) {
	t.Helper()
	for key, value := range values {
		if err := os.Setenv(key, value); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}
}
