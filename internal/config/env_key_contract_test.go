package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/config/renamedkey"
	"github.com/go-jappe/heterolog/internal/testutil"
)

// 配布用の設定例を直接検査します。生成した2つの派生fileを比べても、公開している
// 設定項目ではなく、fixture生成処理を再検証するだけになるためです。
func TestPublicEnvExampleHasUniqueKeys(t *testing.T) {
	root := testutil.RepositoryRoot(t)
	if keys := readUniqueEnvKeys(t, filepath.Join(root, ".env.example")); len(keys) == 0 {
		t.Fatal("public example has no settings")
	}
}

// Delivery loggerはbackend recordの再送spoolを使用しません。ただし、複数recordを含む
// 圧縮log fileのupload失敗には専用failure storeを使用でき、その保存方式として
// compressed log file spoolを選択できます。配布用.env.exampleには、その専用prefix
// 以外のDelivery spool設定を再導入させません。
func TestPublicEnvExampleDoesNotExposeDeliverySpoolKeys(t *testing.T) {
	repoRoot := testutil.RepositoryRoot(t)
	for _, name := range []string{".env.example"} {
		keys := readUniqueEnvKeys(t, filepath.Join(repoRoot, name))
		for key := range keys {
			if strings.Contains(key, "DELIVERY") &&
				strings.Contains(key, "SPOOL") &&
				!strings.HasPrefix(key, "HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_") {
				t.Errorf("%s declares Delivery spool key %s", name, key)
			}
			if strings.HasPrefix(key, "HETEROLOG_MAX_DELIVERY_") && strings.HasSuffix(key, "_FAILURE_HANDLER_CONCURRENT") {
				t.Errorf("%s declares Delivery failure-handler concurrency key %s", name, key)
			}
		}
	}
}

// failure spoolは、選択されたsync／async backendのfailure storeでDurableTypeSpoolを
// 指定するとwriterとrunnerが一組で有効になります。独立したrunner有効化やbackend
// filterを.envへ再導入するとwriterだけが動く不整合を作れるため、公開設定に含めません。
func TestPublicEnvExampleDoesNotExposeIndependentFailureSpoolActivationKeys(t *testing.T) {
	removedKeys := []string{
		"HETEROLOG_ENABLE_SPOOL_RUNNER",
		"HETEROLOG_SPOOL_SERVICES",
	}

	repoRoot := testutil.RepositoryRoot(t)
	for _, name := range []string{".env.example"} {
		keys := readUniqueEnvKeys(t, filepath.Join(repoRoot, name))
		for _, key := range removedKeys {
			if _, exists := keys[key]; exists {
				t.Errorf("%s declares removed independent failure spool activation key %s", name, key)
			}
		}
	}
}

// LokiとCloudWatch Logsの即時送信はNO_BULK_FLUSH_MODEを正本とします。
// 同じ動作を表す旧IMMEDIATE_*キーを再導入すると設定の優先順位が曖昧になるため、
// 配布用env fileに公開しません。
func TestPublicEnvExampleDoesNotExposeRedundantImmediateSendModeKeys(t *testing.T) {
	removedKeys := []string{
		"HETEROLOG_ENABLE_LOKI_IMMEDIATE_PUSH_MODE",
		"HETEROLOG_ENABLE_CLOUDWATCH_IMMEDIATE_PUT_MODE",
	}

	repoRoot := testutil.RepositoryRoot(t)
	for _, name := range []string{".env.example"} {
		keys := readUniqueEnvKeys(t, filepath.Join(repoRoot, name))
		for _, key := range removedKeys {
			if _, exists := keys[key]; exists {
				t.Errorf("%s declares removed duplicate immediate-send key %s", name, key)
			}
		}
	}
}

// OpenSearch spoolのpurge設定は、全ownerを処理するspool runnerの
// HETEROLOG_ENABLE_PURGE_* / HETEROLOG_MAX_*_SPOOL_{DONE,DEAD,QUARANTINE}_* が所有します。
// 旧file lifecycleの未読キーや、現実装に存在しないactive file件数制限を
// .envへ戻しても動作へ反映されないため、設定可能であるかのように掲載しません。
func TestPublicEnvExampleDoesNotExposeLegacyOpenSearchSpoolKeys(t *testing.T) {
	legacyKeys := []string{
		"HETEROLOG_ENABLE_OPENSEARCH_SPOOL_FILE_DONE_DELETE",
		"HETEROLOG_MAX_OPENSEARCH_SPOOL_FILE_DONE_AGE",
		"HETEROLOG_MAX_OPENSEARCH_SPOOL_FILE_DONE_DIR_SIZE_MB",
		"HETEROLOG_ENABLE_OPENSEARCH_SPOOL_FILE_FAILED_DELETE",
		"HETEROLOG_MAX_OPENSEARCH_SPOOL_FILE_FAILED_AGE",
		"HETEROLOG_MAX_OPENSEARCH_SPOOL_FILE_FAILED_DIR_SIZE_MB",
		"HETEROLOG_MAX_OPENSEARCH_ACTIVE_SPOOL_FILE_NUMBER",
		"HETEROLOG_ENABLE_ASYNC_OPENSEARCH_SPOOL_FILE_DONE_DELETE",
		"HETEROLOG_MAX_ASYNC_OPENSEARCH_SPOOL_FILE_DONE_AGE",
		"HETEROLOG_MAX_ASYNC_OPENSEARCH_SPOOL_FILE_DONE_DIR_SIZE_MB",
		"HETEROLOG_ENABLE_ASYNC_OPENSEARCH_SPOOL_FILE_FAILED_DELETE",
		"HETEROLOG_MAX_ASYNC_OPENSEARCH_SPOOL_FILE_FAILED_AGE",
		"HETEROLOG_MAX_ASYNC_OPENSEARCH_SPOOL_FILE_FAILED_DIR_SIZE_MB",
		"HETEROLOG_MAX_ASYNC_OPENSEARCH_ACTIVE_SPOOL_FILE_NUMBER",
	}

	repoRoot := testutil.RepositoryRoot(t)
	for _, name := range []string{".env.example"} {
		keys := readUniqueEnvKeys(t, filepath.Join(repoRoot, name))
		for _, key := range legacyKeys {
			if _, exists := keys[key]; exists {
				t.Errorf("%s declares unused legacy key %s", name, key)
			}
		}
	}
}

// 公開キーは英単語の正しい綴り（HANDLER、RATIO、SCANNED）を使用します。配布用
// .env.exampleで旧綴りへ戻る誤りを検出します。旧綴りのkeyはloaderが設定エラーにします。
func TestPublicEnvExampleDoesNotExposeMisspelledKeys(t *testing.T) {
	misspellings := []string{"HAND" + "ER", "RAI" + "TO", "SCAN" + "ED"}
	keys := readUniqueEnvKeys(t, publicEnvExamplePath(t))
	for key := range keys {
		for _, misspelling := range misspellings {
			if strings.Contains(key, misspelling) {
				t.Errorf(".env.example declares misspelled env key %s", key)
			}
		}
		if renamed, found := renamedkey.Current(key); found {
			t.Errorf(".env.example declares renamed env key %s; use %s", key, renamed)
		}
	}
}

func TestPublicEnvExampleExposesDebugLogDirectories(t *testing.T) {
	repoRoot := testutil.RepositoryRoot(t)
	for _, name := range []string{".env.example"} {
		keys := readUniqueEnvKeys(t, filepath.Join(repoRoot, name))
		for _, prefix := range []string{"", "ASYNC_", "DELIVERY_"} {
			for _, key := range []string{
				"HETEROLOG_" + prefix + "DEBUG_LOG_DIR",
				"HETEROLOG_ENABLE_" + prefix + "LOG_FILE_SEPARATE_DEBUG_LOG_DIR",
				"HETEROLOG_" + prefix + "LOG_INTERNAL_LOG_FILE_DEBUG_LOG_DIR",
				"HETEROLOG_" + prefix + "LOG_INTERNAL_LOG_FILE_SEPARATE_DEBUG_LOG_DIR",
			} {
				if _, exists := keys[key]; !exists {
					t.Errorf("%s is missing debug log directory key %s", name, key)
				}
			}
		}
	}
}

func readUniqueEnvKeys(t *testing.T, path string) map[string]struct{} {
	t.Helper()

	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	keys := make(map[string]struct{})
	for lineNumber, line := range strings.Split(string(contents), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, _, ok := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" {
			continue
		}
		if _, exists := keys[key]; exists {
			t.Fatalf("%s:%d declares duplicate env key %s", path, lineNumber+1, key)
		}
		keys[key] = struct{}{}
	}
	return keys
}
