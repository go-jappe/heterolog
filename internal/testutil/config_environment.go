package testutil

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/config/renamedkey"
)

// commentedConfigAssignmentは、公開設定例で既定値を示すためにコメントにした項目です。
var commentedConfigAssignment = regexp.MustCompile(`^#\s*([A-Z][A-Z0-9_]*)=`)

// ConfigLoaderOnlyKeysは、設定loaderが読みますが公開設定例には記載していない項目を返します。
// 返り値は呼び出し側で変更できる複製です。
func ConfigLoaderOnlyKeys() []string {
	return append([]string(nil), configLoaderOnlyKeys...)
}

// configLoaderOnlyKeysは、設定loaderが読みますが公開設定例には記載していない項目です。
var configLoaderOnlyKeys = func() []string {
	keys := []string{
		// 読み込む環境設定fileの選択です。
		"HETEROLOG_ENV_PATH",
		// S3とCloudWatch Logsの設定を省略した場合に、loaderが既定値として参照します。
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_REGION",
	}
	for _, prefix := range []string{"", "ASYNC_", "DELIVERY_"} {
		for _, backend := range []string{"OPENSEARCH", "LOKI", "CLOUDWATCH", "CLICKHOUSE"} {
			for _, directory := range []string{"RETRYABLE", "PERMANENT"} {
				keys = append(keys, "HETEROLOG_"+prefix+backend+"_DURABLE_"+directory+"_DIR")
			}
		}
	}
	return keys
}()

// UnsetConfigEnvironmentは、設定loaderが読む環境変数だけをprocess environmentから外します。
// 対象は公開設定例.env.exampleの項目（既定値を示すコメントの項目を含む）と、HETEROLOG_ENV_PATH、
// loaderが既定値として参照するAWSの認証情報・regionです。loaderが改名を案内して
// 設定エラーにする旧名のkeyも外します。PATH、HOME、TMP・TEMP・TMPDIR、SystemRoot、
// proxyや証明書の指定など、設定項目以外の実行環境は保持します。
// テスト終了時に環境全体を元へ戻します。呼び出し側は並列実行しないでください。
func UnsetConfigEnvironment(t testing.TB) {
	t.Helper()
	keys := configEnvironmentKeys(t)
	PreserveEnvironment(t)
	for _, entry := range os.Environ() {
		if key, _, ok := splitEnvironmentEntry(entry); ok && isRenamedConfigKey(key) {
			keys = append(keys, key)
		}
	}
	for _, key := range keys {
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset config variable %s: %v", key, err)
		}
	}
}

// isRenamedConfigKeyは、設定loaderが改名を案内して拒否する旧名のkeyかを返します。
// 規則は設定loaderと共有するleaf package renamedkeyに定義しています。
func isRenamedConfigKey(key string) bool {
	_, renamed := renamedkey.Current(key)
	return renamed
}

// configEnvironmentKeysは、隔離する設定項目を公開設定例から読み取ります。
// 開発者の.envは読みません。
func configEnvironmentKeys(t testing.TB) []string {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join(RepositoryRoot(t), ".env.example"))
	if err != nil {
		t.Fatalf("read public config example: %v", err)
	}
	unique := make(map[string]struct{})
	for _, key := range configLoaderOnlyKeys {
		unique[key] = struct{}{}
	}
	for _, line := range strings.Split(string(contents), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#") {
			if match := commentedConfigAssignment.FindStringSubmatch(line); match != nil {
				unique[match[1]] = struct{}{}
			}
			continue
		}
		key, _, ok := strings.Cut(line, "=")
		key = strings.TrimSpace(strings.TrimPrefix(key, "export "))
		if ok && key != "" {
			unique[key] = struct{}{}
		}
	}
	keys := make([]string, 0, len(unique))
	for key := range unique {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
