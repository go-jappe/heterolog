package testutil

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/joho/godotenv"
)

// RequireIntegrationEnvironment opts in to a live test without making HETEROLOG_ENV_PATH
// affect unrelated tests. Only the explicitly selected file supplies config
// settings: config keys inherited from the process are unset first, while the
// runtime environment such as PATH, HOME, TMP, SystemRoot, proxies, and
// certificate locations is kept. The environment is restored after the test.
// The caller must not run in parallel because config.LoadConfig reads process env.
func RequireIntegrationEnvironment(t testing.TB, optInVariable string) map[string]string {
	t.Helper()
	if !strings.EqualFold(os.Getenv(optInVariable), "true") && os.Getenv(optInVariable) != "1" {
		t.Skipf("set %s=true and HETEROLOG_INTEGRATION_ENV_PATH to run this live integration test", optInVariable)
	}
	envPath := strings.TrimSpace(os.Getenv("HETEROLOG_INTEGRATION_ENV_PATH"))
	if envPath == "" {
		t.Fatal("live integration requires HETEROLOG_INTEGRATION_ENV_PATH pointing to a dedicated environment file")
	}
	absolute, err := filepath.Abs(envPath)
	if err != nil {
		t.Fatal(err)
	}
	settings, err := godotenv.Read(absolute)
	if err != nil {
		t.Fatalf("read integration environment file: %v", err)
	}
	// godotenvは既存の環境変数を上書きしないため、file外の設定が混入しないよう
	// 設定項目だけを外します。接続や一時fileに必要な実行環境は保持します。
	UnsetConfigEnvironment(t)
	for key, value := range settings {
		if err := os.Setenv(key, value); err != nil {
			t.Fatalf("set integration variable %s: %v", key, err)
		}
	}
	if err := os.Setenv("HETEROLOG_ENV_PATH", absolute); err != nil {
		t.Fatal(err)
	}
	return settings
}
