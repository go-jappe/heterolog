package testutil

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestIntegrationEnvironmentDoesNotOptInWithOrdinaryEnvPath(t *testing.T) {
	t.Setenv("TESTUTIL_LIVE_TEST", "")
	t.Setenv("HETEROLOG_ENV_PATH", "/an/unrelated/application.env")
	skipped := false
	t.Run("requires opt in", func(t *testing.T) {
		defer func() { skipped = t.Skipped() }()
		RequireIntegrationEnvironment(t, "TESTUTIL_LIVE_TEST")
		t.Fatal("ordinary HETEROLOG_ENV_PATH must not enable a live test")
	})
	if !skipped {
		t.Fatal("live test was not skipped")
	}
}

func TestIntegrationEnvironmentUsesDedicatedFileAndRestoresEnvironment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "integration.env")
	if err := os.WriteFile(path, []byte("TESTUTIL_DESTINATION=http://integration.invalid\nHETEROLOG_LOKI_ENDPOINT=http://integration.invalid:3100\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TESTUTIL_LIVE_TEST", "true")
	t.Setenv("HETEROLOG_INTEGRATION_ENV_PATH", path)
	t.Setenv("HETEROLOG_ENV_PATH", "/unrelated/application.env")
	t.Setenv("TESTUTIL_DESTINATION", "http://production.invalid")
	t.Setenv("HETEROLOG_LOKI_ENDPOINT", "http://production.invalid:3100")
	t.Setenv("HETEROLOG_OPENSEARCH_ENDPOINT", "http://production.invalid:9200")
	t.Setenv("TESTUTIL_RUNTIME_SETTING", "required by the live test process")
	before := os.Environ()
	t.Run("isolated settings", func(t *testing.T) {
		settings := RequireIntegrationEnvironment(t, "TESTUTIL_LIVE_TEST")
		if got := os.Getenv("TESTUTIL_DESTINATION"); got != "http://integration.invalid" {
			t.Fatal("integration file did not determine endpoint")
		}
		if got := os.Getenv("HETEROLOG_LOKI_ENDPOINT"); got != "http://integration.invalid:3100" {
			t.Fatal("integration file did not override the process config key")
		}
		// godotenvは既存値を上書きしないため、file外の設定項目はprocessから外します。
		if _, found := os.LookupEnv("HETEROLOG_OPENSEARCH_ENDPOINT"); found {
			t.Fatal("process config key missing from the integration file leaked into live test")
		}
		// 設定項目以外の実行環境は、接続や一時fileのために保持します。
		if got := os.Getenv("TESTUTIL_RUNTIME_SETTING"); got != "required by the live test process" {
			t.Fatal("runtime environment was removed from live test")
		}
		if os.Getenv("HETEROLOG_ENV_PATH") != path {
			t.Fatal("config.LoadConfig would not read the dedicated file")
		}
		if settings["TESTUTIL_DESTINATION"] != "http://integration.invalid" || settings["HETEROLOG_LOKI_ENDPOINT"] != "http://integration.invalid:3100" {
			t.Fatal("returned settings differ from the integration file")
		}
	})
	after := os.Environ()
	slices.Sort(before)
	slices.Sort(after)
	if !slices.Equal(before, after) {
		t.Fatal("integration test did not restore the original environment")
	}
}
