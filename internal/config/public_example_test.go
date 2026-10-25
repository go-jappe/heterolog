package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil"
)

// The distributed example must be usable without private credentials. The
// broader backend contract tests use generated fixtures with dummy credentials.
// application全体の検証も通し、公開例の保存先設定が所有権検証で拒否されないことを確認します。
func TestPublicExampleMapsWithoutCredentials(t *testing.T) {
	root := testutil.RepositoryRoot(t)
	// Windowsの一時directoryは環境変数から決まるため、設定項目を外す前に作成します。
	env := map[string]string{
		"HETEROLOG_ENV_PATH":       filepath.Join(root, ".env.example"),
		"HETEROLOG_SYSTEM_LOG_DIR": t.TempDir(),
		"HETEROLOG_SYSTEM_LIB_DIR": t.TempDir(),
		"HETEROLOG_APP_STATE_DIR":  t.TempDir(),
	}
	// 実行中のprocessの設定項目を外し、TMP・TEMPなどの実行環境は残します。
	testutil.UnsetConfigEnvironment(t)
	for key, value := range env {
		if err := os.Setenv(key, value); err != nil {
			t.Fatal(err)
		}
	}
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateApplicationConfig(cfg); err != nil {
		t.Fatalf("validate example: %v", err)
	}
	if _, err := MapLoggerConfig(cfg); err != nil {
		t.Fatalf("sync example: %v", err)
	}
	if _, err := MapAsyncLoggerConfig(cfg); err != nil {
		t.Fatalf("async example: %v", err)
	}
	if _, err := MapDeliveryLoggerConfig(cfg); err != nil {
		t.Fatalf("delivery example: %v", err)
	}
}
