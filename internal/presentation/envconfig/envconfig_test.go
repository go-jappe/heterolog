package envconfig_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/presentation/envconfig"
	"github.com/go-jappe/heterolog/internal/presentation/http/admin"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/env"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// writeEnvFileForTestは、linesを書いた環境設定fileのpathを返します。
func writeEnvFileForTest(t *testing.T, lines ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "heterolog.env")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// isolateEnvConfigForTestは、開発者のshellにある設定項目を外し、既定の保存先を一時directoryへ
// 向けます。LoadEnvConfigは保存先を作成しませんが、試験が利用者のdirectoryを参照しないためです。
func isolateEnvConfigForTest(t *testing.T) {
	t.Helper()
	testutil.UnsetConfigEnvironment(t)
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Chdir(t.TempDir())
}

func TestLoadReadsTheFileWithoutChangingTheEnvironment(t *testing.T) {
	isolateEnvConfigForTest(t)
	path := writeEnvFileForTest(t, "HETEROLOG_STDOUT_LOG_LEVEL=ERROR", "HETEROLOG_TARGET=env-config-test")

	loaded, err := envconfig.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	cfg, _, err := loaded.Config()
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	if got := cfg.Stdout.RuntimeConfig.LogLevel; got != pkglogger.LogLevelError {
		t.Errorf("stdout level = %v, want ERROR from the file", got)
	}
	if got := cfg.Stdout.RuntimeConfig.Target; got != "env-config-test" {
		t.Errorf("target = %q, want the file value", got)
	}
	for _, key := range []string{"HETEROLOG_STDOUT_LOG_LEVEL", "HETEROLOG_TARGET"} {
		if value, ok := os.LookupEnv(key); ok {
			t.Errorf("Load set %s=%q in the process environment", key, value)
		}
	}
}

// 同じkeyがprocess environmentにもある場合は、process environmentの値を使います。
func TestLoadPrefersTheProcessEnvironment(t *testing.T) {
	isolateEnvConfigForTest(t)
	t.Setenv("HETEROLOG_STDOUT_LOG_LEVEL", "WARN")
	path := writeEnvFileForTest(t, "HETEROLOG_STDOUT_LOG_LEVEL=ERROR")

	loaded, err := envconfig.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	cfg, _, err := loaded.Config()
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	if got := cfg.Stdout.RuntimeConfig.LogLevel; got != pkglogger.LogLevelWarn {
		t.Errorf("stdout level = %v, want WARN from the process environment", got)
	}
}

// pathを省略した場合は、現在のdirectoryの.envもHETEROLOG_ENV_PATHも読みません。
func TestLoadWithoutPathReadsOnlyTheProcessEnvironment(t *testing.T) {
	isolateEnvConfigForTest(t)
	if err := os.WriteFile(".env", []byte("HETEROLOG_MAX_LOG_ENTRY_BYTES=not-a-number\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HETEROLOG_ENV_PATH", filepath.Join(t.TempDir(), "missing.env"))

	if _, err := envconfig.Load(""); err != nil {
		t.Fatalf("Load(\"\") read a file it was not given: %v", err)
	}
}

func TestLoadReportsAMissingFile(t *testing.T) {
	isolateEnvConfigForTest(t)
	path := filepath.Join(t.TempDir(), "missing.env")

	_, err := envconfig.Load(path)
	var fileErr *env.EnvFileError
	if !errors.As(err, &fileErr) || fileErr.Path != path || !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Load(missing) error = %v, want an EnvFileError for %s that matches fs.ErrNotExist", err, path)
	}
}

// 不正な値と旧名のkeyは、どちらもkeyを特定できるerrorとしてまとめて返します。
func TestLoadReportsInvalidAndRenamedKeys(t *testing.T) {
	isolateEnvConfigForTest(t)
	oldRatioKey := "HETEROLOG_MEMORY_CRITICAL_RAI" + "TO"
	path := writeEnvFileForTest(t, "HETEROLOG_MAX_LOG_ENTRY_BYTES=not-a-number", oldRatioKey+"=0.8")

	_, err := envconfig.Load(path)
	var variableErr *env.VariableError
	if !errors.As(err, &variableErr) || variableErr.Key != "HETEROLOG_MAX_LOG_ENTRY_BYTES" {
		t.Errorf("Load error = %v, want a VariableError for HETEROLOG_MAX_LOG_ENTRY_BYTES", err)
	}
	var renamedErr *env.RenamedEnvironmentKeyError
	if !errors.As(err, &renamedErr) || renamedErr.Old != oldRatioKey || renamedErr.New != "HETEROLOG_MEMORY_CRITICAL_RATIO" {
		t.Errorf("Load error = %v, want a RenamedEnvironmentKeyError for %s", err, oldRatioKey)
	}
}

func TestEnvConfigRejectsDisabledLoggers(t *testing.T) {
	isolateEnvConfigForTest(t)
	path := writeEnvFileForTest(t,
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
	)
	loaded, err := envconfig.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, _, err := loaded.AsyncConfig(); !errors.Is(err, envconfig.ErrLoggerDisabled) {
		t.Errorf("AsyncConfig error = %v, want ErrEnvLoggerDisabled", err)
	}
	if _, _, err := loaded.DeliveryConfig(); !errors.Is(err, envconfig.ErrLoggerDisabled) {
		t.Errorf("DeliveryConfig error = %v, want ErrEnvLoggerDisabled", err)
	}
	if _, _, err := loaded.Config(); err != nil {
		t.Errorf("Config of the enabled synchronous logger: %v", err)
	}
}

// 環境設定のHETEROLOG_ADMIN_*は、AdminServerConfigで取り出せ、読み込み時に検証します。
func TestEnvConfigReadsTheAdminServerSettings(t *testing.T) {
	isolateEnvConfigForTest(t)
	const token = "0123456789abcdef0123456789abcdef"
	loaded, err := envconfig.Load(writeEnvFileForTest(t,
		"HETEROLOG_ADMIN_ADDR=127.0.0.1:9100", "HETEROLOG_ADMIN_TOKEN="+token))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	cfg, ok := loaded.AdminServerConfig()
	if !ok || cfg.Addr != "127.0.0.1:9100" || cfg.Token != token {
		t.Errorf("AdminServerConfig() = %+v, %t; want the address and token from the file", cfg, ok)
	}

	disabled, err := envconfig.Load("")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := disabled.AdminServerConfig(); ok {
		t.Error("AdminServerConfig() is enabled without HETEROLOG_ADMIN_ADDR")
	}

	_, err = envconfig.Load(writeEnvFileForTest(t,
		"HETEROLOG_ADMIN_ADDR=0.0.0.0:9100", "HETEROLOG_ADMIN_TOKEN="+token))
	if !errors.Is(err, admin.ErrServerInsecure) || strings.Contains(err.Error(), token) {
		t.Errorf("Load(non-loopback without TLS) error = %v, want ErrServerInsecure without the token", err)
	}
}
