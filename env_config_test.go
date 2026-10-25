package heterolog_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/env"
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

func TestLoadEnvConfigReadsTheFileWithoutChangingTheEnvironment(t *testing.T) {
	isolateEnvConfigForTest(t)
	path := writeEnvFileForTest(t, "HETEROLOG_STDOUT_LOG_LEVEL=ERROR", "HETEROLOG_TARGET=env-config-test")

	loaded, err := heterolog.LoadEnvConfig(path)
	if err != nil {
		t.Fatalf("LoadEnvConfig: %v", err)
	}
	cfg, _, err := loaded.Config()
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	if got := cfg.Stdout.RuntimeConfig.LogLevel; got != heterolog.LevelError {
		t.Errorf("stdout level = %v, want ERROR from the file", got)
	}
	if got := cfg.Stdout.RuntimeConfig.Target; got != "env-config-test" {
		t.Errorf("target = %q, want the file value", got)
	}
	for _, key := range []string{"HETEROLOG_STDOUT_LOG_LEVEL", "HETEROLOG_TARGET"} {
		if value, ok := os.LookupEnv(key); ok {
			t.Errorf("LoadEnvConfig set %s=%q in the process environment", key, value)
		}
	}
}

// 同じkeyがprocess environmentにもある場合は、process environmentの値を使います。
func TestLoadEnvConfigPrefersTheProcessEnvironment(t *testing.T) {
	isolateEnvConfigForTest(t)
	t.Setenv("HETEROLOG_STDOUT_LOG_LEVEL", "WARN")
	path := writeEnvFileForTest(t, "HETEROLOG_STDOUT_LOG_LEVEL=ERROR")

	loaded, err := heterolog.LoadEnvConfig(path)
	if err != nil {
		t.Fatalf("LoadEnvConfig: %v", err)
	}
	cfg, _, err := loaded.Config()
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	if got := cfg.Stdout.RuntimeConfig.LogLevel; got != heterolog.LevelWarn {
		t.Errorf("stdout level = %v, want WARN from the process environment", got)
	}
}

// pathを省略した場合は、現在のdirectoryの.envもHETEROLOG_ENV_PATHも読みません。
func TestLoadEnvConfigWithoutPathReadsOnlyTheProcessEnvironment(t *testing.T) {
	isolateEnvConfigForTest(t)
	if err := os.WriteFile(".env", []byte("HETEROLOG_MAX_LOG_ENTRY_BYTES=not-a-number\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HETEROLOG_ENV_PATH", filepath.Join(t.TempDir(), "missing.env"))

	if _, err := heterolog.LoadEnvConfig(""); err != nil {
		t.Fatalf("LoadEnvConfig(\"\") read a file it was not given: %v", err)
	}
}

func TestLoadEnvConfigReportsAMissingFile(t *testing.T) {
	isolateEnvConfigForTest(t)
	path := filepath.Join(t.TempDir(), "missing.env")

	_, err := heterolog.LoadEnvConfig(path)
	var fileErr *heterolog.EnvFileError
	if !errors.As(err, &fileErr) || fileErr.Path != path || !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("LoadEnvConfig(missing) error = %v, want an EnvFileError for %s that matches fs.ErrNotExist", err, path)
	}
}

// 不正な値と旧名のkeyは、どちらもkeyを特定できるerrorとしてまとめて返します。
func TestLoadEnvConfigReportsInvalidAndRenamedKeys(t *testing.T) {
	isolateEnvConfigForTest(t)
	oldRatioKey := "HETEROLOG_MEMORY_CRITICAL_RAI" + "TO"
	path := writeEnvFileForTest(t, "HETEROLOG_MAX_LOG_ENTRY_BYTES=not-a-number", oldRatioKey+"=0.8")

	_, err := heterolog.LoadEnvConfig(path)
	var variableErr *env.VariableError
	if !errors.As(err, &variableErr) || variableErr.Key != "HETEROLOG_MAX_LOG_ENTRY_BYTES" {
		t.Errorf("LoadEnvConfig error = %v, want a VariableError for HETEROLOG_MAX_LOG_ENTRY_BYTES", err)
	}
	var renamedErr *heterolog.RenamedEnvironmentKeyError
	if !errors.As(err, &renamedErr) || renamedErr.Old != oldRatioKey || renamedErr.New != "HETEROLOG_MEMORY_CRITICAL_RATIO" {
		t.Errorf("LoadEnvConfig error = %v, want a RenamedEnvironmentKeyError for %s", err, oldRatioKey)
	}
}

func TestEnvConfigRejectsDisabledLoggers(t *testing.T) {
	isolateEnvConfigForTest(t)
	path := writeEnvFileForTest(t,
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
	)
	loaded, err := heterolog.LoadEnvConfig(path)
	if err != nil {
		t.Fatalf("LoadEnvConfig: %v", err)
	}
	ctx := context.Background()

	if _, _, err := loaded.AsyncConfig(); !errors.Is(err, heterolog.ErrEnvLoggerDisabled) {
		t.Errorf("AsyncConfig error = %v, want ErrEnvLoggerDisabled", err)
	}
	if _, err := loaded.NewAsync(ctx); !errors.Is(err, heterolog.ErrEnvLoggerDisabled) {
		t.Errorf("NewAsync error = %v, want ErrEnvLoggerDisabled", err)
	}
	if _, _, err := loaded.DeliveryConfig(); !errors.Is(err, heterolog.ErrEnvLoggerDisabled) {
		t.Errorf("DeliveryConfig error = %v, want ErrEnvLoggerDisabled", err)
	}
	if _, err := loaded.NewDelivery(ctx); !errors.Is(err, heterolog.ErrEnvLoggerDisabled) {
		t.Errorf("NewDelivery error = %v, want ErrEnvLoggerDisabled", err)
	}
	if _, _, err := loaded.Config(); err != nil {
		t.Errorf("Config of the enabled synchronous logger: %v", err)
	}
}

// 設定項目が一つもない環境でも、3種類のloggerを既定値（標準出力）で作成して記録できます。
func TestEnvConfigConstructsEveryLoggerFromAnEmptyEnvironment(t *testing.T) {
	isolateEnvConfigForTest(t)
	loaded, err := heterolog.LoadEnvConfig("")
	if err != nil {
		t.Fatalf("LoadEnvConfig with no settings: %v", err)
	}
	ctx := context.Background()

	output := captureRuntimeStdout(t, func() {
		runtime, err := loaded.New(ctx)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if err := runtime.Info(ctx, "env config sync entry"); err != nil {
			t.Errorf("sync Info: %v", err)
		}
		if err := runtime.Close(); err != nil {
			t.Errorf("sync Close: %v", err)
		}

		async, err := loaded.NewAsync(ctx)
		if err != nil {
			t.Fatalf("NewAsync: %v", err)
		}
		async.Info(ctx, "env config async entry")
		if err := async.Close(); err != nil {
			t.Errorf("async Close: %v", err)
		}

		delivery, err := loaded.NewDelivery(ctx)
		if err != nil {
			t.Fatalf("NewDelivery: %v", err)
		}
		if _, err := delivery.Info(ctx, "env config delivery entry"); err != nil {
			t.Errorf("delivery Info: %v", err)
		}
		if err := delivery.Close(); err != nil {
			t.Errorf("delivery Close: %v", err)
		}
	})
	for _, message := range []string{"env config sync entry", "env config async entry", "env config delivery entry"} {
		if !strings.Contains(output, message) {
			t.Errorf("stdout does not contain %q:\n%s", message, output)
		}
	}
}
