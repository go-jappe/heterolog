package bootstrap_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/internal/presentation/envconfig"
	"github.com/go-jappe/heterolog/internal/testutil"
)

// 設定項目が一つもない環境でも、3種類のloggerを既定値（標準出力）で作成して記録できます。
func TestFromEnvConstructsEveryLoggerFromAnEmptyEnvironment(t *testing.T) {
	isolateEnvConfigForTest(t)
	loaded, err := envconfig.Load("")
	if err != nil {
		t.Fatalf("Load with no settings: %v", err)
	}
	ctx := context.Background()

	output := testutil.CaptureStdout(t, func() {
		runtime, err := bootstrap.NewFromEnv(ctx, loaded)
		if err != nil {
			t.Fatalf("NewFromEnv: %v", err)
		}
		if err := runtime.Info(ctx, "env config sync entry"); err != nil {
			t.Errorf("sync Info: %v", err)
		}
		if err := runtime.Close(); err != nil {
			t.Errorf("sync Close: %v", err)
		}

		async, err := bootstrap.NewAsyncFromEnv(ctx, loaded)
		if err != nil {
			t.Fatalf("NewAsyncFromEnv: %v", err)
		}
		async.Info(ctx, "env config async entry")
		if err := async.Close(); err != nil {
			t.Errorf("async Close: %v", err)
		}

		delivery, err := bootstrap.NewDeliveryFromEnv(ctx, loaded)
		if err != nil {
			t.Fatalf("NewDeliveryFromEnv: %v", err)
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

// 環境設定が無効にしたloggerは、作らずにErrLoggerDisabledを返します。監査用loggerは、
// HETEROLOG_ADMIN_ADDRが空ならErrAdminAuditNotEnabledを返します。
func TestFromEnvRejectsDisabledLoggers(t *testing.T) {
	isolateEnvConfigForTest(t)
	path := writeEnvFileForTest(t,
		"HETEROLOG_ENABLE_LOGGING=false",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
	)
	loaded, err := envconfig.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	ctx := context.Background()

	if _, err := bootstrap.NewFromEnv(ctx, loaded); !errors.Is(err, envconfig.ErrLoggerDisabled) {
		t.Errorf("NewFromEnv error = %v, want ErrLoggerDisabled", err)
	}
	if _, err := bootstrap.NewAsyncFromEnv(ctx, loaded); !errors.Is(err, envconfig.ErrLoggerDisabled) {
		t.Errorf("NewAsyncFromEnv error = %v, want ErrLoggerDisabled", err)
	}
	if _, err := bootstrap.NewDeliveryFromEnv(ctx, loaded); !errors.Is(err, envconfig.ErrLoggerDisabled) {
		t.Errorf("NewDeliveryFromEnv error = %v, want ErrLoggerDisabled", err)
	}
	if _, err := bootstrap.NewAdminAuditFromEnv(ctx, loaded); !errors.Is(err, envconfig.ErrAdminAuditNotEnabled) {
		t.Errorf("NewAdminAuditFromEnv error = %v, want ErrAdminAuditNotEnabled", err)
	}
}

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
