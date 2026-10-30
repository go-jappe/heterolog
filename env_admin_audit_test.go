package heterolog_test

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog"
	"github.com/go-jappe/heterolog/internal/testutil"
)

func loadEnvConfigForTest(t *testing.T, lines ...string) *heterolog.EnvConfig {
	t.Helper()
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	path := filepath.Join(t.TempDir(), "heterolog.env")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	env, err := heterolog.LoadEnvConfig(path)
	if err != nil {
		t.Fatalf("LoadEnvConfig: %v", err)
	}
	return env
}

// 環境設定から作った監査用loggerで管理用serverを動かすと、変更の監査logが、標準出力と
// 監査logのdirectoryのfileへ残ります。
func TestEnvAdminAuditRecordsTheAdminAPIInTheAuditDirectory(t *testing.T) {
	logs := t.TempDir()
	token := strings.Repeat("t", heterolog.MinAdminTokenLength)
	env := loadEnvConfigForTest(t,
		"HETEROLOG_TARGET=audit-env",
		"HETEROLOG_SYSTEM_LOG_DIR="+logs,
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ADMIN_ADDR=127.0.0.1:0",
		"HETEROLOG_ADMIN_TOKEN="+token,
	)
	ctx := context.Background()
	var requestID string
	output := testutil.CaptureStdout(t, func() {
		runtime, err := env.New(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = runtime.Close() }()
		audit, err := env.NewAdminAudit(ctx)
		if err != nil {
			t.Fatalf("NewAdminAudit: %v", err)
		}
		defer func() { _ = audit.Close() }()
		adminCfg, ok := env.AdminServerConfig()
		if !ok {
			t.Fatal("AdminServerConfig reports the admin server as disabled")
		}
		server, err := heterolog.StartAdminServer(adminCfg, heterolog.AdminMembers{Loggers: map[string]heterolog.SettingsController{"app": runtime}}, audit)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = server.Close() }()
		request, err := http.NewRequest(http.MethodPatch, "http://"+server.Addr()+"/settings",
			strings.NewReader(`{"loggers":{"app":{"outputs":{"stdout":{"level":"DEBUG"}}}}}`))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("PATCH = %d, want 200", response.StatusCode)
		}
		requestID = response.Header.Get("X-Request-Id")
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("server Shutdown: %v", err)
		}
		if err := audit.Close(); err != nil {
			t.Errorf("audit Close: %v", err)
		}
	})
	files, err := filepath.Glob(filepath.Join(logs, "audit-env", "audit_logs", "*.log"))
	if err != nil || len(files) != 1 {
		t.Fatalf("audit log files = %q, %v; want one file in audit-env/audit_logs", files, err)
	}
	file, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	// 記録の形式は出力先の作法に従います。fileは一行のtext、環境設定の既定の標準出力は
	// OpenTelemetryのJSONです。
	for _, phase := range []string{"requested", "started", "applied"} {
		record := `"audit_phase":"` + phase + `"`
		for name, check := range map[string]struct{ text, requestID string }{
			"file":   {string(file), "request_id: " + requestID},
			"stdout": {output, `"request_id":"` + requestID + `"`},
		} {
			if requestID == "" || !strings.Contains(check.text, check.requestID) || !strings.Contains(check.text, record) {
				t.Errorf("audit log on %s lacks the %s record of request %q:\n%s", name, phase, requestID, check.text)
			}
		}
	}
}

// 管理用serverを有効にしない環境設定には、監査用loggerの設定がありません。
func TestEnvAdminAuditRequiresTheAdminServer(t *testing.T) {
	env := loadEnvConfigForTest(t, "HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false")
	if _, _, err := env.AdminAuditConfig(); !errors.Is(err, heterolog.ErrEnvAdminAuditNotEnabled) {
		t.Errorf("AdminAuditConfig() error = %v, want ErrEnvAdminAuditNotEnabled", err)
	}
	if _, err := env.NewAdminAudit(context.Background()); !errors.Is(err, heterolog.ErrEnvAdminAuditNotEnabled) {
		t.Errorf("NewAdminAudit() error = %v, want ErrEnvAdminAuditNotEnabled", err)
	}
}
