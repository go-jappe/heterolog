package heterolog_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

const adminTokenForTest = "0123456789abcdef0123456789abcdef"

// 公開APIのNewAdminHandlerとStartAdminServerで、実際のloggerの出力先の設定を変えられ、
// 変更の監査logが監査用の送達確認loggerへ届くことを確かめます。HTTPの受け口の振る舞いは
// internal/presentation/http/admin、変更の検証・反映と監査logの内容は
// internal/application/outputsettingsで試験します。
func TestAdminAPIChangesARunningLoggerAndRecordsTheAuditLog(t *testing.T) {
	ctx := context.Background()
	var requestIDs []string
	output := testutil.CaptureStdout(t, func() {
		runtime, err := heterolog.New(ctx, heterolog.DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = runtime.Close() }()
		audit, err := heterolog.NewDelivery(ctx, heterolog.DefaultDeliveryConfig())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = audit.Close() }()
		loggers := map[string]heterolog.SettingsController{"app": runtime}
		patch := func(level string) *strings.Reader {
			return strings.NewReader(`{"loggers":{"app":{"outputs":{"stdout":{"level":"` + level + `"}}}}}`)
		}

		handler, err := heterolog.NewAdminHandler(heterolog.AdminHandlerConfig{Token: adminTokenForTest, Loggers: loggers}, audit)
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodPatch, "/settings", patch("DEBUG"))
		request.Header.Set("Authorization", "Bearer "+adminTokenForTest)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK {
			t.Fatalf("handler PATCH = %d %s, want 200", recorder.Code, recorder.Body)
		}
		requestIDs = append(requestIDs, recorder.Header().Get("X-Request-Id"))
		if got := runtime.OutputSettings()[pkglogger.LogOutputStdout].Level; got != heterolog.LevelDebug {
			t.Fatalf("stdout level = %v after the handler PATCH, want DEBUG", got)
		}

		server, err := heterolog.StartAdminServer(heterolog.AdminServerConfig{Addr: ":0", Token: adminTokenForTest}, loggers, audit)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = server.Close() }()
		serverRequest, err := http.NewRequest(http.MethodPatch, "http://"+server.Addr()+"/settings", patch("WARN"))
		if err != nil {
			t.Fatal(err)
		}
		serverRequest.Header.Set("Authorization", "Bearer "+adminTokenForTest)
		response, err := http.DefaultClient.Do(serverRequest)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("server PATCH = %d %s, want 200", response.StatusCode, body)
		}
		requestIDs = append(requestIDs, response.Header.Get("X-Request-Id"))
		if got := runtime.OutputSettings()[pkglogger.LogOutputStdout].Level; got != heterolog.LevelWarn {
			t.Errorf("stdout level = %v after the server PATCH, want WARN", got)
		}
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("server Shutdown: %v", err)
		}
	})
	for _, id := range requestIDs {
		for _, phase := range []string{"requested", "started", "applied"} {
			if !strings.Contains(output, "request_id: "+id) || !strings.Contains(output, `"audit_phase":"`+phase+`"`) {
				t.Errorf("audit log on stdout lacks the %s record of request %q:\n%s", phase, id, output)
			}
		}
	}
}

// 監査logの記録先がない管理用APIは作りません。
func TestAdminAPIRequiresTheAuditLogger(t *testing.T) {
	loggers := map[string]heterolog.SettingsController{"app": &heterolog.Runtime{}}
	if _, err := heterolog.NewAdminHandler(heterolog.AdminHandlerConfig{Token: adminTokenForTest, Loggers: loggers}, nil); !errors.Is(err, heterolog.ErrAdminAuditLoggerRequired) {
		t.Errorf("NewAdminHandler(nil audit) error = %v, want ErrAdminAuditLoggerRequired", err)
	}
	if _, err := heterolog.StartAdminServer(heterolog.AdminServerConfig{Addr: ":0", Token: adminTokenForTest}, loggers, &heterolog.DeliveryRuntime{}); !errors.Is(err, heterolog.ErrAdminAuditLoggerRequired) {
		t.Errorf("StartAdminServer(zero audit) error = %v, want ErrAdminAuditLoggerRequired", err)
	}
}
