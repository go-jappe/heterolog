package heterolog_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

const adminTokenForTest = "0123456789abcdef0123456789abcdef"

// 公開APIのNewAdminHandlerとStartAdminServerで、実際のloggerの出力先の設定を変えられる
// ことを確かめます。HTTPの受け口の振る舞いはinternal/presentation/http/admin、変更の検証と
// 反映はinternal/application/outputsettingsで試験します。
func TestAdminAPIChangesARunningLogger(t *testing.T) {
	runtime, err := heterolog.New(context.Background(), heterolog.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	loggers := map[string]heterolog.SettingsController{"app": runtime}
	patch := func(level string) *strings.Reader {
		return strings.NewReader(`{"loggers":{"app":{"outputs":{"stdout":{"level":"` + level + `"}}}}}`)
	}

	handler, err := heterolog.NewAdminHandler(heterolog.AdminHandlerConfig{
		Token: adminTokenForTest, Loggers: loggers, OnChange: func(heterolog.AdminChange) {},
	})
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
	if got := runtime.OutputSettings()[pkglogger.LogOutputStdout].Level; got != heterolog.LevelDebug {
		t.Fatalf("stdout level = %v after the handler PATCH, want DEBUG", got)
	}

	server, err := heterolog.StartAdminServer(heterolog.AdminServerConfig{
		Addr: ":0", Token: adminTokenForTest, OnChange: func(heterolog.AdminChange) {},
	}, loggers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
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
	if got := runtime.OutputSettings()[pkglogger.LogOutputStdout].Level; got != heterolog.LevelWarn {
		t.Errorf("stdout level = %v after the server PATCH, want WARN", got)
	}
	if err := server.Shutdown(context.Background()); err != nil {
		t.Errorf("server Shutdown: %v", err)
	}
}
