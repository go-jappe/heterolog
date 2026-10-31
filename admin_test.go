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

		handler, err := heterolog.NewAdminHandler(heterolog.AdminHandlerConfig{Token: adminTokenForTest, Members: heterolog.AdminMembers{Loggers: loggers}}, audit)
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

		server, err := heterolog.StartAdminServer(heterolog.AdminServerConfig{Addr: ":0", Token: adminTokenForTest}, heterolog.AdminMembers{Loggers: loggers}, audit)
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
	if _, err := heterolog.NewAdminHandler(heterolog.AdminHandlerConfig{Token: adminTokenForTest, Members: heterolog.AdminMembers{Loggers: loggers}}, nil); !errors.Is(err, heterolog.ErrAdminAuditLoggerRequired) {
		t.Errorf("NewAdminHandler(nil audit) error = %v, want ErrAdminAuditLoggerRequired", err)
	}
	if _, err := heterolog.StartAdminServer(heterolog.AdminServerConfig{Addr: ":0", Token: adminTokenForTest}, heterolog.AdminMembers{Loggers: loggers}, &heterolog.DeliveryRuntime{}); !errors.Is(err, heterolog.ErrAdminAuditLoggerRequired) {
		t.Errorf("StartAdminServer(zero audit) error = %v, want ErrAdminAuditLoggerRequired", err)
	}
}

// 管理用APIに渡すloggerの出力先がLiveSettingsを共有していれば、NewAdminHandlerと
// StartAdminServerはErrAdminSharedLiveSettingsを返します。公開の型で渡しても見つかる
// ことを確かめます。共有の検査そのものはinternal/application/outputsettingsで試験します。
func TestAdminAPIRejectsLoggersSharingLiveSettings(t *testing.T) {
	ctx := context.Background()
	live, err := pkglogger.NewLiveSettings(pkglogger.LogLevelInfo, pkglogger.SamplingConfig{})
	if err != nil {
		t.Fatal(err)
	}
	newRuntime := func(cfg heterolog.Config) *heterolog.Runtime {
		t.Helper()
		runtime, err := heterolog.New(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = runtime.Close() })
		return runtime
	}
	sharedConfig := heterolog.DefaultConfig()
	sharedConfig.Stdout.RuntimeConfig.Live = live
	app := newRuntime(sharedConfig)
	sharedAsyncConfig := heterolog.DefaultAsyncConfig()
	sharedAsyncConfig.Stdout.RuntimeConfig.Live = live
	async, err := heterolog.NewAsync(ctx, sharedAsyncConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = async.Close() })
	separate := newRuntime(heterolog.DefaultConfig())
	audit, err := heterolog.NewDelivery(ctx, heterolog.DefaultDeliveryConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = audit.Close() })

	for name, loggers := range map[string]map[string]heterolog.SettingsController{
		"a synchronous and an asynchronous logger": {"app": app, "async": async},
		"one logger under two names":               {"app": separate, "copy": separate},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := heterolog.NewAdminHandler(heterolog.AdminHandlerConfig{Token: adminTokenForTest, Members: heterolog.AdminMembers{Loggers: loggers}}, audit); !errors.Is(err, heterolog.ErrAdminSharedLiveSettings) {
				t.Errorf("NewAdminHandler() error = %v, want ErrAdminSharedLiveSettings", err)
			}
			server, err := heterolog.StartAdminServer(heterolog.AdminServerConfig{Addr: "127.0.0.1:0", Token: adminTokenForTest}, heterolog.AdminMembers{Loggers: loggers}, audit)
			if err == nil {
				_ = server.Close()
			}
			if !errors.Is(err, heterolog.ErrAdminSharedLiveSettings) {
				t.Errorf("StartAdminServer() error = %v, want ErrAdminSharedLiveSettings", err)
			}
		})
	}

	// それぞれ自分の設定を持つloggerは受け付けます。
	loggers := map[string]heterolog.SettingsController{"app": app, "separate": separate}
	if _, err := heterolog.NewAdminHandler(heterolog.AdminHandlerConfig{Token: adminTokenForTest, Members: heterolog.AdminMembers{Loggers: loggers}}, audit); err != nil {
		t.Errorf("NewAdminHandler(own settings) error = %v, want no error", err)
	}
}

// 公開APIで渡したlogger・delivery logger・telemetryの設定を、GET /configが秘密の値を
// 伏せて返すことを確かめます。文書の形はinternal/application/configviewで試験します。
func TestAdminAPIReportsTheConfigurationOfEveryMember(t *testing.T) {
	ctx := context.Background()
	cfg := heterolog.DefaultConfig()
	cfg.Slack = &pkglogger.SlackLoggerConfig{Token: "xoxb-secret", Channel: "#ops"}
	runtime, err := heterolog.New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = runtime.Close() }()
	audit, err := heterolog.NewDelivery(ctx, heterolog.DefaultDeliveryConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = audit.Close() }()
	telemetryCfg := heterolog.DefaultTelemetryConfig()
	telemetryCfg.Resource.ServiceName = "admin-test"
	tel, err := heterolog.NewTelemetry(ctx, telemetryCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tel.Shutdown(ctx) }()
	handler, err := heterolog.NewAdminHandler(heterolog.AdminHandlerConfig{Token: adminTokenForTest, Members: heterolog.AdminMembers{
		Loggers:         map[string]heterolog.SettingsController{"app": runtime},
		DeliveryLoggers: map[string]heterolog.DeliveryLogger{"audit": audit},
		Telemetries:     map[string]heterolog.TelemetryMember{"tel": tel},
	}}, audit)
	if err != nil {
		t.Fatal(err)
	}
	get := func(path string) (int, string) {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Authorization", "Bearer "+adminTokenForTest)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return recorder.Code, recorder.Body.String()
	}
	status, body := get("/config")
	if status != http.StatusOK {
		t.Fatalf("GET /config = %d %s, want 200", status, body)
	}
	for _, want := range []string{`"app": {`, `"kind": "logger"`, `"token": "***"`, `"audit": {`, `"kind": "delivery_logger"`, `"tel": {`, `"service_name": "admin-test"`} {
		if !strings.Contains(body, want) {
			t.Errorf("GET /config lacks %s:\n%s", want, body)
		}
	}
	if strings.Contains(body, "xoxb-secret") {
		t.Errorf("GET /config contains the Slack token:\n%s", body)
	}
	if status, body := get("/config/loggers/app/config/slack/channel"); status != http.StatusOK || strings.TrimSpace(body) != `"#ops"` {
		t.Errorf("GET /config/.../channel = %d %q, want 200 \"#ops\"", status, body)
	}
}

// 公開APIで渡したloggerの様子を、GET /statusが返すことを確かめます。報告の中身は
// internal/application/statusとinternal/bootstrapで試験します。
func TestAdminAPIReportsTheStatusOfEveryMember(t *testing.T) {
	ctx := context.Background()
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
	handler, err := heterolog.NewAdminHandler(heterolog.AdminHandlerConfig{
		Token:   adminTokenForTest,
		Members: heterolog.AdminMembers{Loggers: map[string]heterolog.SettingsController{"app": runtime}},
		Status:  heterolog.AdminStatusConfig{DisableDirectoryScan: true},
	}, audit)
	if err != nil {
		t.Fatal(err)
	}
	output := testutil.CaptureStdout(t, func() {
		if err := runtime.Info(ctx, "hello"); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(output, "hello") {
		t.Fatalf("stdout = %q, want the log line", output)
	}
	request := httptest.NewRequest(http.MethodGet, "/status", nil)
	request.Header.Set("Authorization", "Bearer "+adminTokenForTest)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /status = %d %s, want 200", recorder.Code, recorder.Body)
	}
	body := recorder.Body.String()
	for _, want := range []string{`"process": {`, `"uptime_seconds"`, `"app": {`, `"name": "stdout"`, `"records": {`, `"warnings": []`} {
		if !strings.Contains(body, want) {
			t.Errorf("GET /status lacks %s:\n%s", want, body)
		}
	}
}
