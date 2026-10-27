package admin

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/outputsettings"
	pkgadmin "github.com/go-jappe/heterolog/pkg/admin"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

const tokenForTest = "0123456789abcdef0123456789abcdef"

// controllerForTestは、application層のRegistryを使うControllerです。実際のloggerを
// 作らずに、管理用APIの受け口だけを確かめます。
type controllerForTest struct{ *outputsettings.Registry }

func (c controllerForTest) OutputSettings() map[pkglogger.LogOutputDestination]pkglogger.OutputSettings {
	return c.Snapshot()
}

func (c controllerForTest) UpdateOutputSettings(changes map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange) error {
	return c.Update(changes)
}

// newControllerForTestは、stdoutをINFO・samplingなしで持つControllerを返します。
func newControllerForTest(t *testing.T) controllerForTest {
	t.Helper()
	live, err := pkglogger.NewLiveSettings(pkglogger.LogLevelInfo, pkglogger.SamplingConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return controllerForTest{outputsettings.NewRegistry("test logger", map[pkglogger.LogOutputDestination]*pkglogger.LiveSettings{
		pkglogger.LogOutputStdout: live,
	})}
}

func stdoutSettings(c controllerForTest) pkglogger.OutputSettings {
	return c.OutputSettings()[pkglogger.LogOutputStdout]
}

// requestForTestは、handlerへ要求を送り、状態codeと本文を返します。
func requestForTest(t *testing.T, handler http.Handler, method, path, token, body string) (int, string) {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder.Code, recorder.Body.String()
}

func TestNewHandlerRejectsAShortTokenAndNoLoggers(t *testing.T) {
	if _, err := NewHandler(pkgadmin.HandlerConfig{
		Token: "short", Loggers: map[string]pkglogger.SettingsController{"app": newControllerForTest(t)},
	}); !errors.Is(err, ErrTokenTooShort) {
		t.Errorf("NewHandler(short token) error = %v, want ErrTokenTooShort", err)
	}
	if _, err := NewHandler(pkgadmin.HandlerConfig{Token: tokenForTest}); !errors.Is(err, outputsettings.ErrNoLoggers) {
		t.Errorf("NewHandler(no loggers) error = %v, want ErrNoLoggers", err)
	}
}

// tokenのない要求、違うtokenの要求は、どの経路でも401で拒否し、本文にtokenを含めません。
func TestHandlerRequiresTheBearerToken(t *testing.T) {
	controller := newControllerForTest(t)
	handler, err := NewHandler(pkgadmin.HandlerConfig{Token: tokenForTest, Loggers: map[string]pkglogger.SettingsController{"app": controller}})
	if err != nil {
		t.Fatal(err)
	}
	for _, token := range []string{"", "wrong-token-wrong-token-wrong-token", tokenForTest + "x"} {
		for _, method := range []string{http.MethodGet, http.MethodPatch} {
			status, body := requestForTest(t, handler, method, "/settings", token, `{"loggers":{"app":{"outputs":{"stdout":{"level":"DEBUG"}}}}}`)
			if status != http.StatusUnauthorized || strings.Contains(body, tokenForTest) {
				t.Errorf("%s with token %q = %d %s, want 401 without the token", method, token, status, body)
			}
		}
	}
	if got := stdoutSettings(controller).Level; got != pkglogger.LogLevelInfo {
		t.Errorf("stdout level = %v after unauthorized requests, want INFO", got)
	}
}

// GETは現在の設定を返し、PATCHは指定した項目だけを変えて、変えた後の設定を返します。
func TestHandlerReadsAndChangesSettings(t *testing.T) {
	controller := newControllerForTest(t)
	var changes []pkgadmin.Change
	handler, err := NewHandler(pkgadmin.HandlerConfig{
		Token:    tokenForTest,
		Loggers:  map[string]pkglogger.SettingsController{"app": controller},
		OnChange: func(change pkgadmin.Change) { changes = append(changes, change) },
	})
	if err != nil {
		t.Fatal(err)
	}

	status, body := requestForTest(t, handler, http.MethodGet, "/settings", tokenForTest, "")
	if status != http.StatusOK {
		t.Fatalf("GET = %d %s, want 200", status, body)
	}
	var document settingsDocument
	if err := json.Unmarshal([]byte(body), &document); err != nil {
		t.Fatalf("GET body is not JSON: %v: %s", err, body)
	}
	if got := document.Loggers["app"].Outputs[pkglogger.LogOutputStdout].Level; got != "INFO" {
		t.Fatalf("GET stdout level = %q, want INFO: %s", got, body)
	}

	status, body = requestForTest(t, handler, http.MethodPatch, "/settings", tokenForTest,
		`{"loggers":{"app":{"outputs":{"stdout":{"level":"debug","sampling":{"enabled":true,"debug_rate":1,"info_rate":10,"warn_rate":1,"error_rate":1}}}}}}`)
	if status != http.StatusOK {
		t.Fatalf("PATCH = %d %s, want 200", status, body)
	}
	if err := json.Unmarshal([]byte(body), &document); err != nil {
		t.Fatalf("PATCH body is not JSON: %v: %s", err, body)
	}
	if got := document.Loggers["app"].Outputs[pkglogger.LogOutputStdout]; got.Level != "DEBUG" || !got.Sampling.Enabled || got.Sampling.InfoRate != 10 {
		t.Errorf("PATCH response stdout = %+v, want DEBUG with sampling enabled at info_rate 10", got)
	}
	if got := stdoutSettings(controller); got.Level != pkglogger.LogLevelDebug || got.Sampling.SamplingInfoRate != 10 {
		t.Errorf("stdout settings = %+v, want the patched values", got)
	}

	// 省略したsamplingの項目は、今の値のままです。
	status, body = requestForTest(t, handler, http.MethodPatch, "/settings", tokenForTest,
		`{"loggers":{"app":{"outputs":{"stdout":{"sampling":{"info_rate":20}}}}}}`)
	if status != http.StatusOK {
		t.Fatalf("second PATCH = %d %s, want 200", status, body)
	}
	if got := stdoutSettings(controller); got.Level != pkglogger.LogLevelDebug || !got.Sampling.IsSamplingEnabled ||
		got.Sampling.SamplingInfoRate != 20 || got.Sampling.SamplingWarnRate != 1 {
		t.Errorf("stdout settings = %+v, want only info_rate changed to 20", got)
	}

	if len(changes) != 2 || changes[0].Logger != "app" || changes[0].Output != pkglogger.LogOutputStdout ||
		changes[0].Before.Level != pkglogger.LogLevelInfo || changes[0].After.Level != pkglogger.LogLevelDebug ||
		changes[1].Before.Sampling.SamplingInfoRate != 10 || changes[1].After.Sampling.SamplingInfoRate != 20 {
		t.Errorf("OnChange calls = %+v, want the two changes with their before and after settings", changes)
	}
}

// 不正な要求は、一つも反映せずに拒否し、errorを状態codeとJSONの本文で返します。
func TestHandlerRejectsInvalidRequestsWithoutChanges(t *testing.T) {
	app, other := newControllerForTest(t), newControllerForTest(t)
	handler, err := NewHandler(pkgadmin.HandlerConfig{
		Token:    tokenForTest,
		Loggers:  map[string]pkglogger.SettingsController{"app": app, "other": other},
		OnChange: func(pkgadmin.Change) { t.Error("OnChange called for a rejected request") },
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name   string
		body   string
		status int
	}{
		{"not JSON", `level=DEBUG`, http.StatusBadRequest},
		{"unknown field", `{"loggers":{"app":{"outputs":{"stdout":{"colour":"red"}}}}}`, http.StatusBadRequest},
		{"two objects", `{} {}`, http.StatusBadRequest},
		{"invalid level", `{"loggers":{"app":{"outputs":{"stdout":{"level":"LOUD"}}}}}`, http.StatusBadRequest},
		{"invalid sampling rate", `{"loggers":{"app":{"outputs":{"stdout":{"sampling":{"enabled":true}}}}}}`, http.StatusBadRequest},
		{"unknown logger", `{"loggers":{"missing":{"outputs":{"stdout":{"level":"DEBUG"}}}}}`, http.StatusNotFound},
		{"unknown output", `{"loggers":{"app":{"outputs":{"file":{"level":"DEBUG"}}}}}`, http.StatusNotFound},
		// 有効な変更があっても、別のloggerの不正な変更があれば反映しません。
		{"one invalid among valid", `{"loggers":{"app":{"outputs":{"stdout":{"level":"DEBUG"}}},"other":{"outputs":{"stdout":{"sampling":{"enabled":true}}}}}}`, http.StatusBadRequest},
		{"too large", `{"loggers":{"app":{"outputs":{"stdout":{"level":"` + strings.Repeat("D", 70<<10) + `"}}}}}`, http.StatusRequestEntityTooLarge},
	} {
		t.Run(tt.name, func(t *testing.T) {
			status, body := requestForTest(t, handler, http.MethodPatch, "/settings", tokenForTest, tt.body)
			if status != tt.status || !strings.Contains(body, `"error"`) {
				t.Errorf("PATCH = %d %s, want %d with an error message", status, body, tt.status)
			}
			for name, controller := range map[string]controllerForTest{"app": app, "other": other} {
				if got := stdoutSettings(controller); got.Level != pkglogger.LogLevelInfo || got.Sampling.IsSamplingEnabled {
					t.Errorf("%s stdout settings = %+v after a rejected request, want the original settings", name, got)
				}
			}
		})
	}
}

func TestHandlerRejectsOtherMethodsAndPaths(t *testing.T) {
	handler, err := NewHandler(pkgadmin.HandlerConfig{Token: tokenForTest, Loggers: map[string]pkglogger.SettingsController{"app": newControllerForTest(t)}})
	if err != nil {
		t.Fatal(err)
	}
	if status, body := requestForTest(t, handler, http.MethodPost, "/settings", tokenForTest, "{}"); status != http.StatusMethodNotAllowed {
		t.Errorf("POST = %d %s, want 405", status, body)
	}
	if status, body := requestForTest(t, handler, http.MethodGet, "/other", tokenForTest, ""); status != http.StatusNotFound {
		t.Errorf("GET /other = %d %s, want 404", status, body)
	}
}
