package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/configview"
	"github.com/go-jappe/heterolog/internal/application/outputsettings"
	"github.com/go-jappe/heterolog/internal/application/status"
	pkgadmin "github.com/go-jappe/heterolog/pkg/admin"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
)

const tokenForTest = "0123456789abcdef0123456789abcdef"

// controllerForTestは、application層のRegistryを使うControllerです。実際のloggerを
// 作らずに、管理用APIの受け口だけを確かめます。
type controllerForTest struct{ *outputsettings.Registry }

func (c controllerForTest) OutputSettings() map[pkglogger.LogOutputDestination]pkglogger.OutputSettings {
	return c.Snapshot()
}

func (c controllerForTest) UpdateOutputSettings(changes map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange) (map[pkglogger.LogOutputDestination]pkglogger.AppliedOutputSettings, error) {
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

// auditLoggerForTestは、受け取った監査logの段階・要求ID・理由を順に保持する試験用の
// 送達確認loggerです。undeliveredがtrueなら、どの記録も記録先へ届かなかったと報告します。
type auditLoggerForTest struct {
	mu          sync.Mutex
	records     []string
	undelivered bool
}

func (l *auditLoggerForTest) record(fields []pkglogger.Field) (pkglogger.DeliveryReport, error) {
	values := map[string]any{}
	for _, field := range fields {
		values[field.Key()] = field.Value()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, strings.TrimSpace(strings.Join([]string{
		asString(values["request_id"]), asString(values["audit_phase"]), asString(values["reason"]),
	}, " ")))
	if l.undelivered {
		return pkglogger.DeliveryReport{}, errors.New("audit record was not delivered")
	}
	return pkglogger.DeliveryReport{Targets: []pkglogger.DeliveryResult{{Backend: "stdout", Status: pkglogger.DeliverySucceeded}}}, nil
}

func asString(value any) string { s, _ := value.(string); return s }

func (l *auditLoggerForTest) Debug(_ context.Context, _ string, fields ...pkglogger.Field) (pkglogger.DeliveryReport, error) {
	return l.record(fields)
}

func (l *auditLoggerForTest) Info(_ context.Context, _ string, fields ...pkglogger.Field) (pkglogger.DeliveryReport, error) {
	return l.record(fields)
}

func (l *auditLoggerForTest) Warn(_ context.Context, _ string, fields ...pkglogger.Field) (pkglogger.DeliveryReport, error) {
	return l.record(fields)
}

func (l *auditLoggerForTest) Error(_ context.Context, _ string, fields ...pkglogger.Field) (pkglogger.DeliveryReport, error) {
	return l.record(fields)
}

func (l *auditLoggerForTest) ErrorWithStack(_ context.Context, _ string, _ error, fields ...pkglogger.Field) (pkglogger.DeliveryReport, error) {
	return l.record(fields)
}

func (l *auditLoggerForTest) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.records)
}

func newHandlerForTest(t *testing.T, loggers map[string]pkglogger.SettingsController) (http.Handler, *auditLoggerForTest) {
	t.Helper()
	audit := &auditLoggerForTest{}
	handler, err := NewHandler(pkgadmin.HandlerConfig{Token: tokenForTest, Members: pkgadmin.Members{Loggers: loggers}}, audit, status.Probes{})
	if err != nil {
		t.Fatal(err)
	}
	return handler, audit
}

// requestForTestは、handlerへ要求を送り、状態code・本文・要求IDを返します。
func requestForTest(t *testing.T, handler http.Handler, method, path, token, body string) (int, string, string) {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder.Code, recorder.Body.String(), recorder.Header().Get("X-Request-Id")
}

func TestNewHandlerRejectsAShortTokenNoLoggersAndNoAuditLogger(t *testing.T) {
	loggers := map[string]pkglogger.SettingsController{"app": newControllerForTest(t)}
	if _, err := NewHandler(pkgadmin.HandlerConfig{Token: "short", Members: pkgadmin.Members{Loggers: loggers}}, &auditLoggerForTest{}, status.Probes{}); !errors.Is(err, ErrTokenTooShort) {
		t.Errorf("NewHandler(short token) error = %v, want ErrTokenTooShort", err)
	}
	if _, err := NewHandler(pkgadmin.HandlerConfig{Token: tokenForTest}, &auditLoggerForTest{}, status.Probes{}); !errors.Is(err, outputsettings.ErrNoLoggers) {
		t.Errorf("NewHandler(no loggers) error = %v, want ErrNoLoggers", err)
	}
	if _, err := NewHandler(pkgadmin.HandlerConfig{Token: tokenForTest, Members: pkgadmin.Members{Loggers: loggers}}, nil, status.Probes{}); !errors.Is(err, outputsettings.ErrNoAuditLogger) {
		t.Errorf("NewHandler(no audit logger) error = %v, want ErrNoAuditLogger", err)
	}
}

// tokenのない要求、違うtokenの要求は、どの経路でも401で拒否して監査logへ記録し、本文に
// tokenを含めません。監査logが届かなかったことも、tokenのない要求には知らせません。
func TestHandlerRequiresTheBearerToken(t *testing.T) {
	controller := newControllerForTest(t)
	handler, audit := newHandlerForTest(t, map[string]pkglogger.SettingsController{"app": controller})
	audit.undelivered = true
	var ids []string
	for _, token := range []string{"", "wrong-token-wrong-token-wrong-token", tokenForTest + "x"} {
		for _, method := range []string{http.MethodGet, http.MethodPatch} {
			status, body, id := requestForTest(t, handler, method, "/settings", token, `{"loggers":{"app":{"outputs":{"stdout":{"level":"DEBUG"}}}}}`)
			if status != http.StatusUnauthorized || strings.Contains(body, tokenForTest) || strings.Contains(body, "warnings") {
				t.Errorf("%s with token %q = %d %s, want 401 without the token and without warnings", method, token, status, body)
			}
			ids = append(ids, id)
		}
	}
	if got := stdoutSettings(controller).Level; got != pkglogger.LogLevelInfo {
		t.Errorf("stdout level = %v after unauthorized requests, want INFO", got)
	}
	var want []string
	for _, id := range ids {
		want = append(want, id+" rejected missing or invalid bearer token", id+" undelivered")
	}
	if got := audit.snapshot(); !slices.Equal(got, want) {
		t.Errorf("audit records = %q, want a rejection of every request", got)
	}
}

// GETは現在の設定を返して監査logへは記録せず、PATCHは指定した項目だけを変えて、要求・
// 開始時点・変更後を記録し、変えた後の設定と要求IDを返します。
func TestHandlerReadsAndChangesSettings(t *testing.T) {
	controller := newControllerForTest(t)
	handler, audit := newHandlerForTest(t, map[string]pkglogger.SettingsController{"app": controller})

	status, body, _ := requestForTest(t, handler, http.MethodGet, "/settings", tokenForTest, "")
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
	if got := audit.snapshot(); len(got) != 0 {
		t.Fatalf("audit records after GET = %q, want none", got)
	}

	status, body, first := requestForTest(t, handler, http.MethodPatch, "/settings", tokenForTest,
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
	if len(document.Warnings) != 0 {
		t.Errorf("PATCH response warnings = %q, want none", document.Warnings)
	}
	if got := stdoutSettings(controller); got.Level != pkglogger.LogLevelDebug || got.Sampling.SamplingInfoRate != 10 {
		t.Errorf("stdout settings = %+v, want the patched values", got)
	}

	// 省略したsamplingの項目は、今の値のままです。
	status, body, second := requestForTest(t, handler, http.MethodPatch, "/settings", tokenForTest,
		`{"loggers":{"app":{"outputs":{"stdout":{"sampling":{"info_rate":20}}}}}}`)
	if status != http.StatusOK {
		t.Fatalf("second PATCH = %d %s, want 200", status, body)
	}
	if got := stdoutSettings(controller); got.Level != pkglogger.LogLevelDebug || !got.Sampling.IsSamplingEnabled ||
		got.Sampling.SamplingInfoRate != 20 || got.Sampling.SamplingWarnRate != 1 {
		t.Errorf("stdout settings = %+v, want only info_rate changed to 20", got)
	}

	if first == "" || first == second {
		t.Fatalf("request IDs = %q and %q, want two different IDs", first, second)
	}
	want := []string{first + " requested", first + " started", first + " applied", second + " requested", second + " started", second + " applied"}
	if got := audit.snapshot(); !slices.Equal(got, want) {
		t.Errorf("audit records = %q, want %q", got, want)
	}
}

// 監査logが記録先のすべてへ届かなくても変更は反映し、応答の警告で知らせます。
func TestHandlerWarnsWhenAuditRecordsAreNotDelivered(t *testing.T) {
	controller := newControllerForTest(t)
	handler, audit := newHandlerForTest(t, map[string]pkglogger.SettingsController{"app": controller})
	audit.undelivered = true
	status, body, _ := requestForTest(t, handler, http.MethodPatch, "/settings", tokenForTest, `{"loggers":{"app":{"outputs":{"stdout":{"level":"DEBUG"}}}}}`)
	var document settingsDocument
	if err := json.Unmarshal([]byte(body), &document); err != nil || status != http.StatusOK {
		t.Fatalf("PATCH = %d %s, want 200 with a JSON body", status, body)
	}
	if !slices.Equal(document.Warnings, []string{warningAuditUndelivered}) {
		t.Errorf("warnings = %q, want the undelivered audit warning", document.Warnings)
	}
	if got := stdoutSettings(controller).Level; got != pkglogger.LogLevelDebug {
		t.Errorf("stdout level = %v, want DEBUG applied", got)
	}
}

// interferingControllerは、変更を反映した直後に、別の処理がlevelを書き換えたControllerです。
type interferingController struct{ controllerForTest }

func (c interferingController) UpdateOutputSettings(changes map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange) (map[pkglogger.LogOutputDestination]pkglogger.AppliedOutputSettings, error) {
	applied, err := c.controllerForTest.UpdateOutputSettings(changes)
	if err != nil {
		return applied, err
	}
	warn := pkglogger.LogLevelWarn
	if _, err := c.controllerForTest.UpdateOutputSettings(map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
		pkglogger.LogOutputStdout: {Level: &warn},
	}); err != nil {
		return applied, err
	}
	for output, a := range applied {
		a.After = c.controllerForTest.OutputSettings()[output]
		applied[output] = a
	}
	return applied, nil
}

// 変更後の設定が要求どおりでなければ、設定は戻さず、500と今の設定を返して記録します。
func TestHandlerReportsAMismatchWithoutReverting(t *testing.T) {
	controller := newControllerForTest(t)
	handler, audit := newHandlerForTest(t, map[string]pkglogger.SettingsController{"app": interferingController{controller}})
	status, body, id := requestForTest(t, handler, http.MethodPatch, "/settings", tokenForTest, `{"loggers":{"app":{"outputs":{"stdout":{"level":"DEBUG"}}}}}`)
	var document errorDocument
	if err := json.Unmarshal([]byte(body), &document); err != nil || status != http.StatusInternalServerError {
		t.Fatalf("PATCH = %d %s, want 500 with a JSON body", status, body)
	}
	if document.Error == "" || document.Loggers["app"].Outputs[pkglogger.LogOutputStdout].Level != "WARN" {
		t.Errorf("error document = %+v, want an error with the current WARN settings", document)
	}
	if got := stdoutSettings(controller).Level; got != pkglogger.LogLevelWarn {
		t.Errorf("stdout level = %v, want WARN left as it is", got)
	}
	want := []string{id + " requested", id + " started", id + " applied", id + " mismatch"}
	if got := audit.snapshot(); !slices.Equal(got, want) {
		t.Errorf("audit records = %q, want %q", got, want)
	}
}

// slowReadControllerは、設定を読んだ後に少し待つControllerです。同時の2つのPATCHが、
// どちらかが反映される前に、どちらも元の設定を読む順番を作ります。
type slowReadController struct {
	controllerForTest
	delay time.Duration
}

func (c slowReadController) OutputSettings() map[pkglogger.LogOutputDestination]pkglogger.OutputSettings {
	settings := c.controllerForTest.OutputSettings()
	time.Sleep(c.delay)
	return settings
}

// 同じ出力先のsamplingの別の項目を同時にPATCHしても、どちらの変更も残ります。受け口が
// 今の値を読んで補ったsampling設定全体で置き換えると、後から反映したPATCHが、先の
// PATCHの変更を古い値へ戻します。
func TestConcurrentPatchesOfDifferentSamplingFieldsBothApply(t *testing.T) {
	controller := newControllerForTest(t)
	handler, _ := newHandlerForTest(t, map[string]pkglogger.SettingsController{"app": slowReadController{controllerForTest: controller, delay: 20 * time.Millisecond}})
	bodies := []string{
		`{"loggers":{"app":{"outputs":{"stdout":{"sampling":{"info_rate":10}}}}}}`,
		`{"loggers":{"app":{"outputs":{"stdout":{"sampling":{"warn_rate":20}}}}}}`,
	}
	statuses := make([]int, len(bodies))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, body := range bodies {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			statuses[i], _, _ = requestForTest(t, handler, http.MethodPatch, "/settings", tokenForTest, body)
		}()
	}
	close(start)
	wg.Wait()
	if got := stdoutSettings(controller).Sampling; statuses[0] != http.StatusOK || statuses[1] != http.StatusOK ||
		got.SamplingInfoRate != 10 || got.SamplingWarnRate != 20 {
		t.Fatalf("PATCH statuses = %v, stdout sampling = %+v; want both 200 with info_rate 10 and warn_rate 20", statuses, got)
	}
}

// 不正な要求は、一つも反映せずに拒否して監査logへ記録し、errorを状態codeとJSONの本文で
// 返します。
func TestHandlerRejectsInvalidRequestsWithoutChanges(t *testing.T) {
	app, other := newControllerForTest(t), newControllerForTest(t)
	handler, audit := newHandlerForTest(t, map[string]pkglogger.SettingsController{"app": app, "other": other})
	for _, tt := range []struct {
		name   string
		body   string
		status int
		phases []string
	}{
		{"not JSON", `level=DEBUG`, http.StatusBadRequest, []string{"rejected"}},
		{"unknown field", `{"loggers":{"app":{"outputs":{"stdout":{"colour":"red"}}}}}`, http.StatusBadRequest, []string{"rejected"}},
		{"two objects", `{} {}`, http.StatusBadRequest, []string{"rejected"}},
		{"invalid level", `{"loggers":{"app":{"outputs":{"stdout":{"level":"LOUD"}}}}}`, http.StatusBadRequest, []string{"rejected"}},
		{"invalid sampling rate", `{"loggers":{"app":{"outputs":{"stdout":{"sampling":{"enabled":true}}}}}}`, http.StatusBadRequest, []string{"requested", "rejected"}},
		{"unknown logger", `{"loggers":{"missing":{"outputs":{"stdout":{"level":"DEBUG"}}}}}`, http.StatusNotFound, []string{"requested", "rejected"}},
		{"unknown output", `{"loggers":{"app":{"outputs":{"file":{"level":"DEBUG"}}}}}`, http.StatusNotFound, []string{"requested", "rejected"}},
		// 有効な変更があっても、別のloggerの不正な変更があれば反映しません。
		{"one invalid among valid", `{"loggers":{"app":{"outputs":{"stdout":{"level":"DEBUG"}}},"other":{"outputs":{"stdout":{"sampling":{"enabled":true}}}}}}`, http.StatusBadRequest, []string{"requested", "rejected"}},
		{"too large", `{"loggers":{"app":{"outputs":{"stdout":{"level":"` + strings.Repeat("D", 70<<10) + `"}}}}}`, http.StatusRequestEntityTooLarge, []string{"rejected"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := len(audit.snapshot())
			status, body, id := requestForTest(t, handler, http.MethodPatch, "/settings", tokenForTest, tt.body)
			if status != tt.status || !strings.Contains(body, `"error"`) {
				t.Errorf("PATCH = %d %s, want %d with an error message", status, body, tt.status)
			}
			for name, controller := range map[string]controllerForTest{"app": app, "other": other} {
				if got := stdoutSettings(controller); got.Level != pkglogger.LogLevelInfo || got.Sampling.IsSamplingEnabled {
					t.Errorf("%s stdout settings = %+v after a rejected request, want the original settings", name, got)
				}
			}
			var got []string
			for _, record := range audit.snapshot()[before:] {
				phase, ok := strings.CutPrefix(record, id+" ")
				if !ok {
					t.Fatalf("audit record %q does not belong to request %q", record, id)
				}
				got = append(got, strings.Fields(phase)[0])
			}
			if !slices.Equal(got, tt.phases) {
				t.Errorf("audit phases = %q, want %q", got, tt.phases)
			}
		})
	}
}

// 認証した要求でも、ほかの経路や方法は拒否して記録します。
func TestHandlerRejectsOtherMethodsAndPaths(t *testing.T) {
	handler, audit := newHandlerForTest(t, map[string]pkglogger.SettingsController{"app": newControllerForTest(t)})
	status, body, post := requestForTest(t, handler, http.MethodPost, "/settings", tokenForTest, "{}")
	if status != http.StatusMethodNotAllowed {
		t.Errorf("POST = %d %s, want 405", status, body)
	}
	status, body, other := requestForTest(t, handler, http.MethodGet, "/other", tokenForTest, "")
	if status != http.StatusNotFound {
		t.Errorf("GET /other = %d %s, want 404", status, body)
	}
	want := []string{post + " rejected method not allowed", other + " rejected not found"}
	if got := audit.snapshot(); !slices.Equal(got, want) {
		t.Errorf("audit records = %q, want %q", got, want)
	}
}

// configSourceForTestは、設定を示せるController・delivery logger・telemetryの代わりです。
type configSourceForTest struct {
	controllerForTest
	auditLoggerForTest
	cfg configview.Configuration
}

func (s *configSourceForTest) Configuration() configview.Configuration { return s.cfg }

func (s *configSourceForTest) Tracer() pkgobserver.Tracer     { return nil }
func (s *configSourceForTest) Meter() pkgobserver.Meter       { return nil }
func (s *configSourceForTest) Shutdown(context.Context) error { return nil }

func TestHandlerServesTheConfigWithSecretsMasked(t *testing.T) {
	app := &configSourceForTest{controllerForTest: newControllerForTest(t), cfg: configview.Configuration{
		Kind: configview.KindLogger,
		Value: pkglogger.LoggerConfig{Timeout: 3 * time.Second, LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputSlack},
			Slack: &pkglogger.SlackLoggerConfig{Token: "xoxb-secret", Channel: "#ops"}},
	}}
	audit := &configSourceForTest{cfg: configview.Configuration{Kind: configview.KindDeliveryLogger, Value: pkglogger.DeliveryLoggerConfig{MaxLogEntryBytes: 64}}}
	tel := &configSourceForTest{cfg: configview.Configuration{Kind: configview.KindTelemetry, Value: pkgtelemetry.Config{Traces: pkgtelemetry.TracesConfig{SamplingRatio: 0.25}}}}
	handler, err := NewHandler(pkgadmin.HandlerConfig{Token: tokenForTest, Members: pkgadmin.Members{
		Loggers:         map[string]pkglogger.SettingsController{"app": app, "plain": newControllerForTest(t)},
		DeliveryLoggers: map[string]pkglogger.DeliveryLogger{"audit": audit},
		Telemetries:     map[string]pkgtelemetry.Telemetry{"tel": tel},
	}}, &auditLoggerForTest{}, status.Probes{})
	if err != nil {
		t.Fatal(err)
	}
	status, body, _ := requestForTest(t, handler, http.MethodGet, "/config", tokenForTest, "")
	if status != http.StatusOK {
		t.Fatalf("GET /config = %d %s, want 200", status, body)
	}
	for _, want := range []string{
		`"app": {`, `"kind": "logger"`, `"timeout": "3s"`, `"log_output_destinations": [`, `"token": "***"`, `"channel": "#ops"`,
		`"audit": {`, `"kind": "delivery_logger"`, `"max_log_entry_bytes": 64`,
		`"telemetries": {`, `"tel": {`, `"kind": "telemetry"`, `"sampling_ratio": 0.25`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("GET /config lacks %s:\n%s", want, body)
		}
	}
	// 設定を示せないControllerと、秘密の値は、文書に入りません。
	for _, unwanted := range []string{`"plain"`, "xoxb-secret"} {
		if strings.Contains(body, unwanted) {
			t.Errorf("GET /config contains %s:\n%s", unwanted, body)
		}
	}
	for path, want := range map[string]string{
		"/config/loggers/app/config/slack/channel":             `"#ops"`,
		"/config/loggers/app/config/log_output_destinations/0": `"slack"`,
		"/config/telemetries/tel/kind":                         `"telemetry"`,
	} {
		status, body, _ := requestForTest(t, handler, http.MethodGet, path, tokenForTest, "")
		if status != http.StatusOK || strings.TrimSpace(body) != want {
			t.Errorf("GET %s = %d %q, want 200 %s", path, status, body, want)
		}
	}
}

func TestHandlerServesSeveralConfigValuesInOneRequest(t *testing.T) {
	app := &configSourceForTest{controllerForTest: newControllerForTest(t), cfg: configview.Configuration{
		Kind: configview.KindLogger, Value: pkglogger.LoggerConfig{Timeout: 3 * time.Second, MaxLogEntryBytes: 512},
	}}
	handler, err := NewHandler(pkgadmin.HandlerConfig{Token: tokenForTest, Members: pkgadmin.Members{
		Loggers: map[string]pkglogger.SettingsController{"app": app},
	}}, &auditLoggerForTest{}, status.Probes{})
	if err != nil {
		t.Fatal(err)
	}
	status, body, _ := requestForTest(t, handler, http.MethodGet, "/config?path=loggers/app/config/timeout&path=/loggers/app/kind", tokenForTest, "")
	if status != http.StatusOK {
		t.Fatalf("GET /config?path=... = %d %s, want 200", status, body)
	}
	var document struct {
		Values map[string]any `json:"values"`
	}
	if err := json.Unmarshal([]byte(body), &document); err != nil {
		t.Fatal(err)
	}
	if document.Values["loggers/app/config/timeout"] != "3s" || document.Values["/loggers/app/kind"] != "logger" {
		t.Errorf("values = %v, want the timeout and the kind under the given paths", document.Values)
	}
	// 一つでも辿れないpathがあれば404にし、そのpathを理由に含めます。
	status, body, _ = requestForTest(t, handler, http.MethodGet, "/config?path=loggers/app/config/timeout&path=loggers/nope", tokenForTest, "")
	if status != http.StatusNotFound || !strings.Contains(body, "loggers/nope") || strings.Contains(body, `"3s"`) {
		t.Errorf("GET with a missing path = %d %s, want 404 naming loggers/nope and no values", status, body)
	}
}

func TestHandlerRejectsUnknownConfigPathsAndOtherMethods(t *testing.T) {
	handler, audit := newHandlerForTest(t, map[string]pkglogger.SettingsController{"app": newControllerForTest(t)})
	status, body, missing := requestForTest(t, handler, http.MethodGet, "/config/loggers/nope", tokenForTest, "")
	if status != http.StatusNotFound {
		t.Errorf("GET /config/loggers/nope = %d %s, want 404", status, body)
	}
	status, body, trailing := requestForTest(t, handler, http.MethodGet, "/config/", tokenForTest, "")
	if status != http.StatusNotFound {
		t.Errorf("GET /config/ = %d %s, want 404", status, body)
	}
	status, body, patch := requestForTest(t, handler, http.MethodPatch, "/config", tokenForTest, "{}")
	if status != http.StatusMethodNotAllowed {
		t.Errorf("PATCH /config = %d %s, want 405", status, body)
	}
	want := []string{missing + " rejected not found", trailing + " rejected not found", patch + " rejected method not allowed"}
	if got := audit.snapshot(); !slices.Equal(got, want) {
		t.Errorf("audit records = %q, want %q", got, want)
	}
}

func TestNewHandlerRejectsANameUsedTwice(t *testing.T) {
	app := &configSourceForTest{controllerForTest: newControllerForTest(t)}
	_, err := NewHandler(pkgadmin.HandlerConfig{Token: tokenForTest, Members: pkgadmin.Members{
		Loggers:     map[string]pkglogger.SettingsController{"app": app},
		Telemetries: map[string]pkgtelemetry.Telemetry{"app": app},
	}}, &auditLoggerForTest{}, status.Probes{})
	if !errors.Is(err, ErrDuplicateName) {
		t.Errorf("NewHandler(name used twice) error = %v, want ErrDuplicateName", err)
	}
}

// statusMemberForTestは、様子を報告できるControllerの代わりです。
type statusMemberForTest struct {
	controllerForTest
	member status.MemberStatus
}

func (m *statusMemberForTest) Status(context.Context) status.MemberStatus { return m.member }

func TestHandlerServesTheStatusWithWarnings(t *testing.T) {
	app := &statusMemberForTest{controllerForTest: newControllerForTest(t), member: status.MemberStatus{
		Kind: "logger",
		Inspection: status.Inspection{Backends: []status.BackendInspection{{Name: "opensearch", Spool: &status.SpoolInspection{
			Root: "/spool/opensearch", StateDirs: map[string]string{"active": "/spool/opensearch/active"},
			Replay: status.Replay{State: "running"},
		}}}},
	}}
	handler, err := NewHandler(pkgadmin.HandlerConfig{Token: tokenForTest, Members: pkgadmin.Members{
		Loggers: map[string]pkglogger.SettingsController{"app": app, "plain": newControllerForTest(t)},
	}, Status: pkgadmin.StatusConfig{DisableDirectoryScan: true}}, &auditLoggerForTest{}, status.Probes{})
	if err != nil {
		t.Fatal(err)
	}
	code, body, _ := requestForTest(t, handler, http.MethodGet, "/status", tokenForTest, "")
	if code != http.StatusOK {
		t.Fatalf("GET /status = %d %s, want 200", code, body)
	}
	for _, want := range []string{
		`"generated_at"`, `"process": {`, `"goroutines"`, `"members": {`, `"app": {`, `"kind": "logger"`,
		`"name": "opensearch"`, `"spool": {`, `"root": "/spool/opensearch"`, `"replay": {`, `"state": "running"`,
		`"directories": [`, `"role": "spool"`, `"warnings": []`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("GET /status lacks %s:\n%s", want, body)
		}
	}
	// 様子を報告できないControllerは、membersに入りません。
	if strings.Contains(body, `"plain"`) {
		t.Errorf("GET /status contains the plain controller:\n%s", body)
	}
	code, body, _ = requestForTest(t, handler, http.MethodGet, "/status/members/app/backends/0/spool/replay/state", tokenForTest, "")
	if code != http.StatusOK || strings.TrimSpace(body) != `"running"` {
		t.Errorf("GET /status/.../state = %d %q, want 200 \"running\"", code, body)
	}
	code, body, _ = requestForTest(t, handler, http.MethodGet, "/status?path=process/max_procs&path=members/app/kind", tokenForTest, "")
	if code != http.StatusOK || !strings.Contains(body, `"members/app/kind": "logger"`) || !strings.Contains(body, `"process/max_procs": `) {
		t.Errorf("GET /status?path=... = %d %s, want both values", code, body)
	}
}

func TestNewHandlerRejectsAStatusConfigOutOfRange(t *testing.T) {
	loggers := map[string]pkglogger.SettingsController{"app": newControllerForTest(t)}
	for name, cfg := range map[string]pkgadmin.StatusConfig{
		"negative interval": {DirectoryScanInterval: -time.Second},
		"percent over 100":  {DiskFreeWarningPercent: 101},
		"negative percent":  {BackendFailureWarningPercent: -1},
		"negative slow":     {SlowRequest: -time.Second},
	} {
		_, err := NewHandler(pkgadmin.HandlerConfig{Token: tokenForTest, Members: pkgadmin.Members{Loggers: loggers}, Status: cfg}, &auditLoggerForTest{}, status.Probes{})
		if !errors.Is(err, ErrInvalidStatusConfig) {
			t.Errorf("NewHandler(%s) error = %v, want ErrInvalidStatusConfig", name, err)
		}
	}
	if err := ValidateStatusConfig(pkgadmin.DefaultStatusConfig()); err != nil {
		t.Errorf("ValidateStatusConfig(defaults) = %v, want nil", err)
	}
}

// ブラウザ画面のfileは、tokenなしで返し、監査logに記録しません。scriptとstyleは同じ
// 配信元に限ります。
func TestHandlerServesTheBrowserPageWithoutAToken(t *testing.T) {
	handler, audit := newHandlerForTest(t, map[string]pkglogger.SettingsController{"app": newControllerForTest(t)})
	for path, want := range map[string]string{"/": "text/html", "/ui.css": "text/css", "/ui.js": "text/javascript"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK || !strings.HasPrefix(recorder.Header().Get("Content-Type"), want) ||
			!strings.Contains(recorder.Header().Get("Content-Security-Policy"), "script-src 'self'") ||
			recorder.Header().Get("Cache-Control") != "no-store" || recorder.Body.Len() == 0 {
			t.Errorf("GET %s = %d %v, want 200 %s with a CSP", path, recorder.Code, recorder.Header(), want)
		}
	}
	page := httptest.NewRecorder()
	handler.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/", nil))
	for _, want := range []string{`<script src="ui.js">`, `href="ui.css"`, `id="token"`, `data-tab="status"`} {
		if !strings.Contains(page.Body.String(), want) {
			t.Errorf("the page lacks %s", want)
		}
	}
	script := httptest.NewRecorder()
	handler.ServeHTTP(script, httptest.NewRequest(http.MethodGet, "/ui.js", nil))
	for _, want := range []string{`"status"`, `"settings"`, `"config"`, `sessionStorage`, `Authorization`} {
		if !strings.Contains(script.Body.String(), want) {
			t.Errorf("the script lacks %s", want)
		}
	}
	post := httptest.NewRecorder()
	handler.ServeHTTP(post, httptest.NewRequest(http.MethodPost, "/", nil))
	if post.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST / = %d, want 405", post.Code)
	}
	if got := audit.snapshot(); len(got) != 0 {
		t.Errorf("audit records after the page = %q, want none", got)
	}
	// 画面のfile以外は、これまでどおりtokenが要ります。
	status, _, _ := requestForTest(t, handler, http.MethodGet, "/index.html", "", "")
	if status != http.StatusUnauthorized {
		t.Errorf("GET /index.html without a token = %d, want 401", status)
	}
}

// 画面のJavaScriptの構文を、nodeがあれば確かめます。
func TestBrowserPageScriptParses(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	script, err := uiFiles.ReadFile("ui/ui.js")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(node, "--check", "-")
	cmd.Stdin = strings.NewReader(string(script))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("node --check: %v\n%s", err, out)
	}
}
