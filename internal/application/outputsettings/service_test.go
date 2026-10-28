package outputsettings

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// registryControllerは、RegistryをControllerとして使う試験用の型です。
type registryController struct{ *Registry }

func (c registryController) OutputSettings() map[pkglogger.LogOutputDestination]pkglogger.OutputSettings {
	return c.Snapshot()
}

func (c registryController) UpdateOutputSettings(changes map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange) (map[pkglogger.LogOutputDestination]pkglogger.AppliedOutputSettings, error) {
	return c.Update(changes)
}

// failingControllerは、検証を通った変更の反映に失敗するControllerです。
type failingController struct{ registryController }

func (failingController) UpdateOutputSettings(map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange) (map[pkglogger.LogOutputDestination]pkglogger.AppliedOutputSettings, error) {
	return nil, errors.New("applying failed")
}

// interferingControllerは、変更を反映した直後に、別の処理がLiveSettingsへ直接levelを
// 書いたControllerです。変更後の設定が要求どおりにならない場合を作ります。
type interferingController struct {
	registryController
	live  *pkglogger.LiveSettings
	level pkglogger.LogLevel
}

func (c interferingController) UpdateOutputSettings(changes map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange) (map[pkglogger.LogOutputDestination]pkglogger.AppliedOutputSettings, error) {
	applied, err := c.registryController.UpdateOutputSettings(changes)
	if err != nil {
		return applied, err
	}
	if err := c.live.Set(c.level, c.live.Sampling()); err != nil {
		return applied, err
	}
	for output, a := range applied {
		a.After = pkglogger.OutputSettings{Level: c.live.Level(), Sampling: c.live.Sampling()}
		applied[output] = a
	}
	return applied, nil
}

// blockingControllerは、変更の反映を、releaseが閉じられるまで止めるControllerです。
type blockingController struct {
	registryController
	entered chan struct{}
	release chan struct{}
}

func (c blockingController) UpdateOutputSettings(changes map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange) (map[pkglogger.LogOutputDestination]pkglogger.AppliedOutputSettings, error) {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	<-c.release
	return c.registryController.UpdateOutputSettings(changes)
}

func newLiveForTest(t *testing.T) *pkglogger.LiveSettings {
	t.Helper()
	live, err := pkglogger.NewLiveSettings(pkglogger.LogLevelInfo, pkglogger.SamplingConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return live
}

func newControllerForTest(t *testing.T) registryController {
	t.Helper()
	return registryController{NewRegistry("test logger", map[pkglogger.LogOutputDestination]*pkglogger.LiveSettings{
		pkglogger.LogOutputStdout: newLiveForTest(t),
	})}
}

func level(l pkglogger.LogLevel) *pkglogger.LogLevel { return &l }

func ptr[T any](v T) *T { return &v }

// auditRecordは、試験用の監査loggerが受け取った一件の記録です。fieldsのAnyの値は、
// 比べやすいようJSONの文字列にします。
type auditRecord struct {
	level   pkglogger.LogLevel
	message string
	fields  map[string]any
}

func (r auditRecord) phase() string     { s, _ := r.fields["audit_phase"].(string); return s }
func (r auditRecord) requestID() string { s, _ := r.fields["request_id"].(string); return s }

// auditLoggerForTestは、受け取った記録を順に保持する試験用の送達確認loggerです。
// undeliveredに含まれる段階の記録は、記録先へ届かなかったと報告します。
type auditLoggerForTest struct {
	mu          sync.Mutex
	records     []auditRecord
	undelivered map[string]bool
}

func (l *auditLoggerForTest) record(level pkglogger.LogLevel, message string, fields []pkglogger.Field) (pkglogger.DeliveryReport, error) {
	values := map[string]any{}
	for _, field := range fields {
		value := field.Value()
		switch value.(type) {
		case map[string]any, []any:
			if encoded, err := json.Marshal(value); err == nil {
				value = string(encoded)
			}
		}
		values[field.Key()] = value
	}
	record := auditRecord{level: level, message: message, fields: values}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, record)
	if l.undelivered[record.phase()] {
		return pkglogger.DeliveryReport{Targets: []pkglogger.DeliveryResult{{Backend: "file", Status: pkglogger.DeliveryFailed}}},
			errors.New("audit record was not delivered")
	}
	return pkglogger.DeliveryReport{Targets: []pkglogger.DeliveryResult{{Backend: "file", Status: pkglogger.DeliverySucceeded}}}, nil
}

func (l *auditLoggerForTest) Debug(_ context.Context, message string, fields ...pkglogger.Field) (pkglogger.DeliveryReport, error) {
	return l.record(pkglogger.LogLevelDebug, message, fields)
}

func (l *auditLoggerForTest) Info(_ context.Context, message string, fields ...pkglogger.Field) (pkglogger.DeliveryReport, error) {
	return l.record(pkglogger.LogLevelInfo, message, fields)
}

func (l *auditLoggerForTest) Warn(_ context.Context, message string, fields ...pkglogger.Field) (pkglogger.DeliveryReport, error) {
	return l.record(pkglogger.LogLevelWarn, message, fields)
}

func (l *auditLoggerForTest) Error(_ context.Context, message string, fields ...pkglogger.Field) (pkglogger.DeliveryReport, error) {
	return l.record(pkglogger.LogLevelError, message, fields)
}

func (l *auditLoggerForTest) ErrorWithStack(_ context.Context, message string, _ error, fields ...pkglogger.Field) (pkglogger.DeliveryReport, error) {
	return l.record(pkglogger.LogLevelError, message, fields)
}

func (l *auditLoggerForTest) snapshot() []auditRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.records)
}

func phases(records []auditRecord) []string {
	result := make([]string, 0, len(records))
	for _, record := range records {
		result = append(result, record.phase())
	}
	return result
}

func newServiceForTest(t *testing.T, loggers map[string]pkglogger.SettingsController) (*Service, *auditLoggerForTest) {
	t.Helper()
	audit := &auditLoggerForTest{}
	service, err := NewService(loggers, audit)
	if err != nil {
		t.Fatal(err)
	}
	return service, audit
}

var requestForTest = Request{ID: "request-1", RemoteAddr: "192.0.2.10:54321"}

func TestNewServiceRequiresALoggerAndAnAuditLogger(t *testing.T) {
	if _, err := NewService(nil, &auditLoggerForTest{}); !errors.Is(err, ErrNoLoggers) {
		t.Errorf("NewService(nil loggers) error = %v, want ErrNoLoggers", err)
	}
	if _, err := NewService(map[string]pkglogger.SettingsController{"a": newControllerForTest(t)}, nil); !errors.Is(err, ErrNoAuditLogger) {
		t.Errorf("NewService(nil audit) error = %v, want ErrNoAuditLogger", err)
	}
}

// 出力先どうしが同じLiveSettingsを共有していると、一方への変更がもう一方も変え、監査logで
// 記録も照合もできないため、Serviceを作りません。errorは共有している組を名前で示します。
func TestNewServiceRejectsOutputsSharingLiveSettings(t *testing.T) {
	shared, other := newLiveForTest(t), newLiveForTest(t)
	registry := func(outputs map[pkglogger.LogOutputDestination]*pkglogger.LiveSettings) registryController {
		return registryController{NewRegistry("test logger", outputs)}
	}
	app := registry(map[pkglogger.LogOutputDestination]*pkglogger.LiveSettings{pkglogger.LogOutputStdout: shared})
	for _, tt := range []struct {
		name    string
		loggers map[string]pkglogger.SettingsController
		want    string
	}{
		{
			name: "outputs of one logger",
			loggers: map[string]pkglogger.SettingsController{"app": registry(map[pkglogger.LogOutputDestination]*pkglogger.LiveSettings{
				pkglogger.LogOutputStdout: shared,
				pkglogger.LogOutputFile:   shared,
			})},
			want: `logger "app" output "file" and logger "app" output "stdout"`,
		},
		{
			name: "outputs of two loggers",
			loggers: map[string]pkglogger.SettingsController{"app": app, "worker": registry(map[pkglogger.LogOutputDestination]*pkglogger.LiveSettings{
				pkglogger.LogOutputFile:   shared,
				pkglogger.LogOutputStdout: other,
			})},
			want: `logger "app" output "stdout" and logger "worker" output "file"`,
		},
		{
			name:    "one logger under two names",
			loggers: map[string]pkglogger.SettingsController{"app": app, "copy": app},
			want:    `logger "app" output "stdout" and logger "copy" output "stdout"`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewService(tt.loggers, &auditLoggerForTest{})
			if !errors.Is(err, ErrSharedLiveSettings) || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("NewService() error = %v, want ErrSharedLiveSettings naming %s", err, tt.want)
			}
		})
	}
}

// plainControllerは、LiveSettingsOwnerを実装しないControllerです。
type plainController struct{ inner registryController }

func (c plainController) OutputSettings() map[pkglogger.LogOutputDestination]pkglogger.OutputSettings {
	return c.inner.OutputSettings()
}

func (c plainController) UpdateOutputSettings(changes map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange) (map[pkglogger.LogOutputDestination]pkglogger.AppliedOutputSettings, error) {
	return c.inner.UpdateOutputSettings(changes)
}

// LiveSettingsを示さないloggerは共有を確かめられないため、共有していてもServiceを作ります。
// 別々のLiveSettingsを示すloggerも、そのまま受け付けます。
func TestNewServiceChecksOnlyLoggersThatShowTheirLiveSettings(t *testing.T) {
	shared := newLiveForTest(t)
	hidden := plainController{registryController{NewRegistry("test logger", map[pkglogger.LogOutputDestination]*pkglogger.LiveSettings{
		pkglogger.LogOutputStdout: shared,
	})}}
	loggers := map[string]pkglogger.SettingsController{
		"hidden": hidden,
		"copy":   hidden,
		"a":      newControllerForTest(t),
		"b":      newControllerForTest(t),
	}
	if _, err := NewService(loggers, &auditLoggerForTest{}); err != nil {
		t.Fatalf("NewService() error = %v, want no error", err)
	}
}

// Updateは、要求・変更を始める時点の設定・変更後の設定を、この順にINFOで監査logへ記録し、
// どの記録にも監査logの印・要求ID・送り元を付けます。
func TestServiceUpdateAppliesAndRecordsTheAuditTrail(t *testing.T) {
	a, b := newControllerForTest(t), newControllerForTest(t)
	service, audit := newServiceForTest(t, map[string]pkglogger.SettingsController{"a": a, "b": b})
	result, err := service.Update(context.Background(), requestForTest, map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
		"b": {pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelError)}},
		"a": {pkglogger.LogOutputStdout: {Sampling: &pkglogger.SamplingChange{SamplingInfoRate: ptr(10)}}},
	})
	if err != nil || len(result.Mismatches) != 0 || result.AuditUndelivered {
		t.Fatalf("Update() = %+v, %v; want no mismatch, delivered audit records and no error", result, err)
	}
	if got := service.Settings()["b"][pkglogger.LogOutputStdout].Level; got != pkglogger.LogLevelError {
		t.Errorf("b level = %v, want ERROR", got)
	}
	records := audit.snapshot()
	if got, want := phases(records), []string{auditPhaseRequested, auditPhaseStarted, auditPhaseApplied}; !slices.Equal(got, want) {
		t.Fatalf("audit phases = %q, want %q", got, want)
	}
	for _, record := range records {
		if record.level != pkglogger.LogLevelInfo || record.fields["log_kind"] != "audit" ||
			record.requestID() != requestForTest.ID || record.fields["remote_addr"] != requestForTest.RemoteAddr {
			t.Errorf("audit record = %+v, want INFO with log_kind, the request ID and the remote address", record)
		}
	}
	if got, want := records[0].fields["changes"], `{"a":{"stdout":{"sampling":{"info_rate":10}}},"b":{"stdout":{"level":"ERROR"}}}`; got != want {
		t.Errorf("requested changes = %v, want only the requested fields %s", got, want)
	}
	before := `{"level":"INFO","sampling":{"by_request_id":false,"debug_rate":0,"enabled":false,"error_rate":0,"info_rate":0,"request_id_rate":0,"warn_rate":0}}`
	if got, want := records[1].fields["settings"], `{"a":{"stdout":`+before+`},"b":{"stdout":`+before+`}}`; got != want {
		t.Errorf("started settings = %v, want %s", got, want)
	}
	if got, want := records[2].fields["settings"],
		`{"a":{"stdout":{"level":"INFO","sampling":{"by_request_id":false,"debug_rate":0,"enabled":false,"error_rate":0,"info_rate":10,"request_id_rate":0,"warn_rate":0}}},`+
			`"b":{"stdout":{"level":"ERROR","sampling":{"by_request_id":false,"debug_rate":0,"enabled":false,"error_rate":0,"info_rate":0,"request_id_rate":0,"warn_rate":0}}}}`; got != want {
		t.Errorf("applied settings = %v, want %s", got, want)
	}
	if records[2].fields["verified"] != true {
		t.Errorf("applied verified = %v, want true", records[2].fields["verified"])
	}
}

// 一つでも不正な変更があれば、別のloggerの有効な変更も反映せず、要求と拒否だけを記録します。
func TestServiceUpdateAppliesNothingWhenAnyChangeIsInvalid(t *testing.T) {
	for _, tt := range []struct {
		name    string
		changes map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange
		want    error
	}{
		{"invalid level", map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
			"a": {pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug)}},
			"b": {pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevel(42))}},
		}, pkglogger.ErrInvalidLogLevel},
		{"invalid sampling", map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
			"a": {pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug)}},
			"b": {pkglogger.LogOutputStdout: {Sampling: &pkglogger.SamplingChange{IsSamplingEnabled: ptr(true)}}},
		}, pkglogger.ErrInvalidSamplingRate},
		{"unknown logger", map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
			"a":       {pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug)}},
			"missing": {pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug)}},
		}, ErrUnknownLogger},
		{"unknown output", map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
			"a": {pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug)}},
			"b": {pkglogger.LogOutputFile: {Level: level(pkglogger.LogLevelDebug)}},
		}, pkglogger.ErrUnknownOutput},
	} {
		t.Run(tt.name, func(t *testing.T) {
			service, audit := newServiceForTest(t, map[string]pkglogger.SettingsController{"a": newControllerForTest(t), "b": newControllerForTest(t)})
			if _, err := service.Update(context.Background(), requestForTest, tt.changes); !errors.Is(err, tt.want) {
				t.Fatalf("Update() error = %v, want %v", err, tt.want)
			}
			for name, outputs := range service.Settings() {
				if got := outputs[pkglogger.LogOutputStdout].Level; got != pkglogger.LogLevelInfo {
					t.Errorf("%s level = %v after a rejected update, want INFO", name, got)
				}
			}
			records := audit.snapshot()
			if got, want := phases(records), []string{auditPhaseRequested, auditPhaseRejected}; !slices.Equal(got, want) {
				t.Fatalf("audit phases = %q, want %q", got, want)
			}
			if records[1].level != pkglogger.LogLevelWarn || records[1].fields["reason"] == "" {
				t.Errorf("rejected record = %+v, want WARN with the reason", records[1])
			}
		})
	}
}

// 検証の後で反映に失敗した場合は、ErrApplyFailedを返し、失敗をERRORで記録します。
func TestServiceUpdateRecordsAFailureAfterValidation(t *testing.T) {
	service, audit := newServiceForTest(t, map[string]pkglogger.SettingsController{
		"a": newControllerForTest(t), "b": failingController{newControllerForTest(t)},
	})
	_, err := service.Update(context.Background(), requestForTest, map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
		"a": {pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug)}},
		"b": {pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug)}},
	})
	if !errors.Is(err, ErrApplyFailed) {
		t.Fatalf("Update() error = %v, want ErrApplyFailed", err)
	}
	records := audit.snapshot()
	if got, want := phases(records), []string{auditPhaseRequested, auditPhaseStarted, auditPhaseFailed}; !slices.Equal(got, want) {
		t.Fatalf("audit phases = %q, want %q", got, want)
	}
	if failed := records[2]; failed.level != pkglogger.LogLevelError || failed.fields["reason"] == "" ||
		failed.fields["settings"] != `{"a":{"stdout":{"level":"DEBUG","sampling":{"by_request_id":false,"debug_rate":0,"enabled":false,"error_rate":0,"info_rate":0,"request_id_rate":0,"warn_rate":0}}}}` {
		t.Errorf("failed record = %+v, want ERROR with the reason and only the change of a", failed)
	}
}

// 変更後の設定が要求どおりでなければ、設定は戻さず、Resultで知らせてERRORで記録します。
func TestServiceUpdateRecordsAMismatchWithoutReverting(t *testing.T) {
	live := newLiveForTest(t)
	controller := interferingController{
		registryController: registryController{NewRegistry("test logger", map[pkglogger.LogOutputDestination]*pkglogger.LiveSettings{pkglogger.LogOutputStdout: live})},
		live:               live,
		level:              pkglogger.LogLevelWarn,
	}
	service, audit := newServiceForTest(t, map[string]pkglogger.SettingsController{"a": controller})
	result, err := service.Update(context.Background(), requestForTest, map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
		"a": {pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := Mismatch{
		Logger: "a", Output: pkglogger.LogOutputStdout,
		Expected: pkglogger.OutputSettings{Level: pkglogger.LogLevelDebug},
		Actual:   pkglogger.OutputSettings{Level: pkglogger.LogLevelWarn},
	}
	if len(result.Mismatches) != 1 || result.Mismatches[0] != want {
		t.Fatalf("Mismatches = %+v, want %+v", result.Mismatches, want)
	}
	if got := live.Level(); got != pkglogger.LogLevelWarn {
		t.Errorf("level = %v, want WARN left as it is", got)
	}
	records := audit.snapshot()
	if got, want := phases(records), []string{auditPhaseRequested, auditPhaseStarted, auditPhaseApplied, auditPhaseMismatch}; !slices.Equal(got, want) {
		t.Fatalf("audit phases = %q, want %q", got, want)
	}
	if records[2].fields["verified"] != false {
		t.Errorf("applied verified = %v, want false", records[2].fields["verified"])
	}
	mismatch := records[3]
	if mismatch.level != pkglogger.LogLevelError || mismatch.fields["logger"] != "a" || mismatch.fields["output"] != "stdout" ||
		mismatch.fields["expected"] == mismatch.fields["actual"] {
		t.Errorf("mismatch record = %+v, want ERROR with the logger, the output, and different expected and actual settings", mismatch)
	}
}

// 監査logが記録先のすべてへ届かなくても変更は続け、Resultで知らせて、届かなかった段階を
// ERRORで記録します。
func TestServiceUpdateContinuesWhenAuditRecordsAreNotDelivered(t *testing.T) {
	controller := newControllerForTest(t)
	service, audit := newServiceForTest(t, map[string]pkglogger.SettingsController{"a": controller})
	audit.undelivered = map[string]bool{auditPhaseRequested: true, auditPhaseApplied: true}
	result, err := service.Update(context.Background(), requestForTest, map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
		"a": {pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug)}},
	})
	if err != nil || !result.AuditUndelivered {
		t.Fatalf("Update() = %+v, %v; want AuditUndelivered without an error", result, err)
	}
	if got := service.Settings()["a"][pkglogger.LogOutputStdout].Level; got != pkglogger.LogLevelDebug {
		t.Errorf("level = %v, want DEBUG applied even though the audit records were not delivered", got)
	}
	records := audit.snapshot()
	last := records[len(records)-1]
	if last.phase() != auditPhaseUndelivered || last.level != pkglogger.LogLevelError ||
		last.fields["undelivered_phases"] != `["requested","applied"]` {
		t.Errorf("last audit record = %+v, want ERROR naming the undelivered phases", last)
	}
}

// 変更は一つずつ順に処理し、後から来た変更は、前の変更の記録を終えてから始めます。
func TestServiceUpdateProcessesChangesOneAtATime(t *testing.T) {
	controller := blockingController{registryController: newControllerForTest(t), entered: make(chan struct{}, 1), release: make(chan struct{})}
	service, audit := newServiceForTest(t, map[string]pkglogger.SettingsController{"a": controller})
	change := map[string]map[pkglogger.LogOutputDestination]pkglogger.OutputSettingsChange{
		"a": {pkglogger.LogOutputStdout: {Level: level(pkglogger.LogLevelDebug)}},
	}
	var wg sync.WaitGroup
	update := func(id string) {
		defer wg.Done()
		if _, err := service.Update(context.Background(), Request{ID: id}, change); err != nil {
			t.Errorf("Update(%s): %v", id, err)
		}
	}
	wg.Add(1)
	go update("first")
	<-controller.entered
	wg.Add(1)
	go update("second")
	// 順に処理しない実装なら、この間に二つ目の要求の記録が始まります。
	time.Sleep(50 * time.Millisecond)
	close(controller.release)
	wg.Wait()
	var order []string
	for _, record := range audit.snapshot() {
		order = append(order, record.requestID()+" "+record.phase())
	}
	want := []string{
		"first requested", "first started", "first applied",
		"second requested", "second started", "second applied",
	}
	if !slices.Equal(order, want) {
		t.Errorf("audit records = %q, want every record of the first change before the second", order)
	}
}

// Rejectは拒否した要求をWARNで記録し、届かなかった場合はそのこともERRORで記録します。
func TestServiceRejectRecordsTheReason(t *testing.T) {
	service, audit := newServiceForTest(t, map[string]pkglogger.SettingsController{"a": newControllerForTest(t)})
	if undelivered := service.Reject(context.Background(), requestForTest, "missing or invalid bearer token"); undelivered {
		t.Fatal("Reject() reported undelivered audit records")
	}
	audit.undelivered = map[string]bool{auditPhaseRejected: true}
	if undelivered := service.Reject(context.Background(), requestForTest, "not found"); !undelivered {
		t.Fatal("Reject() did not report the undelivered audit record")
	}
	records := audit.snapshot()
	if got, want := phases(records), []string{auditPhaseRejected, auditPhaseRejected, auditPhaseUndelivered}; !slices.Equal(got, want) {
		t.Fatalf("audit phases = %q, want %q", got, want)
	}
	if records[0].level != pkglogger.LogLevelWarn || records[0].fields["reason"] != "missing or invalid bearer token" {
		t.Errorf("rejected record = %+v, want WARN with the reason", records[0])
	}
}
