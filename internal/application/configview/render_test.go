package configview

import (
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
)

// foreignProviderは、このmoduleの外の型の代わりです。interfaceに入れると型の名前だけに
// なることを、net/httpの型で確かめます。
type sampleConfig struct {
	Name          string
	Timeout       time.Duration
	Level         pkglogger.LogLevel
	Outputs       pkglogger.LogOutputDestinations
	Password      string
	Token         string
	Auth          pkgopensearch.AuthConfig
	Transport     http.RoundTripper
	HTTPClient    *http.Client
	Live          *pkglogger.LiveSettings
	Missing       *sampleConfig
	OnChange      func()
	Labels        map[string]string
	AccessKeyID   string
	unexportedKey string
	Embedded
	Next *sampleConfig
}

type Embedded struct {
	MaxLogEntryBytes int
}

func TestRenderWritesTheConfigAsJSONWithSecretsMasked(t *testing.T) {
	live, err := pkglogger.NewLiveSettings(pkglogger.LogLevelInfo, pkglogger.SamplingConfig{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := &sampleConfig{
		Name: "app", Timeout: 90 * time.Second, Level: pkglogger.LogLevelWarn,
		Outputs:  pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout, pkglogger.LogOutputFile},
		Password: "hunter2", Token: "",
		Auth:        pkgopensearch.AuthConfig{Type: pkgopensearch.AuthTypeBasic, Config: pkgopensearch.BasicAuthConfig{Username: "admin", Password: "pw"}},
		Transport:   http.DefaultTransport,
		HTTPClient:  &http.Client{Timeout: 5 * time.Second},
		Live:        live,
		OnChange:    func() {},
		Labels:      map[string]string{"env": "prod"},
		AccessKeyID: "AKIA123", unexportedKey: "hidden",
		Embedded: Embedded{MaxLogEntryBytes: 1024},
	}
	cfg.Next = cfg
	raw, err := json.Marshal(Render(cfg))
	if err != nil {
		t.Fatal(err)
	}
	got := string(raw)
	for _, want := range []string{
		`"name":"app"`, `"timeout":"1m30s"`, `"level":"WARN"`, `"outputs":["stdout","file"]`,
		`"password":"***"`, `"token":""`, `"access_key_id":"***"`,
		`"auth":{"config":{"password":"***","username":"admin"},"type":"basic"}`,
		`"transport":"*http.Transport"`, `"http_client":{`, `"timeout":"5s"`,
		`"missing":null`, `"labels":{"env":"prod"}`, `"max_log_entry_bytes":1024`, `"next":null`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("document does not contain %s:\n%s", want, got)
		}
	}
	for _, unwanted := range []string{"hunter2", "AKIA123", "hidden", `"live"`, `"on_change"`, "pw"} {
		if strings.Contains(got, unwanted) {
			t.Errorf("document contains %s:\n%s", unwanted, got)
		}
	}
}

func TestJSONKeySplitsWordsAndAcronyms(t *testing.T) {
	for name, want := range map[string]string{
		"Timeout": "timeout", "MaxLogEntryBytes": "max_log_entry_bytes", "HTTPClient": "http_client",
		"AccessKeyID": "access_key_id", "TLSCertFile": "tls_cert_file", "S3Bucket": "s3_bucket",
		"LogOutputDestinations": "log_output_destinations", "ID": "id", "OnErrorConfig": "on_error_config",
	} {
		if got := JSONKey(name); got != want {
			t.Errorf("JSONKey(%q) = %q, want %q", name, got, want)
		}
	}
}

func TestLookupWalksObjectsAndArrays(t *testing.T) {
	document := Render(&sampleConfig{Outputs: pkglogger.LogOutputDestinations{"stdout", "file"}, Labels: map[string]string{"env": "prod"}})
	for _, tc := range []struct {
		path []string
		want any
		ok   bool
	}{
		{[]string{"outputs", "1"}, "file", true},
		{[]string{"labels", "env"}, "prod", true},
		{[]string{"outputs", "2"}, nil, false},
		{[]string{"outputs", "01"}, nil, false},
		{[]string{"outputs", "-1"}, nil, false},
		{[]string{"name", "x"}, nil, false},
		{[]string{"nope"}, nil, false},
	} {
		got, ok := Lookup(document, tc.path)
		if ok != tc.ok || (ok && !reflect.DeepEqual(got, tc.want)) {
			t.Errorf("Lookup(%v) = %v, %v; want %v, %v", tc.path, got, ok, tc.want, tc.ok)
		}
	}
	if got, ok := Lookup(document, nil); !ok || !reflect.DeepEqual(got, document) {
		t.Errorf("Lookup(empty path) = %v, %v; want the document", got, ok)
	}
}

// 設定の型に、秘密らしい名前のfieldを足したときに、伏せる一覧への追加を忘れないための
// 試験です。文字列のfieldの名前がtoken・password・secret・credential・access keyを含めば、
// secretFieldNamesにあるか、秘密でないと分かる名前（fileのpathなど）でなければなりません。
func TestEverySecretLikeConfigFieldIsMasked(t *testing.T) {
	secretLike := regexp.MustCompile(`(?i)token|password|secret|credential|accesskey|apikey`)
	notSecret := map[string]bool{
		// 認証の種類やfileのpathなど、値が秘密でない名前です。
		"CredentialsProvider": true,
	}
	seen := map[reflect.Type]bool{}
	var walk func(t reflect.Type, path string)
	walk = func(rt reflect.Type, path string) {
		switch rt.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Map:
			walk(rt.Elem(), path)
		case reflect.Struct:
			if seen[rt] {
				return
			}
			seen[rt] = true
			for i := range rt.NumField() {
				field := rt.Field(i)
				if !field.IsExported() {
					continue
				}
				name := path + "." + field.Name
				if field.Type.Kind() == reflect.String && secretLike.MatchString(field.Name) && !secretFieldNames[field.Name] && !notSecret[field.Name] {
					t.Errorf("%s looks like a secret but is not in secretFieldNames", name)
				}
				walk(field.Type, name)
			}
		}
	}
	for _, cfg := range []any{
		pkglogger.LoggerConfig{}, pkglogger.AsyncBestEffortMultiLoggerConfig{}, pkglogger.DeliveryLoggerConfig{}, pkgtelemetry.Config{},
		// interfaceの中の認証の設定です。
		pkgopensearch.BasicAuthConfig{}, pkgopensearch.APIKeyAuthConfig{},
	} {
		walk(reflect.TypeOf(cfg), reflect.TypeOf(cfg).Name())
	}
}

type sourceForTest Configuration

func (s sourceForTest) Configuration() Configuration { return Configuration(s) }

func TestServiceGroupsLoggersAndTelemetries(t *testing.T) {
	service, err := NewService(map[string]Source{
		"app":   sourceForTest{Kind: KindLogger, Value: &sampleConfig{Name: "app", Password: "x"}},
		"audit": sourceForTest{Kind: KindDeliveryLogger, Value: &sampleConfig{Name: "audit"}},
		"tel":   sourceForTest{Kind: KindTelemetry, Value: pkgtelemetry.Config{Traces: pkgtelemetry.TracesConfig{SamplingRatio: 0.5}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(service.Document())
	for _, want := range []string{
		`"loggers":{"app":{"config":{`, `"kind":"logger"`, `"audit":{"config":{`, `"kind":"delivery_logger"`,
		`"telemetries":{"tel":{"config":{`, `"sampling_ratio":0.5`,
	} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("document does not contain %s:\n%s", want, raw)
		}
	}
	if got, ok := Lookup(service.Document(), []string{"loggers", "app", "config", "password"}); !ok || got != Masked {
		t.Errorf("Lookup(password) = %v, %v; want %q", got, ok, Masked)
	}
	if _, err := NewService(map[string]Source{"x": sourceForTest{Kind: "other"}}); !errors.Is(err, ErrUnknownKind) {
		t.Errorf("NewService(unknown kind) error = %v, want ErrUnknownKind", err)
	}
}
