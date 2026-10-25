package loki

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type receivedPush struct {
	Streams []struct {
		Stream map[string]string `json:"stream"`
		Values [][]string        `json:"values"`
	} `json:"streams"`
}

func TestNewUsesDefaultTimeoutOnlyWhenTimeoutIsZero(t *testing.T) {
	client, err := New(Config{Endpoint: "http://localhost:3100"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if got, want := client.httpClient.Timeout, defaultTimeout; got != want {
		t.Fatalf("httpClient.Timeout = %s, want %s", got, want)
	}

	_, err = New(Config{Endpoint: "http://localhost:3100", Timeout: -time.Second})
	if !errors.Is(err, ErrNegativeTimeout) {
		t.Fatalf("New() error = %v, want %v", err, ErrNegativeTimeout)
	}
}

func TestPushSendsLokiJSON(t *testing.T) {
	first := time.Unix(1_700_000_000, 123)
	second := time.Unix(1_700_000_001, 456)

	var received receivedPush
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected method POST, got %s", r.Method)
		}
		if r.URL.Path != pushPath {
			t.Errorf("expected path %s, got %s", pushPath, r.URL.Path)
		}
		if contentType := r.Header.Get("Content-Type"); contentType != "application/json" {
			t.Errorf("expected Content-Type application/json, got %s", contentType)
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read request body: %v", err)
		}
		if err := json.Unmarshal(body, &received); err != nil {
			t.Errorf("failed to unmarshal request body: %v", err)
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.Push(context.Background(), []Stream{{
		Labels: map[string]string{
			"app":   "service",
			"level": "info",
		},
		Entries: []Entry{
			{Timestamp: first, Line: "first log"},
			{Timestamp: second, Line: "second log"},
		},
	}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(received.Streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(received.Streams))
	}
	stream := received.Streams[0]
	if stream.Stream["app"] != "service" {
		t.Errorf("expected app label service, got %s", stream.Stream["app"])
	}
	if stream.Stream["level"] != "info" {
		t.Errorf("expected level label info, got %s", stream.Stream["level"])
	}
	if len(stream.Values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(stream.Values))
	}

	assertValue(t, stream.Values[0], first, "first log")
	assertValue(t, stream.Values[1], second, "second log")
}

func TestPushCompressesRequestWhenEnabled(t *testing.T) {
	var received receivedPush
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if encoding := r.Header.Get("Content-Encoding"); encoding != "gzip" {
			t.Fatalf("expected Content-Encoding gzip, got %s", encoding)
		}

		reader, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Fatalf("failed to create gzip reader: %v", err)
		}
		defer reader.Close()

		body, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("failed to read gzipped request body: %v", err)
		}
		if err := json.Unmarshal(body, &received); err != nil {
			t.Fatalf("failed to unmarshal request body: %v", err)
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := New(Config{
		Endpoint:        server.URL,
		CompressRequest: true,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.PushEntry(context.Background(), map[string]string{"appName": "service"}, Entry{
		Timestamp: time.Unix(1_700_000_000, 0),
		Line:      "compressed log",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(received.Streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(received.Streams))
	}
	if got := received.Streams[0].Stream["appName"]; got != "service" {
		t.Fatalf("expected appName label service, got %s", got)
	}
}

func TestPushUsesFullEndpointAndHeaders(t *testing.T) {
	basicAuth := &BasicAuthConfig{
		Username: "user",
		Password: "pass",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != pushPath {
			t.Errorf("expected path %s, got %s", pushPath, r.URL.Path)
		}
		if tenant := r.Header.Get(tenantHeader); tenant != "tenant-a" {
			t.Errorf("expected tenant tenant-a, got %s", tenant)
		}
		if requestID := r.Header.Get("X-Request-ID"); requestID != "request-1" {
			t.Errorf("expected X-Request-ID request-1, got %s", requestID)
		}
		username, password, ok := r.BasicAuth()
		if !ok || username != "user" || password != "pass" {
			t.Errorf("unexpected basic auth: ok=%v username=%s password=%s", ok, username, password)
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := New(Config{
		Endpoint:  server.URL + pushPath,
		TenantID:  "tenant-a",
		BasicAuth: basicAuth,
		Headers: map[string]string{
			"X-Request-ID": "request-1",
		},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	basicAuth.Username = "changed"
	basicAuth.Password = "changed"

	err = client.PushEntry(context.Background(), map[string]string{"app": "service"}, Entry{
		Timestamp: time.Unix(1_700_000_000, 0),
		Line:      "hello",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPushKeepsManagedHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if contentType := r.Header.Get("Content-Type"); contentType != "application/json" {
			t.Errorf("expected Content-Type application/json, got %s", contentType)
		}
		if tenant := r.Header.Get(tenantHeader); tenant != "tenant-a" {
			t.Errorf("expected tenant tenant-a, got %s", tenant)
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := New(Config{
		Endpoint: server.URL,
		TenantID: "tenant-a",
		Headers: map[string]string{
			"Content-Type":  "text/plain",
			"X-Scope-OrgID": "other-tenant",
		},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.PushEntry(context.Background(), map[string]string{"app": "service"}, Entry{
		Timestamp: time.Unix(1_700_000_000, 0),
		Line:      "hello",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewAcceptsValidTenantID(t *testing.T) {
	client, err := New(Config{
		Endpoint: "http://localhost:3100",
		TenantID: "tenant-01_ABC.!*'()",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client.tenantID != "tenant-01_ABC.!*'()" {
		t.Fatalf("expected tenant id to be stored, got %q", client.tenantID)
	}
}

func TestNewRejectsEndpointWithUnexpectedPath(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	_, err := New(Config{Endpoint: server.URL + "/gateway/"})
	if !errors.Is(err, ErrInvalidEndpoint) {
		t.Fatalf("expected endpoint path error %v, got %v", ErrInvalidEndpoint, err)
	}
}

func TestPushTreatsIngestionBlockedAsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "ingestion blocked", statusIngestionBlocked)
	}))
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.PushEntry(context.Background(), map[string]string{"app": "service"}, Entry{
		Timestamp: time.Unix(1_700_000_000, 0),
		Line:      "hello",
	})
	if err == nil {
		t.Fatal("expected error")
	}

	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected HTTPStatusError, got %T", err)
	}
	if statusErr.Code != statusIngestionBlocked {
		t.Errorf("expected status code %d, got %d", statusIngestionBlocked, statusErr.Code)
	}
	if !strings.Contains(string(statusErr.Body), "ingestion blocked") {
		t.Errorf("expected response body to contain ingestion blocked, got %q", string(statusErr.Body))
	}
}

func TestIsIngestionBlockedStatus(t *testing.T) {
	tests := []struct {
		name string
		code int
		want bool
	}{
		{
			name: "ingestion blocked status",
			code: statusIngestionBlocked,
			want: true,
		},
		{
			name: "bad request is not ingestion blocked",
			code: http.StatusBadRequest,
			want: false,
		},
		{
			name: "service unavailable is not ingestion blocked",
			code: http.StatusServiceUnavailable,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsIngestionBlockedStatus(tt.code); got != tt.want {
				t.Fatalf("IsIngestionBlockedStatus(%d) = %v, want %v", tt.code, got, tt.want)
			}
		})
	}
}

func TestHTTPStatusErrorIsIngestionBlocked(t *testing.T) {
	err := &HTTPStatusError{
		Code:   statusIngestionBlocked,
		Status: "260 Ingestion Blocked",
	}

	if !err.IsIngestionBlocked() {
		t.Fatal("expected HTTP status error to be ingestion blocked")
	}
}

func TestHTTPStatusErrorErrorLimitsBody(t *testing.T) {
	body := append([]byte{0x00, 0x01}, bytes.Repeat([]byte("a"), maxHTTPStatusErrorBodyBytes+100)...)

	msg := (&HTTPStatusError{
		Code:   http.StatusInternalServerError,
		Status: "500 Internal Server Error",
		Body:   body,
	}).Error()

	if !strings.Contains(msg, `\x00\x01`) {
		t.Fatalf("expected binary bytes to be escaped, got %s", msg)
	}
	if !strings.Contains(msg, "body truncated") {
		t.Fatalf("expected truncated marker, got %s", msg)
	}
	if len(msg) > maxHTTPStatusErrorBodyBytes+300 {
		t.Fatalf("expected bounded error message length, got %d", len(msg))
	}

	// 読み取り時に上限で切り詰めたbodyは上限長ちょうどなので、markerはBodyTruncatedから付けます。
	// Error()はoperatorが読む表示形式の契約であるため、ここでは文字列で確認します。
	readLimited := (&HTTPStatusError{
		Code:          http.StatusInternalServerError,
		Status:        "500 Internal Server Error",
		Body:          bytes.Repeat([]byte("x"), maxHTTPStatusErrorBodyBytes),
		BodyTruncated: true,
	}).Error()
	if !strings.HasSuffix(readLimited, truncatedHTTPStatusErrorMessage) {
		t.Fatalf("expected truncated marker for BodyTruncated, got %s", readLimited)
	}
}

func TestPushLimitsHTTPStatusErrorBodyRead(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write(bytes.Repeat([]byte("x"), maxHTTPStatusErrorBodyBytes+512))
	}))
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.PushEntry(context.Background(), map[string]string{"app": "service"}, Entry{
		Timestamp: time.Unix(1_700_000_000, 0),
		Line:      "hello",
	})
	if err == nil {
		t.Fatal("expected error")
	}

	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected HTTPStatusError, got %T", err)
	}
	if got := len(statusErr.Body); got != maxHTTPStatusErrorBodyBytes {
		t.Fatalf("expected stored body length %d, got %d", maxHTTPStatusErrorBodyBytes, got)
	}
	if !statusErr.BodyTruncated {
		t.Fatal("expected BodyTruncated to be true")
	}
}

func TestPushAcceptsOrdinary2xxStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.PushEntry(context.Background(), map[string]string{"app": "service"}, Entry{
		Timestamp: time.Unix(1_700_000_000, 0),
		Line:      "hello",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPushReturnsHTTPStatusError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "2")
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.PushEntry(context.Background(), map[string]string{"app": "service"}, Entry{
		Timestamp: time.Unix(1_700_000_000, 0),
		Line:      "hello",
	})
	if err == nil {
		t.Fatal("expected error")
	}

	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected HTTPStatusError, got %T", err)
	}
	if statusErr.Code != http.StatusTooManyRequests {
		t.Errorf("expected status code 429, got %d", statusErr.Code)
	}
	if !strings.Contains(string(statusErr.Body), "rate limited") {
		t.Errorf("expected response body to contain rate limited, got %q", string(statusErr.Body))
	}
	if got, want := statusErr.RetryAfter(), 2*time.Second; got != want {
		t.Errorf("RetryAfter() = %s, want %s", got, want)
	}
}

// time.Durationで表せないRetry-Afterは、HTTP-dateと同じく最大値へ飽和させ、呼び出し側の
// 待機上限を適用させます。0にすると、長い待機を求めたserverへ即時再送してしまいます。
func TestParseRetryAfterSaturatesDurationOverflow(t *testing.T) {
	const maxDelay = time.Duration(1<<63 - 1)
	for _, tt := range []struct {
		value string
		want  time.Duration
	}{
		{value: "9223372036", want: 9223372036 * time.Second},
		{value: "9223372037", want: maxDelay},
		{value: "99999999999999999999", want: maxDelay},
		{value: "Fri, 31 Dec 9999 23:59:59 GMT", want: maxDelay},
		{value: "12a", want: 0},
		{value: "-1", want: 0},
	} {
		resp := &http.Response{Header: http.Header{"Retry-After": []string{tt.value}}}
		if got := parseRetryAfter(resp); got != tt.want {
			t.Errorf("parseRetryAfter(%q) = %s, want %s", tt.value, got, tt.want)
		}
	}
}

func TestPushValidatesInputBeforeSending(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	tests := []struct {
		name    string
		streams []Stream
		want    error
		// wantInvalidNameは、wantがErrInvalidLabelNameのときに報告されるlabel名です。
		wantInvalidName string
	}{
		{
			name: "ラベルなし",
			streams: []Stream{{
				Entries: []Entry{{Timestamp: time.Unix(1_700_000_000, 0), Line: "hello"}},
			}},
			want: ErrLabelsRequired,
		},
		{
			name: "不正なラベル名",
			streams: []Stream{{
				Labels:  map[string]string{"bad-label": "value"},
				Entries: []Entry{{Timestamp: time.Unix(1_700_000_000, 0), Line: "hello"}},
			}},
			want:            ErrInvalidLabelName,
			wantInvalidName: "bad-label",
		},
		{
			name: "エントリなし",
			streams: []Stream{{
				Labels: map[string]string{"app": "service"},
			}},
			want: ErrStreamWithoutEntries,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := client.Push(context.Background(), tt.streams)
			if !errors.Is(err, tt.want) {
				t.Fatalf("expected %v, got %v", tt.want, err)
			}
			if tt.wantInvalidName == "" {
				return
			}
			var nameErr *InvalidLabelNameError
			if !errors.As(err, &nameErr) || nameErr.Name != tt.wantInvalidName {
				t.Fatalf("expected InvalidLabelNameError{Name: %q}, got %v", tt.wantInvalidName, err)
			}
		})
	}

	if requestCount != 0 {
		t.Fatalf("expected no requests for invalid input, got %d", requestCount)
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want error
	}{
		{
			name: "endpointなし",
			cfg:  Config{},
			want: ErrEndpointRequired,
		},
		{
			name: "schemeなし",
			cfg:  Config{Endpoint: "localhost:3100"},
			want: ErrInvalidEndpoint,
		},
		{
			// schemeはhttpなので、host欠落の検査だけが拒否理由になります。
			name: "hostなし",
			cfg:  Config{Endpoint: "http://"},
			want: ErrInvalidEndpoint,
		},
		{
			name: "http/https以外のscheme",
			cfg:  Config{Endpoint: "ftp://localhost:3100"},
			want: ErrInvalidEndpoint,
		},
		{
			name: "URL内userinfo",
			cfg:  Config{Endpoint: "https://user:pass@example.com/loki/api/v1/push"},
			want: ErrInvalidEndpoint,
		},
		{
			name: "空query",
			cfg:  Config{Endpoint: "http://localhost:3100?"},
			want: ErrInvalidEndpoint,
		},
		{
			name: "basicとbearer併用",
			cfg: Config{
				Endpoint:    "http://localhost:3100",
				BasicAuth:   &BasicAuthConfig{Username: "user", Password: "pass"},
				BearerToken: "token",
			},
			want: ErrConflictingAuth,
		},
		{
			name: "push以外のpath",
			cfg:  Config{Endpoint: "http://localhost:3100/gateway"},
			want: ErrInvalidEndpoint,
		},
		{
			name: "tenantが長すぎる",
			cfg: Config{
				Endpoint: "http://localhost:3100",
				TenantID: strings.Repeat("a", maxTenantIDBytes+1),
			},
			want: ErrInvalidTenantID,
		},
		{
			name: "tenantが単一ピリオド",
			cfg: Config{
				Endpoint: "http://localhost:3100",
				TenantID: ".",
			},
			want: ErrInvalidTenantID,
		},
		{
			name: "tenantが二重ピリオド",
			cfg: Config{
				Endpoint: "http://localhost:3100",
				TenantID: "..",
			},
			want: ErrInvalidTenantID,
		},
		{
			name: "tenantにpipe",
			cfg: Config{
				Endpoint: "http://localhost:3100",
				TenantID: "tenant-a|tenant-b",
			},
			want: ErrInvalidTenantID,
		},
		{
			name: "tenantにslash",
			cfg: Config{
				Endpoint: "http://localhost:3100",
				TenantID: "tenant/a",
			},
			want: ErrInvalidTenantID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.cfg)
			if !errors.Is(err, tt.want) {
				t.Fatalf("expected %v, got %v", tt.want, err)
			}
		})
	}
}

func TestPushRequestSizeMatchesCanonicalJSONBody(t *testing.T) {
	t.Parallel()

	streams := []Stream{{
		Labels: map[string]string{"app": "test", "level": "info"},
		Entries: []Entry{{
			Timestamp: time.Date(2026, 8, 15, 12, 0, 0, 123, time.UTC),
			Line:      "line with \"escaped\" text",
		}},
	}}
	wantBody := `{"streams":[{"stream":{"app":"test","level":"info"},` +
		`"values":[["1786795200000000123","line with \"escaped\" text"]]}]}`

	for _, compress := range []bool{false, true} {
		body := sentPushRequestBody(t, compress, streams)
		if got := string(body); got != wantBody {
			t.Fatalf("compress=%t Push() sent %q, want %q", compress, got, wantBody)
		}
		size, err := PushRequestSize(streams)
		if err != nil {
			t.Fatalf("PushRequestSize() error = %v", err)
		}
		// PushRequestSizeは圧縮の有無にかかわらず、gzip圧縮前の送信bodyを数えます。
		if size != len(body) {
			t.Fatalf("compress=%t PushRequestSize() = %d, sent body length = %d", compress, size, len(body))
		}
	}
}

// sentPushRequestBodyは、PushがHTTP serverへ実際に送信したbodyを返します。
// 圧縮送信ではContent-Encodingを確認し、PushRequestSizeと比べられるよう展開します。
func sentPushRequestBody(t *testing.T, compress bool, streams []Stream) []byte {
	t.Helper()

	type sentRequest struct {
		body            []byte
		contentEncoding string
		readErr         error
	}
	requests := make(chan sentRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		requests <- sentRequest{body: body, contentEncoding: r.Header.Get("Content-Encoding"), readErr: err}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL, CompressRequest: compress})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := client.Push(context.Background(), streams); err != nil {
		t.Fatalf("Push() error = %v", err)
	}

	var sent sentRequest
	select {
	case sent = <-requests:
	default:
		t.Fatal("Push() returned without sending a request")
	}
	if sent.readErr != nil {
		t.Fatalf("read push request body: %v", sent.readErr)
	}
	if !compress {
		if sent.contentEncoding != "" {
			t.Fatalf("Content-Encoding = %q, want uncompressed request", sent.contentEncoding)
		}
		return sent.body
	}
	if sent.contentEncoding != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", sent.contentEncoding)
	}
	reader, err := gzip.NewReader(bytes.NewReader(sent.body))
	if err != nil {
		t.Fatalf("open gzip push body: %v", err)
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read gzip push body: %v", err)
	}
	return body
}

func assertValue(t *testing.T, got []string, wantTime time.Time, wantLine string) {
	t.Helper()

	if len(got) != 2 {
		t.Fatalf("expected loki value to contain timestamp and line, got %v", got)
	}
	if got[0] != fmt.Sprintf("%d", wantTime.UnixNano()) {
		t.Errorf("expected timestamp %d, got %s", wantTime.UnixNano(), got[0])
	}
	if got[1] != wantLine {
		t.Errorf("expected line %q, got %q", wantLine, got[1])
	}
}
