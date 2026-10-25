package cloudwatchlogs

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestNewUsesDefaultTimeoutOnlyWhenTimeoutIsZero(t *testing.T) {
	cfg := Config{
		Region:          "ap-northeast-1",
		Endpoint:        "http://localhost:4566",
		LogGroupName:    "/app/test",
		AccessKeyID:     "access-key",
		SecretAccessKey: "secret-key",
	}
	client, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if got, want := client.httpClient.Timeout, defaultTimeout; got != want {
		t.Fatalf("httpClient.Timeout = %s, want %s", got, want)
	}

	cfg.Timeout = -time.Second
	_, err = New(cfg)
	if !errors.Is(err, ErrNegativeTimeout) {
		t.Fatalf("New() error = %v, want %v", err, ErrNegativeTimeout)
	}
}

func TestSendSendsSignedCloudWatchRequest(t *testing.T) {
	var gotTarget string
	var gotAuth string
	var got putLogEventsRequest

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTarget = r.Header.Get("X-Amz-Target")
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	client, err := New(Config{
		Region:          "ap-northeast-1",
		Endpoint:        server.URL,
		LogGroupName:    "/app/test",
		AccessKeyID:     "access-key",
		SecretAccessKey: "secret-key",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = client.PutLogEvents(context.Background(), PutLogEventsRequest{
		LogStreamName: "stream-a",
		Events: []Event{
			{Timestamp: time.Unix(10, 0), Message: "first"},
			{Timestamp: time.Unix(20, 0), Message: "second"},
		},
		AttemptedAt: time.Unix(30, 0),
	})
	if err != nil {
		t.Fatalf("PutLogEvents() error = %v", err)
	}

	if gotTarget != targetPutLogEvents {
		t.Fatalf("X-Amz-Target = %q, want %q", gotTarget, targetPutLogEvents)
	}
	if gotAuth == "" {
		t.Fatalf("Authorization header is empty")
	}
	if got.LogGroupName != "/app/test" {
		t.Fatalf("logGroupName = %q", got.LogGroupName)
	}
	if got.LogStreamName != "stream-a" {
		t.Fatalf("logStreamName = %q", got.LogStreamName)
	}
	if len(got.LogEvents) != 2 {
		t.Fatalf("event count = %d, want 2", len(got.LogEvents))
	}
	if got.LogEvents[0].Message != "first" || got.LogEvents[1].Message != "second" {
		t.Fatalf("unexpected events: %#v", got.LogEvents)
	}
}

func TestSendUsesCallerResolvedStreamAndAttemptedAt(t *testing.T) {
	var got putLogEventsRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	client, err := New(Config{
		Region:          "ap-northeast-1",
		Endpoint:        server.URL,
		LogGroupName:    "/app/test",
		AccessKeyID:     "access-key",
		SecretAccessKey: "secret-key",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	attemptedAt := time.Date(2026, time.August, 14, 17, 42, 3, 0, time.UTC)

	_, err = client.PutLogEvents(context.Background(), PutLogEventsRequest{
		LogStreamName: "caller-resolved/2026/08/14/17",
		Events:        []Event{{Message: "zero timestamp uses attempted time"}},
		AttemptedAt:   attemptedAt,
	})
	if err != nil {
		t.Fatalf("PutLogEvents() error = %v", err)
	}

	if got.LogStreamName != "caller-resolved/2026/08/14/17" {
		t.Fatalf("logStreamName = %q, want caller-resolved stream", got.LogStreamName)
	}
	if len(got.LogEvents) != 1 || got.LogEvents[0].Timestamp != eventTimestampMillis(attemptedAt) {
		t.Fatalf("logEvents = %#v, want attemptedAt timestamp", got.LogEvents)
	}
}

func TestSendRejectsUnsortedEvents(t *testing.T) {
	client, err := New(Config{
		Region:          "ap-northeast-1",
		Endpoint:        "http://example.com",
		LogGroupName:    "/app/test",
		AccessKeyID:     "access-key",
		SecretAccessKey: "secret-key",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = client.PutLogEvents(context.Background(), PutLogEventsRequest{
		LogStreamName: "stream-a",
		Events: []Event{
			{Timestamp: time.Unix(20, 0), Message: "second"},
			{Timestamp: time.Unix(10, 0), Message: "first"},
		},
		AttemptedAt: time.Unix(30, 0),
	})
	if !errors.Is(err, ErrEventsNotSorted) {
		t.Fatalf("PutLogEvents() error = %v, want %v", err, ErrEventsNotSorted)
	}
}

func TestSendCompressesRequestWhenEnabled(t *testing.T) {
	var gotEncoding string
	var gotAuth string
	var got putLogEventsRequest

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotEncoding = r.Header.Get("Content-Encoding")
		gotAuth = r.Header.Get("Authorization")

		reader, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Fatalf("create gzip reader: %v", err)
		}
		defer reader.Close()

		body, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("read gzipped request body: %v", err)
		}
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("decode request body: %v", err)
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	client, err := New(Config{
		Region:          "ap-northeast-1",
		Endpoint:        server.URL,
		LogGroupName:    "/app/test",
		AccessKeyID:     "access-key",
		SecretAccessKey: "secret-key",
		CompressRequest: true,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = client.PutLogEvents(context.Background(), PutLogEventsRequest{
		LogStreamName: "stream-a",
		Events:        []Event{{Timestamp: time.Unix(10, 0), Message: "compressed"}},
		AttemptedAt:   time.Unix(20, 0),
	})
	if err != nil {
		t.Fatalf("PutLogEvents() error = %v", err)
	}

	if gotEncoding != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", gotEncoding)
	}
	if gotAuth == "" {
		t.Fatalf("Authorization header is empty")
	}
	if got.LogGroupName != "/app/test" || got.LogStreamName != "stream-a" {
		t.Fatalf("unexpected log target: %#v", got)
	}
	if len(got.LogEvents) != 1 || got.LogEvents[0].Message != "compressed" {
		t.Fatalf("unexpected log events: %#v", got.LogEvents)
	}
}

func TestSendUsesCredentialsProviderWhenConfigured(t *testing.T) {
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	provider := &recordingCredentialsProvider{
		credentials: aws.Credentials{
			AccessKeyID:     "provider-access-key",
			SecretAccessKey: "provider-secret-key",
			Source:          "test-provider",
		},
	}
	client, err := New(Config{
		Region:              "ap-northeast-1",
		Endpoint:            server.URL,
		LogGroupName:        "/app/test",
		CredentialsProvider: provider,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = client.PutLogEvents(context.Background(), PutLogEventsRequest{
		LogStreamName: "stream-a",
		Events:        []Event{{Timestamp: time.Unix(10, 0), Message: "provider"}},
		AttemptedAt:   time.Unix(20, 0),
	})
	if err != nil {
		t.Fatalf("PutLogEvents() error = %v", err)
	}

	if provider.calls == 0 {
		t.Fatal("credentials provider was not called")
	}
	if !strings.Contains(gotAuth, "Credential=provider-access-key/") {
		t.Fatalf("Authorization = %q, want provider access key", gotAuth)
	}
}

func TestSendReturnsCredentialsProviderError(t *testing.T) {
	providerErr := errors.New("credentials unavailable")
	serverCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverCalled = true
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(Config{
		Region:              "ap-northeast-1",
		Endpoint:            server.URL,
		LogGroupName:        "/app/test",
		CredentialsProvider: &recordingCredentialsProvider{err: providerErr},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = client.PutLogEvents(context.Background(), PutLogEventsRequest{
		LogStreamName: "stream-a",
		Events:        []Event{{Timestamp: time.Unix(10, 0), Message: "provider error"}},
		AttemptedAt:   time.Unix(20, 0),
	})
	if !errors.Is(err, providerErr) {
		t.Fatalf("PutLogEvents() error = %v, want provider error", err)
	}
	if serverCalled {
		t.Fatal("server was called even though signing credentials failed")
	}
}

func TestSendAutoCreatesCallerResolvedStreamOnResourceNotFound(t *testing.T) {
	var targets []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target := r.Header.Get("X-Amz-Target")
		targets = append(targets, target)
		switch len(targets) {
		case 1:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"__type":"ResourceNotFoundException","message":"stream missing"}`))
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer server.Close()

	client, err := New(Config{
		Region:              "ap-northeast-1",
		Endpoint:            server.URL,
		LogGroupName:        "/app/test",
		AccessKeyID:         "access-key",
		SecretAccessKey:     "secret-key",
		AutoCreateLogStream: true,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = client.PutLogEvents(context.Background(), PutLogEventsRequest{
		LogStreamName: "stream-a",
		Events:        []Event{{Timestamp: time.Unix(10, 0), Message: "hello"}},
		AttemptedAt:   time.Unix(20, 0),
	})
	if err != nil {
		t.Fatalf("PutLogEvents() error = %v", err)
	}

	want := []string{targetPutLogEvents, targetCreateLogStream, targetPutLogEvents}
	if len(targets) != len(want) {
		t.Fatalf("targets = %#v, want %#v", targets, want)
	}
	for i := range want {
		if targets[i] != want[i] {
			t.Fatalf("targets[%d] = %q, want %q", i, targets[i], want[i])
		}
	}
}

func TestPreparePutLogEventsFreezesZeroTimestampBeforeValidation(t *testing.T) {
	now := time.Date(2026, 6, 26, 12, 0, 0, 123_000_000, time.UTC)

	events, err := preparePutLogEvents([]Event{
		{Timestamp: now.Add(-time.Hour), Message: "older"},
		{Message: "zero-1"},
		{Message: "zero-2"},
	}, now)
	if err != nil {
		t.Fatalf("preparePutLogEvents() error = %v", err)
	}

	if len(events) != 3 {
		t.Fatalf("event count = %d, want 3", len(events))
	}
	if events[0].Message != "older" || events[1].Message != "zero-1" || events[2].Message != "zero-2" {
		t.Fatalf("unexpected sorted order: %#v", events)
	}
	wantZeroTimestamp := eventTimestampMillis(now)
	if events[1].TimestampMillis != wantZeroTimestamp || events[2].TimestampMillis != wantZeroTimestamp {
		t.Fatalf("zero timestamps = %d, %d; want both %d", events[1].TimestampMillis, events[2].TimestampMillis, wantZeroTimestamp)
	}
}

func TestPreparePutLogEventsRejectsBatchLimitViolations(t *testing.T) {
	now := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)

	tooManyEvents := make([]Event, MaxPutLogEventsCount+1)
	for i := range tooManyEvents {
		tooManyEvents[i] = Event{Timestamp: now, Message: "x"}
	}

	tests := []struct {
		name    string
		events  []Event
		wantErr error
	}{
		{
			name:    "event count",
			events:  tooManyEvents,
			wantErr: ErrTooManyEvents,
		},
		{
			name: "empty message",
			events: []Event{
				{Timestamp: now, Message: ""},
			},
			wantErr: ErrEventMessageEmpty,
		},
		{
			name: "single event bytes",
			events: []Event{
				{Timestamp: now, Message: strings.Repeat("x", MaxPutLogEventMessageBytes+1)},
			},
			wantErr: &EventTooLargeError{
				Index: 0, MessageBytes: MaxPutLogEventMessageBytes + 1, MaxBytes: MaxPutLogEventMessageBytes,
			},
		},
		{
			name: "batch bytes",
			events: []Event{
				{Timestamp: now, Message: strings.Repeat("x", 600_000)},
				{Timestamp: now.Add(time.Second), Message: strings.Repeat("x", 600_000)},
			},
			wantErr: ErrBatchTooLarge,
		},
		{
			name: "timestamp order",
			events: []Event{
				{Timestamp: now.Add(time.Second), Message: "second"},
				{Timestamp: now, Message: "first"},
			},
			wantErr: ErrEventsNotSorted,
		},
		{
			name: "time span",
			events: []Event{
				{Timestamp: now, Message: "first"},
				{Timestamp: now.Add(24*time.Hour + time.Millisecond), Message: "last"},
			},
			wantErr: ErrBatchTimeSpanTooLong,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			_, err := preparePutLogEvents(tt.events, now)
			if want, ok := tt.wantErr.(*EventTooLargeError); ok {
				var tooLarge *EventTooLargeError
				if !errors.As(err, &tooLarge) || *tooLarge != *want {
					t.Fatalf("preparePutLogEvents() error = %v, want %+v", err, *want)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("preparePutLogEvents() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestPreparePutLogEventsAllowsWhitespaceMessage(t *testing.T) {
	now := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)

	events, err := preparePutLogEvents([]Event{
		{Timestamp: now, Message: " "},
	}, now)
	if err != nil {
		t.Fatalf("preparePutLogEvents() error = %v, want nil", err)
	}
	if len(events) != 1 || events[0].Message != " " {
		t.Fatalf("prepared events = %#v, want whitespace message preserved", events)
	}
}

func TestPreparePutLogEventsDoesNotClassifyCloudWatchTimeWindowRejections(t *testing.T) {
	now := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)

	for _, event := range []Event{
		{Timestamp: now.AddDate(-1, 0, 0), Message: "old event without local cutoff"},
		{Timestamp: now.Add(3 * time.Hour), Message: "future event without local cutoff"},
	} {
		if _, err := preparePutLogEvents([]Event{event}, now); err != nil {
			t.Fatalf("preparePutLogEvents(%q) error = %v, want nil", event.Message, err)
		}
	}
}

func TestRejectedLogEventsInfoRejectedEventsExpandsAWSIndexRanges(t *testing.T) {
	integer := func(value int) *int { return &value }
	tests := []struct {
		name       string
		info       RejectedLogEventsInfo
		eventCount int
		want       []RejectedLogEvent
	}{
		{
			name:       "too old uses exclusive end",
			info:       RejectedLogEventsInfo{TooOldLogEventEndIndex: integer(1)},
			eventCount: 3,
			want: []RejectedLogEvent{
				{Index: 0, Reason: RejectedLogEventReasonTooOld},
			},
		},
		{
			name:       "expired uses exclusive end",
			info:       RejectedLogEventsInfo{ExpiredLogEventEndIndex: integer(1)},
			eventCount: 3,
			want: []RejectedLogEvent{
				{Index: 0, Reason: RejectedLogEventReasonExpired},
			},
		},
		{
			name:       "too new uses inclusive start",
			info:       RejectedLogEventsInfo{TooNewLogEventStartIndex: integer(1)},
			eventCount: 3,
			want: []RejectedLogEvent{
				{Index: 1, Reason: RejectedLogEventReasonTooNew},
				{Index: 2, Reason: RejectedLogEventReasonTooNew},
			},
		},
		{
			name:       "range ends are clamped",
			info:       RejectedLogEventsInfo{TooOldLogEventEndIndex: integer(100)},
			eventCount: 2,
			want: []RejectedLogEvent{
				{Index: 0, Reason: RejectedLogEventReasonTooOld},
				{Index: 1, Reason: RejectedLogEventReasonTooOld},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.info.RejectedEvents(tt.eventCount)
			if len(got) != len(tt.want) {
				t.Fatalf("RejectedEvents(%d) = %#v, want %#v", tt.eventCount, got, tt.want)
			}
			for index := range tt.want {
				if got[index] != tt.want[index] {
					t.Fatalf("RejectedEvents(%d)[%d] = %#v, want %#v", tt.eventCount, index, got[index], tt.want[index])
				}
			}
		})
	}
}

func TestSendDecodesRejectedLogEventsInfo(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"nextSequenceToken":"deprecated-token",
			"rejectedLogEventsInfo":{
				"tooOldLogEventEndIndex":1,
				"tooNewLogEventStartIndex":2
			},
			"rejectedEntityInfo":{
				"errorType":"InvalidEntity"
			}
		}`))
	}))
	defer server.Close()

	client, err := New(Config{
		Region:          "ap-northeast-1",
		Endpoint:        server.URL,
		LogGroupName:    "/app/test",
		AccessKeyID:     "access-key",
		SecretAccessKey: "secret-key",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	output, err := client.PutLogEvents(context.Background(), PutLogEventsRequest{
		LogStreamName: "stream-a",
		Events: []Event{
			{Timestamp: time.Unix(10, 0), Message: "too old"},
			{Timestamp: time.Unix(20, 0), Message: "accepted"},
			{Timestamp: time.Unix(30, 0), Message: "too new"},
		},
		AttemptedAt: time.Unix(40, 0),
	})
	if err != nil {
		t.Fatalf("PutLogEvents() error = %v", err)
	}

	if output.NextSequenceToken != "deprecated-token" {
		t.Fatalf("NextSequenceToken = %q", output.NextSequenceToken)
	}
	if !output.HasRejectedLogEvents() {
		t.Fatalf("expected rejected log events")
	}
	if output.RejectedEntityInfo == nil || output.RejectedEntityInfo.ErrorType != "InvalidEntity" {
		t.Fatalf("RejectedEntityInfo = %#v", output.RejectedEntityInfo)
	}

	rejected := output.RejectedLogEventsInfo.RejectedEvents(3)
	if len(rejected) != 2 {
		t.Fatalf("rejected event count = %d, want 2: %#v", len(rejected), rejected)
	}
	if rejected[0].Index != 0 || rejected[0].Reason != RejectedLogEventReasonTooOld || rejected[0].Retryable() {
		t.Fatalf("unexpected first rejection: %#v", rejected[0])
	}
	if rejected[1].Index != 2 || rejected[1].Reason != RejectedLogEventReasonTooNew || !rejected[1].Retryable() {
		t.Fatalf("unexpected second rejection: %#v", rejected[1])
	}
}

type recordingCredentialsProvider struct {
	credentials aws.Credentials
	err         error
	calls       int
}

func (p *recordingCredentialsProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	_ = ctx
	p.calls++
	if p.err != nil {
		return aws.Credentials{}, p.err
	}
	return p.credentials, nil
}

func TestNormalizeAWSErrorTypeUsesFirstSeparators(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "ThrottlingException", want: "ThrottlingException"},
		{in: "ThrottlingException:http://internal.example/", want: "ThrottlingException"},
		{in: "com.amazonaws.logs#ThrottlingException", want: "ThrottlingException"},
		{in: "com.amazonaws.logs#ThrottlingException:http://internal.example/", want: "ThrottlingException"},
		{in: "  com.amazonaws.logs#ThrottlingException:http://internal.example/  ", want: "ThrottlingException"},
		{in: "com.amazonaws.logs#", want: ""},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.in, func(t *testing.T) {
			if got := normalizeAWSErrorType(tt.in); got != tt.want {
				t.Fatalf("normalizeAWSErrorType(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestNewHTTPStatusErrorUsesAWSErrorTypePriority(t *testing.T) {
	tests := []struct {
		name       string
		headerType string
		body       string
		wantType   string
		wantMsg    string
	}{
		{
			name:       "header wins over body code and type",
			headerType: "HeaderThrottle:http://internal.example/",
			body:       `{"code":"BodyCode","__type":"BodyType","message":"from body"}`,
			wantType:   "HeaderThrottle",
			wantMsg:    "from body",
		},
		{
			name:     "body code wins over type",
			body:     `{"code":"com.amazonaws.logs#CodeThrottle:http://internal.example/","__type":"BodyType","message":"from code"}`,
			wantType: "CodeThrottle",
			wantMsg:  "from code",
		},
		{
			name:     "body type is used when header and code are empty",
			body:     `{"__type":"com.amazonaws.logs#TypeThrottle:http://internal.example/","message":"from type"}`,
			wantType: "TypeThrottle",
			wantMsg:  "from type",
		},
		{
			name:       "header is used even when body is not json",
			headerType: "HeaderOnlyException:http://internal.example/",
			body:       `not-json`,
			wantType:   "HeaderOnlyException",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			resp := &http.Response{
				StatusCode: http.StatusBadRequest,
				Status:     "400 Bad Request",
				Header:     make(http.Header),
			}
			if tt.headerType != "" {
				resp.Header.Set("X-Amzn-Errortype", tt.headerType)
			}

			statusErr := newHTTPStatusError(resp, []byte(tt.body), false)
			if got := statusErr.ErrorType(); got != tt.wantType {
				t.Fatalf("ErrorType() = %q, want %q", got, tt.wantType)
			}
			if statusErr.AwsMessage != tt.wantMsg {
				t.Fatalf("AwsMessage = %q, want %q", statusErr.AwsMessage, tt.wantMsg)
			}
		})
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

// RejectedLogEventErrorのmessageは、拒否eventの位置と理由をfailure recordの理由として
// 保存する表示形式です。errors.Asで取り出したfieldも確認します。
func TestRejectedLogEventErrorReportsIndexAndReason(t *testing.T) {
	var err error = &RejectedLogEventError{Index: 3, Reason: RejectedLogEventReasonTooOld}
	var rejected *RejectedLogEventError
	if !errors.As(err, &rejected) || rejected.Index != 3 || rejected.Reason != RejectedLogEventReasonTooOld {
		t.Fatalf("errors.As() = %+v, want index 3 too_old", rejected)
	}
	// 表示形式は保存されるfailure reasonの契約なので、文言そのものを確認します。
	if got, want := err.Error(), "cloudwatch logs rejected log event at index 3: too_old"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}
