package retryable

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestClassifyRetryFromResponseSaturatesUnrepresentableRetryAfter(t *testing.T) {
	t.Parallel()

	const maxDelay = time.Duration(1<<63 - 1)
	tests := []struct {
		name       string
		retryAfter string
		want       time.Duration
	}{
		// time.Durationで表せる最大の秒数は、そのまま待機時間にします。
		{name: "largest representable seconds", retryAfter: "9223372036", want: 9223372036 * time.Second},
		// 表せない秒数とint64を超える桁数は、即時再送ではなく最大値へ飽和させます。
		{name: "unrepresentable seconds", retryAfter: "9223372037", want: maxDelay},
		{name: "seconds beyond int64", retryAfter: "99999999999999999999", want: maxDelay},
		{name: "far-future HTTP-date", retryAfter: "Fri, 31 Dec 9999 23:59:59 GMT", want: maxDelay},
		{name: "past HTTP-date", retryAfter: "Mon, 01 Jan 2001 00:00:00 GMT", want: 0},
		{name: "invalid", retryAfter: "soon", want: 0},
		{name: "negative seconds", retryAfter: "-5", want: 0},
		{name: "fractional seconds", retryAfter: "1.5", want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			resp := &http.Response{StatusCode: http.StatusServiceUnavailable, Header: http.Header{"Retry-After": []string{tt.retryAfter}}}
			want := RetryDecision{Retry: true, Reason: RetryReasonHTTPStatus, Code: http.StatusServiceUnavailable, RetryAfter: tt.want}
			if got := ClassifyRetryFromResponse(resp); got != want {
				t.Fatalf("ClassifyRetryFromResponse(Retry-After %q) = %+v, want %+v", tt.retryAfter, got, want)
			}
		})
	}
}

// 飽和したRetry-Afterにも、呼び出し側の待機上限を適用します。
func TestPolicyDelayCapsSaturatedRetryAfter(t *testing.T) {
	t.Parallel()

	resp := &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Retry-After": []string{"99999999999999999999"}}}
	decision := ClassifyRetryFromResponse(resp)
	delay, ok := PolicyDelay(context.Background(), 1, time.Millisecond, 5*time.Second, decision)
	if !ok || delay != 5*time.Second {
		t.Fatalf("PolicyDelay() = %s, %t, want capped delay %s", delay, ok, 5*time.Second)
	}
}
