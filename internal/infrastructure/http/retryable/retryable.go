package retryable

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// 一時的なエラー（通信失敗・タイムアウト・HTTP 5xx/429 など）に対して、
// 再試行すべきかどうかを判定するパッケージ。
// ※レスポンス本文（BulkResponse/items 等）の中身は解析しません。
// _id　createの場合は先に_idを指定して作成することを想定しています。そのため409エラーはスルーしています。
type RetryReason string

const (
	RetryReasonNone               RetryReason = ""
	RetryReasonHTTPStatus         RetryReason = "http_status"
	RetryReasonHTTPThrottle       RetryReason = "http_throttling"
	RetryReasonNetTimeout         RetryReason = "net_timeout"
	RetryReasonEOF                RetryReason = "io_eof"
	RetryReasonCtxDeadline        RetryReason = "ctx_deadline_exceeded"
	RetryReasonDisconnect         RetryReason = "transient_disconnect"
	RetryReasonServiceTransient   RetryReason = "service_transient"
	RetryReasonCircuitBreakerOpen RetryReason = "circuit_breaker_open"
	// RetryReasonRequestNotSentには、任意のcontext errorではなく、操作がHTTPの送信前に
	// 失敗したことを示す明示的な証拠が必要です。
	RetryReasonRequestNotSent RetryReason = "request_not_sent"
)

type RetryDecision struct {
	Retry bool
	// Quarantineは、replayの前に確認が必要な、不確かなdurableの結果を表します。
	// Retryとは相互に排他的です。
	Quarantine bool
	Reason     RetryReason
	Code       int           // HTTP status など。なければ 0
	RetryAfter time.Duration // 取れたら入れる（任意）
}

// HTTPStatusProvider は「HTTPステータスとRetry-After相当」を取り出せる error 用のインタフェース。
// 例: opensearch.HTTPStatusError がこれを実装する。
type HTTPStatusProvider interface {
	StatusCode() int
	RetryAfter() time.Duration // 不明なら 0 を返す
}

// ClassifyRetry は ctx と error だけから再試行可否を判定します。
// 1) ctx都合（キャンセル/期限）
// 2) 通信系エラー（timeout/EOF/切断など）
// 3) err が HTTP status を持つなら status による判定（429/5xx/408 等）
// それ以外は retry しません。
func ClassifyRetry(ctx context.Context, err error) RetryDecision {
	// 0) ctx優先
	if ctx != nil {
		switch ctx.Err() {
		case context.Canceled:
			return RetryDecision{Retry: false}
		case context.DeadlineExceeded:
			return RetryDecision{Retry: false, Reason: RetryReasonCtxDeadline}
		}
	}

	// err が無ければ retry不要
	if err == nil {
		return RetryDecision{Retry: false}
	}

	// HTTP statusが判明していれば、本文やラッパーの文字列から通信障害を推測しない。
	// 判定を元のerror chainから保持し、url.Errorをunwrapした後も引き継ぐ。
	var hp HTTPStatusProvider
	hasHTTPStatus := errors.As(err, &hp)

	// 1) context、型付き通信error、circuit breakerは従来どおり優先する。
	if dec := classifyByError(err, !hasHTTPStatus); dec.Retry || dec.Reason != RetryReasonNone || errors.Is(err, context.Canceled) {
		return dec
	}

	// 2) HTTP status を持つ error なら status で判定
	if hasHTTPStatus {
		return ClassifyRetryByStatus(hp.StatusCode(), hp.RetryAfter())
	}

	return RetryDecision{Retry: false}
}

// ClassifyRetryByStatus は HTTP ステータスコードだけで再試行可否を判定します。
// retryAfter は Retry-After 相当の待ち時間ヒント（不明なら 0）。
func ClassifyRetryByStatus(code int, retryAfter time.Duration) RetryDecision {
	if code == http.StatusTooManyRequests {
		return RetryDecision{Retry: true, Reason: RetryReasonHTTPThrottle, Code: code, RetryAfter: retryAfter}
	}
	if isRetryableHTTPStatus(code) {
		return RetryDecision{Retry: true, Reason: RetryReasonHTTPStatus, Code: code, RetryAfter: retryAfter}
	}
	return RetryDecision{Retry: false, Reason: RetryReasonHTTPStatus, Code: code}
}

// ClassifyRetryFromResponse は *http.Response がある場合に直接判定したい時用（任意）。
func ClassifyRetryFromResponse(resp *http.Response) RetryDecision {
	if resp == nil {
		return RetryDecision{Retry: false}
	}
	code := resp.StatusCode
	after := parseRetryAfter(resp)

	return ClassifyRetryByStatus(code, after)
}

func classifyByError(err error, allowDisconnectMessage bool) RetryDecision {
	var circuitBreakerErr *CircuitBreakerOpenError
	if errors.As(err, &circuitBreakerErr) {
		return RetryDecision{Retry: false, Reason: RetryReasonCircuitBreakerOpen}
	}

	// err 内に混ざる context
	if errors.Is(err, context.Canceled) {
		return RetryDecision{Retry: false}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return RetryDecision{Retry: true, Reason: RetryReasonCtxDeadline}
	}

	// url.Errorをunwrapします
	var ue *url.Error
	if errors.As(err, &ue) && ue != nil && ue.Err != nil {
		return classifyByError(ue.Err, allowDisconnectMessage)
	}

	// netのtimeout
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return RetryDecision{Retry: true, Reason: RetryReasonNetTimeout}
	}

	// 一時的なdnsエラーであれば拾う
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && (dnsErr.IsTimeout || dnsErr.Temporary()) {
		return RetryDecision{Retry: true, Reason: RetryReasonNetTimeout}
	}

	// EOF
	// EOFまたはunexpected EOF
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return RetryDecision{Retry: true, Reason: RetryReasonEOF}
	}

	// 接続の切断・拒否は、OSごとに異なる文言ではなくerror番号で判定します。
	if isTransientDisconnectErrno(err) {
		return RetryDecision{Retry: true, Reason: RetryReasonDisconnect}
	}

	// 切断らしいmessage
	if allowDisconnectMessage && looksLikeTransientDisconnect(err.Error()) {
		return RetryDecision{Retry: true, Reason: RetryReasonDisconnect}
	}

	return RetryDecision{Retry: false}
}

func isRetryableHTTPStatus(code int) bool {
	switch code {
	case http.StatusRequestTimeout: // 408
		return true
	case http.StatusTooManyRequests: // 429
		return true
	case http.StatusInternalServerError: // 500
		return true
	case http.StatusBadGateway: // 502
		return true
	case http.StatusServiceUnavailable: // 503
		return true
	case http.StatusGatewayTimeout: // 504
		return true
	default:
		return false
	}
}

func looksLikeTransientDisconnect(msg string) bool {
	s := strings.ToLower(msg)
	switch {
	case strings.Contains(s, "connection reset by peer"):
		return true
	case strings.Contains(s, "broken pipe"):
		return true
	case strings.Contains(s, "connection refused"):
		return true
	case strings.Contains(s, "use of closed network connection"):
		return true
	case strings.Contains(s, "tls handshake timeout"):
		return true
	case strings.Contains(s, "i/o timeout"):
		return true
	case strings.Contains(s, "unexpected eof"):
		return true
	case strings.Contains(s, "http2: client connection lost"):
		return true
	default:
		return false
	}
}
func parseRetryAfter(resp *http.Response) time.Duration {
	v := strings.TrimSpace(resp.Header.Get("Retry-After"))
	if v == "" {
		return 0
	}

	// 秒数
	if secs, ok := parseRetryAfterSeconds(v); ok {
		// time.Durationで表せない秒数は最大値へ飽和させ、呼び出し側の待機上限を適用させます。
		// 0にすると、serverが長い待機を求めたのに即時再送になります。HTTP-dateも同じく飽和します。
		const maxDelay = time.Duration(1<<63 - 1)
		if secs > int64(maxDelay/time.Second) {
			return maxDelay
		}
		return time.Duration(secs) * time.Second
	}

	// HTTP-date: time.Untilは表せない期間を最大値へ飽和させます。
	if t, err := http.ParseTime(v); err == nil {
		d := time.Until(t)
		if d < 0 {
			return 0
		}
		return d
	}

	return 0
}

// parseRetryAfterSecondsはRetry-Afterのdelay-secondsを解析します。int64を超える桁数の
// 値も有効なdelay-secondsなので、最大値へ飽和させます。数字以外を含む値は解析しません。
func parseRetryAfterSeconds(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}

	const maxInt64 = int64(^uint64(0) >> 1)

	var n int64
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, false
		}

		d := int64(c - '0')
		if n > (maxInt64-d)/10 {
			n = maxInt64
			continue
		}

		n = n*10 + d
	}
	return n, true
}
