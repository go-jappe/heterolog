// Package otlphttpは、spanやmetricsをOTLP/HTTPのprotobufで受信側（OpenTelemetry
// Collector、Grafana Tempoなど）へ送る送り先です。一時的な失敗は、OTLPの仕様
// （https://opentelemetry.io/docs/specs/otlp/）の規則で送り直します。送るものの種類に
// よる違いは、Signalで渡します。
package otlphttp

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/circuitbreaker"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// ExporterNameは、Signal.Nameを指定しない送り先の、metricsのexporter属性の値です。
const ExporterName = "otlp"

// protobufContentTypeは、Signal.ContentTypeを指定しない送り先の、要求の本文の種類です。
const protobufContentType = "application/x-protobuf"

// maxResponseBytesは、読む応答の本文の上限です。partial_successを読むためだけに読みます。
const maxResponseBytes = 64 << 10

// Configは、OTLP/HTTPの送り先の設定です。値の範囲は、pkg/telemetryの検証で確かめます。
type Config struct {
	Endpoint string
	Headers  map[string]string
	// Compressionは、gzipかnoneです。
	Compression    string
	Timeout        time.Duration
	Retries        int
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	// CircuitBreakerは、続けて失敗した後、しばらく要求を送らずに失敗させます。
	CircuitBreaker bool
}

// Signalは、送るものの種類（spanやmetric）による違いです。
type Signal[T any] struct {
	// Encodeは、itemsを1回の要求の本文（protobuf）にします。
	Encode func(items []T) []byte
	// RetriesMetricは、送り直した回数を数えるmetricsの名前です。
	RetriesMetric string
	// BackendNameは、circuit breakerのmetricsのbackend属性の値です。ログの出力先と
	// 区別できるよう、送るものの種類を名前に入れます（otlp_tracesなど）。
	BackendName string
	// Unitは、部分的な成功の応答で受信側が数える単位（spans、data pointsなど）です。
	Unit string
	// Nameは、metricsのexporter属性の値です。空ならExporterNameです。
	Name string
	// ContentTypeは、要求の本文の種類です。空ならOTLPのprotobufです。部分的な成功の応答は、
	// protobufの応答だけを読みます。
	ContentType string
}

// Exporterは、delivery.Exporterの実装です。
type Exporter[T any] struct {
	cfg       Config
	signal    Signal[T]
	meter     pkgobserver.Meter
	transport *http.Transport
	client    *http.Client
	// breakerは、Config.CircuitBreakerのときだけ持ちます。
	breaker *circuitbreaker.OperationGuard
	// sleepは、再試行の前に待ちます。ctxが終わったらfalseを返します。試験で置き換えます。
	sleep func(ctx context.Context, d time.Duration) bool
	// jitterは、0からnまでの時間を無作為に返します。試験で置き換えます。
	jitter func(n time.Duration) time.Duration
}

// Newは、Exporterを返します。利用者のprogramのほかの通信に影響しないよう、HTTPの接続は
// 専用のtransportで持ちます。
func New[T any](cfg Config, signal Signal[T], meter pkgobserver.Meter) *Exporter[T] {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	var breaker *circuitbreaker.OperationGuard
	if cfg.CircuitBreaker {
		breaker = circuitbreaker.NewOperationGuard(circuitbreaker.OperationGuardOptions{
			Meter:     meter,
			Backend:   signal.BackendName,
			EventName: signal.BackendName + ".circuit_breaker",
			Classify:  classify,
		})
	}
	return &Exporter[T]{
		breaker:   breaker,
		cfg:       cfg,
		signal:    signal,
		meter:     observer.EnsureMeter(meter),
		transport: transport,
		client: &http.Client{
			Transport: transport,
			// redirectは、設定した送信先が本来の送信先ではないことを表します。ログの送信と
			// 同じく追従せず、元のHTTP statusを失敗として返します。
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		sleep:  sleepContext,
		jitter: randomDuration,
	}
}

func (e *Exporter[T]) Name() string {
	if e.signal.Name != "" {
		return e.signal.Name
	}
	return ExporterName
}

// Exportは、itemsを1回の要求で送ります。一時的な失敗は、Retries回まで送り直します。
// 受信側が要求を大きすぎると拒否した場合（413）は、itemsを半分ずつに分けて送り直します。
// 送れなかったものがあれば、範囲ごとの扱いを入れた*delivery.ExportErrorを返します。
func (e *Exporter[T]) Export(ctx context.Context, items []T) error {
	run := exportRun[T]{exporter: e}
	run.export(ctx, items, 0)
	if len(run.failures) == 0 {
		return nil
	}
	return &delivery.ExportError{Failures: run.failures}
}

// exportRunは、1回のExportで分けて送った要求の結果を集めます。
type exportRun[T any] struct {
	exporter *Exporter[T]
	failures []delivery.Failure
	// stoppedは、送り直す扱いで諦めた要求の失敗です。受信側の状態が悪いか、Retry-Afterで
	// 待つよう求められているため、残りの要求は送らずに同じ扱いにします。
	stopped *delivery.Failure
}

// exportは、batchのoffsetから始まるitemsを送ります。
func (r *exportRun[T]) export(ctx context.Context, items []T, offset int) {
	if r.stopped != nil {
		failure := *r.stopped
		failure.Start, failure.End = offset, offset+len(items)
		failure.Err = fmt.Errorf("not sent after an earlier request failed: %w", r.stopped.Err)
		r.failures = append(r.failures, failure)
		return
	}
	err := r.exporter.sendWithRetries(ctx, items)
	if err == nil {
		return
	}
	var statusErr *StatusError
	if errors.As(err, &statusErr) && statusErr.Code == http.StatusRequestEntityTooLarge && len(items) > 1 {
		r.exporter.countRetry(ctx, retryable.RetryReasonRequestTooLarge)
		half := len(items) / 2
		r.export(ctx, items[:half], offset)
		r.export(ctx, items[half:], offset+half)
		return
	}
	failure := failureOf(ctx, err)
	failure.Start, failure.End = offset, offset+len(items)
	r.failures = append(r.failures, failure)
	if failure.Disposition == delivery.DispositionRetry {
		r.stopped = &failure
	}
}

// sendWithRetriesは、itemsを1回の要求で送り、一時的な失敗をRetries回まで送り直します。
// Retry-AfterがRetryMaxDelayより長い場合は、待たずに諦めます。仕様はRetry-Afterに従うよう
// 求めており、送り先にfailure storeがあれば、そこで期限まで待ちます。
func (e *Exporter[T]) sendWithRetries(ctx context.Context, items []T) error {
	body, encoding, err := e.encode(items)
	if err != nil {
		return err
	}
	for retry := 1; ; retry++ {
		err = e.sendGuarded(ctx, body, encoding)
		if err == nil {
			return nil
		}
		decision := classify(ctx, err)
		if !decision.Retry || retry > e.cfg.Retries || decision.RetryAfter > e.cfg.RetryMaxDelay {
			return err
		}
		e.countRetry(ctx, decision.Reason)
		if !e.sleep(ctx, e.retryDelay(retry, decision)) {
			return retryable.InterruptedWait(err, ctx.Err())
		}
	}
}

// sendGuardedは、circuit breakerがあれば、その許可を得てから1回の要求を送ります。
// breakerが開いている間は送らず、*retryable.CircuitBreakerOpenErrorを返します。errorには、
// どの送り先のbreakerか分かるよう、Signal.BackendNameを操作の名前として入れます。
func (e *Exporter[T]) sendGuarded(ctx context.Context, body []byte, encoding string) error {
	if e.breaker == nil {
		return e.send(ctx, body, encoding)
	}
	return e.breaker.Do(ctx, e.signal.BackendName, func(ctx context.Context) error {
		return e.send(ctx, body, encoding)
	})
}

func (e *Exporter[T]) countRetry(ctx context.Context, reason retryable.RetryReason) {
	e.meter.Counter(e.signal.RetriesMetric).Add(ctx, 1,
		pkgobserver.Attr{Key: telemetry.AttrExporter, Value: e.Name()},
		pkgobserver.Attr{Key: telemetry.AttrReason, Value: string(reason)},
	)
}

// failureOfは、諦めた要求のerrorから、failure storeでの扱いを決めます。受信側がstatusで
// 拒否した要求は、送り直さずにdeadとして残します。部分的な成功は、どれが受け付け
// られたか分からないため、quarantineとして残します。応答を得られなかった失敗は、
// 受け付けられたか分からなくても、欠落より重複を選んで送り直します。
func failureOf(ctx context.Context, err error) delivery.Failure {
	var partial *partialSuccessError
	if errors.As(err, &partial) {
		return delivery.Failure{
			Disposition: delivery.DispositionQuarantine,
			Rejected:    partial.rejected,
			Reason:      string(retryable.RetryReasonRecordRejected),
			Err:         err,
		}
	}
	decision := classify(ctx, err)
	failure := delivery.Failure{
		Disposition: delivery.DispositionRetry,
		StatusCode:  decision.Code,
		Reason:      string(decision.Reason),
		Err:         err,
	}
	var statusErr *StatusError
	if !errors.As(err, &statusErr) {
		return failure
	}
	if decision.Retry {
		failure.RetryAfter = decision.RetryAfter
		return failure
	}
	failure.Disposition = delivery.DispositionDead
	if statusErr.Code == http.StatusRequestEntityTooLarge {
		failure.Reason = string(retryable.RetryReasonRequestTooLarge)
	}
	return failure
}

// classifyは、OTLPの仕様に従い、失敗した要求を送り直すかを決めます。送り直すのは、
// HTTP status 429・502・503・504と、応答を受け取れなかった通信の失敗（接続できない、
// 応答の前に切れた、時間切れ）です。ほかの4xx・5xxは、仕様が送り直しを禁じています。
func classify(ctx context.Context, err error) retryable.RetryDecision {
	var partial *partialSuccessError
	if errors.As(err, &partial) {
		return retryable.RetryDecision{Reason: retryable.RetryReasonRecordRejected}
	}
	var statusErr *StatusError
	if !errors.As(err, &statusErr) {
		return retryable.ClassifyRetry(ctx, err)
	}
	decision := retryable.RetryDecision{Reason: retryable.RetryReasonHTTPStatus, Code: statusErr.Code}
	switch statusErr.Code {
	case http.StatusTooManyRequests:
		decision.Reason = retryable.RetryReasonHTTPThrottle
		fallthrough
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		decision.Retry = true
		decision.RetryAfter = statusErr.RetryAfterDuration
	}
	return decision
}

// retryDelayは、n回目の送り直しの前に待つ時間です。応答にRetry-Afterがあれば、それを
// 使います（RetryMaxDelayより長い場合、sendWithRetriesは送り直しません）。なければ、
// RetryBaseDelayを1回ごとに2倍にしてRetryMaxDelayまでに抑え、その半分から全部までの間で
// 無作為に選びます。同じ受信側へ送る多くのprocessが、同時に送り直さないようにするためです。
func (e *Exporter[T]) retryDelay(retry int, decision retryable.RetryDecision) time.Duration {
	if decision.RetryAfter > 0 {
		return decision.RetryAfter
	}
	delay := e.cfg.RetryBaseDelay
	for i := 1; i < retry && delay < e.cfg.RetryMaxDelay; i++ {
		if delay > e.cfg.RetryMaxDelay/2 {
			delay = e.cfg.RetryMaxDelay
			break
		}
		delay *= 2
	}
	delay = min(delay, e.cfg.RetryMaxDelay)
	return delay/2 + e.jitter(delay-delay/2)
}

// encodeは、itemsをprotobufにし、設定に従ってgzipで圧縮します。
func (e *Exporter[T]) encode(items []T) ([]byte, string, error) {
	body := e.signal.Encode(items)
	if e.cfg.Compression != "gzip" {
		return body, "", nil
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(body); err != nil {
		_ = zw.Close()
		return nil, "", fmt.Errorf("gzip OTLP request body: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, "", fmt.Errorf("gzip OTLP request body: %w", err)
	}
	return buf.Bytes(), "gzip", nil
}

// sendは、1回の要求を送ります。
func (e *Exporter[T]) send(ctx context.Context, body []byte, encoding string) error {
	attemptCtx, cancel := context.WithTimeout(ctx, e.cfg.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, e.cfg.Endpoint, bytes.NewReader(body))
	if err != nil {
		// URLは認証情報を含み得るため、errorに入れません。
		return errors.New("create OTLP request: the endpoint URL is invalid")
	}
	for name, value := range e.cfg.Headers {
		req.Header.Set(name, value)
	}
	contentType := e.signal.ContentType
	if contentType == "" {
		contentType = protobufContentType
	}
	req.Header.Set("Content-Type", contentType)
	if encoding != "" {
		req.Header.Set("Content-Encoding", encoding)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return &StatusError{
			Code:               resp.StatusCode,
			Status:             resp.Status,
			RetryAfterDuration: retryable.ClassifyRetryFromResponse(resp).RetryAfter,
		}
	}
	if readErr != nil || len(respBody) == 0 || !strings.HasPrefix(resp.Header.Get("Content-Type"), protobufContentType) {
		// spanは受け付けられています。応答を読めなくても、送り直しません。
		return nil
	}
	partial, err := unmarshalPartialSuccess(respBody)
	if err != nil || partial.Rejected <= 0 {
		return nil
	}
	return &partialSuccessError{
		rejected: int(min(partial.Rejected, int64(^uint(0)>>1))),
		unit:     e.signal.Unit,
		message:  partial.ErrorMessage,
	}
}

// partialSuccessErrorは、受信側が要求のうちrejected件（単位はunit）を受け付けなかった
// 応答（partial success）です。OTLPの仕様は、この要求の送り直しを禁じています。
type partialSuccessError struct {
	rejected int
	unit     string
	message  string
}

func (e *partialSuccessError) Error() string {
	return fmt.Sprintf("the OTLP receiver rejected %d %s: %s", e.rejected, e.unit, e.message)
}

// Closeは、待機中の接続を閉じます。
func (e *Exporter[T]) Close() error {
	e.transport.CloseIdleConnections()
	return nil
}

// StatusErrorは、受信側が2xx以外のHTTP statusを返したことを表します。
// retryableのHTTPStatusProviderを実装し、再試行の判定に使います。
type StatusError struct {
	Code               int
	Status             string
	RetryAfterDuration time.Duration
}

func (e *StatusError) Error() string {
	return "OTLP receiver returned HTTP status " + e.Status
}

func (e *StatusError) StatusCode() int { return e.Code }

func (e *StatusError) RetryAfter() time.Duration { return e.RetryAfterDuration }

func randomDuration(n time.Duration) time.Duration {
	if n <= 0 {
		return 0
	}
	return rand.N(n + 1)
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
