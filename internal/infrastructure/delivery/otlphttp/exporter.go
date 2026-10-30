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
	"net/http"
	"strings"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/delivery"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery/retrying"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
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
	Compression string
	Timeout     time.Duration
	// Retryは、送り直しとcircuit breakerの設定です。
	Retry retrying.Config
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

// Exporterは、OTLP/HTTPの送り先です。
type Exporter[T any] = retrying.Exporter[T]

// senderは、1回の要求をOTLP/HTTPで送ります。
type sender[T any] struct {
	cfg    Config
	signal Signal[T]
	client *http.Client
}

// Newは、Exporterを返します。一時的な失敗は、OTLPの仕様の規則で送り直します。受信側が
// 要求を大きすぎると拒否した場合（413）は、itemsを半分ずつに分けて送り直します。利用者の
// programのほかの通信に影響しないよう、HTTPの接続は専用のtransportで持ちます。
func New[T any](cfg Config, signal Signal[T], meter pkgobserver.Meter) *Exporter[T] {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	s := &sender[T]{
		cfg:    cfg,
		signal: signal,
		client: &http.Client{
			Transport: transport,
			// redirectは、設定した送信先が本来の送信先ではないことを表します。ログの送信と
			// 同じく追従せず、元のHTTP statusを失敗として返します。
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
	name := signal.Name
	if name == "" {
		name = ExporterName
	}
	return retrying.New(cfg.Retry, retrying.Backend[T]{
		Name:          name,
		BackendName:   signal.BackendName,
		RetriesMetric: signal.RetriesMetric,
		Send:          s.sendItems,
		Classify:      classify,
		Split:         split,
		Fail:          failureOf,
		Close: func() error {
			transport.CloseIdleConnections()
			return nil
		},
	}, meter)
}

// splitは、受信側が要求を大きすぎると拒否した場合（413）に、要求を分けます。
func split(_ context.Context, err error) retryable.RetryReason {
	var statusErr *StatusError
	if errors.As(err, &statusErr) && statusErr.Code == http.StatusRequestEntityTooLarge {
		return retryable.RetryReasonRequestTooLarge
	}
	return ""
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

// sendItemsは、itemsを1回の要求で送ります。
func (s *sender[T]) sendItems(ctx context.Context, items []T) error {
	body, encoding, err := s.encode(items)
	if err != nil {
		return err
	}
	return s.send(ctx, body, encoding)
}

// encodeは、itemsをprotobufにし、設定に従ってgzipで圧縮します。
func (s *sender[T]) encode(items []T) ([]byte, string, error) {
	body := s.signal.Encode(items)
	if s.cfg.Compression != "gzip" {
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
func (s *sender[T]) send(ctx context.Context, body []byte, encoding string) error {
	attemptCtx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, s.cfg.Endpoint, bytes.NewReader(body))
	if err != nil {
		// URLは認証情報を含み得るため、errorに入れません。
		return errors.New("create OTLP request: the endpoint URL is invalid")
	}
	for name, value := range s.cfg.Headers {
		req.Header.Set(name, value)
	}
	contentType := s.signal.ContentType
	if contentType == "" {
		contentType = protobufContentType
	}
	req.Header.Set("Content-Type", contentType)
	if encoding != "" {
		req.Header.Set("Content-Encoding", encoding)
	}
	resp, err := s.client.Do(req)
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
		unit:     s.signal.Unit,
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
