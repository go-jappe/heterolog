package opensearch

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery/retrying"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	osbackend "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch/retryclassifier"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

// ExporterNameは、metricsのexporter属性の値です。
const ExporterName = "opensearch"

// BackendNameは、circuit breakerとfailure storeのmetricsのbackend属性と、failure storeの
// recordのbackendの値です。ログの出力先と区別できるよう、traceの送り先であることを名前に
// 入れます。
const BackendName = "opensearch_traces"

// Configは、OpenSearchの送り先の設定です。値の範囲は、pkg/telemetryの検証で確かめます。
type Config struct {
	// Indexは、文書を書くindex（write aliasやdate mathの式も使えます）です。
	Index string
	// Retryは、送り直しとcircuit breakerの設定です。
	Retry retrying.Config
}

// NewExporterは、spanをclientでOpenSearchへ書く送り先を返します。resourceは、Resourceを
// 持たないspanに付けます。
//
// 何を一時的な失敗として送り直すかは、ログのOpenSearchの出力先と同じです。要求には、
// HTTP status 408・429・500・502・503・504と、応答を受け取れなかった通信の失敗を送り
// 直します。Bulk APIの応答で失敗した文書は、同じ規則のstatusの文書だけを送り直し、
// 文書の内容の不正（mapper_parsing_exceptionなど）はquarantine、ほかはdeadとして残します。
// 要求が大きすぎる（413）か、文書の内容の不正で要求ごと拒否された場合は、spanを
// 半分ずつに分けて送り直します。
func NewExporter(client pkgopensearch.Client, cfg Config, resource *tracing.Resource, meter pkgobserver.Meter) *retrying.Exporter[tracing.SpanData] {
	s := &sender{client: client, index: cfg.Index, resource: resource}
	return retrying.New(cfg.Retry, retrying.Backend[tracing.SpanData]{
		Name:          ExporterName,
		BackendName:   BackendName,
		RetriesMetric: telemetry.TraceExportRetries,
		Send:          s.send,
		Classify:      classify,
		Split:         split,
		Fail:          failureOf,
	}, meter)
}

// senderは、spanをBulk APIの1回の要求で書きます。
type sender struct {
	client   pkgopensearch.Client
	index    string
	resource *tracing.Resource
}

func (s *sender) send(ctx context.Context, spans []tracing.SpanData) error {
	docs := make([]pkgopensearch.DocumentWriteRequest, len(spans))
	for i, span := range spans {
		docs[i] = pkgopensearch.DocumentWriteRequest{
			Index: s.index,
			ID:    documentID(span),
			Doc:   document(span, s.resource),
			Op:    pkgopensearch.WriteOpCreate,
		}
	}
	resp, err := s.client.IndexDocuments(ctx, docs)
	if err != nil {
		return err
	}
	result, err := osbackend.ClassifyFailedBulkDocs(resp, docs)
	if err != nil {
		// 応答を要求と対応付けられないため、どの文書を書けたか分かりません。
		return &pkgopensearch.ResponseDecodeError{Operation: "bulk", Err: err}
	}
	var errs []retrying.ItemError
	add := func(failed []osbackend.BulkFailedRequest, disposition delivery.Disposition) {
		for _, f := range failed {
			errs = append(errs, retrying.ItemError{Index: f.InputIndex, Err: &documentError{
				status: f.Status, errorType: f.ErrorType, reason: f.ErrorReason, disposition: disposition,
			}})
		}
	}
	add(result.RetryableFailures, delivery.DispositionRetry)
	add(result.DeadFailures, delivery.DispositionDead)
	add(result.QuarantineFailures, delivery.DispositionQuarantine)
	if len(errs) == 0 {
		return nil
	}
	slices.SortFunc(errs, func(a, b retrying.ItemError) int { return a.Index - b.Index })
	return &retrying.ItemErrors{Errs: errs}
}

// documentErrorは、Bulk APIの応答で、OpenSearchが1件の文書を書かなかった理由です。
// retryableのHTTPStatusProviderを実装し、再試行の判定に使います。
type documentError struct {
	status      int
	errorType   pkgopensearch.ErrorType
	reason      string
	disposition delivery.Disposition
}

func (e *documentError) Error() string {
	return fmt.Sprintf("opensearch did not write the span: status=%d type=%s reason=%s", e.status, e.errorType, e.reason)
}

func (e *documentError) StatusCode() int { return e.status }

func (e *documentError) RetryAfter() time.Duration { return 0 }

// classifyは、失敗した要求や文書を、すぐに送り直すかを決めます。
func classify(ctx context.Context, err error) retryable.RetryDecision {
	var docErr *documentError
	if errors.As(err, &docErr) {
		decision := retryable.ClassifyRetryByStatus(docErr.status, 0)
		decision.Retry = docErr.disposition == delivery.DispositionRetry
		return decision
	}
	return retryclassifier.ClassifyRetry(ctx, err)
}

// splitは、要求ごと拒否された理由が、送ったspanを分ければ特定できるものなら、要求を
// 分けます。どの文書も書かれていないため、分けて送り直しても重複しません。
func split(_ context.Context, err error) retryable.RetryReason {
	switch {
	case osbackend.IsRequestEntityTooLarge(err):
		return retryable.RetryReasonRequestTooLarge
	case osbackend.IsRecordScopedRequestRejection(err):
		return retryable.RetryReasonRecordRejected
	}
	return ""
}

// failureOfは、諦めたerrorから、failure storeでの扱いを決めます。ログのOpenSearchの
// 出力先と同じく分類しますが、書けたか分からない失敗（応答を読めない、送る途中で止めた
// など）は、quarantineにせず送り直します。createと固定の_idで書くため、既に書かれて
// いたspanは409になり、重複しません。
func failureOf(ctx context.Context, err error) delivery.Failure {
	var docErr *documentError
	if errors.As(err, &docErr) {
		reason := retryable.ClassifyRetryByStatus(docErr.status, 0).Reason
		if docErr.disposition == delivery.DispositionQuarantine {
			reason = retryable.RetryReasonRecordRejected
		}
		return delivery.Failure{Disposition: docErr.disposition, StatusCode: docErr.status, Reason: string(reason), Err: err}
	}
	decision := retryclassifier.ClassifyDurableRequestFailure(ctx, err)
	failure := delivery.Failure{StatusCode: decision.Code, Reason: string(decision.Reason), Err: err}
	switch {
	case decision.Retry:
		failure.Disposition = delivery.DispositionRetry
		failure.RetryAfter = decision.RetryAfter
	case decision.Reason == retryable.RetryReasonUnknownOutcome || decision.Reason == retryclassifier.FailureReasonResponseDecode:
		failure.Disposition = delivery.DispositionRetry
	case decision.Quarantine:
		failure.Disposition = delivery.DispositionQuarantine
	default:
		failure.Disposition = delivery.DispositionDead
	}
	return failure
}
