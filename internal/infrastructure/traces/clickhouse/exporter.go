package clickhouse

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery/retrying"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse/retryclassifier"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// ExporterNameは、metricsのexporter属性の値です。
const ExporterName = "clickhouse"

// BackendNameは、circuit breakerとfailure storeのmetricsのbackend属性と、failure storeの
// recordのbackendの値です。ログの出力先と区別できるよう、traceの送り先であることを名前に
// 入れます。
const BackendName = "clickhouse_traces"

// Inserterは、pkg/clickhouseのClientのうち、送り先が使う部分です。
type Inserter interface {
	InsertJSONEachRow(ctx context.Context, body []byte, opts pkgclickhouse.InsertOptions) (*pkgclickhouse.InsertResponse, error)
}

// Configは、ClickHouseの送り先の設定です。値の範囲は、pkg/telemetryの検証で確かめます。
type Config struct {
	// Retryは、送り直しとcircuit breakerの設定です。
	Retry retrying.Config
	// Closeは、clientの資源（HTTPの接続など）を手放します。nilなら何もしません。
	Close func() error
}

// NewExporterは、spanをclientの表へ書く送り先を返します。resourceは、Resourceを持たない
// spanに付けます。
//
// 何を一時的な失敗として送り直すかは、ログのClickHouseの出力先と同じです。HTTP status
// 408・429・500・502・503・504と、応答を受け取れなかった通信の失敗を送り直し、表や列が
// ないなど、要求の内容によらない拒否は送り直しません。要求が大きすぎる（413）か、行の
// 値を読めずに拒否された場合は、spanを半分ずつに分けて送り直し、原因の行を確かめます。
// 送り直す要求には、同じINSERTの重複を除けるよう、本文から決めたinsert_deduplication_token
// を付けます。
func NewExporter(client Inserter, cfg Config, resource *tracing.Resource, meter pkgobserver.Meter) *retrying.Exporter[tracing.SpanData] {
	s := &sender{client: client, resource: resource}
	return retrying.New(cfg.Retry, retrying.Backend[tracing.SpanData]{
		Name:          ExporterName,
		BackendName:   BackendName,
		RetriesMetric: telemetry.TraceExportRetries,
		Send:          s.send,
		Classify:      retryclassifier.ClassifyRetry,
		Split:         split,
		Fail:          failureOf,
		Close:         cfg.Close,
	}, meter)
}

// senderは、spanを1回のINSERTで書きます。
type sender struct {
	client   Inserter
	resource *tracing.Resource
}

func (s *sender) send(ctx context.Context, spans []tracing.SpanData) error {
	body := marshalRows(spans, s.resource)
	sum := sha256.Sum256(body)
	resp, err := s.client.InsertJSONEachRow(ctx, body, pkgclickhouse.InsertOptions{InsertDeduplicationToken: hex.EncodeToString(sum[:])})
	if err != nil {
		return err
	}
	if resp == nil {
		return retryclassifier.ErrMissingInsertResponse
	}
	return nil
}

// splitは、要求ごと拒否された理由が、送ったspanを分ければ特定できるものなら、要求を
// 分けます。ClickHouseは、拒否したINSERTの行を1つも書かないため、分けて送り直しても
// 重複しません。
func split(ctx context.Context, err error) retryable.RetryReason {
	var statusErr *pkgclickhouse.HTTPStatusError
	if errors.As(err, &statusErr) && statusErr.StatusCode() == http.StatusRequestEntityTooLarge {
		return retryable.RetryReasonRequestTooLarge
	}
	if retryclassifier.ClassifyDurableInsertFailure(ctx, err, 2).Reason == retryclassifier.RetryReasonRecordIsolationRequired {
		return retryable.RetryReasonRecordRejected
	}
	return ""
}

// failureOfは、諦めたerrorから、failure storeでの扱いを決めます。ログのClickHouseの出力先と
// 同じく分類します。値を読めずに拒否された行と、1行でも大きすぎる要求はquarantine、表が
// ないなど要求の内容によらない拒否はdeadです。ただし、書けたか分からない失敗（応答を読め
// ない、送る途中で止めたなど）は、quarantineにせず、欠落より重複を選んで送り直します。
func failureOf(ctx context.Context, err error) delivery.Failure {
	decision := retryclassifier.ClassifyDurableInsertFailure(ctx, err, 1)
	failure := delivery.Failure{StatusCode: decision.Code, Reason: string(decision.Reason), Err: err}
	switch {
	case decision.Retry:
		failure.Disposition = delivery.DispositionRetry
		failure.RetryAfter = decision.RetryAfter
	case decision.Reason == retryable.RetryReasonUnknownOutcome:
		failure.Disposition = delivery.DispositionRetry
	case decision.Quarantine:
		failure.Disposition = delivery.DispositionQuarantine
	default:
		failure.Disposition = delivery.DispositionDead
	}
	return failure
}
