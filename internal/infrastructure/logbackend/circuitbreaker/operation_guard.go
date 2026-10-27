package circuitbreaker

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// ClassifyFunc は、バックエンド固有のエラーを retryable.RetryDecision に変換します。
// 返された判定は、そのエラーをバックエンド処理の一時的な失敗として
// サーキットブレーカーへ記録するかどうかの判断に使われます。
type ClassifyFunc func(context.Context, error) retryable.RetryDecision

// OperationGuardは、各backendのCircuitBreakerClientが一回のbackend操作を実行する前後に、
// Circuit Breakerの許可判定と結果記録を適用します。周期loopやretryは実行しません。
// Circuit Breakerの状態管理はretryable.CircuitBreakerに委譲します。
// 一つのOperationGuardが保持するbreaker状態は、同じbackend clientの全操作で共有します。
// リトライと組み合わせる場合は、外側の RetryingClient または RetryPolicyClient が
// 試行ごとにOperationGuard.Doを呼び出します。
type OperationGuard struct {
	circuitBreaker *retryable.CircuitBreaker
	tracer         pkgobserver.Tracer

	// meter は初期化して保持しますが、現時点ではメトリクスを記録していません。
	// 判定結果はトレースイベントにのみ記録します。
	meter pkgobserver.Meter

	eventName string
	classify  ClassifyFunc
}

// OperationGuardOptionsは、OperationGuardの生成に必要な設定をまとめます。
// CircuitBreaker には利用する状態管理オブジェクトを、Classify には
// バックエンド固有のエラー判定処理を指定します。
type OperationGuardOptions struct {
	CircuitBreaker *retryable.CircuitBreaker
	Tracer         pkgobserver.Tracer
	Meter          pkgobserver.Meter
	EventName      string
	Classify       ClassifyFunc
}

// NewOperationGuardは、CircuitBreakerClientから利用するOperationGuardを生成します。
// CircuitBreaker が nil の場合は既定の設定で生成し、Classify が nil の場合は
// retryable.ClassifyRetry を使用します。
func NewOperationGuard(opts OperationGuardOptions) *OperationGuard {
	if opts.CircuitBreaker == nil {
		opts.CircuitBreaker = retryable.NewCircuitBreaker(retryable.CircuitBreakerOptions{})
	}
	if opts.Classify == nil {
		opts.Classify = retryable.ClassifyRetry
	}

	return &OperationGuard{
		circuitBreaker: opts.CircuitBreaker,
		tracer:         observer.EnsureTracer(opts.Tracer),
		meter:          observer.EnsureMeter(opts.Meter),
		eventName:      opts.EventName,
		classify:       opts.Classify,
	}
}

// Do は、サーキットブレーカーの事前判定を行い、許可された場合だけ op を1回実行して結果を記録します。
// opにはCircuitBreakerClientの内側にあるclientを呼び出す、backend送信1回分の処理を渡します。
// retry loopはopやDoではなく、外側のRetryingClientまたはRetryPolicyClientが担当します。
// サーキットブレーカーがオープン中の場合は op を実行せず、
// CircuitBreakerOpenError を返します。
// クールダウン後は、同じbackend clientへの次のリクエスト1件だけを
// 復旧確認リクエスト（probe）として許可します。
// op が成功した場合は、開始後にopen/復旧へ遷移していなければbreaker状態をリセットします。
// 以前の通常requestの完了は、進行中の復旧確認や新しいcooldownへ影響させません。
// エラーが返った場合は、リトライ対象と判定されたものだけを
// バックエンド処理の一時的な失敗としてサーキットブレーカーへ記録します。
// 復旧確認リクエスト（probe）がリトライ対象外のエラーになった場合は、HTTP statusを持つ
// 明示的な応答（4xxなど）を受け取ったときだけbackendが応答できる状態に戻ったとみなし、
// エラーを返しつつbreakerをclosedへ戻します。呼び出し元contextの終了や、応答を得られない
// 失敗・送信前のlocal errorでは復旧を確認できないため、breaker状態を変えずにprobe枠だけを
// 解放し、次のrequestを改めてprobeとして許可します。
func (g *OperationGuard) Do(ctx context.Context, name string, op func(context.Context) error) error {
	requestDecision := g.circuitBreaker.BeforeRequest()

	if !requestDecision.AllowRequest {
		err := retryable.NewCircuitBreakerOpenError(
			name,
			requestDecision.OpenUntil,
		)

		_, span := g.tracer.StartSpan(ctx, name)
		g.addDecisionEvent(
			span,
			retryable.RetryDecision{
				Retry:  false,
				Reason: retryable.RetryReasonCircuitBreakerOpen,
			},
			err,
			0,
			false,
			nil,
			true,
			requestDecision.Probe,
		)
		span.End()

		return err
	}

	spanCtx, span := g.tracer.StartSpan(ctx, name)
	err := op(spanCtx)

	if err == nil {
		g.circuitBreaker.RecordSuccess(requestDecision)
		span.End()
		return nil
	}

	failureClassification := g.classify(spanCtx, err)

	if requestDecision.Probe {
		switch {
		case failureClassification.Retry:
			failureReport := g.circuitBreaker.RecordProbeFailure(
				retryable.CircuitBreakerKey{
					Reason: failureClassification.Reason,
					Code:   failureClassification.Code,
				},
				failureClassification,
				requestDecision,
			)
			g.addDecisionEvent(
				span,
				failureClassification,
				err,
				1,
				false,
				&failureReport,
				!failureReport.OpenUntil.IsZero(),
				requestDecision.Probe,
			)
		case retryable.ProbeReachedBackend(spanCtx, err):
			// 再試行しない拒否でも、backendは明示的に応答しています。
			g.circuitBreaker.RecordSuccess(requestDecision)
		default:
			// 再試行しないことは復旧の証拠になりません。open状態を維持したまま
			// probe枠だけを解放し、後続のprobeで復旧を確認します。
			g.circuitBreaker.ReleaseProbe(requestDecision)
		}

		span.End()
		return err
	}

	if failureClassification.Retry {
		// 前回の失敗から今回の開始までを比較するため、開始時の許可情報も渡します。
		failureReport := g.circuitBreaker.RecordRequestFailure(
			retryable.CircuitBreakerKey{
				Reason: failureClassification.Reason,
				Code:   failureClassification.Code,
			},
			failureClassification,
			requestDecision,
		)
		g.addDecisionEvent(
			span,
			failureClassification,
			err,
			1,
			false,
			&failureReport,
			!failureReport.OpenUntil.IsZero(),
			requestDecision.Probe,
		)
	}

	span.End()
	return err
}

func (g *OperationGuard) addDecisionEvent(
	span pkgobserver.Span,
	failureClassification retryable.RetryDecision,
	err error,
	attempt int,
	willRetry bool,
	failureReport *retryable.CircuitBreakerFailureReport,
	suppressed bool,
	probe bool,
) {
	if span == nil {
		return
	}

	errorText := ""
	if err != nil {
		errorText = err.Error()
	}
	consecutive := 0
	failureDuration := int64(0)
	if failureReport != nil {
		consecutive = failureReport.Consecutive
		failureDuration = failureReport.FailureDuration.Milliseconds()
	}

	span.AddEvent(
		g.eventName,
		pkgobserver.Attr{Key: "reason", Value: string(failureClassification.Reason)},
		pkgobserver.Attr{Key: "attempt", Value: attempt},
		pkgobserver.Attr{Key: "retry", Value: willRetry},
		pkgobserver.Attr{Key: "http_status", Value: failureClassification.Code},
		pkgobserver.Attr{Key: "error", Value: errorText},
		pkgobserver.Attr{Key: "retry_policy_client", Value: false},
		pkgobserver.Attr{Key: "circuit_breaker_client", Value: true},
		pkgobserver.Attr{
			Key:   "circuit_breaker_suppressed",
			Value: suppressed,
		},
		pkgobserver.Attr{
			Key:   "circuit_breaker_probe",
			Value: probe,
		},
		pkgobserver.Attr{
			Key:   "circuit_breaker_consecutive",
			Value: consecutive,
		},
		pkgobserver.Attr{
			Key:   "circuit_breaker_failure_millisecond",
			Value: failureDuration,
		},
	)
}
