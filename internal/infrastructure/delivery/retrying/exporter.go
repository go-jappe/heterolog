// Package retryingは、spanやmetricsのbatchを送り先へ送る処理のうち、送り先によらない
// 部分です。batchを1回の要求で送り、一時的な失敗を送り直し、要求を分けて送り直し、
// 送れなかったものの範囲とfailure storeでの扱いをdelivery.ExportErrorにします。要求の
// 送り方と、どの失敗を一時的とするかは、送り先がBackendで渡します。
package retrying

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/circuitbreaker"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// Configは、送り直しとcircuit breakerの設定です。値の範囲は、pkg/telemetryの検証で
// 確かめます。
type Config struct {
	// Retriesは、一時的に失敗した要求を送り直す回数です。0なら送り直しません。
	Retries int
	// RetryBaseDelayとRetryMaxDelayは、送り直す前に待つ時間の幅です（retryDelayを参照）。
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	// CircuitBreakerは、続けて失敗した後、しばらく要求を送らずに失敗させます。
	CircuitBreaker bool

	// Sleepは、送り直す前に待ちます。ctxが終わったらfalseを返します。nilなら、実際に
	// 待ちます。試験で置き換えます。
	Sleep func(ctx context.Context, d time.Duration) bool
	// Jitterは、0からnまでの時間を無作為に返します。nilなら、math/randで選びます。
	// 試験で置き換えます。
	Jitter func(n time.Duration) time.Duration
	// Nowは、circuit breakerの時刻です。nilなら、time.Nowです。試験で置き換えます。
	Now func() time.Time
}

// Backendは、送り先による違いです。
type Backend[T any] struct {
	// Nameは、metricsのexporter属性の値です。
	Name string
	// BackendNameは、circuit breakerのmetricsのbackend属性の値です。breakerが開いて
	// いるときのerrorにも、操作の名前として入れます。ログの出力先と区別できるよう、
	// 送るものの種類を名前に入れます（otlp_tracesなど）。
	BackendName string
	// RetriesMetricは、送り直した回数を数えるmetricsの名前です。
	RetriesMetric string
	// Sendは、itemsを1回の要求で送ります。受信側が要求を処理し、itemごとの結果の
	// うち一部を受け付けなかった場合は、*ItemErrorsを返します。ほかのerrorは、items
	// のすべてが届いていないか、届いたか分からないことを表します。
	Send func(ctx context.Context, items []T) error
	// Classifyは、失敗した要求や、*ItemErrorsの1件のerrorを、すぐに送り直すかを
	// 決めます。circuit breakerが数える失敗も、この判定で決めます。
	Classify func(ctx context.Context, err error) retryable.RetryDecision
	// Splitは、2件以上のitemを送った要求のerrorから、itemsを半分ずつに分けて送り
	// 直すかを決め、分ける理由を返します。分けない場合は空の理由を返します。nilなら
	// 分けません。
	Split func(ctx context.Context, err error) retryable.RetryReason
	// Failは、送り直すのを諦めたerrorから、failure storeでの扱いを決めます。範囲
	// （StartとEnd）は、Exporterが入れます。
	Fail func(ctx context.Context, err error) delivery.Failure
	// Closeは、送り先の資源（HTTPの接続など）を手放します。nilなら何もしません。
	Close func() error
}

// ItemErrorsは、受信側が要求を処理し、itemごとの結果のうち一部を受け付けなかったことを
// 表します（OpenSearchのBulk APIなど）。
type ItemErrors struct {
	// Errsは、受け付けなかったitemの、要求の中の位置とerrorです。位置の順に並びます。
	Errs []ItemError
}

// ItemErrorは、要求のIndex番目のitemを受け付けなかった理由です。
type ItemError struct {
	Index int
	Err   error
}

func (e *ItemErrors) Error() string {
	if len(e.Errs) == 1 {
		return e.Errs[0].Err.Error()
	}
	return fmt.Sprintf("the receiver did not accept %d items: %v", len(e.Errs), errors.Join(e.Unwrap()...))
}

func (e *ItemErrors) Unwrap() []error {
	errs := make([]error, 0, len(e.Errs))
	for _, item := range e.Errs {
		errs = append(errs, item.Err)
	}
	return errs
}

// Exporterは、delivery.Exporterの実装です。
type Exporter[T any] struct {
	cfg     Config
	backend Backend[T]
	meter   pkgobserver.Meter
	// breakerは、Config.CircuitBreakerのときだけ持ちます。
	breaker *circuitbreaker.OperationGuard
}

// Newは、Exporterを返します。
func New[T any](cfg Config, backend Backend[T], meter pkgobserver.Meter) *Exporter[T] {
	if cfg.Sleep == nil {
		cfg.Sleep = sleepContext
	}
	if cfg.Jitter == nil {
		cfg.Jitter = randomDuration
	}
	meter = observer.EnsureMeter(meter)
	var breaker *circuitbreaker.OperationGuard
	if cfg.CircuitBreaker {
		breaker = circuitbreaker.NewOperationGuard(circuitbreaker.OperationGuardOptions{
			CircuitBreaker: retryable.NewCircuitBreaker(retryable.CircuitBreakerOptions{Clock: cfg.Now}),
			Meter:          meter,
			Backend:        backend.BackendName,
			EventName:      backend.BackendName + ".circuit_breaker",
			Classify:       backend.Classify,
		})
	}
	return &Exporter[T]{cfg: cfg, backend: backend, meter: meter, breaker: breaker}
}

func (e *Exporter[T]) Name() string { return e.backend.Name }

// Closeは、Backend.Closeを呼びます。
func (e *Exporter[T]) Close() error {
	if e.backend.Close == nil {
		return nil
	}
	return e.backend.Close()
}

// Exportは、itemsを1回の要求で送ります。一時的な失敗は、Retries回まで送り直します。
// Backend.Splitが分けると決めた要求は、itemsを半分ずつに分けて送り直します。受信側が
// 一部のitemだけを受け付けなかった場合は、送り直すitemだけを送り直します。送れなかった
// ものがあれば、範囲ごとの扱いを入れた*delivery.ExportErrorを返します。
func (e *Exporter[T]) Export(ctx context.Context, items []T) error {
	run := exportRun[T]{exporter: e, items: items}
	all := make([]int, len(items))
	for i := range all {
		all[i] = i
	}
	run.export(ctx, all)
	if len(run.failures) == 0 {
		return nil
	}
	slices.SortFunc(run.failures, func(a, b delivery.Failure) int { return a.Start - b.Start })
	return &delivery.ExportError{Failures: run.failures}
}

// exportRunは、1回のExportで分けて送った要求の結果を集めます。
type exportRun[T any] struct {
	exporter *Exporter[T]
	items    []T
	failures []delivery.Failure
	// stoppedは、送り直す扱いで諦めた要求の失敗です。受信側の状態が悪いか、Retry-Afterで
	// 待つよう求められているため、残りの要求は送らずに同じ扱いにします。
	stopped *delivery.Failure
}

// exportは、batchのうちindexesの位置のitemsを送ります。indexesは昇順です。
func (r *exportRun[T]) export(ctx context.Context, indexes []int) {
	if r.stopped != nil {
		failure := *r.stopped
		failure.Err = fmt.Errorf("not sent after an earlier request failed: %w", r.stopped.Err)
		r.record(failure, indexes)
		return
	}
	e := r.exporter
	pending := indexes
	for retry := 1; ; retry++ {
		err := e.send(ctx, r.selectItems(pending))
		if err == nil {
			return
		}
		var decision retryable.RetryDecision
		var itemErrs *ItemErrors
		if errors.As(err, &itemErrs) {
			pending, err, decision = r.settleItems(ctx, pending, itemErrs)
			if len(pending) == 0 {
				return
			}
		} else {
			if reason := e.split(ctx, err, len(pending)); reason != "" {
				e.countRetry(ctx, reason)
				half := len(pending) / 2
				r.export(ctx, pending[:half])
				r.export(ctx, pending[half:])
				return
			}
			decision = e.backend.Classify(ctx, err)
		}
		// Retry-AfterがRetryMaxDelayより長い場合は、待たずに諦めます。送り先にfailure storeが
		// あれば、そこで期限まで待ちます。
		if !decision.Retry || retry > e.cfg.Retries || decision.RetryAfter > e.cfg.RetryMaxDelay {
			r.giveUp(ctx, err, pending)
			return
		}
		e.countRetry(ctx, decision.Reason)
		if !e.cfg.Sleep(ctx, e.retryDelay(retry, decision)) {
			r.giveUp(ctx, retryable.InterruptedWait(err, ctx.Err()), pending)
			return
		}
	}
}

// settleItemsは、受信側が受け付けなかったitemsのうち、すぐに送り直さないものを諦め、
// 送り直すitemsの位置と、その最初のerror、送り直す判定を返します。判定のRetryAfterは、
// 送り直すitemsのうち最も長いものです。
func (r *exportRun[T]) settleItems(ctx context.Context, pending []int, itemErrs *ItemErrors) ([]int, error, retryable.RetryDecision) {
	var retryIndexes []int
	var retryErr error
	var decision retryable.RetryDecision
	for _, item := range itemErrs.Errs {
		if item.Index < 0 || item.Index >= len(pending) {
			continue
		}
		itemDecision := r.exporter.backend.Classify(ctx, item.Err)
		if !itemDecision.Retry {
			r.giveUp(ctx, item.Err, pending[item.Index:item.Index+1])
			continue
		}
		retryIndexes = append(retryIndexes, pending[item.Index])
		if retryErr == nil {
			retryErr, decision = item.Err, itemDecision
		}
		decision.RetryAfter = max(decision.RetryAfter, itemDecision.RetryAfter)
	}
	return retryIndexes, retryErr, decision
}

// giveUpは、indexesの位置のitemsを、errから決めた扱いで諦めます。送り直す扱いなら、
// 残りの要求を送らないようにします。
func (r *exportRun[T]) giveUp(ctx context.Context, err error, indexes []int) {
	failure := r.exporter.backend.Fail(ctx, err)
	r.record(failure, indexes)
	if failure.Disposition == delivery.DispositionRetry && r.stopped == nil {
		r.stopped = &failure
	}
}

// recordは、indexesの位置のitemsの失敗を、連続した位置ごとの範囲にして残します。
func (r *exportRun[T]) record(failure delivery.Failure, indexes []int) {
	for start := 0; start < len(indexes); {
		end := start + 1
		for end < len(indexes) && indexes[end] == indexes[end-1]+1 {
			end++
		}
		failure.Start, failure.End = indexes[start], indexes[end-1]+1
		r.failures = append(r.failures, failure)
		start = end
	}
}

// selectItemsは、indexesの位置のitemsを返します。すべての位置なら、batchをそのまま返します。
func (r *exportRun[T]) selectItems(indexes []int) []T {
	if len(indexes) == len(r.items) {
		return r.items
	}
	items := make([]T, len(indexes))
	for i, index := range indexes {
		items[i] = r.items[index]
	}
	return items
}

// splitは、n件のitemを送った要求のerrorから、要求を分ける理由を返します。
func (e *Exporter[T]) split(ctx context.Context, err error, n int) retryable.RetryReason {
	if n < 2 || e.backend.Split == nil {
		return ""
	}
	return e.backend.Split(ctx, err)
}

// sendは、circuit breakerがあれば、その許可を得てから1回の要求を送ります。breakerが
// 開いている間は送らず、*retryable.CircuitBreakerOpenErrorを返します。受信側がitemごとの
// 結果を返した要求は、受信側が処理できたため、breakerには成功として数えます。
func (e *Exporter[T]) send(ctx context.Context, items []T) error {
	if e.breaker == nil {
		return e.backend.Send(ctx, items)
	}
	var itemErrs *ItemErrors
	err := e.breaker.Do(ctx, e.backend.BackendName, func(ctx context.Context) error {
		err := e.backend.Send(ctx, items)
		if errors.As(err, &itemErrs) {
			return nil
		}
		return err
	})
	if err == nil && itemErrs != nil {
		return itemErrs
	}
	return err
}

func (e *Exporter[T]) countRetry(ctx context.Context, reason retryable.RetryReason) {
	e.meter.Counter(e.backend.RetriesMetric).Add(ctx, 1,
		pkgobserver.Attr{Key: telemetry.AttrExporter, Value: e.backend.Name},
		pkgobserver.Attr{Key: telemetry.AttrReason, Value: string(reason)},
	)
}

// retryDelayは、n回目の送り直しの前に待つ時間です。応答にRetry-Afterがあれば、それを
// 使います（RetryMaxDelayより長い場合、exportは送り直しません）。なければ、
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
	return delay/2 + e.cfg.Jitter(delay-delay/2)
}

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
