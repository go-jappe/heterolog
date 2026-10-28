package retryable

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	DefaultCircuitBreakerMinFailureDurationToOpen         = time.Minute
	DefaultCircuitBreakerMaxFailureIntervalForConsecutive = 15 * time.Second
	DefaultCircuitBreakerMinConsecutiveFailuresToOpen     = 3
	DefaultCircuitBreakerOpenCooldown                     = 30 * time.Second
)

// CircuitBreakerKeyは連続失敗を数える失敗種別です。
// backendや操作の識別子は含めず、CircuitBreakerインスタンスの所有境界で分離します。
type CircuitBreakerKey struct {
	Reason RetryReason
	Code   int
}

// CircuitBreakerFailureReportは、retryable failureを記録した直後のbreaker状態を表すreportです。
// 永続状態そのものではなく、RecordRequestFailure・RecordProbeFailureの結果として返します。
type CircuitBreakerFailureReport struct {
	// Consecutive は、同じ CircuitBreakerKey に対して短い間隔で連続して記録された retryable failure の回数です。
	// 前回失敗から circuit breaker の MaxFailureIntervalForConsecutive 設定より長く空いた場合は 1 から数え直します。
	Consecutive int

	// FailureDuration は、現在の連続失敗列における最初の retryable failure から今回までの経過時間です。
	// circuit breaker は Consecutive が設定回数以上、かつこの値が MinFailureDurationToOpen 設定以上になった場合に open します。
	FailureDuration time.Duration

	// OpenUntil は、circuit breaker が open している期限です。
	// zero value の場合、この判断では circuit breaker は open していません。
	OpenUntil time.Time

	// Openedは、この失敗の記録でcircuit breakerが開いたかを表します。すでに開いていた場合は
	// falseです。
	Opened bool
}

// CircuitBreakerRequestDecisionは、backend処理を実行する前に
// CircuitBreaker.BeforeRequestが返す接続可否と復旧確認状態です。
// 呼び出し側はAllowRequestがtrueの場合だけbackend処理を実行します。
type CircuitBreakerRequestDecision struct {
	// AllowRequestは、今回のbackend処理を実行してよい場合にtrueです。
	// open中、または別の復旧確認リクエストが実行中の場合はfalseです。
	AllowRequest bool

	// Probeは、この判断が復旧確認リクエスト（probe）に関係する場合にtrueです。
	// AllowRequestもtrueなら今回の処理がprobeとして許可され、falseなら別のprobeが実行中です。
	Probe bool

	// OpenUntilは、有効なopen期間によってbackend処理を抑止した場合の終了時刻です。
	// AllowRequestがtrueの場合、または別のprobeが実行中の場合はzero valueです。
	OpenUntil time.Time

	// attemptStartedAtはBeforeRequestが今回のbackend処理を許可した時刻です。
	// 長時間かかる処理でも、前回処理の終了から次回処理の開始までの間隔を
	// 連続失敗判定に使えるよう、RecordRequestFailureへdecisionごと渡します。
	attemptStartedAt time.Time

	// ownerとgenerationは、open/復旧より前に開始した処理の遅い完了が
	// 新しい状態を書き換えることを防ぎます。BeforeRequestから得た値をそのまま渡します。
	owner      *CircuitBreaker
	generation uint64
}

type CircuitBreakerOptions struct {
	// MinFailureDurationToOpen は、連続失敗列がこの期間以上続いたら open 条件の一部を満たします。
	MinFailureDurationToOpen time.Duration

	// MaxFailureIntervalForConsecutive は、同じ CircuitBreakerKey の失敗を連続扱いする最大間隔です。
	// RecordRequestFailureでは前回処理の終了から次回処理の開始まで、
	// この値より長く空いた場合に連続失敗列をresetします。
	MaxFailureIntervalForConsecutive time.Duration

	// MinConsecutiveFailuresToOpen は、circuit breaker を open するために必要な連続失敗回数です。
	MinConsecutiveFailuresToOpen int

	// OpenCooldown は、circuit breaker を open して初回接続や retry を抑制する期間です。
	OpenCooldown time.Duration

	// Clock はテスト用の時刻注入です。nil の場合は time.Now を使います。
	Clock func() time.Time
}

// CircuitBreakerは1つのbackend clientが単独所有し、そのclientの全操作をまとめて保護します。
// 異なるbackend client間で同じインスタンスを共有しません。
type CircuitBreaker struct {
	mu sync.Mutex
	// entries は失敗観測中またはopen中の失敗種別ごとの状態だけを保持します。
	// entry が存在しない状態は closed、つまり通常状態として扱います。
	entries map[string]circuitBreakerEntry
	// generationはopenへの遷移とprobe完了時に進めます。
	generation uint64
	// probeInFlight はcooldown後の復旧確認をbackend client全体で1件だけ通すために使います。
	probeInFlight                    bool
	minFailureDurationToOpen         time.Duration
	maxFailureIntervalForConsecutive time.Duration
	minConsecutiveFailuresToOpen     int
	openCooldown                     time.Duration
	clock                            func() time.Time
}

type circuitBreakerEntry struct {
	// firstSeen は、現在の連続失敗列で最初に同じ CircuitBreakerKey の失敗を観測した時刻です。
	// FailureDuration の計算基準であり、連続失敗列が reset された場合はこの時刻も更新します。
	// FailureDuration が MinFailureDurationToOpen 以上になると open 条件の一部を満たします。
	firstSeen time.Time

	// lastSeen は、同じ CircuitBreakerKey の失敗した処理が最後に終了した時刻です。
	// 次の失敗観測、または次の失敗した処理の開始までの間隔が
	// MaxFailureIntervalForConsecutive以内かどうかを判定するために使います。
	lastSeen time.Time

	// consecutive は、現在の連続失敗列に含まれる retryable failure の回数です。
	// MaxFailureIntervalForConsecutive より長く間隔が空くと 1 から数え直します。
	consecutive int

	// openUntil は、circuit breaker open 状態を維持する期限です。
	// zero value の場合、この entry は open 中ではありません。
	openUntil time.Time
}

// NewCircuitBreakerは1つのCircuitBreakerClientが単独所有するcircuit breakerを作成します。
// retryable failureが短い間隔で連続し、その状態が一定時間続くとopenにして接続を抑制します。
// open 中は BeforeRequest が初回接続も抑制し、open cooldown 後に 1 件だけ probe として通します。
// probe の結果を記録するまで、後続の処理は通常の接続として許可しません。
func NewCircuitBreaker(opts CircuitBreakerOptions) *CircuitBreaker {
	if opts.MinFailureDurationToOpen <= 0 {
		opts.MinFailureDurationToOpen = DefaultCircuitBreakerMinFailureDurationToOpen
	}
	if opts.MaxFailureIntervalForConsecutive <= 0 {
		opts.MaxFailureIntervalForConsecutive = DefaultCircuitBreakerMaxFailureIntervalForConsecutive
	}
	if opts.MinConsecutiveFailuresToOpen <= 0 {
		opts.MinConsecutiveFailuresToOpen = DefaultCircuitBreakerMinConsecutiveFailuresToOpen
	}
	if opts.OpenCooldown <= 0 {
		opts.OpenCooldown = DefaultCircuitBreakerOpenCooldown
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}

	return &CircuitBreaker{
		entries:                          make(map[string]circuitBreakerEntry),
		minFailureDurationToOpen:         opts.MinFailureDurationToOpen,
		maxFailureIntervalForConsecutive: opts.MaxFailureIntervalForConsecutive,
		minConsecutiveFailuresToOpen:     opts.MinConsecutiveFailuresToOpen,
		openCooldown:                     opts.OpenCooldown,
		clock:                            opts.Clock,
	}
}

func NewCircuitBreakerOpenError(operation string, openUntil time.Time) *CircuitBreakerOpenError {
	return &CircuitBreakerOpenError{Operation: operation, OpenUntil: openUntil}
}

type CircuitBreakerOpenError struct {
	Operation string
	OpenUntil time.Time
}

func (e *CircuitBreakerOpenError) Error() string {
	if e == nil {
		return "retry circuit breaker is open"
	}
	if e.Operation == "" {
		if e.OpenUntil.IsZero() {
			return "retry circuit breaker is open"
		}
		return fmt.Sprintf("retry circuit breaker is open until %s", e.OpenUntil.Format(time.RFC3339Nano))
	}
	if e.OpenUntil.IsZero() {
		return fmt.Sprintf("retry circuit breaker is open for %s", e.Operation)
	}
	return fmt.Sprintf("retry circuit breaker is open for %s until %s", e.Operation, e.OpenUntil.Format(time.RFC3339Nano))
}

// BeforeRequest は circuit breaker open 中の初回接続を抑制するかを返します。
// open cooldown 後は復旧確認のため、このbreakerを所有するbackend client全体で1件だけprobeとして接続を許可します。
func (c *CircuitBreaker) BeforeRequest() CircuitBreakerRequestDecision {
	if c == nil {
		return CircuitBreakerRequestDecision{AllowRequest: true}
	}

	now := c.clock()

	c.mu.Lock()
	defer c.mu.Unlock()

	var latestOpenUntil time.Time
	hasExpiredOpen := false
	hasActiveOpen := false
	for _, entry := range c.entries {
		if entry.openUntil.IsZero() {
			continue
		}
		if entry.openUntil.After(latestOpenUntil) {
			latestOpenUntil = entry.openUntil
		}
		if now.Before(entry.openUntil) {
			hasActiveOpen = true
			continue
		}
		hasExpiredOpen = true
	}

	if hasActiveOpen {
		return CircuitBreakerRequestDecision{
			AllowRequest: false,
			OpenUntil:    latestOpenUntil,
		}
	}
	if c.probeInFlight {
		return CircuitBreakerRequestDecision{
			AllowRequest: false,
			Probe:        true,
		}
	}
	if !hasExpiredOpen {
		return CircuitBreakerRequestDecision{
			AllowRequest:     true,
			attemptStartedAt: now,
			owner:            c,
			generation:       c.generation,
		}
	}

	c.probeInFlight = true
	return CircuitBreakerRequestDecision{
		AllowRequest:     true,
		Probe:            true,
		attemptStartedAt: now,
		owner:            c,
		generation:       c.generation,
	}
}

// RecordRequestFailureは、backend処理の再試行可能な失敗を記録し、
// 連続失敗の回数・継続時間から、以後の送信を一時停止するか判定します。
// requestDecisionには、今回の処理を許可したBeforeRequestの戻り値を渡してください。
// 連続失敗の間隔は、前回の失敗を記録してから今回の処理を開始するまでの時間です。
// 今回の処理にかかった時間は、その間隔に含めません。
// 新たに送信を停止する場合、停止期間は今回の失敗を記録した時刻から始めます。
// 別の処理によるopenや復旧より前に許可されたrequestの結果は記録しません。
func (c *CircuitBreaker) RecordRequestFailure(
	key CircuitBreakerKey,
	decision RetryDecision,
	requestDecision CircuitBreakerRequestDecision,
) CircuitBreakerFailureReport {
	if c == nil || !decision.Retry {
		return CircuitBreakerFailureReport{}
	}

	now := c.clock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if requestDecision.Probe || !c.currentRequestLocked(requestDecision) {
		return CircuitBreakerFailureReport{}
	}
	attemptStartedAt := requestDecision.attemptStartedAt
	if attemptStartedAt.IsZero() || attemptStartedAt.After(now) {
		attemptStartedAt = now
	}
	return c.observeFailureLocked(key, attemptStartedAt, now)
}

func (c *CircuitBreaker) observeFailureLocked(
	key CircuitBreakerKey,
	attemptStartedAt time.Time,
	now time.Time,
) CircuitBreakerFailureReport {
	id := key.id()

	entry, ok := c.entries[id]
	// 初回の失敗、または open cooldown 済みの古い open entry は新しい連続失敗列として扱う。
	if !ok || (!entry.openUntil.IsZero() && !now.Before(entry.openUntil)) {
		entry = circuitBreakerEntry{firstSeen: attemptStartedAt}
	} else if !entry.lastSeen.IsZero() && attemptStartedAt.Sub(entry.lastSeen) > c.maxFailureIntervalForConsecutive {
		// 前回処理の終了から今回処理の開始まで間が空いた場合は、同じfailureでも
		// 連続障害とはみなさない。
		entry = circuitBreakerEntry{firstSeen: attemptStartedAt}
	}

	// ここから今回の失敗を連続失敗列に加算する。
	entry.lastSeen = now
	entry.consecutive++
	failureDuration := now.Sub(entry.firstSeen)

	// すでに open 中なら、原因を追加記録しつつ retry は抑制する。
	if !entry.openUntil.IsZero() && now.Before(entry.openUntil) {
		c.entries[id] = entry
		return CircuitBreakerFailureReport{
			Consecutive:     entry.consecutive,
			FailureDuration: failureDuration,
			OpenUntil:       entry.openUntil,
		}
	}

	// 短い間隔の連続失敗が必要回数に達し、かつ一定時間続いた場合だけ circuit breaker を open する。
	// 回数だけ、または経過時間だけでは open しない。
	if entry.consecutive >= c.minConsecutiveFailuresToOpen && failureDuration >= c.minFailureDurationToOpen {
		entry.openUntil = now.Add(c.openCooldown)
		c.generation++
		c.probeInFlight = false
		c.entries[id] = entry
		return CircuitBreakerFailureReport{
			Consecutive:     entry.consecutive,
			FailureDuration: failureDuration,
			OpenUntil:       entry.openUntil,
			Opened:          true,
		}
	}

	c.entries[id] = entry
	return CircuitBreakerFailureReport{
		Consecutive:     entry.consecutive,
		FailureDuration: failureDuration,
	}
}

// RecordProbeFailureは現在のprobeだけを完了させ、再びcooldownへ移行します。
func (c *CircuitBreaker) RecordProbeFailure(key CircuitBreakerKey, decision RetryDecision, requestDecision CircuitBreakerRequestDecision) CircuitBreakerFailureReport {
	if c == nil || !decision.Retry {
		return CircuitBreakerFailureReport{}
	}

	now := c.clock()
	id := key.id()

	c.mu.Lock()
	defer c.mu.Unlock()
	if !requestDecision.Probe || !c.currentRequestLocked(requestDecision) {
		return CircuitBreakerFailureReport{}
	}

	entry, ok := c.entries[id]
	if !ok {
		entry = circuitBreakerEntry{firstSeen: now}
	}
	entry.lastSeen = now
	entry.consecutive++
	entry.openUntil = now.Add(c.openCooldown)
	// probe結果をbackend全体の最新状態として扱い、古い失敗種別のopen状態を残さない。
	for existingID := range c.entries {
		delete(c.entries, existingID)
	}
	c.entries[id] = entry
	c.probeInFlight = false
	c.generation++

	return CircuitBreakerFailureReport{
		Consecutive:     entry.consecutive,
		FailureDuration: now.Sub(entry.firstSeen),
		OpenUntil:       entry.openUntil,
		Opened:          true,
	}
}

// ReleaseProbeは、backendの復旧も障害の継続も確認できなかった現在のprobeを完了させます。
// 呼び出し元contextの終了や応答を得られない失敗では、open・連続失敗の記録を変更せず、
// probeの実行枠だけを解放します。cooldownを過ぎたopen状態が残るため、次のrequestを
// 改めて1件だけprobeとして許可します。現在のprobe以外の結果は無視します。
func (c *CircuitBreaker) ReleaseProbe(requestDecision CircuitBreakerRequestDecision) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if !requestDecision.Probe || !c.currentRequestLocked(requestDecision) {
		return
	}
	c.probeInFlight = false
	// 解放したprobeの判断を、次に許可するprobeの結果として扱わないようにします。
	c.generation++
}

// ProbeReachedBackendは、復旧確認request（probe）の結果がbackendからの応答に基づくかを返します。
// 成功、またはHTTP statusを持つ明示的な応答だけを、backendが応答できる状態に戻った証拠とします。
// 再試行しない拒否（4xxなど）でもbackendは応答しているため、trueを返します。
// 呼び出し元contextの終了後の結果、応答を得られない通信失敗、送信前のlocal error、
// 応答本文の読込失敗などでは復旧を確認できないため、falseを返します。
// 呼び出し側は、retryableと分類したprobe失敗をRecordProbeFailureへ渡した後、
// 残りの結果をこの関数でRecordSuccessかReleaseProbeへ振り分けます。
func ProbeReachedBackend(ctx context.Context, err error) bool {
	if err == nil {
		return true
	}
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	var statusProvider HTTPStatusProvider
	return errors.As(err, &statusProvider) && statusProvider.StatusCode() > 0
}

// RecordSuccessは現在の状態で許可された処理の成功を記録します。
// 古い通常requestや完了済みprobeの結果は、新しいopen/probe状態を解除しません。
// probeでは、backendの応答を確認できた場合だけ呼び出します。
// probeの成功で開いていたcircuit breakerを閉じた場合は、closedにtrueを返します。
func (c *CircuitBreaker) RecordSuccess(requestDecision CircuitBreakerRequestDecision) (closed bool) {
	if c == nil {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.currentRequestLocked(requestDecision) {
		return false
	}

	// closed内の成功は連続失敗列をリセットし、probeの成功は復旧を確定する。
	for id := range c.entries {
		delete(c.entries, id)
	}
	c.probeInFlight = false
	if requestDecision.Probe {
		c.generation++
	}
	return requestDecision.Probe
}

func (c *CircuitBreaker) currentRequestLocked(request CircuitBreakerRequestDecision) bool {
	return request.AllowRequest && request.owner == c && request.generation == c.generation &&
		request.Probe == c.probeInFlight
}

func (k CircuitBreakerKey) id() string {
	return fmt.Sprintf("%s\x00%d", k.Reason, k.Code)
}
