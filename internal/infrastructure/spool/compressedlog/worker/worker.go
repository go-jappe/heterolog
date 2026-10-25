// Package workerは、spoolに保存された圧縮ログをobject storageへ再送します。
// 宛先ディレクトリのIDと現在の設定を照合し、その宛先内で
// ファイル名に合うログ系列のuploaderを選びます。
package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

var (
	ErrNilStore                          = errors.New("compressed log file upload failure spool store is nil")
	ErrInvalidRouteFileName              = errors.New("object storage compressed log file route requires a file-name target and extension")
	ErrNilRouteUploader                  = errors.New("object storage compressed log file route uploader is nil")
	ErrInvalidRouteDestination           = errors.New("object storage compressed log file route requires exactly one non-empty destination id")
	ErrNoMatchingRoute                   = errors.New("object storage compressed log file matches no upload route")
	ErrAmbiguousRoute                    = errors.New("object storage compressed log file matches multiple upload routes")
	ErrRouteUploaderPanic                = errors.New("object storage compressed log file route uploader panicked")
	ErrInvalidCircuitBreakerOpenCooldown = errors.New(
		"object storage compressed log file circuit breaker open cooldown must be positive",
	)
	ErrInvalidMaxRetryAfter = errors.New(
		"object storage compressed log file max retry-after must not be negative",
	)
)

type Worker struct {
	store           *objectstoragespool.Store
	routes          RouteRegistry
	circuitBreakers *destinationCircuitBreakerRegistry
	// retryAfterは、送信先が返したRetry-Afterによる宛先別の待機期限です。
	retryAfter *destinationRetryAfterRegistry

	claimMu           *infrafsstore.ContextMutex
	routeSnapshot     func() RouteRegistry
	destinationStores *objectstoragespool.DestinationStores
	lastDestination   objectstorage.DestinationID
}

func NewWorker(
	store *objectstoragespool.Store,
	routes RouteRegistry,
	circuitBreakerOpenCooldown time.Duration,
	options ...Option,
) (*Worker, error) {
	if store == nil {
		return nil, ErrNilStore
	}
	w, err := newWorker(routes, circuitBreakerOpenCooldown, options...)
	if err != nil {
		return nil, err
	}
	w.store = store
	return w, nil
}

func newWorker(routes RouteRegistry, circuitBreakerOpenCooldown time.Duration, options ...Option) (*Worker, error) {
	circuitBreakers, err := newDestinationCircuitBreakerRegistry(
		circuitBreakerOpenCooldown,
		nil,
	)
	if err != nil {
		return nil, err
	}

	registry := make(RouteRegistry, len(routes))
	for index, route := range routes {
		if err := ValidateRouteFileName(route.FileName); err != nil {
			return nil, fmt.Errorf("route %d: %w", index, err)
		}
		if route.Uploader == nil {
			return nil, fmt.Errorf("route %d: %w", index, ErrNilRouteUploader)
		}
		if singleDestinationID(route.Uploader) == "" {
			return nil, fmt.Errorf("route %d: %w", index, ErrInvalidRouteDestination)
		}
		registry[index] = route
	}
	worker := &Worker{
		routes:          registry,
		circuitBreakers: circuitBreakers,
		retryAfter:      newDestinationRetryAfterRegistry(nil),
		claimMu:         &infrafsstore.ContextMutex{},
	}
	for _, option := range options {
		option(worker)
	}
	if worker.retryAfter.maxWait < 0 {
		return nil, fmt.Errorf("%w: %s", ErrInvalidMaxRetryAfter, worker.retryAfter.maxWait)
	}
	if worker.retryAfter.maxWait == 0 {
		worker.retryAfter.maxWait = defaultMaxRetryAfter
	}
	return worker, nil
}

// ValidateRouteFileNameは、送信経路がログ系列の対象名と拡張子を持つことを確認します。
func ValidateRouteFileName(fileName logpattern.FileNamePattern) error {
	if strings.TrimSpace(fileName.Target) == "" || strings.TrimSpace(fileName.Extension) == "" {
		return ErrInvalidRouteFileName
	}
	return nil
}

// RunOnceは宛先を確認し、中断fileを回復してから最古の一件を再送します。
// 動的な宛先登録が空ならfileを変更せず、実際の送信先はfile取得後に再確認します。
// 呼び出し側は回復から送信後の状態遷移までを他の再送・purgeと排他してください。
// Runner経由ではRunner.runMuがこの一周期全体を保護します。
func (w *Worker) RunOnce(ctx context.Context) (RunReport, error) {
	if w != nil && w.destinationStores != nil {
		return w.runDestinationOnce(ctx)
	}
	if w == nil || w.store == nil {
		return RunReport{}, ErrNilStore
	}
	if err := ctx.Err(); err != nil {
		return RunReport{}, err
	}
	if w.routeSnapshot != nil && len(w.routeSnapshot()) == 0 {
		return RunReport{}, nil
	}

	recovered, err := w.store.RecoverProcessing(ctx)
	report := RunReport{RecoveredProcessingCount: recovered}
	if err != nil {
		report.RetryAfterMarkers = w.retryAfter.takeReports()
		return report, fmt.Errorf("recover object storage compressed log file processing files: %w", err)
	}
	report.File, err = w.processOldestActiveFile(ctx)
	report.RetryAfterMarkers = w.retryAfter.takeReports()
	return report, err
}

func (w *Worker) processOldestActiveFile(ctx context.Context) (CompressedLogRunReport, error) {
	if err := w.claimMu.LockContext(ctx); err != nil {
		return CompressedLogRunReport{}, err
	}
	file, claimed, err := w.store.ClaimOldestActive(ctx)
	w.claimMu.Unlock()
	if !claimed {
		return CompressedLogRunReport{}, err
	}
	report := CompressedLogRunReport{
		DestinationID: file.DestinationID,
		Claimed:       true,
		FileName:      file.FileName,
		State:         file.State,
	}
	if err != nil {
		return report, err
	}
	// uploadのキャンセル後も、取得済みfileをactiveへ戻す／doneへ移す処理は完了させます。
	durabilityContext := context.WithoutCancel(ctx)

	matchedRoutes, destination, matchErr := w.matchingRoutes(file.Path)
	if matchErr == nil && len(matchedRoutes) != 0 && file.DestinationID != "" && file.DestinationID != destination {
		matchErr = fmt.Errorf("%w: spool destination %q does not match uploader destination %q", ErrNoMatchingRoute, file.DestinationID, destination)
	}
	if matchErr != nil || len(matchedRoutes) == 0 {
		if matchErr == nil {
			matchErr = fmt.Errorf("%w: file %q", ErrNoMatchingRoute, file.Path)
		}
		report.DeferredReason = DeferredReasonRouteUnavailable
		moved, transitionErr := w.store.RequeueProcessing(durabilityContext, file)
		if moved.State != "" {
			report.State = moved.State
		}
		return report, errors.Join(matchErr, transitionErr)
	}

	// 送信先がRetry-Afterで指定した待機中は、circuit breakerの判定やprobeより先に保留します。
	// 宛先別spoolではRunOnceの前に除外するため、ここへ来るのは単一spoolの場合です。
	// 宛先IDを持つspoolなら、再起動前に保存した期限を判定の前に読み込みます。
	if w.store.Root().DestinationID() == destination {
		w.retryAfter.restore(w.store.Root())
	}
	if deferredUntil := w.retryAfter.deferredUntil(destination); !deferredUntil.IsZero() {
		report.DeferredReason = DeferredReasonRetryAfter
		report.DeferredUntil = deferredUntil
		moved, transitionErr := w.store.RequeueProcessing(durabilityContext, file)
		if moved.State != "" {
			report.State = moved.State
		}
		return report, transitionErr
	}

	breaker := w.circuitBreakers.getOrCreate(destination)
	// 保存時に元ファイルとの内容一致を確認済みです。再送時には展開せず、
	// 現在の宛先設定とcircuit breakerに従って保存済みのバイト列を送信します。
	requestDecision := breaker.BeforeRequest()
	if !requestDecision.AllowRequest {
		report.DeferredReason = DeferredReasonCircuitBreaker
		report.DeferredUntil = requestDecision.OpenUntil
		moved, transitionErr := w.store.RequeueProcessing(durabilityContext, file)
		if moved.State != "" {
			report.State = moved.State
		}
		return report, transitionErr
	}

	report.UploadAttempted = true
	uploadResult, breakerDecision, breakerOpenUntil := uploadWithCircuitBreaker(
		ctx,
		matchedRoutes[0].Uploader,
		destination,
		file.Path,
		breaker,
		requestDecision,
	)
	if uploadResult.Err == nil {
		report.UploadSucceeded = true
		moved, transitionErr := w.store.TransitionProcessing(
			durabilityContext,
			file,
			pkgspool.StateDone,
		)
		if moved.State != "" {
			report.State = moved.State
		}
		return report, transitionErr
	}

	report.FailureCategory = uploadResult.FailureCategory
	if uploadResult.FailureCategory == uploader.FailureCategoryPermanent {
		moved, transitionErr := w.store.TransitionProcessing(
			durabilityContext,
			file,
			pkgspool.StateDead,
		)
		if moved.State != "" {
			report.State = moved.State
		}
		return report, errors.Join(uploadResult.Err, transitionErr)
	}

	report.DeferredReason = DeferredReasonRetryableUploadFailure
	report.DeferredUntil = breakerOpenUntil
	// 一時的な失敗とともに返されたRetry-Afterまでは、この宛先の次のファイルも送信しません。
	// 上限を超える指定は上限まで短縮し、reportで通知します。
	if breakerDecision.Retry && breakerDecision.RetryAfter > 0 {
		notBefore, clamp := w.retryAfter.deferAfter(destination, breakerDecision.RetryAfter)
		report.RetryAfterClamp = clamp
		if notBefore.After(report.DeferredUntil) {
			report.DeferredUntil = notBefore
		}
	}
	moved, transitionErr := w.store.RequeueProcessing(durabilityContext, file)
	if moved.State != "" {
		report.State = moved.State
	}
	return report, errors.Join(uploadResult.Err, transitionErr)
}

// uploadWithCircuitBreakerは、送信とcircuit breakerへの結果記録を同じ関数内で行います。
// uploaderのpanicはエラーに変換して結果記録へ進み、回復確認用の送信枠が
// 使用中のまま残って後続の送信を妨げることを防ぎます。
// 送信失敗に対するbreaker用の分類と、新たなopen期限も返します。
func uploadWithCircuitBreaker(
	ctx context.Context,
	value Uploader,
	destination objectstorage.DestinationID,
	filePath string,
	breaker *retryable.CircuitBreaker,
	requestDecision retryable.CircuitBreakerRequestDecision,
) (result uploader.DestinationResult, decision retryable.RetryDecision, openUntil time.Time) {
	result.DestinationID = destination
	defer func() {
		if recovered := recover(); recovered != nil {
			result.Err = fmt.Errorf("%w: %v", ErrRouteUploaderPanic, recovered)
		}
		// 未分類・不正な結果やpanicでも、ファイルをdeadへ移さず再送可能な状態で残します。
		if result.Err != nil && !result.FailureCategory.IsValid() {
			result.FailureCategory = uploader.FailureCategoryRetryable
		}
		decision = value.ClassifyRetry(ctx, result.Err)
		openUntil = recordUploadResult(ctx, breaker, requestDecision, result.Err, decision)
	}()
	request, err := uploader.NewUploadRequest(filePath)
	if err != nil {
		result.Err = err
		return result, retryable.RetryDecision{}, time.Time{}
	}
	report := value.UploadDestinations(ctx, request, []objectstorage.DestinationID{destination})
	// 別宛先・欠落・重複した結果を、このファイルの送信成功として扱いません。
	if len(report.Results) != 1 || report.Results[0].DestinationID != destination {
		result.Err = errors.Join(report.Err(), fmt.Errorf("spool upload must return exactly one result for destination %q", destination))
		return result, retryable.RetryDecision{}, time.Time{}
	}
	return report.Results[0], retryable.RetryDecision{}, time.Time{}
}

// recordUploadResultは一回の送信結果をcircuit breakerへ記録し、新たなopen期限を返します。
// 再試行しない失敗はbreakerの失敗として数えません。probeでは、再試行しない失敗のうち
// 送信先から明示的な応答を得た場合だけ復旧とみなします。呼び出し元contextの終了、
// uploaderのpanic、応答のない失敗では復旧を確認できないため、breaker状態を変えずに
// probe枠だけを解放し、後続の周期で改めて復旧を確認します。
func recordUploadResult(
	ctx context.Context,
	breaker *retryable.CircuitBreaker,
	requestDecision retryable.CircuitBreakerRequestDecision,
	uploadErr error,
	decision retryable.RetryDecision,
) time.Time {
	switch {
	case uploadErr == nil:
		breaker.RecordSuccess(requestDecision)
	case decision.Retry:
		key := retryable.CircuitBreakerKey{
			Reason: decision.Reason,
			Code:   decision.Code,
		}
		if requestDecision.Probe {
			return breaker.RecordProbeFailure(key, decision, requestDecision).OpenUntil
		}
		return breaker.RecordRequestFailure(key, decision, requestDecision).OpenUntil
	case !requestDecision.Probe:
		// 通常送信の再試行しない失敗は、連続失敗にもbreakerの復旧にも数えません。
	case retryable.ProbeReachedBackend(ctx, uploadErr):
		breaker.RecordSuccess(requestDecision)
	default:
		breaker.ReleaseProbe(requestDecision)
	}
	return time.Time{}
}

// matchingRoutesは、パスのファイル名を各送信経路の圧縮ログの命名規則と照合します。
// 同じファイルに一致する登録が複数あっても、宛先が同じなら一つの送信経路として扱います。
// 異なる宛先の登録が一致する場合は、送信先を一意に決められないためエラーを返します。
func (w *Worker) matchingRoutes(path string) ([]RouteRegistration, objectstorage.DestinationID, error) {
	routes := w.routes
	if w.routeSnapshot != nil {
		routes = w.routeSnapshot()
	}
	matched := make([]RouteRegistration, 0, 1)
	for _, route := range routes {
		if !logpattern.CompressedFileNameMatcher(route.FileName)(path) {
			continue
		}
		matched = append(matched, route)
	}
	if len(matched) == 0 {
		return nil, "", nil
	}
	destinations := make(map[objectstorage.DestinationID]struct{}, len(matched))
	var destination objectstorage.DestinationID
	for _, route := range matched {
		destination = singleDestinationID(route.Uploader)
		if destination == "" {
			return nil, "", ErrInvalidRouteDestination
		}
		destinations[destination] = struct{}{}
	}
	logicalRoutes := len(destinations)
	if logicalRoutes > 1 {
		return nil, "", fmt.Errorf(
			"%w: file %q matched %d registrations across %d destinations",
			ErrAmbiguousRoute,
			path,
			len(matched),
			logicalRoutes,
		)
	}
	return matched, destination, nil
}

func singleDestinationID(value Uploader) objectstorage.DestinationID {
	if value == nil {
		return ""
	}
	destinationIDs := value.DestinationIDs()
	if len(destinationIDs) != 1 || strings.TrimSpace(string(destinationIDs[0])) == "" {
		return ""
	}
	return destinationIDs[0]
}
