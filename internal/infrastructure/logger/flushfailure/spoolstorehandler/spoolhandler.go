package spoolstorehandler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/overload"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/runtime/memory"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

const failureSpoolInternalLogSource = "failure_spool"

// defaultMaxRetryAfterは、spool runnerの設定を受け取らないhandlerが使うRetry-After待機の上限です。
const defaultMaxRetryAfter = time.Hour

// SpoolStoreHandler はbackend送信失敗をstate別spoolへ保存します。
type SpoolStoreHandler struct {
	clock  clock.Clock
	Store  *store.SpoolStore
	Config OverloadConfigProvider
	// dirStatsScannerはactive spool directoryの統計を取得します。constructorで実装を
	// 固定し、運用中は変更しません。
	dirStatsScanner func(spool.StateDir) (DirStats, error)
	overloadCache   dirStatsCache // ストレージ過負荷判定集計データキャッシュ

	// operationMuは受付状態と受理済み件数を保護します。保存・通知中は保持しません。
	// Shutdownの待機contextが終了しても、受理済み処理とその完了通知は継続します。
	operationMu    sync.Mutex
	operationCount int
	operationDone  chan struct{}
	shutdownState  shutdownState

	// active spool の滞留警告を一日一回に抑制するための状態
	activeBacklogStaleWarn dailyWarnState

	// maxRetryAfterは、送信先が指定したRetry-Afterから保存する再送下限時刻の上限です。
	// 同じspoolを再送するrunnerの設定をSetMaxRetryAfterで受け取ります。0は既定値です。
	maxRetryAfter time.Duration

	// retryAfterRecorderは、送信先が指定した期限を、同じspoolを再送するrunnerの送信先全体の
	// 待機期限として記録・保存する先です。SetRetryAfterRecorderで受け取り、nilなら期限を
	// 保存するrecordだけに残します。
	retryAfterRecorder RetryAfterRecorder

	MemoryMonitor memory.MemoryCriticalChecker // memoryがcriticalな状況かどうかを判定するinterface

	OnError         logging.LoggerErrorHandler // 通知用の関数　内部エラー発生時にハンドル
	OnWarn          logging.LoggerHandlerFunc
	OnInfo          logging.LoggerHandlerFunc
	OnDebug         logging.LoggerHandlerFunc
	Tracer          observer.Tracer // observebility計装用
	Meter           observer.Meter  // observebility計装用
	OverloadHandler overload.Handler
}

type dailyWarnState struct {
	mu           sync.Mutex
	lastWarnedAt time.Time
}

// SetMaxRetryAfterは、送信先が指定したRetry-Afterから保存する再送下限時刻の上限を設定します。
// 同じspoolを再送するrunnerの設定と揃えるため、失敗の受付を始める前の構築中にだけ呼びます。
// 0以下は既定の1時間として扱い、無制限にはしません。
func (s *SpoolStoreHandler) SetMaxRetryAfter(maxRetryAfter time.Duration) {
	if s == nil {
		return
	}
	s.maxRetryAfter = maxRetryAfter
}

// RetryAfterRecorderは、通常送信で受け取ったRetry-Afterの期限を、同じspool rootを再送する
// runnerの送信先全体の待機期限として記録し、再起動後も参照できるよう保存します。
// より後の期限を記録済みなら短縮せず、保存に失敗しても再送中のrunnerは期限を守ります。
type RetryAfterRecorder interface {
	RecordRetryAfter(notBefore time.Time)
}

// SetRetryAfterRecorderは、送信先が指定した期限を記録する、同じspoolを再送するrunnerの待機期限を
// 設定します。失敗の受付を始める前の構築中にだけ呼びます。
func (s *SpoolStoreHandler) SetRetryAfterRecorder(recorder RetryAfterRecorder) {
	if s == nil {
		return
	}
	s.retryAfterRecorder = recorder
}

func (s *SpoolStoreHandler) retryAfterLimit() time.Duration {
	if s == nil || s.maxRetryAfter <= 0 {
		return defaultMaxRetryAfter
	}
	return s.maxRetryAfter
}

// HandleFailure はbackend送信失敗結果をoverload判定後にspoolへ保存します。
func (s *SpoolStoreHandler) HandleFailure(
	ctx context.Context,
	result loggerfailure.SendResult,
) error {
	if s == nil {
		return errors.New("spool store handler is nil")
	}

	s.operationMu.Lock()
	if s.shutdownState != shutdownStateAccepting {
		s.operationMu.Unlock()
		return ErrSpoolStoreHandlerClosing
	}
	s.operationCount++
	s.operationMu.Unlock()
	defer s.endOperation()
	return s.handleFailure(ctx, result)
}

func (s *SpoolStoreHandler) endOperation() {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	s.operationCount--
	if s.operationCount == 0 && s.operationDone != nil {
		close(s.operationDone)
		s.operationDone = nil
	}
}

func (s *SpoolStoreHandler) handleFailure(
	ctx context.Context,
	result loggerfailure.SendResult,
) error {
	if result.FailedCount() == 0 {
		return nil
	}
	// 送信先が指定した期限は、過負荷でspoolへ保存しない場合も含め、保存より先に送信先全体の
	// 待機期限として記録します。runnerはその送信先のspoolのログを期限まで送りません。
	// 上限を超える指定の短縮は、spoolへ保存したrecordの短縮として通知しなかった場合（drop・
	// degradeや保存の失敗）に、記録した期限の短縮として一度だけ警告します。
	deadlineClamp := s.recordRetryAfter(result)
	clampNotified := false
	defer func() {
		if !clampNotified {
			s.notifyRetryAfterDeadlineClamp(ctx, result.Meta, deadlineClamp)
		}
	}()

	// メモリ逼迫を処理
	// monitor は既にバックグラウンドで更新済みの値を取得
	isMemoryOverload, err := s.isMemoryOverloaded(ctx)
	if err != nil {
		// monitor 取得失敗だけで drop するのは危険なので、基本は fail-open
		// warn/error 通知だけして、通常の spool 保存へ進む。
		s.notifyError(ctx, fmt.Errorf("failed to check memory overload: %w", err), "failed to check memory overload")
	} else if isMemoryOverload {
		return s.handleOverload(ctx, result, "failed to handle memory overload")
	}

	// ストレージオーバーロード判定
	// 保存先はactiveのみのためメソッド名にActiveとつけている
	// 統計走査の前に、activeまたは今回の日時別保存先を塞ぐ不正な項目を保全します。
	// 走査エラーや妨害ファイルの容量による過負荷判定で、保存へ進めない状態を防ぎます。
	if err := s.Store.EnsureCurrentTimePartitionDirectory(ctx, pkgspool.StateActive); err != nil {
		s.notifyError(ctx, err, "failed to prepare active spool time partition directory")
		return err
	}
	isOverload, err := s.isActiveSpoolStorageOverloaded(ctx, s.Store.RootDir.ActiveDir())
	if err != nil {
		s.notifyError(ctx, fmt.Errorf("failed to check spool overload: %w", err), "failed to check spool overload")
		return err
	}

	// オーバーロード時は別処理を行う
	if isOverload {
		return s.handleOverload(ctx, result, "failed to handle spool overload")
	}

	// 失敗データをspoolに分類して保存
	notified, storeErr := s.storeSendResultByState(ctx, result)
	clampNotified = notified
	if storeErr != nil {
		s.notifyError(ctx, storeErr, "failed to store send result in spool")
		return storeErr
	}

	return nil
}

// handleOverloadは過負荷時の処置をoverload handlerへ委ねます。drop policyでrecordを
// 破棄した場合は、pkglogger.ErrLogDroppedを包んだerrorを呼び出し元へ返します。
// dropは設定した処置の結果であり、metricとspan eventで記録済みのため内部エラーとして
// 重ねて通知しません。退避自体の失敗だけを通知します。
func (s *SpoolStoreHandler) handleOverload(
	ctx context.Context,
	result loggerfailure.SendResult,
	errorMessage string,
) error {
	if s.OverloadHandler == nil {
		err := fmt.Errorf("overload handler is nil")
		s.notifyError(ctx, err, "overload handler is nil")
		return err
	}
	if err := s.OverloadHandler.Handle(ctx, result); err != nil {
		if !errors.Is(err, pkglogger.ErrLogDropped) {
			s.notifyError(ctx, err, errorMessage)
		}
		return err
	}
	return nil
}

func toSpoolRecord(
	clock clock.Clock,
	rec loggerfailure.FailedRecord,
	meta loggerfailure.Meta,
) (pkgspool.SpoolRecord, error) {
	if rec.Payload == nil {
		return pkgspool.SpoolRecord{}, ErrNilDurablePayload
	}

	payload, err := json.Marshal(rec.Payload)
	if err != nil {
		return pkgspool.SpoolRecord{}, fmt.Errorf("failed to marshal durable payload: %w", err)
	}

	occurredAt := resolveOccurredAt(clock, rec, meta)
	record := pkgspool.SpoolRecord{
		OccurredAt:               occurredAt,
		RequestID:                meta.RequestID,
		Payload:                  payload,
		InitialFailureOperation:  rec.Operation,
		InitialFailureStatusCode: rec.StatusCode,
		InitialFailureCode:       string(rec.FailureCode),
		InitialFailureReason:     rec.FailureReason,
		LastFailureReason:        rec.FailureReason,
	}
	// 送信先がRetry-Afterで待機を指定した場合は、初回のspool再送もその時刻まで行いません。
	// RetryAfterはFailedAtから数えた時間で、FailedAtが空の場合は失敗時刻と同じ時刻から数えます。
	// 上限による短縮は保存時刻を基準にStoreSendResultByStateが行います。
	if rec.RetryAfter > 0 {
		record.NotBefore = occurredAt.Add(rec.RetryAfter)
	}
	return record, nil
}

// capRetryAfterNotBeforeは、保存するrecordの再送下限時刻が現在時刻から上限より先であれば、
// 上限までに短縮して短縮内容を返します。短縮しない場合はnilです。
func capRetryAfterNotBefore(
	record *pkgspool.SpoolRecord,
	now time.Time,
	limit time.Duration,
) *pkgspool.RetryAfterClamp {
	if record.NotBefore.IsZero() {
		return nil
	}
	maxNotBefore := now.Add(limit).Round(0)
	if !record.NotBefore.After(maxNotBefore) {
		return nil
	}
	clamp := &pkgspool.RetryAfterClamp{
		Requested:     record.NotBefore.Sub(now),
		MaxRetryAfter: limit,
		NotBefore:     maxNotBefore,
	}
	record.NotBefore = maxNotBefore
	return clamp
}

// retryAfterDeadlineClampは、送信先全体の待機期限として記録する前に、上限を超えるRetry-Afterを
// 短縮した内容です。clampは最も長い指定の短縮で、recordCountは短縮したrecordの件数です。
type retryAfterDeadlineClamp struct {
	clamp       pkgspool.RetryAfterClamp
	recordCount int
}

// recordRetryAfterは、送信先がRetry-Afterで指定した期限を、同じspoolを再送するrunnerの送信先全体の
// 待機期限として記録・保存します。過負荷でspoolへ保存しない場合（drop・degrade）や保存先を用意
// できない場合も記録します。再送中のrunnerは次の周期からこの送信先のすべてのログに期限を適用し、
// 再起動後も読み込みます。期限はrecordへ保存する場合と同じく失敗時刻から数え、現在時刻から上限
// までに短縮します。記録を保存できない場合も、recordへ保存した期限が再送時の手掛かりとして残ります。
// 上限までに短縮して記録した場合は、その内容を返します。警告は呼び出し元が、spoolへ保存する
// recordの短縮と合わせて一回の送信失敗につき一度だけ通知します。記録しない場合はnilです。
func (s *SpoolStoreHandler) recordRetryAfter(result loggerfailure.SendResult) *retryAfterDeadlineClamp {
	if s == nil || s.retryAfterRecorder == nil {
		return nil
	}
	now := time.Now()
	if s.clock != nil {
		now = s.clock.Now()
	}
	limit := s.retryAfterLimit()
	maxNotBefore := now.Add(limit).Round(0)
	var (
		latest  time.Time
		clamped *retryAfterDeadlineClamp
	)
	for _, rec := range result.RetryableRecords {
		if rec.RetryAfter <= 0 {
			continue
		}
		failedAt := rec.FailedAt
		if failedAt.IsZero() {
			failedAt = result.Meta.AttemptedAt
		}
		if failedAt.IsZero() {
			failedAt = now
		}
		notBefore := failedAt.Add(rec.RetryAfter)
		if notBefore.After(maxNotBefore) {
			// recordへ保存する場合と同じく、現在時刻からの待機時間で最も長い指定を通知します。
			requested := notBefore.Sub(now)
			if clamped == nil {
				clamped = &retryAfterDeadlineClamp{}
			}
			clamped.recordCount++
			if clamped.recordCount == 1 || requested > clamped.clamp.Requested {
				clamped.clamp = pkgspool.RetryAfterClamp{Requested: requested, MaxRetryAfter: limit, NotBefore: maxNotBefore}
			}
			notBefore = maxNotBefore
		}
		if notBefore.After(latest) {
			latest = notBefore
		}
	}
	if latest.IsZero() {
		return nil
	}
	s.retryAfterRecorder.RecordRetryAfter(latest)
	return clamped
}

// notifyRetryAfterDeadlineClampは、送信先全体の待機期限として記録したRetry-Afterの短縮を警告します。
// spoolへ保存したrecordの短縮を通知済みの場合は、同じ指定を重ねて通知しないよう呼びません。
func (s *SpoolStoreHandler) notifyRetryAfterDeadlineClamp(
	ctx context.Context,
	meta loggerfailure.Meta,
	clamped *retryAfterDeadlineClamp,
) {
	if clamped == nil {
		return
	}
	s.notifyRetryAfterClamp(ctx, meta, clamped.clamp, clamped.recordCount)
}

func resolveOccurredAt(
	clock clock.Clock,
	rec loggerfailure.FailedRecord,
	meta loggerfailure.Meta,
) time.Time {
	if !rec.FailedAt.IsZero() {
		return rec.FailedAt
	}
	if !meta.AttemptedAt.IsZero() {
		return meta.AttemptedAt
	}

	// 仕様上は
	// 本来ここには到達しないはず
	return clock.Now()
}

func (s *SpoolStoreHandler) notifyWarn(ctx context.Context, msg string) {
	if strings.TrimSpace(msg) == "" {
		return
	}
	if s != nil && s.OnWarn != nil {
		s.OnWarn(
			ctx,
			&logging.LogEntry{
				OccurredAt: logging.ResolveOccurredAt(s.clock),
				Level:      pkglogger.LogLevelWarn,
				Message:    msg,
				Internal:   true,
				Source:     failureSpoolInternalLogSource,
			},
			s.Tracer,
			s.Meter,
		)
	}
}

// notifyRetryAfterClampは、送信先が上限を超えるRetry-Afterを指定したため、保存したrecordの
// 再送下限時刻を上限まで短縮したことを、一回の送信失敗ごとに一度だけ通知します。
func (s *SpoolStoreHandler) notifyRetryAfterClamp(
	ctx context.Context,
	meta loggerfailure.Meta,
	clamp pkgspool.RetryAfterClamp,
	recordCount int,
) {
	if s == nil || s.OnWarn == nil {
		return
	}
	s.OnWarn(
		ctx,
		&logging.LogEntry{
			OccurredAt: logging.ResolveOccurredAt(s.clock),
			Level:      pkglogger.LogLevelWarn,
			Message:    "failure spool Retry-After wait exceeds the maximum",
			Internal:   true,
			Source:     failureSpoolInternalLogSource,
			Fields: map[string]any{
				"backend":               meta.Backend,
				"request_id":            meta.RequestID,
				"requested_retry_after": clamp.Requested.String(),
				"max_retry_after":       clamp.MaxRetryAfter.String(),
				"not_before":            clamp.NotBefore,
				"record_count":          recordCount,
			},
		},
		s.Tracer,
		s.Meter,
	)
}

func (s *SpoolStoreHandler) notifyError(ctx context.Context, err error, msg string) {
	if err == nil {
		return
	}
	if s != nil && s.OnError != nil {
		s.OnError(
			ctx,
			err,
			&logging.LogEntry{
				OccurredAt: logging.ResolveOccurredAt(s.clock),
				Level:      pkglogger.LogLevelError,
				Message:    msg,
				Err:        err,
				Internal:   true,
				Source:     failureSpoolInternalLogSource,
			},
			s.Tracer,
			s.Meter,
		)
	}
}

// StoreSendResultByStateは送信元で確定済みのhandling dispositionをspool stateへ
// 機械的に対応させます。backend固有のfailure codeをこの層で再解釈しません。
// 送信先が指定した期限は、recordを保存する前に送信先全体の待機期限として記録・保存します。
// 上限を超える指定の短縮は、保存したrecordまたは記録した期限の短縮として一度だけ警告します。
func (s *SpoolStoreHandler) StoreSendResultByState(
	ctx context.Context,
	result loggerfailure.SendResult,
) error {
	deadlineClamp := s.recordRetryAfter(result)
	notified, err := s.storeSendResultByState(ctx, result)
	if !notified {
		s.notifyRetryAfterDeadlineClamp(ctx, result.Meta, deadlineClamp)
	}
	return err
}

// storeSendResultByStateは、送信先の待機期限を記録済みの結果をspool stateへ保存します。
// retryAfterClampNotifiedは、保存したrecordのRetry-Afterの短縮を警告したかを表します。
func (s *SpoolStoreHandler) storeSendResultByState(
	ctx context.Context,
	result loggerfailure.SendResult,
) (retryAfterClampNotified bool, _ error) {
	var errs []error
	activeRecords := make([]pkgspool.SpoolRecord, 0, len(result.RetryableRecords))
	deadRecords := make([]pkgspool.SpoolRecord, 0, len(result.DeadRecords))
	quarantineRecords := make([]pkgspool.SpoolRecord, 0, len(result.QuarantineRecords))

	// 送信先が上限を超えるRetry-Afterを指定した場合は、保存時刻から上限までの待機に短縮します。
	// 同じ応答に由来する複数recordの短縮は、最も長い指定で一度だけ通知します。
	limit := s.retryAfterLimit()
	var clamp *pkgspool.RetryAfterClamp
	clampedRecords := 0
	if len(result.RetryableRecords) > 0 {
		now := s.clock.Now()
		for _, rec := range result.RetryableRecords {
			record, err := toSpoolRecord(s.clock, rec, result.Meta)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if recordClamp := capRetryAfterNotBefore(&record, now, limit); recordClamp != nil {
				clampedRecords++
				if clamp == nil || recordClamp.Requested > clamp.Requested {
					clamp = recordClamp
				}
			}
			activeRecords = append(activeRecords, record)
		}
	}

	for _, rec := range result.DeadRecords {
		record, err := toSpoolRecord(s.clock, rec, result.Meta)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		deadRecords = append(deadRecords, record)
	}

	for _, rec := range result.QuarantineRecords {
		record, err := toSpoolRecord(s.clock, rec, result.Meta)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		quarantineRecords = append(quarantineRecords, record)
	}

	if err := s.Store.StoreByState(ctx, pkgspool.StateActive, activeRecords); err != nil {
		errs = append(errs, fmt.Errorf("failed to store retryable records to %s spool: %w", pkgspool.StateActive, err))
	} else if clamp != nil {
		s.notifyRetryAfterClamp(ctx, result.Meta, *clamp, clampedRecords)
		retryAfterClampNotified = true
	}
	if err := s.Store.StoreByState(ctx, pkgspool.StateDead, deadRecords); err != nil {
		errs = append(errs, fmt.Errorf("failed to store dead records to dead spool: %w", err))
	}
	if err := s.Store.StoreByState(ctx, pkgspool.StateQuarantine, quarantineRecords); err != nil {
		errs = append(errs, fmt.Errorf("failed to store quarantine records to quarantine spool: %w", err))
	}

	if len(errs) > 0 {
		return retryAfterClampNotified, errors.Join(errs...)
	}

	return retryAfterClampNotified, nil
}
