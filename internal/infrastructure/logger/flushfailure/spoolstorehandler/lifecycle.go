package spoolstorehandler

import (
	"context"
	"errors"
	"fmt"
)

var ErrSpoolStoreHandlerClosing = errors.New("spool store handler is closing")

var (
	// ErrNilDurablePayloadは、spoolへ保存するrecordのpayloadがnilであることを表します。
	ErrNilDurablePayload = errors.New("durable payload is nil")
	// ErrInvalidSpoolOverloadPolicyConfigは、過負荷時の設定が不正であることを表します。
	// 原因の検証errorも同時に判定できます。
	ErrInvalidSpoolOverloadPolicyConfig = errors.New("invalid spool overload policy config")
	// ErrUnsupportedSpoolOverloadPolicyは、過負荷時のpolicyが対応外であることを表します。
	ErrUnsupportedSpoolOverloadPolicy = errors.New("unsupported spool overload policy")
)

// shutdownStateは、failure処理の受付可否とShutdownの進行状態を表します。
type shutdownState uint8

const (
	// shutdownStateAcceptingは、新しいfailure処理を受け付ける通常状態です。
	shutdownStateAccepting shutdownState = iota
	// shutdownStateStartedは、受付停止後に受理済み処理または非同期scanの
	// 終了待ちが未完了の状態です。
	shutdownStateStarted
	// shutdownStateCompletedは、受理済み処理とdirectory統計cacheを更新する
	// 非同期scanの終了待ちが完了した状態です。
	shutdownStateCompleted
)

// Shutdownは新しいfailureの受付を停止し、受理済みのHandleFailureと、
// そこから開始した非同期directory scanがすべて終了するまで待機します。
// context終了で待機を完了できなかった場合は終了未完の状態を維持し、
// 次回のShutdownで同じ処理の完了を待ち直せます。受理済み処理へ中止は伝えません。
func (s *SpoolStoreHandler) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// 受付停止と完了通知の取得を同じlock内で行い、待機自体にはlockを持ちません。
	// 待つ呼び出し元の期限が切れても、実際の保存・通知の完了は別に追跡します。
	s.operationMu.Lock()
	if s.shutdownState == shutdownStateCompleted {
		s.operationMu.Unlock()
		return nil
	}
	s.shutdownState = shutdownStateStarted
	var operationDone <-chan struct{}
	if s.operationCount > 0 {
		if s.operationDone == nil {
			s.operationDone = make(chan struct{})
		}
		operationDone = s.operationDone
	}
	s.operationMu.Unlock()
	if operationDone != nil {
		select {
		case <-operationDone:
		case <-ctx.Done():
			return fmt.Errorf("spool store handler shutdown incomplete: %w", ctx.Err())
		}
	}

	// HandleFailureが終わるまでに開始した非同期scanも待ちます。
	if done := s.currentDirStatsScanDone(); done != nil {
		select {
		case <-done:
			// directory統計scanが完了したため、Shutdownの完了処理へ進みます。
		case <-ctx.Done():
			return fmt.Errorf("spool store handler shutdown incomplete: %w", ctx.Err())
		}
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("spool store handler shutdown incomplete: %w", err)
	}

	s.operationMu.Lock()
	s.shutdownState = shutdownStateCompleted
	s.operationMu.Unlock()
	return nil
}

// currentDirStatsScanDoneはactive spool directoryの統計scanが実行中なら、
// cache更新とscan終了時にcloseされるchannelを返します。
func (s *SpoolStoreHandler) currentDirStatsScanDone() <-chan struct{} {
	s.overloadCache.mu.RLock()
	defer s.overloadCache.mu.RUnlock()
	return s.overloadCache.activeScanDone
}
