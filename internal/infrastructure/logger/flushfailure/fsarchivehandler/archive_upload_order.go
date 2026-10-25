package fsarchivehandler

import (
	"context"
	"slices"
	"sync"
	"time"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// readyArchiveDestinationIDsは、古い世代の未完了処理を追い越さずに、今回の世代を送信できる宛先IDを返します。
// 順序の確認には、未圧縮の元ログと、保護中の処理が扱うパスも含めます。
// 古い世代の圧縮・送信が未完了なら、その世代が未完了の宛先だけを今回の送信対象から外します。
func (f *FsArchiveHandler) readyArchiveDestinationIDs(
	ctx context.Context,
	operation *archiveFileOperation,
	destinations []objectstorage.DestinationID,
) ([]objectstorage.DestinationID, error) {
	var ready []objectstorage.DestinationID
	check := func() error {
		keys, err := checkpointstore.ScanExistingFileKeys(operation.fileName)
		if err != nil {
			return err
		}
		paths := make([]string, 0)
		for path := range f.protectedArchivePaths(operation.category) {
			paths = append(paths, path)
		}
		for _, generation := range checkpointstore.ArchiveCheckpointGenerationKeys(paths, operation.fileName) {
			keys = append(keys, checkpointstore.CheckpointKey{Generation: generation})
		}
		ready, err = checkpointstore.ReadyDestinationIDs(ctx, operation.store, operation.key, destinations, keys)
		return err
	}
	state := f.directoryForFailureCategory(operation.category)
	var err error
	if state != nil && state.guard != nil {
		err = state.guard.RunWithLifecycleLockContext(ctx, check)
	} else {
		err = check()
	}
	return ready, err
}

// archiveGenerationOrderWaitsは、古い世代の送信・完了記録を追い越さないために待機している世代の処理です。
// 系列の完了記録が進んだ時点で受理済みの再開処理へ戻し、再開間隔を待たずに再試行させます。
// 完了記録を伴わずに待機が解けた場合に備え、再開間隔の経過でも再試行します。
// ゼロ値から使用できます。Shutdownで受付を閉じると、残りを受理済みの再開処理へ移します。
type archiveGenerationOrderWaits struct {
	mu      sync.Mutex
	closed  bool
	entries map[archiveLogFileProcessingRecoveryKey]archiveGenerationOrderWait
	// progressは系列ごとに完了記録が進んだ回数です。試行の間に進んでいれば、
	// 待機へ移さずに再試行し、完了の通知を取りこぼさないようにします。
	progress map[failurearchive.FailureCategory]uint64
	fallback *time.Timer
	// fallbackIntervalは完了記録の通知を伴わない待機解除に備えた再試行間隔です。
	// 0なら受理済み再開処理の既定の間隔を使います。
	fallbackInterval time.Duration
	// handoffNotifyHookForTestは試験だけが設定する観測点（test seam）で、本番では常にnilです。
	// 引き渡しの完了を通知する（handoffをcloseする）直前に、muを保持したまま引き渡したkeyで
	// 呼びます。試験は、通知の時点で世代が受理済みの再開処理へ登録済みであることを確かめます。
	// muを保持しているため、hookからmuを取る処理を呼んではいけません。設定もmuを保持して行います。
	handoffNotifyHookForTest func(archiveLogFileProcessingRecoveryKey)
}

// archiveGenerationOrderWaitは待機中の一世代の処理と、待機の間の保護です。
type archiveGenerationOrderWait struct {
	operation  *archiveFileOperation
	protection archivePathProtection
	// handoffは、待機へ移した受理済みの再開処理が同じkeyでまだ登録されている間だけ非nilです。
	// その間は同じkeyで再登録できないため、再開の要求をresumeRequestedへ記録し、
	// 登録の解除後にcompleteArchiveGenerationOrderHandoffが再開処理へ戻してから閉じます。
	handoff         chan struct{}
	resumeRequested bool
}

type archiveGenerationOrderDecision uint8

const (
	// archiveGenerationOrderWaitingは、待機へ移して保護を引き継いだことを表します。
	archiveGenerationOrderWaiting archiveGenerationOrderDecision = iota
	// archiveGenerationOrderRetryは、試行の間に完了記録が進んだため直ちに再試行することを表します。
	archiveGenerationOrderRetry
	// archiveGenerationOrderClosedは、受付を閉じたため受理済みの再開処理の間隔で待つことを表します。
	archiveGenerationOrderClosed
)

// archiveGenerationOrderProgressは、送信の試行を始める前に観測する系列の完了記録の進み具合です。
func (f *FsArchiveHandler) archiveGenerationOrderProgress(category failurearchive.FailureCategory) uint64 {
	waits := &f.generationOrderWaits
	waits.mu.Lock()
	defer waits.mu.Unlock()
	return waits.progress[category]
}

// awaitArchiveGenerationOrderは、古い世代を待つだけで失敗していない処理を待機へ移します。
// observedは試行を始める前のarchiveGenerationOrderProgressです。待機の間も世代を保護し、
// 呼び出し元の保護や受理済み再開処理の登録を解除してよい状態にしてから戻ります。
// handoffは、同じkeyで登録した受理済みの再開処理から移す場合にtrueを指定します。その場合は
// 登録の解除後にcompleteArchiveGenerationOrderHandoffを呼んでください。
func (f *FsArchiveHandler) awaitArchiveGenerationOrder(
	operation *archiveFileOperation,
	observed uint64,
	handoff bool,
) archiveGenerationOrderDecision {
	waits := &f.generationOrderWaits
	waits.mu.Lock()
	defer waits.mu.Unlock()
	if waits.closed {
		return archiveGenerationOrderClosed
	}
	if waits.progress[operation.category] != observed {
		return archiveGenerationOrderRetry
	}
	if waits.entries == nil {
		waits.entries = make(map[archiveLogFileProcessingRecoveryKey]archiveGenerationOrderWait)
	}
	entry := archiveGenerationOrderWait{
		operation:  operation,
		protection: f.protectArchiveOperationPaths(operation),
	}
	if handoff {
		entry.handoff = make(chan struct{})
	}
	waits.entries[failureArchiveRecoveryKey(operation.category, operation.key)] = entry
	f.armArchiveGenerationOrderFallbackLocked()
	return archiveGenerationOrderWaiting
}

// completeArchiveGenerationOrderHandoffは、待機へ移した受理済みの再開処理の登録が解除された後に呼び、
// 引き渡しの間に再開を要求された世代、または受付を閉じた後の世代を受理済みの再開処理へ戻します。
// 引き渡しの完了は、受理済みの再開処理へ戻し終えてから通知します。先に通知すると、待っていた
// closeArchiveGenerationOrderWaitsの後で再開処理が閉じられ、戻す登録が拒否されて世代を送信しません。
func (f *FsArchiveHandler) completeArchiveGenerationOrderHandoff(key archiveLogFileProcessingRecoveryKey) {
	waits := &f.generationOrderWaits
	waits.mu.Lock()
	defer waits.mu.Unlock()
	entry, ok := waits.entries[key]
	if !ok || entry.handoff == nil {
		return
	}
	defer waits.notifyHandoffLocked(key, entry.handoff)
	entry.handoff = nil
	if !entry.resumeRequested && !waits.closed {
		waits.entries[key] = entry
		return
	}
	delete(waits.entries, key)
	f.acceptArchiveOperationContinuation(entry.operation, nil, entry.protection)
}

// notifyHandoffLockedは、muを保持したまま引き渡しの完了を通知し、待っている
// closeArchiveGenerationOrderWaitsを再開させます。試験用の観測点が設定されていれば通知の直前に呼びます。
func (w *archiveGenerationOrderWaits) notifyHandoffLocked(key archiveLogFileProcessingRecoveryKey, handoff chan struct{}) {
	if w.handoffNotifyHookForTest != nil {
		w.handoffNotifyHookForTest(key)
	}
	close(handoff)
}

// resumeArchiveGenerationOrderWaitsは、系列の完了記録が進んだことを記録し、
// 待機中の世代を古い順に受理済みの再開処理へ戻して直ちに再試行させます。
// 再開処理の登録順に試行するため、古い世代の完了後に新しい世代を確認します。
func (f *FsArchiveHandler) resumeArchiveGenerationOrderWaits(category failurearchive.FailureCategory) {
	waits := &f.generationOrderWaits
	waits.mu.Lock()
	defer waits.mu.Unlock()
	if waits.progress == nil {
		waits.progress = make(map[failurearchive.FailureCategory]uint64)
	}
	waits.progress[category]++
	if waits.closed {
		return
	}
	// 待機から外す処理と再開処理への登録を同じlock内で行い、
	// 所有の確認が両方に存在しない瞬間を観測しないようにします。
	for _, entry := range waits.takeLocked(func(key archiveLogFileProcessingRecoveryKey) bool {
		return key.Category == category
	}) {
		f.acceptArchiveOperationContinuation(entry.operation, nil, entry.protection)
	}
}

// closeArchiveGenerationOrderWaitsは待機の受付を閉じ、残りを古い順に受理済みの再開処理へ移します。
// 受理済みの再開処理を閉じる前に呼び、終了時もその完了を待てるようにします。
// 引き渡し中の世代は、元の登録が解除されて再開処理へ戻るまで待ってから戻ります。
func (f *FsArchiveHandler) closeArchiveGenerationOrderWaits() {
	waits := &f.generationOrderWaits
	waits.mu.Lock()
	if !waits.closed {
		waits.closed = true
		if waits.fallback != nil {
			waits.fallback.Stop()
			waits.fallback = nil
		}
		for _, entry := range waits.takeLocked(func(archiveLogFileProcessingRecoveryKey) bool { return true }) {
			f.acceptArchiveOperationContinuation(entry.operation, nil, entry.protection)
		}
	}
	var handoffs []chan struct{}
	for _, entry := range waits.entries {
		if entry.handoff != nil {
			handoffs = append(handoffs, entry.handoff)
		}
	}
	waits.mu.Unlock()
	// 引き渡しの完了処理は受付を閉じたことを確認し、受理済みの再開処理へ移します。
	for _, handoff := range handoffs {
		<-handoff
	}
}

// archiveGenerationContinuationAcceptedは、世代の処理を受理済みの再開処理または順序待ちが所有しているかを返します。
func (f *FsArchiveHandler) archiveGenerationContinuationAccepted(
	category failurearchive.FailureCategory,
	key checkpointstore.CheckpointKey,
) bool {
	recoveryKey := failureArchiveRecoveryKey(category, key)
	waits := &f.generationOrderWaits
	// 待機から再開処理への移動は同じlock内で行うため、両方を同じlock内で確認します。
	waits.mu.Lock()
	defer waits.mu.Unlock()
	if _, waiting := waits.entries[recoveryKey]; waiting {
		return true
	}
	return f.logFileProcessingRecovery.Has(recoveryKey)
}

// takeLockedは条件に一致する待機を、系列ごとに古い世代から順に取り出します。
// 引き渡し中の世代は取り出さず、引き渡しの完了後に再開するよう記録します。
func (w *archiveGenerationOrderWaits) takeLocked(
	match func(archiveLogFileProcessingRecoveryKey) bool,
) []archiveGenerationOrderWait {
	keys := make([]archiveLogFileProcessingRecoveryKey, 0, len(w.entries))
	for key, entry := range w.entries {
		if !match(key) {
			continue
		}
		if entry.handoff != nil {
			entry.resumeRequested = true
			w.entries[key] = entry
			continue
		}
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(left, right archiveLogFileProcessingRecoveryKey) int {
		if left.Category != right.Category {
			if left.Category < right.Category {
				return -1
			}
			return 1
		}
		return checkpointstore.CompareGenerations(left.File.Generation, right.File.Generation)
	})
	entries := make([]archiveGenerationOrderWait, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, w.entries[key])
		delete(w.entries, key)
	}
	return entries
}

// armArchiveGenerationOrderFallbackLockedは、完了記録の通知を伴わない待機解除に備えて、
// 再開間隔の経過後に待機中の世代を再試行します。
func (f *FsArchiveHandler) armArchiveGenerationOrderFallbackLocked() {
	waits := &f.generationOrderWaits
	if waits.closed || waits.fallback != nil || len(waits.entries) == 0 {
		return
	}
	interval := waits.fallbackInterval
	if interval <= 0 {
		interval = logfile.DefaultLogFileProcessingRecoveryInterval
	}
	var timer *time.Timer
	timer = time.AfterFunc(interval, func() {
		waits.mu.Lock()
		if waits.fallback == timer {
			waits.fallback = nil
		}
		waits.mu.Unlock()
		for _, category := range []failurearchive.FailureCategory{
			failurearchive.FailureCategoryRetryable,
			failurearchive.FailureCategoryPermanent,
		} {
			f.resumeArchiveGenerationOrderWaits(category)
		}
	})
	waits.fallback = timer
}
