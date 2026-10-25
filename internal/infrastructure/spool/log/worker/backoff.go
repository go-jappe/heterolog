package worker

import "time"

const (
	defaultProcessingBackoffMaxPathCacheEntries = 8192
	defaultProcessingPathSkipHoldDuration       = 30 * time.Second
)

// ProcessingPathBackoffConfigは、送信前の読み込み・隔離に失敗したファイルを
// 一時的に処理対象から外すための設定です。spool上の配置状態は変更しません。
type ProcessingPathBackoffConfig struct {
	// MaxPathCacheEntriesは、待機対象として保持するパスの最大件数です。
	// 上限に達したら新しいパスは記録せず、既存の記録を維持します。
	// 未記録のファイルについても、Workerが探索位置を進めて後続へ順番を回します。
	MaxPathCacheEntries int

	// PathSkipHoldDurationは、最初の登録から記録全体を保持する時間です。
	// 個別の期限は持たず、期限切れで全件を解除します。
	// 解除してもWorkerの探索位置は戻さないため、後続の探索は継続します。
	PathSkipHoldDuration time.Duration
}

// ProcessingPathBackoffは、送信前の処理に失敗したパスを上限付きで保持します。
// 候補のパスが記録と一致したときだけ待機対象とし、件数による先頭の読み飛ばしは行いません。
// 探索位置はWorkerが別に保持します。この記録は再起動時に失われても構いません。
// 単一Workerの直列実行から使用し、内部では排他しません。
type ProcessingPathBackoff struct {
	cfg ProcessingPathBackoffConfig

	// pathSkipCacheは、待機対象のパスと、その原因を保持します。
	pathSkipCache map[string]ProcessingPathBackoffEntry
	// pathSkipCacheExpireAtは記録全体の期限です。空なら待機対象はありません。
	pathSkipCacheExpireAt time.Time
}

// ProcessingPathBackoffEntryは、待機対象になった原因の診断情報です。
// パスはmapのキーに保持し、再試行回数や個別の期限は記録しません。
type ProcessingPathBackoffEntry struct {
	// Operationは、読み込みや隔離など、待機の原因となった操作です。
	Operation ProcessingPathBackoffOp
	// ErrorTextは原因のエラー概要です。永続化や再試行の判定には使用しません。
	ErrorText string
}

func NewProcessingPathBackoff(cfg ProcessingPathBackoffConfig) *ProcessingPathBackoff {
	if cfg.MaxPathCacheEntries <= 0 {
		cfg.MaxPathCacheEntries = defaultProcessingBackoffMaxPathCacheEntries
	}
	if cfg.PathSkipHoldDuration <= 0 {
		cfg.PathSkipHoldDuration = defaultProcessingPathSkipHoldDuration
	}
	return &ProcessingPathBackoff{
		cfg:           cfg,
		pathSkipCache: make(map[string]ProcessingPathBackoffEntry),
	}
}

// PathCacheCountは、期限切れの解除を行わず、保持中のパスの件数を返します。
func (b *ProcessingPathBackoff) PathCacheCount() int {
	if b == nil {
		return 0
	}
	return len(b.pathSkipCache)
}

// PathCacheCountAtは、期限切れの記録を解除してから、保持中のパスの件数を返します。
func (b *ProcessingPathBackoff) PathCacheCountAt(now time.Time) int {
	if b == nil {
		return 0
	}
	b.clearExpiredPathCache(now)
	return len(b.pathSkipCache)
}

// ShouldSkipPathは、指定パスが期限内の待機対象であればtrueを返します。
func (b *ProcessingPathBackoff) ShouldSkipPath(path string, now time.Time) bool {
	if b == nil || path == "" {
		return false
	}
	b.clearExpiredPathCache(now)
	_, exists := b.pathSkipCache[path]
	return exists
}

// BlockPathは、指定パスを待機対象に登録します。
// 上限に達した場合は追加せず、既存の記録を保持します。
// 記録できなかったパスについても、Workerは探索位置を進めてから次の候補を選びます。
// そのパスに戻るのは探索が一周した後です。
func (b *ProcessingPathBackoff) BlockPath(
	path string,
	operation ProcessingPathBackoffOp,
	err error,
	now time.Time,
) {
	if b == nil || path == "" {
		return
	}
	b.clearExpiredPathCache(now)
	if _, exists := b.pathSkipCache[path]; exists {
		return
	}
	if len(b.pathSkipCache) >= b.cfg.MaxPathCacheEntries {
		return
	}
	if b.pathSkipCacheExpireAt.IsZero() {
		b.pathSkipCacheExpireAt = now.Add(b.cfg.PathSkipHoldDuration)
	}
	b.pathSkipCache[path] = ProcessingPathBackoffEntry{
		Operation: operation,
		ErrorText: errorText(err),
	}
}

// UnblockPathは、指定パスの待機を解除します。
// 送信結果が確定した後の後処理は、別のProcessingFileTransitionRetryが管理します。
func (b *ProcessingPathBackoff) UnblockPath(path string) {
	if b == nil || path == "" {
		return
	}
	delete(b.pathSkipCache, path)
	if len(b.pathSkipCache) == 0 {
		b.pathSkipCacheExpireAt = time.Time{}
	}
}

// clearExpiredPathCacheは、保持期限が切れていれば記録全体を解除します。
// Workerの探索位置には影響しません。
func (b *ProcessingPathBackoff) clearExpiredPathCache(now time.Time) {
	if len(b.pathSkipCache) == 0 {
		b.pathSkipCacheExpireAt = time.Time{}
		return
	}
	if !b.pathSkipCacheExpireAt.IsZero() && now.Before(b.pathSkipCacheExpireAt) {
		return
	}
	clear(b.pathSkipCache)
	b.pathSkipCacheExpireAt = time.Time{}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
