package file

import (
	"sync"
	"time"
)

// fileSeriesUploadRetryScheduleは、inlineのFileLoggerが担当する一つのログ系列について、
// 残存ログの送信・退避・削除を次に試してよい時刻を保持します。
// 予約時刻を過ぎた後のログ書き込みを契機に、cleanupが残存ログを走査して処理します。
// この型は時刻の記録と判定だけを行い、タイマーやgoroutineは起動しません。
// 時刻は呼び出し元のClockから受け取り、タイムゾーンを変換せずに使います。
// 対象ファイルのパスも保持せず、cleanupの走査で再発見します。
// ディレクトリ使用量の記録とは別に管理し、同じディレクトリでもログ系列ごとに予約を持ちます。
type fileSeriesUploadRetrySchedule struct {
	// muは、cleanupとrotationが並行して読み書きする予約情報を保護します。
	mu sync.Mutex

	// retryAtは次に処理してよい時刻です。nilは予約なし、ゼロ時刻へのポインタは即時の予約です。
	// 読み書きはmuで保護し、更新時は時刻の値を変更せず、新しいポインタへ置き換えます。
	retryAt *time.Time
}

// scheduleImmediateRetryは、次のcleanupで待ち時間なく残存ログを確認できるよう予約します。
// FileLoggerの初期化時に使い、再起動前から残っているファイルも最初の書き込み後に確認します。
// 呼び出した時点では走査せず、既存の予約がある場合は即時の予約へ置き換えます。
func (s *fileSeriesUploadRetrySchedule) scheduleImmediateRetry() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.retryAt = new(time.Time)
	s.mu.Unlock()
}

// scheduleRetryIfUnscheduledは、予約がない場合だけ、発見時刻discoveredAtからretryInterval後を予約します。
// cleanupで残存ファイルを発見したときや、残っていた元ログの圧縮を完了したときに使います。
// 即時の予約を含め、既存の予約は変更しません。
func (s *fileSeriesUploadRetrySchedule) scheduleRetryIfUnscheduled(
	discoveredAt time.Time,
	retryInterval time.Duration,
) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retryAt != nil {
		return
	}
	retryAt := discoveredAt.Add(retryInterval)
	s.retryAt = &retryAt
}

// scheduleRetryは、処理が未完了だった時刻failedAtからretryInterval後に再試行できるよう予約します。
// 既存の予約と新しい予約では早い方を採用し、失敗が続いても予約時刻を後ろへ延ばしません。
// ただし、初期化時の即時の予約は置き換え、失敗直後の書き込みで再試行が続くことを防ぎます。
func (s *fileSeriesUploadRetrySchedule) scheduleRetry(
	failedAt time.Time,
	retryInterval time.Duration,
) {
	if s == nil {
		return
	}
	retryAt := failedAt.Add(retryInterval)

	s.mu.Lock()
	if s.retryAt == nil || s.retryAt.IsZero() || retryAt.Before(*s.retryAt) {
		s.retryAt = &retryAt
	}
	s.mu.Unlock()
}

// isRetryDueは、予約があり、nowの時点で残存ログの処理を始めてよいかを返します。予約は消しません。
// 予約時刻に達した場合に加え、時計が予約の基準時刻より前へ戻った場合もtrueです。
// retryIntervalには予約時と同じ間隔を渡し、その基準時刻を予約時刻から逆算します。
func (s *fileSeriesUploadRetrySchedule) isRetryDue(
	now time.Time,
	retryInterval time.Duration,
) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isRetryDueLocked(now, retryInterval)
}

// claimForAttemptは、isRetryDueと同じ判定で実行可能な予約がある場合に、その予約を使用済みとして
// 解除し、trueを返します。予約がない場合や、まだ実行可能でない場合はfalseです。
// 呼び出し元は、走査と対象ファイルの保護を終え、送信を始める前にlifecycle lock内で呼びます。
// 予約は処理開始前に解除し、未完了なら呼び出し元がscheduleRetryで再予約します。成功後に予約を
// 消す処理を設けないことで、送信中の別のrotationが登録した新しい予約を消さないようにします。
func (s *fileSeriesUploadRetrySchedule) claimForAttempt(
	now time.Time,
	retryInterval time.Duration,
) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isRetryDueLocked(now, retryInterval) {
		return false
	}
	s.retryAt = nil
	return true
}

// backoffDueは、実行可能な予約がある場合、その時刻をnowからretryInterval後へ延期します。
// cleanupでの走査などに失敗し、予約を使って処理を開始できなかった場合に呼びます。
// 予約は残し、ログを書き込むたびに同じ失敗を繰り返さないよう、次の試行まで間隔を空けます。
// 予約がない場合や、まだ実行可能でない予約は変更しません。
func (s *fileSeriesUploadRetrySchedule) backoffDue(
	now time.Time,
	retryInterval time.Duration,
) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isRetryDueLocked(now, retryInterval) {
		return
	}
	retryAt := now.Add(retryInterval)
	s.retryAt = &retryAt
}

// isRetryDueLockedは、予約があり、実行可能な時刻になっているかを判定します。呼び出し元はmuを保持します。
// 判定後に予約を解除・延期する場合は、その更新までmuを保持し、途中で予約が変更されることを防ぎます。
func (s *fileSeriesUploadRetrySchedule) isRetryDueLocked(
	now time.Time,
	retryInterval time.Duration,
) bool {
	if s.retryAt == nil {
		return false
	}
	// 時計が予約の基準時刻より前へ戻った場合は、待ち時間を延ばさず実行を許可します。
	scheduleBaseTime := s.retryAt.Add(-retryInterval)
	return now.Before(scheduleBaseTime) || !now.Before(*s.retryAt)
}
