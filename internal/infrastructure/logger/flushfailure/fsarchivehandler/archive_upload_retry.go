package fsarchivehandler

import (
	"sync"
	"time"
)

// archiveUploadRetryScheduleは一つのfailure archive file系列について、localに残った
// compressed archiveを次にtarget-only走査する時刻を保持します。
// directory使用量の概算Trackerとは独立しており、upload失敗だけでdirectory全体観測を
// 起動しません。local compressed file自体がpendingの永続状態であるため、
// process内にpath一覧は保持せず、期限時にGlobByTargetで再発見します。
type archiveUploadRetrySchedule struct {
	mu        sync.Mutex
	scheduled bool
	retryAt   time.Time
}

func (s *archiveUploadRetrySchedule) due(
	now time.Time,
	retryInterval time.Duration,
) bool {
	now = now.UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dueLocked(now, retryInterval)
}

// claimForAttemptは期限済みscheduleをremote I/O開始前にclearします。この後の
// 成功経路はscheduleを変更しないため、upload中に別rotationがarmした
// 新しい失敗状態を古い成功がclearすることはありません。
func (s *archiveUploadRetrySchedule) claimForAttempt(
	now time.Time,
	retryInterval time.Duration,
) bool {
	now = now.UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.scheduled || !s.dueLocked(now, retryInterval) {
		return false
	}
	s.scheduled = false
	s.retryAt = time.Time{}
	return true
}

// armはuploadまたはlocal削除失敗をUsageCacheTTL後の再試行へ予約します。
// 実際の失敗から作られたscheduleが先にある場合は早い方の時刻を維持します。
// process起動時の即時bootstrapだけは、実際の失敗が判明した時点で
// その失敗時刻を起点とするbackoffへ置き換えます。
func (s *archiveUploadRetrySchedule) arm(
	failedAt time.Time,
	ttl time.Duration,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	retryAt := failedAt.UTC().Add(ttl)
	if !s.scheduled || s.retryAt.IsZero() || retryAt.Before(s.retryAt) {
		s.retryAt = retryAt
	}
	s.scheduled = true
}

// armImmediateはprocess再起動後にlocalへ残ったcompressed archiveを発見する
// bootstrap用です。zero timeは常に期限済みを表し、最初の通常cleanupで走査します。
func (s *archiveUploadRetrySchedule) armImmediate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scheduled = true
	s.retryAt = time.Time{}
}

// armIfUnscheduledはfull scanでlocal compressed archiveを再発見した場合に、失われた
// scheduleだけを復元します。bootstrapやupload failureの既存scheduleは変更しません。
func (s *archiveUploadRetrySchedule) armIfUnscheduled(
	discoveredAt time.Time,
	retryInterval time.Duration,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.scheduled {
		return
	}
	s.scheduled = true
	s.retryAt = discoveredAt.UTC().Add(retryInterval)
}

// backoffDueはtarget-only走査自体が失敗したとき、同じ失敗を毎writeで
// 即時反復しないよう再試行をUsageCacheTTL後へ遅延します。
func (s *archiveUploadRetrySchedule) backoffDue(
	failedAt time.Time,
	ttl time.Duration,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := failedAt.UTC()
	if !s.dueLocked(now, ttl) {
		return
	}
	s.retryAt = now.Add(ttl)
}

func (s *archiveUploadRetrySchedule) dueLocked(
	now time.Time,
	retryInterval time.Duration,
) bool {
	if !s.scheduled {
		return false
	}
	armedAt := s.retryAt.Add(-retryInterval)
	return now.Before(armedAt) || !now.Before(s.retryAt)
}
