package worker

import (
	"errors"
	"sync"
	"time"

	"github.com/go-jappe/heterolog/pkg/spool"
)

// RetryAfterStateは、送信先の待機期限をprocessの外へ保存する内容です。
type RetryAfterState struct {
	// NotBeforeは、送信先へ次に送信してよい時刻です。上限までに短縮済みの値です。
	NotBefore time.Time
	// AppliedThroughは、recordに保存された再送下限時刻（SpoolRecord.NotBefore）のうち、
	// NotBeforeへ反映済みの最も遅い時刻です。この時刻以前の保存済み期限は、再起動後も
	// 送信先の待機を延ばしません。NotBefore以上の値です。
	AppliedThrough time.Time
}

// RetryAfterStoreは、Workerが担当する送信先の待機期限を、再起動後も参照できる場所へ保存します。
// RetryAfterDeadlineは最初の利用時にLoadを一度だけ呼び、期限を記録・延長するたびにSave、
// 期限と反映済みの保存期限をすべて過ぎた後にRemoveを呼びます。呼び出しは直列化します。
// 各メソッドのerrorは警告として扱い、memory上の期限で待機を続けます。
type RetryAfterStore interface {
	// Loadは保存済みの状態を返します。期限を過ぎた保存内容は削除してzero valueを返し、
	// nowからmaxWaitより先の期限はnow+maxWaitへ短縮して保存し、短縮内容も返します。
	// 短縮内容とerrorを両方返す場合は、短縮した状態を保存できていない可能性があり、
	// RetryAfterDeadlineは以後の周期で保存を再試行します。
	// 読めない・壊れた保存内容はzero valueとerrorを返します。errorがあっても返した状態は有効です。
	Load(now time.Time, maxWait time.Duration) (RetryAfterState, *spool.RetryAfterClamp, error)
	// Saveは状態を置き換えて保存します。
	Save(state RetryAfterState) error
	// Removeは保存内容を削除します。保存内容がなければ何もしません。
	Remove() error
}

// WithRetryAfterStoreは、Retry-Afterによる送信先の待機期限を保存する先を設定します。
// nilの場合は期限をmemoryだけに保持し、再起動後は引き継ぎません。
func WithRetryAfterStore(store RetryAfterStore) Option {
	return func(w *Worker) { w.retryAfterStore = store }
}

// WithRetryAfterSaveRetryIntervalは、保存に失敗した待機期限を再び保存するまでの最初の間隔です。
// runnerの周期を渡します。失敗が続くたびに間隔を倍にし、maxRetryAfterSaveRetryBackoffを
// 上限とします。0以下ならdefaultRetryAfterSaveRetryIntervalを使います。
func WithRetryAfterSaveRetryInterval(interval time.Duration) Option {
	return func(w *Worker) { w.retryAfterSaveRetryInterval = interval }
}

const (
	// defaultRetryAfterSaveRetryIntervalは、runnerの周期を渡さずに構築したWorkerが、保存に
	// 失敗した待機期限を最初に再び保存するまでの間隔です。
	defaultRetryAfterSaveRetryInterval = time.Second
	// maxRetryAfterSaveRetryBackoffは、保存の再試行の間隔の上限です。
	maxRetryAfterSaveRetryBackoff = time.Minute
)

// RetryAfterDeadlineは、一つの送信先のRetry-Afterによる待機期限です。Workerが所有し、同じ
// spool rootへ書き込む通常送信の保存処理とも共有します。通常送信で受け取った期限も、次の周期
// から送信先のすべてのrecordに適用します。RetryAfterStoreがあれば期限を保存し、最初の利用時
// （最初の送信・保存の前）に一度だけ読み込みます。保存に失敗した期限もmemoryで守り、期限が
// 来るまでは、runnerの周期から始めて失敗のたびに倍にする間隔（上限1分）で保存を再試行します。
// メソッドは並行に呼び出せます。
type RetryAfterDeadline struct {
	mu      sync.Mutex
	store   RetryAfterStore
	maxWait time.Duration
	// retryIntervalは、保存に失敗した期限を最初に再び保存するまでの間隔です。
	retryInterval time.Duration

	loaded bool
	// notBeforeは、送信先へ次に送信してよい時刻です。期限を過ぎたらzero valueへ戻します。
	notBefore time.Time
	// appliedThroughは、recordに保存された再送下限時刻のうちnotBeforeへ反映済みの最も遅い
	// 時刻で、notBefore以上に保ちます。この時刻以前の期限は読み込んでも待機を延ばさないため、
	// 上限を超える期限を短縮した後は、短縮後の期限を過ぎたfileを保存済みの時刻にかかわらず
	// 送信します。storeへ保存するため、再起動後も短縮を数え直しません。
	appliedThrough time.Time

	// onDiskは保存した期限が残っている可能性を表し、期限後の削除に使います。
	onDisk bool
	// dirtyは、memory上の期限をまだ保存できていないことを表します。
	dirty bool
	// failingは保存の失敗が続いていることを表します。警告は一連の失敗につき一度だけ残します。
	failing bool
	// retryBackoffは、直前に失敗した保存から次の再試行までの間隔です。保存に成功するか、
	// 期限が来たらzero valueへ戻します。nextRetryAtより前の周期では再試行しません。
	retryBackoff time.Duration
	nextRetryAt  time.Time

	// clampとwarningsは、まだWorkerのreportで通知していない読み込み時の短縮と警告です。
	clamp    *spool.RetryAfterClamp
	warnings []error
}

func newRetryAfterDeadline(store RetryAfterStore, maxWait, retryInterval time.Duration) *RetryAfterDeadline {
	if maxWait <= 0 {
		maxWait = defaultMaxRetryAfter
	}
	if retryInterval <= 0 {
		retryInterval = defaultRetryAfterSaveRetryInterval
	}
	return &RetryAfterDeadline{store: store, maxWait: maxWait, retryInterval: retryInterval}
}

// RecordRetryAfterは、通常送信で受け取ったRetry-Afterの期限を送信先の期限として記録し、保存します。
// 期限は記録した時点から上限までに短縮し、より後の期限を記録済みなら短縮しません。
// 保存に失敗しても期限はmemoryで守り、Workerが以後の周期で保存を再試行して警告します。
func (d *RetryAfterDeadline) RecordRetryAfter(notBefore time.Time) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	// 保存済みのより後の期限を短縮しないよう、最初の記録の前に読み込みます。
	d.restoreLocked(now)
	if limit := now.Add(d.maxWait); notBefore.After(limit) {
		notBefore = limit
	}
	if !now.Before(notBefore) {
		return
	}
	if d.deferLocked(notBefore) {
		d.persistLocked(now)
	}
}

// isLoadedは、保存済みの期限を読み込んだかを返します。
func (d *RetryAfterDeadline) isLoaded() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.loaded
}

// beginCycleは周期の最初に呼び、保存済みの期限の読み込み、期限切れの削除、失敗した保存の
// 再試行を行います。再試行は前回の失敗から間隔を空け、期限が来たらやめます。
func (d *RetryAfterDeadline) beginCycle(now time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.restoreLocked(now)
	d.expireLocked(now)
	if d.dirty && !now.Before(d.nextRetryAt) {
		d.persistLocked(now)
	}
}

// deferredUntilは、待機期限がnowより後ならその期限とtrueを返します。期限を過ぎていれば記録を
// 解除し、反映済みの保存期限もすべて過ぎていれば保存した期限を削除します。
func (d *RetryAfterDeadline) deferredUntil(now time.Time) (time.Time, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.expireLocked(now)
	return d.notBefore, !d.notBefore.IsZero()
}

// deferUntilは、送信先への次回送信をnotBeforeまで控えて保存し、記録後の期限を返します。
// より後の期限を記録済みなら短縮しません。
func (d *RetryAfterDeadline) deferUntil(notBefore time.Time) time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.deferLocked(notBefore) {
		d.persistLocked(time.Now())
	}
	return d.notBefore
}

// foldStoredは、recordに保存された期限のうち、まだ到来せず送信先の待機へ反映していない時刻を
// 送信先の期限へ反映して保存し、記録後の期限を返します。該当しなければzero valueです。
// 現在時刻から上限より先の時刻は上限までに短縮し、短縮した内容を返します。反映した時刻は
// appliedThroughとして保存するため、同じ保存期限が同じprocessでも再起動後でも待機を延ばしません。
func (d *RetryAfterDeadline) foldStored(stored, now time.Time) (time.Time, *spool.RetryAfterClamp) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !now.Before(stored) || !stored.After(d.appliedThrough) {
		return time.Time{}, nil
	}
	notBefore := stored
	var clamp *spool.RetryAfterClamp
	if requested := stored.Sub(now); requested > d.maxWait {
		notBefore = now.Add(d.maxWait)
		clamp = &spool.RetryAfterClamp{
			Requested:     requested,
			MaxRetryAfter: d.maxWait,
			NotBefore:     notBefore,
			Stored:        true,
		}
	}
	d.deferLocked(notBefore)
	d.appliedThrough = stored
	d.dirty = true
	d.persistLocked(now)
	return d.notBefore, clamp
}

// takeReportは、まだ通知していない警告と、withClampなら読み込み時の短縮を一度だけ取り出します。
// withClampがfalseなら、短縮は次の呼び出しまで残します。
func (d *RetryAfterDeadline) takeReport(withClamp bool) (*spool.RetryAfterClamp, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var clamp *spool.RetryAfterClamp
	if withClamp {
		clamp, d.clamp = d.clamp, nil
	}
	warnings := errors.Join(d.warnings...)
	d.warnings = nil
	return clamp, warnings
}

// restoreLockedは、保存済みの期限をprocessで一度だけ読み込みます。読めない保存内容は警告を残し、
// 期限がないものとして扱います。短縮した期限を保存できなければ、以後の周期で再試行します。
func (d *RetryAfterDeadline) restoreLocked(now time.Time) {
	if d.loaded {
		return
	}
	d.loaded = true
	if d.store == nil {
		return
	}
	state, clamp, err := d.store.Load(now, d.maxWait)
	if err != nil {
		d.warnings = append(d.warnings, err)
	}
	if !state.NotBefore.IsZero() || !state.AppliedThrough.IsZero() {
		d.onDisk = true
	}
	if state.NotBefore.After(d.notBefore) {
		d.notBefore = state.NotBefore
	}
	if state.AppliedThrough.After(d.appliedThrough) {
		d.appliedThrough = state.AppliedThrough
	}
	if d.notBefore.After(d.appliedThrough) {
		d.appliedThrough = d.notBefore
	}
	if clamp != nil {
		d.clamp = clamp
		if err != nil {
			// 短縮した期限を保存できていない可能性があるため、間隔を空けて保存を再試行します。
			d.dirty, d.failing = true, true
			d.scheduleRetryLocked(now)
		}
	}
}

// deferLockedは期限を延長し、memory上の状態が変わったかを返します。より後の期限を短縮しません。
func (d *RetryAfterDeadline) deferLocked(notBefore time.Time) bool {
	changed := false
	if notBefore.After(d.notBefore) {
		d.notBefore = notBefore
		changed = true
	}
	if d.notBefore.After(d.appliedThrough) {
		d.appliedThrough = d.notBefore
		changed = true
	}
	if changed {
		d.dirty = true
	}
	return changed
}

// expireLockedは過ぎた期限を解除し、保存できていない期限の再試行をやめます。反映済みの保存期限も
// すべて過ぎていれば保存内容を削除します。削除できなかった保存内容も読み込み時に削除します。
func (d *RetryAfterDeadline) expireLocked(now time.Time) {
	if !d.notBefore.IsZero() && !now.Before(d.notBefore) {
		d.notBefore = time.Time{}
	}
	if d.notBefore.IsZero() {
		// 期限が来たら、保存できていない期限の再試行をやめます。
		d.stopRetryLocked()
	}
	if !d.notBefore.IsZero() || now.Before(d.appliedThrough) {
		return
	}
	d.appliedThrough = time.Time{}
	if d.store == nil || !d.onDisk {
		return
	}
	d.onDisk = false
	if err := d.store.Remove(); err != nil {
		d.warnings = append(d.warnings, err)
	}
}

// persistLockedはmemory上の期限を保存します。失敗した場合は期限をmemoryで守り、警告は
// 一連の失敗につき一度だけ残して、間隔を空けて以後の周期で再試行します。
func (d *RetryAfterDeadline) persistLocked(now time.Time) {
	if d.store == nil {
		d.stopRetryLocked()
		return
	}
	// 失敗した保存でも公開済みのことがあるため、期限後に削除を試みます。
	d.onDisk = true
	err := d.store.Save(RetryAfterState{
		NotBefore:      d.notBefore.Round(0),
		AppliedThrough: d.appliedThrough.Round(0),
	})
	if err != nil {
		if !d.failing {
			d.warnings = append(d.warnings, err)
		}
		d.dirty, d.failing = true, true
		d.scheduleRetryLocked(now)
		return
	}
	d.stopRetryLocked()
}

// scheduleRetryLockedは、保存に失敗した期限を次に再試行する時刻を決めます。間隔はrunnerの周期から
// 始め、失敗のたびに倍にして、maxRetryAfterSaveRetryBackoffを上限とします。
func (d *RetryAfterDeadline) scheduleRetryLocked(now time.Time) {
	if d.retryBackoff <= 0 {
		d.retryBackoff = d.retryInterval
	} else {
		d.retryBackoff *= 2
	}
	if d.retryBackoff > maxRetryAfterSaveRetryBackoff {
		d.retryBackoff = maxRetryAfterSaveRetryBackoff
	}
	d.nextRetryAt = now.Add(d.retryBackoff)
}

// stopRetryLockedは、保存の成功または期限の到来により、保存の再試行と一連の失敗の記録を終えます。
func (d *RetryAfterDeadline) stopRetryLocked() {
	d.dirty, d.failing = false, false
	d.retryBackoff, d.nextRetryAt = 0, time.Time{}
}
