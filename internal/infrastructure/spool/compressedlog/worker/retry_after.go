package worker

import (
	"sync"
	"time"

	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/retryafter"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// defaultMaxRetryAfterは、設定を渡さずに構築したWorkerが使うRetry-After待機の上限です。
const defaultMaxRetryAfter = time.Hour

const (
	// defaultRetryAfterSaveRetryIntervalは、runnerの周期を渡さずに構築したWorkerが、保存に
	// 失敗した宛先の期限を最初に再び保存するまでの間隔です。
	defaultRetryAfterSaveRetryInterval = time.Second
	// maxRetryAfterSaveRetryBackoffは、保存の再試行の間隔の上限です。
	maxRetryAfterSaveRetryBackoff = time.Minute
)

// retryAfterMarkerStoreは、一つの宛先の待機期限を宛先別spool内へ保存します。
// *retryafter.Storeが実装し、試験では保存の失敗を再現するために差し替えます。
type retryAfterMarkerStore interface {
	Load(now time.Time, maxWait time.Duration) (retryafter.State, *pkgspool.RetryAfterClamp, error)
	Save(state retryafter.State) error
	Remove() error
}

// newRetryAfterMarkerStoreは、宛先別directory直下の.retry-afterへ宛先IDの名前でmarkerを置きます。
func newRetryAfterMarkerStore(root objectstoragespool.Root) (retryAfterMarkerStore, error) {
	return retryafter.NewObjectStorageStore(root.Path(), string(root.DestinationID()))
}

// RetryAfterMarkerReportは、宛先の待機期限を保存したmarkerについて、通知が必要な結果です。
type RetryAfterMarkerReport struct {
	DestinationID objectstorage.DestinationID
	// Clampは、再起動前に保存した期限が上限より先だったため、読み込んだ時点から上限までに
	// 短縮した内容です。短縮した期限は保存し直すため、再起動のたびに待機は延びません。
	Clamp *pkgspool.RetryAfterClamp
	// Errは、markerの読み込み・保存・削除の失敗です。読み込めないmarkerはないものとして
	// 送信を続け、保存に失敗した期限はprocess内で守って、期限が残る間は保存を再試行します。
	// 保存の失敗は一連の失敗につき一度だけ報告します。
	Err error
}

// destinationRetryAfterRegistryは、送信先が一時的な失敗とともに返したRetry-Afterから求めた
// 次回送信の下限時刻を、object storageの物理宛先ごとに保持します。circuit breakerと同じく
// RootRuntimeが所有するWorkerに保持し、送信経路を追加・削除しても同じ宛先の待機を維持します。
// 待機するのはその宛先のファイルだけで、他の宛先の再送やpurgeは止めません。
// spoolへ保存する前の通常のuploadが受け取ったRetry-Afterもrecordで同じ宛先の期限として記録し、
// 再送workerは次の周期からその宛先のファイルを期限まで送りません。
// 下限時刻は宛先別spool内のmarkerにも保存し、再起動後はその宛先へ最初に送信する前に
// restoreで読み込みます。宛先IDを持たないspoolの期限はmemoryだけに保持します。
// maxWaitより長いRetry-AfterはmaxWaitに短縮します。
type destinationRetryAfterRegistry struct {
	mu        sync.Mutex
	notBefore map[objectstorage.DestinationID]time.Time
	clock     func() time.Time
	maxWait   time.Duration
	// retryIntervalは、保存に失敗した期限を最初に再び保存するまでの間隔です。runnerの周期を使い、
	// 失敗が続くたびに倍にしてmaxRetryAfterSaveRetryBackoffを上限とします。
	retryInterval time.Duration

	// newMarkerStoreは、宛先別spoolのmarkerを開きます。nilなら期限を保存しません。
	newMarkerStore func(objectstoragespool.Root) (retryAfterMarkerStore, error)
	// markersは、restore済みの宛先のmarkerです。作成に失敗した宛先もnil storeで記録し、
	// 同じprocessで読み込みを繰り返しません。
	markers map[objectstorage.DestinationID]*destinationRetryAfterMarker
	// reportsは、まだRunReportで通知していないmarkerの結果です。
	reports []RetryAfterMarkerReport
}

type destinationRetryAfterMarker struct {
	store retryAfterMarkerStore
	// onDiskは、保存した期限が残っている可能性を表し、期限後の削除に使います。
	onDisk bool
	// dirtyは、memory上の期限をまだ保存できていないことを表します。期限が残る間は
	// その宛先を確認する周期ごとに保存を再試行します。
	dirty bool
	// failingは保存の失敗が続いていることを表します。警告は一連の失敗につき一度だけ残します。
	failing bool
	// retryBackoffは直前に失敗した保存から次の再試行までの間隔で、nextRetryAtより前は再試行しません。
	// 保存に成功するか期限が来たらzero valueへ戻します。
	retryBackoff time.Duration
	nextRetryAt  time.Time
}

// newDestinationRetryAfterRegistryは、clockで現在時刻を取得するregistryを作成します。
// clockがnilの場合はtime.Nowを使います。待機の上限はdefaultMaxRetryAfterです。
func newDestinationRetryAfterRegistry(clock func() time.Time) *destinationRetryAfterRegistry {
	if clock == nil {
		clock = time.Now
	}
	return &destinationRetryAfterRegistry{
		notBefore:      make(map[objectstorage.DestinationID]time.Time),
		clock:          clock,
		maxWait:        defaultMaxRetryAfter,
		retryInterval:  defaultRetryAfterSaveRetryInterval,
		newMarkerStore: newRetryAfterMarkerStore,
		markers:        make(map[objectstorage.DestinationID]*destinationRetryAfterMarker),
	}
}

// restoreは、rootの宛先に保存された待機期限を、このprocessで最初に一度だけ読み込みます。
// 呼び出し元は、その宛先への送信可否を判定する前に呼びます。期限を過ぎたmarkerは削除し、
// 上限より先の期限は短縮して保存し直します。読み込めないmarkerは期限がないものとして扱い、
// 結果をRunReportで通知します。宛先IDを持たないrootでは何もしません。
func (r *destinationRetryAfterRegistry) restore(root objectstoragespool.Root) {
	id := root.DestinationID()
	if r == nil || r.newMarkerStore == nil || id == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, restored := r.markers[id]; restored {
		return
	}
	marker := &destinationRetryAfterMarker{}
	r.markers[id] = marker
	store, err := r.newMarkerStore(root)
	if err != nil {
		r.reports = append(r.reports, RetryAfterMarkerReport{DestinationID: id, Err: err})
		return
	}
	marker.store = store
	state, clamp, err := store.Load(r.clock(), r.maxWait)
	if !state.NotBefore.IsZero() {
		marker.onDisk = true
		if state.NotBefore.After(r.notBefore[id]) {
			r.notBefore[id] = state.NotBefore
		}
	}
	if clamp != nil && err != nil {
		// 短縮した期限を保存できていない可能性があるため、期限が残る間は保存を再試行します。
		marker.dirty, marker.failing = true, true
		r.scheduleRetryLocked(marker, r.clock())
	}
	if clamp != nil || err != nil {
		r.reports = append(r.reports, RetryAfterMarkerReport{DestinationID: id, Clamp: clamp, Err: err})
	}
}

// deferredUntilは、宛先への送信を待機させる期限を返します。期限がない、または
// 期限を過ぎている場合はzero valueを返し、過ぎた期限とその宛先のmarkerは削除します。
// 期限が残る間に保存できていない期限があれば、前回の失敗から間隔を空けて保存を再試行します。
func (r *destinationRetryAfterRegistry) deferredUntil(
	destinationID objectstorage.DestinationID,
) time.Time {
	if r == nil {
		return time.Time{}
	}
	now := r.clock()
	r.mu.Lock()
	defer r.mu.Unlock()
	notBefore, ok := r.notBefore[destinationID]
	if !ok {
		return time.Time{}
	}
	if !now.Before(notBefore) {
		delete(r.notBefore, destinationID)
		r.removeMarkerLocked(destinationID)
		return time.Time{}
	}
	if marker := r.markers[destinationID]; marker != nil && marker.dirty && !now.Before(marker.nextRetryAt) {
		r.saveMarkerLocked(destinationID, marker, notBefore, now)
	}
	return notBefore
}

// deferAfterは、現在時刻からretryAfterが経過するまで宛先への送信を待機させ、記録後の期限を
// 返します。より後の期限を記録済みなら短縮しません。retryAfterが0以下なら記録しません。
// retryAfterがmaxWaitを超える場合はmaxWaitまで待機し、短縮した内容も返します。
// 期限を延ばした場合はmarkerへ保存し、失敗してもmemory上の期限で待機を続けます。
func (r *destinationRetryAfterRegistry) deferAfter(
	destinationID objectstorage.DestinationID,
	retryAfter time.Duration,
) (time.Time, *pkgspool.RetryAfterClamp) {
	if r == nil || retryAfter <= 0 {
		return time.Time{}, nil
	}
	wait, capped := pkgspool.CapRetryAfter(retryAfter, r.maxWait)
	notBefore := r.clock().Add(wait)
	var clamp *pkgspool.RetryAfterClamp
	if capped {
		clamp = &pkgspool.RetryAfterClamp{Requested: retryAfter, MaxRetryAfter: r.maxWait, NotBefore: notBefore}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if current := r.notBefore[destinationID]; current.After(notBefore) {
		return current, clamp
	}
	r.notBefore[destinationID] = notBefore
	if marker := r.markers[destinationID]; marker != nil && marker.store != nil {
		r.saveMarkerLocked(destinationID, marker, notBefore, r.clock())
	}
	return notBefore, clamp
}

// recordは、通常のupload（spoolへ保存する前の直接upload）が一時的な失敗とともに返した
// Retry-Afterを、rootの宛先の待機期限として記録し、markerへ保存します。保存済みのより後の期限を
// 短縮しないよう、最初の記録の前にmarkerを読み込みます。上限を超える指定は上限までに短縮し、
// 短縮内容は次の周期のRunReportで通知します。記録した期限は、同じregistryを使う再送workerが
// 次の周期からその宛先のすべてのファイルに適用します。宛先IDを持たないrootではmemoryだけに保持します。
func (r *destinationRetryAfterRegistry) record(root objectstoragespool.Root, retryAfter time.Duration) time.Time {
	if r == nil || retryAfter <= 0 {
		return time.Time{}
	}
	r.restore(root)
	notBefore, clamp := r.deferAfter(root.DestinationID(), retryAfter)
	if clamp != nil {
		r.mu.Lock()
		r.reports = append(r.reports, RetryAfterMarkerReport{DestinationID: root.DestinationID(), Clamp: clamp})
		r.mu.Unlock()
	}
	return notBefore
}

// saveMarkerLockedは宛先の期限を保存します。失敗した場合は期限をmemoryで守り、警告は一連の
// 失敗につき一度だけ残して、期限が残る間は間隔を空けて以後の周期で再試行します。
func (r *destinationRetryAfterRegistry) saveMarkerLocked(
	destinationID objectstorage.DestinationID,
	marker *destinationRetryAfterMarker,
	notBefore time.Time,
	now time.Time,
) {
	if marker.store == nil {
		stopMarkerRetry(marker)
		return
	}
	// 失敗した保存でも公開済みのことがあるため、期限後に削除を試みます。
	marker.onDisk = true
	if err := marker.store.Save(retryafter.State{NotBefore: notBefore.Round(0)}); err != nil {
		if !marker.failing {
			r.reports = append(r.reports, RetryAfterMarkerReport{DestinationID: destinationID, Err: err})
		}
		marker.dirty, marker.failing = true, true
		r.scheduleRetryLocked(marker, now)
		return
	}
	stopMarkerRetry(marker)
}

// scheduleRetryLockedは、保存に失敗した期限を次に再試行する時刻を決めます。間隔はrunnerの周期から
// 始め、失敗のたびに倍にして、maxRetryAfterSaveRetryBackoffを上限とします。
func (r *destinationRetryAfterRegistry) scheduleRetryLocked(marker *destinationRetryAfterMarker, now time.Time) {
	if marker.retryBackoff <= 0 {
		marker.retryBackoff = r.retryInterval
		if marker.retryBackoff <= 0 {
			marker.retryBackoff = defaultRetryAfterSaveRetryInterval
		}
	} else {
		marker.retryBackoff *= 2
	}
	if marker.retryBackoff > maxRetryAfterSaveRetryBackoff {
		marker.retryBackoff = maxRetryAfterSaveRetryBackoff
	}
	marker.nextRetryAt = now.Add(marker.retryBackoff)
}

// stopMarkerRetryは、保存の成功または期限の到来により、保存の再試行と一連の失敗の記録を終えます。
func stopMarkerRetry(marker *destinationRetryAfterMarker) {
	marker.dirty, marker.failing = false, false
	marker.retryBackoff, marker.nextRetryAt = 0, time.Time{}
}

// removeMarkerLockedは、期限を過ぎた宛先のmarkerを削除します。削除できなかったmarkerも
// 期限切れのため、再起動後の読み込みで送信を制限せずに削除します。
func (r *destinationRetryAfterRegistry) removeMarkerLocked(destinationID objectstorage.DestinationID) {
	marker := r.markers[destinationID]
	if marker == nil {
		return
	}
	// 期限切れの状態は保存し直す必要がありません。
	stopMarkerRetry(marker)
	if marker.store == nil || !marker.onDisk {
		return
	}
	marker.onDisk = false
	if err := marker.store.Remove(); err != nil {
		r.reports = append(r.reports, RetryAfterMarkerReport{DestinationID: destinationID, Err: err})
	}
}

// takeReportsは、まだ通知していないmarkerの結果を一度だけ取り出します。
func (r *destinationRetryAfterRegistry) takeReports() []RetryAfterMarkerReport {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	reports := r.reports
	r.reports = nil
	return reports
}
