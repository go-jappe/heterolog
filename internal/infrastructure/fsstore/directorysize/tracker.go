// Package directorysize は、directory内のfile size合計を、filesystem観測を開始するための
// 概算値として追跡します。
package directorysize

import (
	"math"
	"sync"
	"time"
)

// ConfigProviderは、directory size観測の開始判定に使用する現在の上限とcache TTLを返します。
// Trackerはproviderを保持せず、判定要求ごとに現在値を読み取ります。
type ConfigProvider interface {
	MaxDirSize() uint64
	UsageCacheTTL() time.Duration
}

// observationGrowthSampleCapacityは、一回のディレクトリ容量の走査中に記録する、
// 直近の追記バイト数を保持する件数の上限です。上限を超えた場合は最古の値を上書きします。
const observationGrowthSampleCapacity = 10

// observationGrowthSamplesは、一回のdirectory観測中にRecordGrowthへ渡された
// 直近のbyte数を固定長ringで保持します。valuesには平均値ではなく個々の追記byte数、
// countには現在保持している件数、nextには次に書き込むindexを保持します。
type observationGrowthSamples struct {
	values [observationGrowthSampleCapacity]uint64
	count  int
	next   int
}

func (s *observationGrowthSamples) record(bytes uint64) {
	s.values[s.next] = bytes
	s.next = (s.next + 1) % observationGrowthSampleCapacity
	if s.count < observationGrowthSampleCapacity {
		s.count++
	}
}

// averageは、走査中の追記一件分を近似するための概算補正値を返します。
// 各sampleを先に割ることで、uint64の合計値をoverflowさせずに
// floor(sum(samples) / count)を計算します。
func (s *observationGrowthSamples) average() uint64 {
	if s.count == 0 {
		return 0
	}

	divisor := uint64(s.count)
	var quotientTotal uint64
	var remainderTotal uint64
	for index := range s.count {
		sample := s.values[index]
		quotientTotal += sample / divisor
		remainderTotal += sample % divisor
	}
	return quotientTotal + remainderTotal/divisor
}

func (s *observationGrowthSamples) reset() {
	*s = observationGrowthSamples{}
}

// Trackerは、directory sizeのfilesystem観測を開始するgateとして、使用byte数を
// best effortで追跡します。概算値は削除判断には使用せず、観測完了時に実測値で
// 補正します。観測中のログ追記は直近10件の平均byte数を実測値へ一度だけ
// 上乗せします。これは走査中の追記がsnapshotに含まれたかを追跡するための
// best-effort補正であり、再走査による高精度化は行いません。すべてのmethodは複数
// goroutineから安全に呼び出せます。
type Tracker struct {
	mu sync.Mutex

	// estimatedBytesは、filesystem走査を開始するか判断するためだけの概算値です。
	// directoryの正確な現在使用量や、削除量の決定には使用しません。
	estimatedBytes uint64
	observing      bool
	// observedGrowthは進行中の観測中に記録した追記sampleです。
	// 観測完了時の概算補正以外には使用しません。
	observedGrowth observationGrowthSamples
	// observationStartedAtは進行中の観測を開始した時刻です。観測成功時に
	// lastSuccessfulObservationへ移し、UsageCacheTTLの起点にします。
	observationStartedAt time.Time
	// observationMaxBytesは進行中の観測を開始した時点のdirectory上限です。
	// FinishObservationへ渡された削除後byte数がこの値を超える場合に、同じ状態を
	// 毎writeで再走査しないための時間backoffを開始します。
	observationMaxBytes uint64
	// lastSuccessfulObservationは、最後に実測値を概算へ反映した観測の開始時刻です。
	// UsageCacheTTLの期限切れ判定に使用します。
	lastSuccessfulObservation time.Time
	// overLimitObservedAtとoverLimitObservedBytesは、全体観測とpurge後にも上限超過が
	// 残った最後の状態です。同じ上限超過状態の再観測をUsageCacheTTLまで抑止します。
	// Invalidateされた既知のfilesystem mutationではclearし、早期再観測を許可します。
	overLimitObservedAt    time.Time
	overLimitObservedBytes uint64
}

// NewTrackerは、directory sizeの概算と観測状態を保持するTrackerを返します。
func NewTracker() *Tracker {
	return &Tracker{}
}

// RecordGrowthは、fileへの追記などで判明した増加byte数を概算へ加算します。
// 加算がuint64の範囲を超える場合は最大値で飽和します。
func (t *Tracker) RecordGrowth(bytes uint64) {
	if t == nil || bytes == 0 {
		return
	}

	t.mu.Lock()
	if math.MaxUint64-t.estimatedBytes < bytes {
		t.estimatedBytes = math.MaxUint64
	} else {
		t.estimatedBytes += bytes
	}
	if t.observing {
		t.observedGrowth.record(bytes)
	}
	t.mu.Unlock()
}

// Invalidateは、directory全体観測外のcompression、purge、upload後削除など、
// 個別fileのmutationによって概算を安全に更新できなくなったことを記録します。
// 次の通常判定ではTTL内でもfilesystem観測を開始します。進行中の観測と競合した場合も、
// その観測完了後にcacheを有効へ戻さず、次の判定で再観測します。
func (t *Tracker) Invalidate() {
	if t == nil {
		return
	}

	t.mu.Lock()
	// FinishObservationはobservationStartedAtを成功時刻へ移すため、進行中の
	// 観測も無効化する場合は開始時刻もclearします。
	t.observationStartedAt = time.Time{}
	t.lastSuccessfulObservation = time.Time{}
	t.overLimitObservedAt = time.Time{}
	t.overLimitObservedBytes = 0
	t.mu.Unlock()
}

// TryStartObservationは、directory sizeが現在の上限を超えた可能性がある場合、
// UsageCacheTTLが期限切れの場合、またはforce=trueの場合にfilesystem観測の開始権を
// 取得します。直前の観測・purge後にも上限超過が残った場合は、同じ状態の再走査を
// UsageCacheTTLまで抑止します。同時に開始権を取得できる呼び出しは一つだけです。
func (t *Tracker) TryStartObservation(
	now time.Time,
	config ConfigProvider,
	force bool,
) bool {
	if t == nil {
		return false
	}
	now = now.UTC()
	maxBytes := config.MaxDirSize()
	usageCacheTTL := config.UsageCacheTTL()

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.observing {
		return false
	}
	if !force && maxBytes == 0 {
		return false
	}
	// 直前の全体観測とpurge後にも上限超過が残った場合、同じfilesystem状態を
	// 各writeで再走査しません。上限とTTLは判定ごとにproviderから読み取ります。
	// 設定は構築後に変更しない前提です。clock rollback時は抑止しません。
	if !force && t.overLimitObservedBytes > maxBytes &&
		!t.overLimitObservedAt.IsZero() &&
		!now.Before(t.overLimitObservedAt) &&
		now.Before(t.overLimitObservedAt.Add(usageCacheTTL)) {
		return false
	}
	if !force && t.estimatedBytes <= maxBytes &&
		!t.lastSuccessfulObservation.IsZero() &&
		!now.Before(t.lastSuccessfulObservation) &&
		now.Before(t.lastSuccessfulObservation.Add(usageCacheTTL)) {
		return false
	}

	t.observing = true
	t.observationStartedAt = now
	t.observationMaxBytes = maxBytes
	t.observedGrowth.reset()
	return true
}

// FinishObservationは、取得済みの観測を完了し、使用byte数の概算値を更新します。
// snapshotの実測値に、観測中の直近10件のログ追記byte数の平均を一度だけ加えます。
// この平均値は、走査中の追記がsnapshotへ含まれたか確定できない場合のbest-effortな
// 推定であり、実際に未計上だったbyte数ではありません。概算値を削除判断には使わないため、
// snapshotと一部重複しても再走査で補正しません。callerはpurge後に残ったbyte数を
// actualBytesへ渡します。その値が観測開始時の上限を超える場合は、即時再走査しても
// 改善しない状態として時間backoffを開始します。開始権がない場合は何もしません。
// 観測中に開始できなかった別callerの要求は保持しません。
func (t *Tracker) FinishObservation(actualBytes uint64) {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.observing {
		return
	}

	t.estimatedBytes = saturatingAdd(actualBytes, t.observedGrowth.average())
	t.lastSuccessfulObservation = t.observationStartedAt
	if !t.observationStartedAt.IsZero() && actualBytes > t.observationMaxBytes {
		t.overLimitObservedAt = t.observationStartedAt
		t.overLimitObservedBytes = actualBytes
	} else {
		t.overLimitObservedAt = time.Time{}
		t.overLimitObservedBytes = 0
	}
	t.observationStartedAt = time.Time{}
	t.observationMaxBytes = 0
	t.observedGrowth.reset()
	t.observing = false
}

// AbortObservationは、directory sizeを取得できなかった観測を、概算の更新なしで終了します。
// 成功観測時刻は更新しないため、同じ開始理由が残っていれば次の判定で再試行できます。
// 開始権がない場合は何もしません。観測中に開始できなかった別callerの
// 要求は保持しません。
func (t *Tracker) AbortObservation() {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.observing {
		return
	}

	t.observationStartedAt = time.Time{}
	t.observationMaxBytes = 0
	t.observedGrowth.reset()
	t.observing = false
}

func saturatingAdd(left, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}
