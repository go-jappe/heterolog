package handler

import (
	"sync"
	"sync/atomic"

	"github.com/go-jappe/heterolog/internal/goroutineid"
)

// currentGoroutineIDは、呼び出し元goroutineのidを返します。
//
// Goはgoroutine idを公開APIにしていません。ここでは内部ログ通知の中から同じgoroutineで
// 同じloggerへ書き込んだこと（contextを引き継がない再入）の判定にだけ使い、runtime.Stackの
// 先頭行「goroutine N [」から読みます。この形式はGo 1.0から変わっていません。読めない場合は
// falseを返し、呼び出し元はこの判定を使わずcontextの印だけで判定するため、形式が変わっても
// 判定が弱くなるだけで、別のgoroutineを再入と誤判定することはありません。
// runtime.Stackはstackの深さに比例した時間（数µs程度）がかかるため、通知を始めるときと、
// 通知が実行中のloggerで書き込みが内部ログを報告するときにだけ呼び、通常の書き込みでは呼びません。
func currentGoroutineID() (uint64, bool) {
	return goroutineid.Current()
}

// parseGoroutineIDは、runtime.Stackの先頭行「goroutine N [...]」からNを読みます。
func parseGoroutineID(stack []byte) (uint64, bool) {
	return goroutineid.Parse(stack)
}

// goroutineDepthsは、Dispatchを実行中のgoroutineのidと、そのgoroutine上の入れ子の深さを
// 数えます。並行に使用でき、panicで抜けた場合も呼び出し元がdeferでleaveします。
type goroutineDepths struct {
	// activeは記録中のDispatchの数です。0ならmapを調べず、goroutine idも取得しません。
	active atomic.Int32
	mu     sync.Mutex
	depth  map[uint64]int32
}

func (d *goroutineDepths) enter(id uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.depth == nil {
		d.depth = make(map[uint64]int32)
	}
	d.depth[id]++
	d.active.Add(1)
}

func (d *goroutineDepths) leave(id uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if next := d.depth[id] - 1; next > 0 {
		d.depth[id] = next
	} else {
		delete(d.depth, id)
	}
	d.active.Add(-1)
}

// containsCurrentは、呼び出し元goroutineが記録中のDispatchを実行しているかを返します。
func (d *goroutineDepths) containsCurrent() bool {
	if d.active.Load() == 0 {
		return false
	}
	id, ok := currentGoroutineID()
	if !ok {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.depth[id] > 0
}

// dispatchingGoroutinesは、いずれかのloggerのDispatchを実行中のgoroutineです。
// InInternalLogDispatchだけが参照します。
var dispatchingGoroutines goroutineDepths
