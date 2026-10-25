// Package notificationは、通知関数を受理順に別のgoroutineで実行する共通部品を提供します。
package notification

import "sync"

// Dispatcherは通知callbackを発行元のgoroutineから切り離してFIFOで実行します。
// callbackのpanicは通知ごとに隔離し、後続通知の実行を妨げません。
// NewDispatcherで生成し、使用開始後はコピーしないでください。
type Dispatcher struct {
	mu      sync.Mutex
	pending []func()
	wake    chan struct{}
	done    chan struct{}
	closed  bool
}

func NewDispatcher() *Dispatcher {
	dispatcher := &Dispatcher{
		wake: make(chan struct{}, 1),
		done: make(chan struct{}),
	}
	go dispatcher.run()
	return dispatcher
}

// Enqueueは通知を受理順に実行するため登録します。nil callbackと終了後の通知は無視します。
// nil receiverではcallbackを同期実行し、その場合もpanicを隔離します。
func (d *Dispatcher) Enqueue(notification func()) {
	if notification == nil {
		return
	}
	if d == nil {
		dispatchNotification(notification)
		return
	}

	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.pending = append(d.pending, notification)
	d.mu.Unlock()
	d.signal()
}

// CloseAfterDrainは新規通知の受付を止め、受理済み通知をFIFOで排出してからworkerを終了させます。
// callback自身が所有者のCloseへ再入できるよう、worker完了は待ちません。
// 返却時には通知完了を保証しません。完了を待つ場合はcallbackの外側からDoneを使います。
// nil receiverでは何もしません。繰り返し呼び出せます。
func (d *Dispatcher) CloseAfterDrain() {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.closed = true
	d.mu.Unlock()
	d.signal()
}

// Doneは受理済み通知の排出とworker終了を知らせます。
func (d *Dispatcher) Done() <-chan struct{} {
	return d.done
}

func (d *Dispatcher) signal() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func (d *Dispatcher) run() {
	defer close(d.done)
	for range d.wake {
		for {
			d.mu.Lock()
			if len(d.pending) == 0 {
				closed := d.closed
				d.mu.Unlock()
				if closed {
					return
				}
				break
			}
			notification := d.pending[0]
			d.pending[0] = nil
			d.pending = d.pending[1:]
			d.mu.Unlock()

			dispatchNotification(notification)
		}
	}
}

func dispatchNotification(notification func()) {
	defer func() { _ = recover() }()
	notification()
}
