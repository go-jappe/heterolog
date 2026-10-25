package logrecovery

import (
	"context"
	"os"
	"sync"
)

// SignalControlは終了要求を通常終了と強制終了に分けます。
// 一度目のsignalはContextをキャンセルし、次のsignalはForceStopを閉じます。
// signalの購読と解除は呼び出し元が行います。この部品はprocessを終了しません。
type SignalControl struct {
	ctx       context.Context
	cancel    context.CancelFunc
	forceStop chan struct{}
	stop      chan struct{}
	stopped   chan struct{}
	closeOnce sync.Once
}

func NewSignalControl(parent context.Context, signals <-chan os.Signal) *SignalControl {
	ctx, cancel := context.WithCancel(parent)
	c := &SignalControl{
		ctx: ctx, cancel: cancel, forceStop: make(chan struct{}),
		stop: make(chan struct{}), stopped: make(chan struct{}),
	}
	go c.run(parent.Done(), signals)
	return c
}

// Contextは一度目の終了要求でキャンセルされる、CLIの実行contextです。
func (c *SignalControl) Context() context.Context { return c.ctx }

// ForceStopは、安全な終了の待機を打ち切る次の終了要求を通知します。
func (c *SignalControl) ForceStop() <-chan struct{} { return c.forceStop }

func (c *SignalControl) run(parentDone <-chan struct{}, signals <-chan os.Signal) {
	defer close(c.stopped)
	for {
		select {
		case <-c.stop:
			return
		case <-parentDone:
			c.cancel()
			parentDone = nil
		case _, open := <-signals:
			if !open {
				return
			}
			if c.ctx.Err() == nil {
				c.cancel()
				continue
			}
			close(c.forceStop)
			return
		}
	}
}

// Closeはsignalの受付を止め、受付goroutineの終了を待ちます。
// CLIが終了した後に呼び、復旧処理自身の終了待機には使いません。
func (c *SignalControl) Close() {
	c.closeOnce.Do(func() {
		close(c.stop)
		c.cancel()
	})
	<-c.stopped
}
