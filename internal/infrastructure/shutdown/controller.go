package shutdown

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

// Controllerは終了処理を一つだけ実行し、未完了の処理を期限なしで再試行します。
// ForceCloseは呼び出し元の待機を解除しますが、実行中の処理やOS I/Oの終了は保証しません。
// ゼロ値を使用できます。終了開始後は対象の資源を再利用しないでください。
type Controller struct {
	mu       sync.Mutex
	started  bool
	finished bool
	forced   bool
	done     chan struct{}
	force    chan struct{}
	err      error
}

func (c *Controller) initializeLocked() {
	if c.done == nil {
		c.done = make(chan struct{})
	}
	if c.force == nil {
		c.force = make(chan struct{})
	}
}

// Closeは受付停止と実行中の処理の終了待ちを行うattemptを一度に一つ実行します。
// attemptは資源を解放できたときだけcomplete=trueを返します。失敗して未完了なら
// 再試行します。実行中の処理を待たずに資源を解放してはいけません。
func (c *Controller) Close(description string, attempt func() (complete bool, err error)) error {
	return c.CloseContext(context.Background(), description, attempt)
}

// CloseContextのctxはこの呼び出しの待機だけを中止します。終了処理は継続します。
func (c *Controller) CloseContext(ctx context.Context, description string, attempt func() (bool, error)) error {
	c.mu.Lock()
	c.initializeLocked()
	if !c.started {
		c.started = true
		go c.run(description, attempt)
	}
	done, force := c.done, c.force
	c.mu.Unlock()
	ticker := time.NewTicker(ReportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			c.mu.Lock()
			err := c.err
			c.mu.Unlock()
			return err
		case <-force:
			return pkgshutdown.ErrForcedClose
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_, _ = fmt.Fprintf(os.Stderr, "waiting for graceful shutdown: %s\n", description)
		}
	}
}

func (c *Controller) run(description string, attempt func() (bool, error)) {
	var result error
	defer func() {
		if recovered := recover(); recovered != nil {
			result = fmt.Errorf("shutdown %s panicked: %v", description, recovered)
		}
		c.mu.Lock()
		if c.forced {
			result = errors.Join(pkgshutdown.ErrForcedClose, result)
		}
		c.err = result
		c.finished = true
		close(c.done)
		c.mu.Unlock()
	}()
	for {
		complete, err := attempt()
		result = err
		if complete {
			return
		}
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "retrying graceful shutdown: %s: %v\n", description, err)
		}
		// forceは呼出し元の待機と復旧処理だけを中止します。資源解放が未完了なら
		// 依存先を保持したまま後片付けを続け、終了graphを途中で放棄しません。
		timer := time.NewTimer(ReportInterval)
		<-timer.C
	}
}

// ForceCloseは待機を直ちに解除し、abortへ復旧処理の中止を一度だけ要求します。
// abortは別goroutineで実行します。通常の後片付けを開始する必要がある所有者は、
// abort内でCloseも呼び出してください。プロセス自体は終了しません。
func (c *Controller) ForceClose(abort func()) error {
	c.mu.Lock()
	c.initializeLocked()
	if c.finished {
		err := c.err
		c.mu.Unlock()
		return err
	}
	if !c.forced {
		c.forced = true
		close(c.force)
		if abort != nil {
			go func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						_, _ = fmt.Fprintf(os.Stderr, "force shutdown callback panicked: %v\n", recovered)
					}
				}()
				abort()
			}()
		}
	}
	c.mu.Unlock()
	return pkgshutdown.ErrForcedClose
}

func (c *Controller) IsForced() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.forced
}

// IsCompleteは終了workerが戻り、共有する終了結果が確定したかを返します。
// panicで終了した場合は所有者固有の資源解放完了も照会してください。
func (c *Controller) IsComplete() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.finished
}

// Doneは終了処理の実行goroutineが戻ると閉じます。forceだけではworkerを放棄しません。
func (c *Controller) Done() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.initializeLocked()
	return c.done
}
