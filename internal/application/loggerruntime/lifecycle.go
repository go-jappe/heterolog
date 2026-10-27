package loggerruntime

import (
	"context"
	"fmt"
	"io"
	"sync"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
)

// lifecycleは、同時に終了を求めた呼び出し元を、進行中の一つのCloseに合流させます。
// 終わりきらなかったCloseがerrorを返した後は、もう一度終了を試せます。
type lifecycle struct {
	closer  io.Closer
	mu      sync.Mutex
	attempt *closeAttempt
}

type closeAttempt struct {
	done     chan struct{}
	err      error
	complete bool
}

// Closeは、新しいlogの受け付けを止め、受け付けたbuffer内のlogを書き出し、資源を
// 解放します。期限なしで待ちます。spoolに残ったlogをすべて送り終える必要はなく、
// durableな記録は後で再送できるように残ります。
func (l *lifecycle) Close() error { return l.Shutdown(context.Background()) }

// Shutdownは、正常な終了を始め、終わるかctxが終わるまで待ちます。ctxがすでに終わって
// いる場合は、終了を始めずにctx.Err()を返します。ctxが終わっても、
// 止まるのはこの呼び出し元の待機だけで、後片付けは続きます。後から呼んだCloseや
// Shutdownは、その後片付けに合流します。復旧の処理を打ち切るにはForceCloseを使います。
func (l *lifecycle) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("shutdown context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if l == nil || l.closer == nil {
		return nil
	}
	l.mu.Lock()
	attempt := l.attempt
	if attempt != nil {
		select {
		case <-attempt.done:
			if !attempt.complete {
				attempt = nil
			}
		default:
		}
	}
	if attempt == nil {
		// contextの期限切れを返す前に、受け付けを同期的に止めます。
		if stop, ok := l.closer.(interface{ StopAccepting() }); ok {
			stop.StopAccepting()
		}
		attempt = &closeAttempt{done: make(chan struct{})}
		l.attempt = attempt
		go func() {
			attempt.err = l.closer.Close()
			attempt.complete = attempt.err == nil
			if state, ok := l.closer.(interface{ ShutdownComplete() bool }); ok {
				attempt.complete = state.ShutdownComplete()
			}
			close(attempt.done)
		}()
	}
	l.mu.Unlock()
	select {
	case <-attempt.done:
		return attempt.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ForceCloseは、新しいlogの受け付けを止め、CloseやShutdownで待っている呼び出し元を
// 解放し、spoolの再送やlog fileの圧縮・uploadなどの後処理と復旧の処理へ中止を求めます。
// 待たずに戻り、通常の後片付けはbackgroundで続きます。終了がまだ終わっていなければ
// ErrForcedCloseを返し、解放されたCloseとShutdownも同じerrorを返します。終了後は、
// その終了の結果を返します。未保存のcheckpointや未完了の退避が残ることがあり、
// OSのI/Oは終わるまで時間がかかることがあります。
func (l *lifecycle) ForceClose() error {
	if l == nil {
		return nil
	}
	return applogging.ForceClose(l.closer)
}
