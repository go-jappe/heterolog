package compressedlogfailure

import (
	"context"
	"errors"
)

var ErrUploaderClosed = errors.New("compressed log file uploader is closing")

// Closeは受付を停止し、受理済みの呼び出しが戻ってから保存先の利用登録を解除します。
// 保存処理ごとのtimeoutは維持し、通常終了では処理や結果通知を中止しません。
// 強制終了で待機を解除しても、資源の解放は実際の処理終了後まで行いません。
func (u *ManagedUploader) Close() error {
	if u == nil {
		return nil
	}
	u.stateMu.Lock()
	u.closing = true
	u.stateMu.Unlock()
	return u.closeController.Close("compressed log file uploader", func() (bool, error) {
		u.inFlight.Wait()
		var errs []error
		for _, targets := range u.failureTargets {
			for _, target := range targets {
				errs = append(errs, target.close())
			}
		}
		return true, errors.Join(errs...)
	})
}

// RequestForceCloseは中止を通知し、通常Closeの待機を解除します。資源は解放しません。
func (u *ManagedUploader) RequestForceClose() error {
	if u == nil {
		return nil
	}
	u.initializeForceContext()
	u.stateMu.Lock()
	u.closing = true
	u.stateMu.Unlock()
	u.forceCancel()
	for _, targets := range u.failureTargets {
		for _, target := range targets {
			target.requestForceClose()
		}
	}
	return u.closeController.ForceClose(nil)
}

// ForceCloseは中止を通知して戻り、受理済み処理が戻った後の後片付けを開始します。
func (u *ManagedUploader) ForceClose() error {
	if u == nil {
		return nil
	}
	err := u.RequestForceClose()
	go u.Close()
	return err
}

// ShutdownDoneは強制終了の待機解除ではなく、保存先の登録解除まで終わった時点で閉じます。
func (u *ManagedUploader) ShutdownDone() <-chan struct{} { return u.closeController.Done() }

func (u *ManagedUploader) ShutdownComplete() bool { return u == nil || u.closeController.IsComplete() }

func (u *ManagedUploader) initializeForceContext() {
	u.forceOnce.Do(func() { u.forceContext, u.forceCancel = context.WithCancel(context.Background()) })
}

// contextUntilForcedは、個々の処理の期限に加えて明示的な強制終了だけを伝えます。
func (u *ManagedUploader) contextUntilForced(parent context.Context) (context.Context, context.CancelFunc) {
	u.initializeForceContext()
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(u.forceContext, cancel)
	if u.forceContext.Err() != nil {
		cancel()
	}
	return ctx, func() { stop(); cancel() }
}
