package checkpoint

import (
	"context"
	"errors"
)

// pendingSaveErrorLockedは、更新内容がすでにstateへ反映されている場合の保存状況です。
// 未保存ならエラーを返すだけで、呼び出し元から保存を再実行しません。
func (s *FilesystemStore) pendingSaveErrorLocked() error {
	if s.lastSaveErr == nil {
		return nil
	}
	return &PendingSaveError{Err: s.lastSaveErr}
}

// PendingSaveErrorは、受理済みの最新stateまたは公開済み宛先登録がまだ同期されていなければ、
// その保存エラーを返します。宛先登録の公開後失敗では、要求されたcursor更新は未受理です。
// 保存済みならnilです。メモリ上の保存状況だけを確認し、保存やファイル読み直しは行いません。
// Loadは保存未完了でも最新stateを返すため、永続化完了を待つ所有者はこの確認を使います。
func (s *FilesystemStore) PendingSaveError(ctx context.Context) error {
	if s == nil {
		return ErrInvalidDirectory
	}
	if err := s.lockContext(ctx); err != nil {
		return err
	}
	defer s.stateOrSaveMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.pendingSaveErrorLocked()
}

// saveLockedは受理済みの最新stateを保存し、失敗時だけ共通の復旧処理を起動します。
// 手動実行用のWithSaveRetryDisabledが指定されている場合、復旧処理は起動しません。
// 呼び出し元はstateOrSaveMuを保持します。メソッド名や元の更新引数は保持しません。
func (s *FilesystemStore) saveLocked(ctx context.Context) error {
	if err := s.persistLocked(ctx); err != nil {
		s.lastSaveErr = err
		if !s.disableSaveRetry {
			s.saveRecovery.StartLocked()
		}
		return s.pendingSaveErrorLocked()
	}
	s.lastSaveErr = nil
	return nil
}

// retrySaveLockedは共通の復旧処理から呼ばれ、公開済みmanifestの同期が未完了なら
// それを先に再保存してから最新stateを保存します。登録時の初期cursor判定や
// AdvanceCompletionCursorは呼び直しません。呼び出し元がstateOrSaveMuを保持します。
func (s *FilesystemStore) retrySaveLocked(ctx context.Context) (bool, error) {
	if s.lastSaveErr == nil {
		return true, nil
	}
	err := s.persistLocked(ctx)
	if err == nil {
		s.lastSaveErr = nil
		return true, nil
	}
	s.lastSaveErr = err
	return false, err
}

// Closeは受理済みの最新stateが保存されるまで期限を設けずに待ちます。
// 復旧処理は継続し、待機状況と保存失敗の通知も継続します。
// 呼び出し元は先に新規操作と後続処理の再開を止めます。Close開始後は再利用しません。
// WithSaveRetryDisabledの場合は再試行せず、未保存のエラーを返します。
func (s *FilesystemStore) Close() error {
	if s == nil {
		return nil
	}
	err := s.saveRecovery.Stop()
	s.stateOrSaveMu.Lock()
	defer s.stateOrSaveMu.Unlock()
	return errors.Join(err, s.lastSaveErr)
}

// ForceCloseは最新stateの保存保証を放棄し、Store内部の再試行を中止します。
// 実行中のI/Oは終了まで残り得ます。呼び出し元は新規操作も先に止めてください。
// CloseとDoneは保存callbackが実際に終了するまで待ちます。
func (s *FilesystemStore) ForceClose() error {
	if s == nil {
		return nil
	}
	return s.saveRecovery.ForceClose()
}

// Doneは終了要求後にStore内部の保存再試行が実際に終了すると閉じます。
// 通常のStore操作の終了は、呼び出し元がClose前に待つ契約です。
func (s *FilesystemStore) Done() <-chan struct{} {
	if s == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	return s.saveRecovery.Done()
}
