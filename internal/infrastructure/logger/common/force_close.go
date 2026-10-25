package common

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
)

// ForceClosePanicErrorは、強制終了を転送した依存先がpanicしたことを表します。Ownerは
// 転送先、Valueは回復したpanicの値です。値がerrorなら、errors.Is/Asで元の原因も判定できます。
type ForceClosePanicError struct {
	Owner any
	Value any
}

func (e *ForceClosePanicError) Error() string {
	return fmt.Sprintf("force close %T panicked: %v", e.Owner, e.Value)
}

func (e *ForceClosePanicError) Unwrap() error {
	err, _ := e.Value.(error)
	return err
}

// ForceCloseIfSupportedは対応している中止要求を転送します。中止通知だけを行う
// RequestForceCloseを優先し、未対応の場合はForceCloseへ委譲します。
// 一つの依存先のpanicで残りの強制終了が止まらないよう、panicをForceClosePanicErrorへ
// 変換します。
func ForceCloseIfSupported(value any) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &ForceClosePanicError{Owner: value, Value: recovered}
		}
	}()
	// 所有者からの転送では中止要求だけを優先し、依存関係順のCloseを開始しません。
	// 公開ForceCloseが独立した後片付けを開始するchildにも、この契約を使用します。
	if requester, ok := value.(interface{ RequestForceClose() error }); ok {
		return requester.RequestForceClose()
	}
	if closer, ok := value.(interface{ ForceClose() error }); ok {
		return closer.ForceClose()
	}
	return nil
}

// WaitForForcedShutdownは公開Closeの待機解除と実際の資源解放を区別します。
// 親所有者はchildの実処理が戻るまで、その処理が使用する依存資源を保持します。
func WaitForForcedShutdown(value any, err error) {
	if !errors.Is(err, shutdown.ErrForcedClose) {
		return
	}
	if owner, ok := value.(interface{ ShutdownDone() <-chan struct{} }); ok {
		shutdown.Wait(fmt.Sprintf("forced child cleanup: %T", value), owner.ShutdownDone())
	}
}

// ShutdownCompleteはCloseの報告errorとは別に、実際の資源解放完了を照会します。
func ShutdownComplete(value any) bool {
	owner, ok := value.(interface{ ShutdownComplete() bool })
	return ok && owner.ShutdownComplete()
}
