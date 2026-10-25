package evacuation

import (
	"context"
	"fmt"
	"io"
	"os"
)

// WriteTextToStdoutは、file loggerの保存予定だった整形済み本文をそのまま退避します。
// level判定やsamplingをやり直さず、失敗レコード退避と同じlockで出力を直列化します。
// lock待ちはctxで中止できますが、開始済みのOS書込みを強制停止するものではありません。
func WriteTextToStdout(ctx context.Context, text string) error {
	return writeText(ctx, os.Stdout, text)
}

func writeText(ctx context.Context, writer io.Writer, text string) error {
	if err := stdoutBatchMu.LockContext(ctx); err != nil {
		return err
	}
	defer stdoutBatchMu.Unlock()
	n, err := io.WriteString(writer, text)
	if err == nil && n != len(text) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return fmt.Errorf("write overloaded file log to stdout: %w", err)
	}
	return nil
}
