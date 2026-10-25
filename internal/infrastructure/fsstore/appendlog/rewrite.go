package appendlog

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
)

// errFileClosedOrNotOpenedは、書き込み先のactive fileが閉じられているか、まだ開かれていないことを表します。
var errFileClosedOrNotOpened = errors.New("file is already closed or not opened")

type EnsureWritableError struct {
	Op  string
	Err error
}

func (e *EnsureWritableError) Error() string { return e.Op + ": " + e.Err.Error() }

// Unwrapは、書き込めない原因のErrを返します。
func (e *EnsureWritableError) Unwrap() error { return e.Err }

func (fs *FileStore) ensureWritable() error {
	if fs.File == nil {
		return &EnsureWritableError{
			Op:  "failed to write log",
			Err: errFileClosedOrNotOpened,
		}
	}
	return nil
}

// WriteStringWithRetryは、呼び出し側が選択したretry設定で現在のactive fileへ
// msgと改行を書き込み、部分書き込み後は未書き込み部分だけを再試行します。
// 再試行を使い切った場合も、実際に追加したbyte数をerrorと共に返します。
// latencyHookには成功した書き込みの経過時間を渡し、通知の要否は判断しません。
func (fs *FileStore) WriteStringWithRetry(
	ctx context.Context,
	spanName string,
	msg string,
	meta any, // requestID 等
	logRetry logging.RetryLogFunc,
	entryFactory logging.EntryFactory,
	wrapMsg string,
	latencyHook LatencyHook,
	retryCfg RetryConfigProvider,
) (uint64, error) {
	return fs.executeWriteStringWithRetry(
		ctx,
		spanName,
		msg,
		meta,
		logRetry,
		entryFactory,
		wrapMsg,
		latencyHook,
		retryCfg,
		fs.File,
	)
}

// executeWriteStringWithRetryは、各retry attemptで使用するStringWriterを引数から受け取り、
// 部分書き込みbyte数を累計します。production経路はFileStore.Fileを渡し、試験では
// filesystemで再現しにくい部分書き込み結果を返すfake writerを渡します。
func (fs *FileStore) executeWriteStringWithRetry(
	ctx context.Context,
	spanName string,
	msg string,
	meta any,
	logRetry logging.RetryLogFunc,
	entryFactory logging.EntryFactory,
	wrapMsg string,
	latencyHook LatencyHook,
	retryCfg RetryConfigProvider,
	writer io.StringWriter,
) (uint64, error) {
	ctx, span := fs.Tracer.StartSpan(ctx, spanName)
	defer span.End()

	// O_APPENDへの部分書き込みはすでにfileへ反映されています。同じprefixを
	// 再度appendしないよう、確定したbyte位置をattempt間で保持します。
	line := msg + "\n"
	writtenBytes := 0
	var finalWriteErr error
	err := logging.RetryWithLog(
		func() error {
			if finalWriteErr != nil {
				// 全量書き込みと同時に返されたerrorや不正なbyte数は、同じ行を
				// 再送しても解決しません。元のerrorを保ち、追加書き込みを止めます。
				return finalWriteErr
			}
			if err := fs.ensureWritable(); err != nil {
				return err
			}

			return measureLatency(ctx, "write log to file", meta,
				func() error {
					remaining := line[writtenBytes:]
					n, e := writer.WriteString(remaining)
					if n < 0 || n > len(remaining) {
						finalWriteErr = errors.Join(e, fmt.Errorf("writer returned invalid byte count %d for %d-byte input", n, len(remaining)))
						return finalWriteErr
					}
					writtenBytes += n
					if n < len(remaining) && e == nil {
						e = io.ErrShortWrite
					}
					if writtenBytes == len(line) && e != nil {
						finalWriteErr = e
					}
					return e
				},
				latencyHook,
			)
		},
		retryCfg.MaxWriteAttempts(),
		retryCfg.FileOperationRetryWaitTime(),
		logRetry,
		entryFactory,
		wrapMsg,
		ctx,
	)
	return uint64(writtenBytes), err
}
