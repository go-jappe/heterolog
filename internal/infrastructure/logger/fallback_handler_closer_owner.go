package logger

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
)

// FallbackHandlerCloseErrorは、一つのstateful fallback handlerのCloseが失敗したことを
// 表します。Sourceはhandlerを所有するlogger・物理file role・durable store、Targetは
// internal logを送るbackend、Levelは受け持つlog levelで、errors.Asで失敗したhandlerを
// 特定できます。Errは原因です。
type FallbackHandlerCloseError struct {
	Source string
	Target string
	Level  string
	Err    error
}

func (e *FallbackHandlerCloseError) Error() string {
	return fmt.Sprintf(
		"close fallback handler source=%q target=%q level=%q: %v",
		e.Source,
		e.Target,
		e.Level,
		e.Err,
	)
}

func (e *FallbackHandlerCloseError) Unwrap() error { return e.Err }

// fallbackHandlerCloseStateは、一つのstateful fallback handlerについて、
// 終了対象のCloserとCloseの完了状態を保持します。
type fallbackHandlerCloseState struct {
	// sourceはfallback handlerを所有するlogger、物理file role、またはdurable storeを
	// 識別する診断名です。例: opensearch、file.error、opensearch.failure_store.spool
	source string
	// targetはfallback handlerがinternal logを送るbackend名です。
	// 例: slack、clickhouse
	target string
	// levelはhandlerが受け持つinternal logのlog levelです。
	level string
	// closerはhandlerが使用するbuffer、timer、backend loggerなどを終了します。
	closer io.Closer
	// closedはcloser.Close()が成功済みであることを表します。trueなら再度呼び出しません。
	closed bool
}

// fallbackHandlerCloserOwnerは、各source logger向けにlevelごと生成されたstateful
// fallback handlerのCloserを登録し、top-level loggerの終了処理からまとめて閉じます。
//
// Closeに失敗したCloserだけを次回Closeで再試行し、成功済みCloserは二重に
// 閉じません。handlerはfile fallbackへ最終eventを書き得るため、そのCloserは
// fallback先のLoggerSetより先に閉じる必要があります。
type fallbackHandlerCloserOwner struct {
	mu      sync.Mutex
	closeMu sync.Mutex
	closers []fallbackHandlerCloseState
}

func newFallbackHandlerCloserOwner() *fallbackHandlerCloserOwner {
	return &fallbackHandlerCloserOwner{}
}

// Registerは一つのsource用handler群が公開するCloserを、source、target backend、levelの
// 診断情報とともに終了対象へ登録します。Closerを持たないstateless handlerは登録しません。
func (o *fallbackHandlerCloserOwner) Register(source string, handlers handlerStruct) {
	if o == nil {
		return
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	for _, candidate := range handlers.OnError {
		o.registerLocked(source, candidate.Name, "error", candidate.Closer)
	}
	for _, candidate := range handlers.OnWarn {
		o.registerLocked(source, candidate.Name, "warn", candidate.Closer)
	}
	for _, candidate := range handlers.OnInfo {
		o.registerLocked(source, candidate.Name, "info", candidate.Closer)
	}
	for _, candidate := range handlers.OnDebug {
		o.registerLocked(source, candidate.Name, "debug", candidate.Closer)
	}
}

func (o *fallbackHandlerCloserOwner) registerLocked(
	source string,
	target string,
	level string,
	closer io.Closer,
) {
	if closer == nil {
		return
	}
	o.closers = append(o.closers, fallbackHandlerCloseState{
		source: source,
		target: target,
		level:  level,
		closer: closer,
	})
}

// Closeは登録と逆の順序で各Closerを呼び出します。Closeに失敗した要素は未完了のまま
// 保持して次回呼び出しで再試行し、成功した要素は以降の呼び出しで省略します。
func (o *fallbackHandlerCloserOwner) Close() error {
	if o == nil {
		return nil
	}

	o.closeMu.Lock()
	defer o.closeMu.Unlock()

	var errs []error
	o.mu.Lock()
	for index := len(o.closers) - 1; index >= 0; index-- {
		if o.closers[index].closed {
			continue
		}

		closer := o.closers[index].closer
		source := o.closers[index].source
		target := o.closers[index].target
		level := o.closers[index].level
		o.mu.Unlock()
		err := closer.Close()
		common.WaitForForcedShutdown(closer, err)
		o.mu.Lock()
		if err != nil {
			errs = append(errs, &FallbackHandlerCloseError{
				Source: source,
				Target: target,
				Level:  level,
				Err:    err,
			})
			if state, ok := closer.(interface{ ShutdownComplete() bool }); !ok || !state.ShutdownComplete() {
				continue
			}
		}
		o.closers[index].closed = true
	}
	o.mu.Unlock()

	return errors.Join(errs...)
}

// ShutdownCompleteは登録された全handlerの資源解放が完了したことを表します。
func (o *fallbackHandlerCloserOwner) ShutdownComplete() bool {
	if o == nil {
		return true
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, closer := range o.closers {
		if !closer.closed {
			return false
		}
	}
	return true
}
