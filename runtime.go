package heterolog

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// Logger synchronously records structured entries and reports failures.
type Logger = pkglogger.Logger

// AsyncLogger accepts entries without returning delivery errors.
type AsyncLogger = pkglogger.AsyncLogger

// DeliveryLogger reports the result for each configured destination.
type DeliveryLogger = pkglogger.DeliveryLogger

// DeliveryReport describes backend delivery, durable storage, and evacuation.
type DeliveryReport = pkglogger.DeliveryReport

// Runtime owns a synchronous logger and its background resources. Obtain one
// with New; the zero value is not a usable logger. Do not copy a Runtime.
type Runtime struct {
	Logger
	// runtimeは処理を受け持つ内部のruntimeです。zero値のRuntimeではnilです。
	runtime *loggerruntime.Runtime
}

// AsyncRuntime owns an asynchronous best-effort logger. Shutdown waits for
// accepted operations and final buffer flushing. Obtain one with NewAsync.
type AsyncRuntime struct {
	AsyncLogger
	runtime *loggerruntime.AsyncRuntime
}

// DeliveryRuntime owns a delivery-reporting logger. Obtain one with NewDelivery.
type DeliveryRuntime struct {
	DeliveryLogger
	runtime *loggerruntime.DeliveryRuntime
}

var (
	// ErrNilContext reports that New, NewAsync or NewDelivery received a nil context.
	ErrNilContext = loggerruntime.ErrNilContext
	// ErrDeliverySpoolUnsupported reports that NewDelivery received WithSpool. A
	// Delivery logger reports each delivery to the caller and has no replay workers.
	ErrDeliverySpoolUnsupported = loggerruntime.ErrDeliverySpoolUnsupported
	// ErrInvalidNotificationTarget reports that a backend selected as an internal
	// log notification target (OnErrorConfig, OnWarnConfig, OnInfoConfig or
	// OnDebugConfig) has settings from which its notification writer cannot be
	// created, for example an unknown TimeZone or an empty LogLineDateFormat. The
	// constructors return it as a *NotificationTargetError before creating any
	// resource, instead of dropping notifications later.
	ErrInvalidNotificationTarget = pkglogger.ErrInvalidNotificationTarget
)

// NotificationTargetError reports the logger (Source) and the notification target
// (Backend) whose settings are invalid, and the cause in Err. errors.Is matches
// ErrInvalidNotificationTarget and the cause.
type NotificationTargetError = pkglogger.NotificationTargetError

const (
	// ConfigFieldTimeout identifies the Timeout of Config, AsyncConfig and
	// DeliveryConfig in a pkg/logger ConfigFieldError. A Timeout that is not
	// positive is reported with the kind pkg/logger ErrNonPositiveSetting.
	ConfigFieldTimeout = loggerruntime.ConfigFieldTimeout
	// ConfigFieldMaxParallel identifies AsyncConfig.MaxParallel in a pkg/logger
	// ConfigFieldError. A MaxParallel that is not positive is reported with the
	// kind pkg/logger ErrNonPositiveSetting.
	ConfigFieldMaxParallel = loggerruntime.ConfigFieldMaxParallel
)

// New constructs a logger from explicit configuration. It does not load the
// environment or modify global providers. Use DefaultConfig as a starting point.
// Close or Shutdown must be called to flush buffers and release resources.
func New(ctx context.Context, cfg Config, opts ...Option) (*Runtime, error) {
	return newRuntime(loggerruntime.New(ctx, cfg, opts...))
}

// NewAsync constructs an asynchronous best-effort logger. MaxParallel and the
// normal timeout and entry-size limits must all be positive.
func NewAsync(ctx context.Context, cfg AsyncConfig, opts ...Option) (*AsyncRuntime, error) {
	return newAsyncRuntime(loggerruntime.NewAsync(ctx, cfg, opts...))
}

// NewDelivery constructs a logger that reports delivery per destination.
// Delivery mode has no replay workers, so NewDelivery returns
// ErrDeliverySpoolUnsupported when opts include WithSpool.
func NewDelivery(ctx context.Context, cfg DeliveryConfig, opts ...Option) (*DeliveryRuntime, error) {
	return newDeliveryRuntime(loggerruntime.NewDelivery(ctx, cfg, opts...))
}

// newRuntime・newAsyncRuntime・newDeliveryRuntimeは、内部のruntimeを公開の型で包みます。
// errがある場合はnilを返します。
func newRuntime(runtime *loggerruntime.Runtime, err error) (*Runtime, error) {
	if err != nil {
		return nil, err
	}
	return &Runtime{Logger: runtime.Logger, runtime: runtime}, nil
}

func newAsyncRuntime(runtime *loggerruntime.AsyncRuntime, err error) (*AsyncRuntime, error) {
	if err != nil {
		return nil, err
	}
	return &AsyncRuntime{AsyncLogger: runtime.AsyncLogger, runtime: runtime}, nil
}

func newDeliveryRuntime(runtime *loggerruntime.DeliveryRuntime, err error) (*DeliveryRuntime, error) {
	if err != nil {
		return nil, err
	}
	return &DeliveryRuntime{DeliveryLogger: runtime.DeliveryLogger, runtime: runtime}, nil
}

// implは、処理を渡す内部のruntimeを返します。zero値のRuntimeには内部のruntimeのzero値を
// 返し、終了や設定の読み取りを、内部の型のzero値と同じく安全に行えるようにします。
func (r *Runtime) impl() *loggerruntime.Runtime {
	if r.runtime == nil {
		return &loggerruntime.Runtime{}
	}
	return r.runtime
}

func (r *AsyncRuntime) impl() *loggerruntime.AsyncRuntime {
	if r.runtime == nil {
		return &loggerruntime.AsyncRuntime{}
	}
	return r.runtime
}

func (r *DeliveryRuntime) impl() *loggerruntime.DeliveryRuntime {
	if r.runtime == nil {
		return &loggerruntime.DeliveryRuntime{}
	}
	return r.runtime
}

// Log records a supported severity. It never terminates the process.
func (r *Runtime) Log(ctx context.Context, level Level, message string, fields ...Field) error {
	return r.impl().Log(ctx, level, message, fields...)
}

// Close stops accepting new entries, flushes accepted buffered entries and
// releases resources. It waits without a deadline. A spool's entire backlog
// need not be delivered; durable records remain available for later replay.
func (r *Runtime) Close() error { return r.impl().Close() }

// Shutdown starts graceful shutdown and waits until it completes or ctx ends.
// If ctx has already ended, it returns ctx.Err() without starting shutdown.
// Cancellation only stops this caller's wait; cleanup continues. A later Close
// or Shutdown joins cleanup. Use ForceClose to request aborting recovery work.
func (r *Runtime) Shutdown(ctx context.Context) error { return r.impl().Shutdown(ctx) }

// ForceClose stops accepting new entries, releases callers waiting in Close
// or Shutdown, and requests the background and recovery work, such as spool
// replay and the compression and upload of log files, to stop. It returns
// without waiting, while the regular cleanup continues in the background.
// Unless shutdown has already finished, it returns ErrForcedClose, which the
// released Close and Shutdown calls also return; after shutdown has finished,
// it returns the result of that shutdown. Unfinished checkpoints or archives
// may remain, and OS I/O may still need time to complete.
func (r *Runtime) ForceClose() error { return r.impl().ForceClose() }

// Close stops accepting new entries, waits for accepted operations and final
// buffer flushing, and releases resources. It waits without a deadline. A
// spool's entire backlog need not be delivered; durable records remain
// available for later replay.
func (r *AsyncRuntime) Close() error { return r.impl().Close() }

// Shutdown starts graceful shutdown and waits until it completes or ctx ends.
// If ctx has already ended, it returns ctx.Err() without starting shutdown.
// Cancellation only stops this caller's wait; cleanup continues. A later Close
// or Shutdown joins cleanup. Use ForceClose to request aborting recovery work.
func (r *AsyncRuntime) Shutdown(ctx context.Context) error { return r.impl().Shutdown(ctx) }

// ForceClose stops accepting new entries, releases callers waiting in Close
// or Shutdown, and requests the background and recovery work, such as spool
// replay and the compression and upload of log files, to stop. It returns
// without waiting, while the regular cleanup continues in the background.
// Unless shutdown has already finished, it returns ErrForcedClose, which the
// released Close and Shutdown calls also return; after shutdown has finished,
// it returns the result of that shutdown. Unfinished checkpoints or archives
// may remain, and OS I/O may still need time to complete.
func (r *AsyncRuntime) ForceClose() error { return r.impl().ForceClose() }

// Close stops accepting new entries, flushes accepted buffered entries and
// releases resources. It waits without a deadline.
func (r *DeliveryRuntime) Close() error { return r.impl().Close() }

// Shutdown starts graceful shutdown and waits until it completes or ctx ends.
// If ctx has already ended, it returns ctx.Err() without starting shutdown.
// Cancellation only stops this caller's wait; cleanup continues. A later Close
// or Shutdown joins cleanup. Use ForceClose to request aborting recovery work.
func (r *DeliveryRuntime) Shutdown(ctx context.Context) error { return r.impl().Shutdown(ctx) }

// ForceClose stops accepting new entries, releases callers waiting in Close
// or Shutdown, and requests the background and recovery work, such as spool
// replay and the compression and upload of log files, to stop. It returns
// without waiting, while the regular cleanup continues in the background.
// Unless shutdown has already finished, it returns ErrForcedClose, which the
// released Close and Shutdown calls also return; after shutdown has finished,
// it returns the result of that shutdown. Unfinished checkpoints or archives
// may remain, and OS I/O may still need time to complete.
func (r *DeliveryRuntime) ForceClose() error { return r.impl().ForceClose() }
