package heterolog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	infralogger "github.com/go-jappe/heterolog/internal/infrastructure/logger"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/otel"
)

// Logger synchronously records structured entries and reports failures.
type Logger = applogging.Logger

// AsyncLogger accepts entries without returning delivery errors.
type AsyncLogger = applogging.AsyncLogger

// DeliveryLogger reports the result for each configured destination.
type DeliveryLogger = applogging.DeliveryLogger

// DeliveryReport describes backend delivery, durable storage, and evacuation.
type DeliveryReport = pkglogger.DeliveryReport

// Runtime owns a synchronous logger and its background resources. Obtain one
// with New; the zero value is not a usable logger. Do not copy a Runtime.
type Runtime struct {
	Logger
	*lifecycle
}

// AsyncRuntime owns an asynchronous best-effort logger. Shutdown waits for
// accepted operations and final buffer flushing. Obtain one with NewAsync.
type AsyncRuntime struct {
	AsyncLogger
	*lifecycle
}

// DeliveryRuntime owns a delivery-reporting logger. Obtain one with NewDelivery.
type DeliveryRuntime struct {
	DeliveryLogger
	*lifecycle
}

var (
	// ErrNilContext reports that New, NewAsync or NewDelivery received a nil context.
	ErrNilContext = errors.New("logger context must not be nil")
	// ErrDeliverySpoolUnsupported reports that NewDelivery received WithSpool. A
	// Delivery logger reports each delivery to the caller and has no replay workers.
	ErrDeliverySpoolUnsupported = errors.New("delivery logging does not support WithSpool")
	// ErrInvalidNotificationTarget reports that a backend selected as an internal
	// log notification target (OnErrorConfig, OnWarnConfig, OnInfoConfig or
	// OnDebugConfig) has settings from which its notification writer cannot be
	// created, for example an unknown TimeZone or an empty LogLineDateFormat. The
	// constructors return it as a *NotificationTargetError before creating any
	// resource, instead of dropping notifications later.
	ErrInvalidNotificationTarget = infralogger.ErrInvalidNotificationTarget
)

// NotificationTargetError reports the logger (Source) and the notification target
// (Backend) whose settings are invalid, and the cause in Err. errors.Is matches
// ErrInvalidNotificationTarget and the cause.
type NotificationTargetError = infralogger.NotificationTargetError

const (
	// ConfigFieldTimeout identifies the Timeout of Config, AsyncConfig and
	// DeliveryConfig in a pkg/logger ConfigFieldError. A Timeout that is not
	// positive is reported with the kind pkg/logger ErrNonPositiveSetting.
	ConfigFieldTimeout pkglogger.ConfigField = "Timeout"
	// ConfigFieldMaxParallel identifies AsyncConfig.MaxParallel in a pkg/logger
	// ConfigFieldError. A MaxParallel that is not positive is reported with the
	// kind pkg/logger ErrNonPositiveSetting.
	ConfigFieldMaxParallel pkglogger.ConfigField = "MaxParallel"
)

// New constructs a logger from explicit configuration. It does not load the
// environment or modify global providers. Use DefaultConfig as a starting point.
// Close or Shutdown must be called to flush buffers and release resources.
func New(ctx context.Context, cfg Config, opts ...Option) (*Runtime, error) {
	if err := validateConstructor(ctx, "logger", cfg.Timeout, cfg.MaxLogEntryBytes); err != nil {
		return nil, err
	}
	if err := validateLoggerPresentation("logger", cfg); err != nil {
		return nil, err
	}
	o, err := resolveOptions(opts)
	if err != nil {
		return nil, err
	}
	if cfg.Resource == nil {
		cfg.Resource = &otel.Resource{}
	}
	logger, err := infralogger.NewLogger(ctx, cfg, o.routing, o.spool, o.monitor, o.tracer, o.meter)
	if err != nil {
		return nil, err
	}
	return &Runtime{Logger: logger, lifecycle: &lifecycle{closer: logger}}, nil
}

// NewAsync constructs an asynchronous best-effort logger. MaxParallel and the
// normal timeout and entry-size limits must all be positive.
func NewAsync(ctx context.Context, cfg AsyncConfig, opts ...Option) (*AsyncRuntime, error) {
	if err := validateConstructor(ctx, "async logger", cfg.Timeout, cfg.MaxLogEntryBytes); err != nil {
		return nil, err
	}
	if cfg.MaxParallel <= 0 {
		return nil, &pkglogger.ConfigFieldError{
			Source: "async logger",
			Field:  ConfigFieldMaxParallel,
			Detail: fmt.Sprintf("max parallel must be positive: %d", cfg.MaxParallel),
			Value:  cfg.MaxParallel,
			Err:    pkglogger.ErrNonPositiveSetting,
		}
	}
	if err := validateLoggerPresentation("async logger", cfg.ToLoggerConfig()); err != nil {
		return nil, err
	}
	o, err := resolveOptions(opts)
	if err != nil {
		return nil, err
	}
	if cfg.Resource == nil {
		cfg.Resource = &otel.Resource{}
	}
	logger, err := infralogger.NewAsyncLogger(ctx, cfg, o.routing, o.spool, o.monitor, o.tracer, o.meter)
	if err != nil {
		return nil, err
	}
	return &AsyncRuntime{AsyncLogger: logger, lifecycle: &lifecycle{closer: logger}}, nil
}

// NewDelivery constructs a logger that reports delivery per destination.
// Delivery mode does not use the replay workers configured by WithSpool.
func NewDelivery(ctx context.Context, cfg DeliveryConfig, opts ...Option) (*DeliveryRuntime, error) {
	if err := validateConstructor(ctx, "delivery logger", cfg.Timeout, cfg.MaxLogEntryBytes); err != nil {
		return nil, err
	}
	if err := validateDeliveryPresentation(cfg); err != nil {
		return nil, err
	}
	o, err := resolveOptions(opts)
	if err != nil {
		return nil, err
	}
	if o.spool != nil {
		return nil, ErrDeliverySpoolUnsupported
	}
	if cfg.Resource == nil {
		cfg.Resource = &otel.Resource{}
	}
	logger, err := infralogger.NewDeliveryLogger(ctx, cfg, o.routing, o.tracer, o.meter)
	if err != nil {
		return nil, err
	}
	return &DeliveryRuntime{DeliveryLogger: logger, lifecycle: &lifecycle{closer: logger}}, nil
}

// validateConstructorは、constructorに共通の引数を検証します。sourceは設定の出所
// （"logger"・"async logger"・"delivery logger"）です。
func validateConstructor(ctx context.Context, source string, timeout time.Duration, maxBytes int) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if timeout <= 0 {
		return &pkglogger.ConfigFieldError{
			Source: source,
			Field:  ConfigFieldTimeout,
			Detail: fmt.Sprintf("timeout must be positive: %s", timeout),
			Value:  timeout,
			Err:    pkglogger.ErrNonPositiveSetting,
		}
	}
	return pkglogger.ValidateMaxLogEntryBytes(maxBytes)
}

// Log records a supported severity. It never terminates the process.
func (r *Runtime) Log(ctx context.Context, level Level, message string, fields ...Field) error {
	switch level {
	case LevelDebug:
		return r.Debug(ctx, message, fields...)
	case LevelInfo:
		return r.Info(ctx, message, fields...)
	case LevelWarn:
		return r.Warn(ctx, message, fields...)
	case LevelError:
		return r.Error(ctx, message, fields...)
	default:
		return fmt.Errorf("unsupported log level: %v", level)
	}
}

// lifecycle joins concurrent shutdown callers onto a single in-progress Close.
// An incomplete close can be retried after it returns an error.
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

// Close stops accepting new entries, flushes accepted buffered entries and
// releases resources. It waits without a deadline. A spool's entire backlog
// need not be delivered; durable records remain available for later replay.
func (l *lifecycle) Close() error { return l.Shutdown(context.Background()) }

// Shutdown starts graceful shutdown and waits until it completes or ctx ends.
// Cancellation only stops this caller's wait; cleanup continues. A later Close
// or Shutdown joins cleanup. Use ForceClose to request aborting recovery work.
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
		// Stop accepting synchronously before returning a context deadline.
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

// ForceClose requests immediate cancellation of recovery work. Unfinished
// checkpoints or archives may remain. OS I/O may still need time to complete.
func (l *lifecycle) ForceClose() error {
	if l == nil {
		return nil
	}
	return applogging.ForceClose(l.closer)
}
