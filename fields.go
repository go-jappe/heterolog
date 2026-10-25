package heterolog

import (
	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// Field is one immutable structured attribute. Its zero value is ignored.
type Field = applogging.Field

// FieldKey is a validated key that can efficiently construct typed fields.
type FieldKey = applogging.FieldKey

// NewFieldKey validates a dynamic attribute key.
func NewFieldKey(key string) (FieldKey, error) { return applogging.NewFieldKey(key) }

// MustFieldKey validates a constant attribute key and panics if it is invalid.
func MustFieldKey(key string) FieldKey { return applogging.MustFieldKey(key) }

// WithRequestID overrides the context request ID for a single log entry.
func WithRequestID(id string) Field { return applogging.WithRequestID(id) }

// String constructs a string attribute with a validated key.
func String(key, value string) (Field, error) { return applogging.String(key, value) }

// Bool constructs a boolean attribute with a validated key.
func Bool(key string, value bool) (Field, error) { return applogging.Bool(key, value) }

// Int constructs an integer attribute with a validated key.
func Int(key string, value int) (Field, error) { return applogging.Int(key, value) }

// Int64 constructs a signed integer attribute with a validated key.
func Int64(key string, value int64) (Field, error) { return applogging.Int64(key, value) }

// Uint64 constructs an unsigned integer attribute with a validated key.
func Uint64(key string, value uint64) (Field, error) { return applogging.Uint64(key, value) }

// Float64 constructs a finite floating-point attribute with a validated key.
func Float64(key string, value float64) (Field, error) { return applogging.Float64(key, value) }

// Any snapshots a JSON-encodable value with a validated key. Values that cannot
// be encoded, or that nest arrays and objects deeper than MaxFieldNestingDepth,
// return ErrFieldValueNotJSON instead of a field.
func Any(key string, value any) (Field, error) { return applogging.Any(key, value) }

// MaxFieldNestingDepth is the deepest nesting of arrays and objects that Any and
// slog attributes accept: a scalar is depth 0 and one enclosing array is depth 1.
// Spool records and backend payloads add a few levels around each value; the
// limit keeps that total well below the 10,000 levels JSON decoders accept, so
// every accepted value can be stored and replayed.
const MaxFieldNestingDepth = applogging.MaxFieldNestingDepth

var (
	ErrFieldKeyInvalid       = applogging.ErrFieldKeyInvalid
	ErrFieldValueNotJSON     = applogging.ErrFieldValueNotJSON
	ErrFieldFloatNotFinite   = applogging.ErrFieldFloatNotFinite
	ErrLoggerClosed          = applogging.ErrLoggerClosed
	ErrLogEntryTooLarge      = applogging.ErrLogEntryTooLarge
	ErrForcedClose           = applogging.ErrForcedClose
	ErrForceCloseUnsupported = applogging.ErrForceCloseUnsupported
	ErrLogDropped            = pkglogger.ErrLogDropped
	ErrNoActiveLogOutput     = pkglogger.ErrNoActiveLogOutput
)

// LogEntryTooLargeError describes an entry rejected before fan-out.
type LogEntryTooLargeError = applogging.LogEntryTooLargeError

// LoggerFailure describes a failure from a single physical handler.
type LoggerFailure = applogging.LoggerFailure

// MultiLoggerError contains the failures from a fan-out operation.
type MultiLoggerError = applogging.MultiLoggerError
