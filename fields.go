package heterolog

import (
	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

// Field is one immutable structured attribute. Its zero value is ignored.
type Field = pkglogger.Field

// FieldKey is a validated key that can efficiently construct typed fields.
type FieldKey = pkglogger.FieldKey

// NewFieldKey validates a dynamic attribute key.
func NewFieldKey(key string) (FieldKey, error) { return pkglogger.NewFieldKey(key) }

// MustFieldKey validates a constant attribute key and panics if it is invalid.
func MustFieldKey(key string) FieldKey { return pkglogger.MustFieldKey(key) }

// WithRequestID sets the request ID of a single log entry. An empty id sets no
// request ID.
func WithRequestID(id string) Field { return pkglogger.WithRequestID(id) }

// String constructs a string attribute with a validated key.
func String(key, value string) (Field, error) { return pkglogger.String(key, value) }

// Bool constructs a boolean attribute with a validated key.
func Bool(key string, value bool) (Field, error) { return pkglogger.Bool(key, value) }

// Int constructs an integer attribute with a validated key.
func Int(key string, value int) (Field, error) { return pkglogger.Int(key, value) }

// Int64 constructs a signed integer attribute with a validated key.
func Int64(key string, value int64) (Field, error) { return pkglogger.Int64(key, value) }

// Uint64 constructs an unsigned integer attribute with a validated key.
func Uint64(key string, value uint64) (Field, error) { return pkglogger.Uint64(key, value) }

// Float64 constructs a finite floating-point attribute with a validated key.
func Float64(key string, value float64) (Field, error) { return pkglogger.Float64(key, value) }

// Any snapshots a JSON-encodable value with a validated key. Values that cannot
// be encoded, or that nest arrays and objects deeper than MaxFieldNestingDepth,
// return ErrFieldValueNotJSON instead of a field.
func Any(key string, value any) (Field, error) { return pkglogger.Any(key, value) }

// MaxFieldNestingDepth is the deepest nesting of arrays and objects that Any and
// slog attributes accept: a scalar is depth 0 and one enclosing array is depth 1.
// Spool records and backend payloads add a few levels around each value; the
// limit leaves 1,000 of the 10,000 levels that JSON decoders accept for them,
// so every accepted value can be stored and replayed.
const MaxFieldNestingDepth = pkglogger.MaxFieldNestingDepth

var (
	ErrFieldKeyInvalid       = pkglogger.ErrFieldKeyInvalid
	ErrFieldValueNotJSON     = pkglogger.ErrFieldValueNotJSON
	ErrFieldFloatNotFinite   = pkglogger.ErrFieldFloatNotFinite
	ErrLoggerClosed          = pkglogger.ErrLoggerClosed
	ErrLogEntryTooLarge      = pkglogger.ErrLogEntryTooLarge
	ErrForcedClose           = pkgshutdown.ErrForcedClose
	ErrForceCloseUnsupported = applogging.ErrForceCloseUnsupported
	ErrLogDropped            = pkglogger.ErrLogDropped
	ErrNoActiveLogOutput     = pkglogger.ErrNoActiveLogOutput
)

// LogEntryTooLargeError describes an entry rejected before fan-out.
type LogEntryTooLargeError = pkglogger.LogEntryTooLargeError

// LoggerFailure describes a failure from a single physical handler.
type LoggerFailure = pkglogger.LoggerFailure

// MultiLoggerError contains the failures from a fan-out operation.
type MultiLoggerError = pkglogger.MultiLoggerError
