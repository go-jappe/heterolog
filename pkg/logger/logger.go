package logger

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

type LogLevel int

type LogOutputDestination string
type LogOutputDestinations []LogOutputDestination

// DefaultMaxLogEntryBytes is the default limit on the input accepted from the
// application in a single log call, counted in bytes of its canonical JSON
// representation. Backend-specific metadata and wire formats are handled
// separately by the final validation of each backend.
const DefaultMaxLogEntryBytes = 1 * 1024 * 1024

// 出力先を追加する場合は、直下のallLogOutputDestinationsにも追加します。parser、
// handler targetの収集、網羅するテストはその一覧に自動で従います。

// The LogOutput constants are the output destinations a logger can write to.
// AllLogOutputDestinations returns them in this order.
const (
	LogOutputFile           LogOutputDestination = "file"
	LogOutputStdout         LogOutputDestination = "stdout"
	LogOutputSlack          LogOutputDestination = "slack"
	LogOutputOpenSearch     LogOutputDestination = "opensearch"
	LogOutputLoki           LogOutputDestination = "loki"
	LogOutputCloudWatchLogs LogOutputDestination = "cloudwatchlogs"
	LogOutputClickHouse     LogOutputDestination = "clickhouse"
)

// allLogOutputDestinationsは、利用可能な出力先とその安定した列挙順の唯一の定義です。
// 出力先を追加する場合は、この一覧にも追加します。IsValid、設定のhandler target収集、
// それらを網羅するテストはすべてこの一覧を参照します。
var allLogOutputDestinations = LogOutputDestinations{
	LogOutputFile,
	LogOutputStdout,
	LogOutputSlack,
	LogOutputOpenSearch,
	LogOutputLoki,
	LogOutputCloudWatchLogs,
	LogOutputClickHouse,
}

const (
	LogLevelDebug LogLevel = iota + 1 // 1: Debug (detailed debugging information)
	LogLevelInfo                      // 2: Info (normal information)
	LogLevelWarn                      // 3: Warn (warning)
	LogLevelError                     // 4: Error (error)
	LogLevelFatal                     // 5: Fatal (fatal error)
)

// LogLevelToString maps each LogLevel to its string.
var LogLevelToString = map[LogLevel]string{
	LogLevelDebug: "DEBUG",
	LogLevelInfo:  "INFO",
	LogLevelWarn:  "WARN",
	LogLevelError: "ERROR",
	LogLevelFatal: "FATAL",
}

// LogLevelMaps maps each string to its LogLevel.
var LogLevelMaps = map[string]LogLevel{
	"DEBUG": LogLevelDebug,
	"INFO":  LogLevelInfo,
	"WARN":  LogLevelWarn,
	"ERROR": LogLevelError,
	"FATAL": LogLevelFatal,
}

var validLogOutputDestinations = func() map[LogOutputDestination]struct{} {
	destinations := make(map[LogOutputDestination]struct{}, len(allLogOutputDestinations))
	for _, destination := range allLogOutputDestinations {
		destinations[destination] = struct{}{}
	}
	return destinations
}()

var (
	// ErrNoDeliveryTargets reports that a DeliveryReport has no destination
	// results. A configuration without outputs is rejected at construction with
	// ErrNoActiveLogOutput, so this error arises only from an internal
	// inconsistency.
	ErrNoDeliveryTargets           = errors.New("delivery report has no target results")
	ErrTargetDeliveryNotRegistered = errors.New("target delivery was not registered")
	// ErrNoActiveLogOutput reports a configuration in which no backend starts as
	// a regular output. When LogOutputDestinations is empty, every non-nil
	// backend configuration is treated as a regular output, so a configuration
	// without any backend configuration also results in this error. Handler-only
	// backends and the dedicated destinations in InternalLogDestinations do not
	// count as regular outputs. Without an output, logs would be lost silently,
	// so the sync, async and Delivery loggers return an error that wraps this
	// error before construction.
	ErrNoActiveLogOutput = errors.New("no active log output: configure at least one backend as a log output")
)

// String converts the LogLevel to a string.
func (l LogLevel) String() string {
	if str, ok := LogLevelToString[l]; ok {
		return str
	}
	return fmt.Sprintf("UNKNOWN(%d)", l)
}

func (l LogLevel) IsValid() bool {
	_, ok := LogLevelToString[l]
	return ok
}

// ParseLogLevel converts a string to a LogLevel. It ignores case and leading
// and trailing white space.
func ParseLogLevel(levelStr string) (LogLevel, error) {
	normalized := strings.ToUpper(strings.TrimSpace(levelStr))
	if normalized == "" {
		return 0, errors.New("log level is empty")
	}

	if level, ok := LogLevelMaps[normalized]; ok {
		return level, nil
	}
	return 0, fmt.Errorf("invalid log level: %q", levelStr)
}

func (d LogOutputDestination) String() string {
	return string(d)
}

func (d LogOutputDestination) IsValid() bool {
	_, ok := validLogOutputDestinations[d]
	return ok
}

// AllLogOutputDestinations returns every available output destination in the
// order of definition. It returns a new copy on each call, so that callers
// cannot modify the canonical list.
func AllLogOutputDestinations() LogOutputDestinations {
	return slices.Clone(allLogOutputDestinations)
}

func ParseLogOutputDestination(value string) (LogOutputDestination, error) {
	d := LogOutputDestination(strings.ToLower(strings.TrimSpace(value)))
	if d == "" {
		return "", errors.New("log output destination is empty")
	}
	if d == "cloudwatch" {
		d = LogOutputCloudWatchLogs
	}
	if !d.IsValid() {
		return "", fmt.Errorf("unsupported log output destination: %q", value)
	}
	return d, nil
}

func ParseLogOutputDestinations(values []string) (LogOutputDestinations, error) {
	destinations := make(LogOutputDestinations, 0, len(values))
	seen := make(map[LogOutputDestination]struct{}, len(values))

	for _, value := range values {
		v := strings.TrimSpace(value)
		if v == "" {
			continue
		}

		destination, err := ParseLogOutputDestination(v)
		if err != nil {
			return nil, err
		}

		if _, ok := seen[destination]; ok {
			continue
		}

		seen[destination] = struct{}{}
		destinations = append(destinations, destination)
	}

	return destinations, nil
}

func (ds LogOutputDestinations) Has(target LogOutputDestination) bool {
	for _, d := range ds {
		if d == target {
			return true
		}
	}
	return false
}

func (ds LogOutputDestinations) Strings() []string {
	values := make([]string, 0, len(ds))
	for _, d := range ds {
		values = append(values, d.String())
	}
	return values
}

func (ds LogOutputDestinations) String() string {
	return strings.Join(ds.Strings(), ",")
}

// DeliveryReport is the result of a DeliveryLogger call. To decide whether to
// stop, for example for audit logs, use HasUnregisteredTarget for a
// fail-closed decision.
type DeliveryReport struct {
	// Targets holds the result for each intended destination backend. It keeps
	// every success, enqueue, skip, failure and unconfirmed result without
	// omission, in an order that is fixed for each logger.
	Targets []DeliveryResult

	// Durables holds the results of durable storage performed after delivery to
	// the intended backend failed. Even a successful save to an archive, a local
	// file, a dead letter store and so on does not substitute for successful
	// delivery to the intended backend.
	Durables []DurableResult

	// Evacuations holds the results of a direct evacuation, used when no
	// durable failure store is selected and degrade is enabled, and of the
	// overload policy applied when the durable store was overloaded: a spill
	// to the evacuation destination under degrade, or a drop under drop. A
	// successful evacuation does not mean successful delivery to the intended
	// backend either.
	Evacuations []EvacuationResult
}

// DeliveryResult is the result of sending to a single backend. Results of
// several backends are aggregated with DeliveryReport; this type does not hold
// child results.
type DeliveryResult struct {
	// Backend is the single destination that returned this result.
	Backend string

	// Status is the result for this destination.
	Status DeliveryStatus

	// Err holds the cause that occurred at this destination. The error that a
	// DeliveryLogger returns is for controlling the call, and Err is for the
	// details of each destination in the report.
	Err error
}

// NewTargetDeliveryReport returns a report whose only intended destination
// result is result. It does not include results of durable storage or
// evacuation; the caller that performs them adds them later.
func NewTargetDeliveryReport(result DeliveryResult) DeliveryReport {
	return DeliveryReport{Targets: []DeliveryResult{result}}
}

type DeliveryStatus string

const (
	// DeliverySucceeded means that registration at, or output to, the intended
	// destination was confirmed. It does not mean that the log was saved to a
	// durable fallback.
	DeliverySucceeded DeliveryStatus = "succeeded"

	// DeliveryFailed means that registration at the backend, the intended
	// destination, explicitly failed.
	DeliveryFailed DeliveryStatus = "failed"

	// DeliveryRejected means that the log was rejected without starting to send
	// it to the backend, for example because the logger was closing, local
	// validation failed, or a circuit breaker was open.
	DeliveryRejected DeliveryStatus = "rejected"

	// DeliverySkipped means that this backend was not a target of this send,
	// for example because of its level setting. It is not a successful
	// registration at the backend, so IsRegistered remains false.
	DeliverySkipped DeliveryStatus = "skipped"

	// DeliveryEnqueued means that the log was accepted into the local send
	// queue. Completion of the send to the backend and of storage there is not
	// confirmed, so IsRegistered remains false.
	DeliveryEnqueued DeliveryStatus = "enqueued"

	// DeliveryUnconfirmed means that whether registration succeeded cannot be
	// confirmed, for example because of a timeout. It is not treated as a
	// success for audit logs.
	DeliveryUnconfirmed DeliveryStatus = "unconfirmed"
)

func (s DeliveryStatus) String() string {
	return string(s)
}

func (s DeliveryStatus) IsRegistered() bool {
	return s == DeliverySucceeded
}

func (s DeliveryStatus) IsFailed() bool {
	return s == DeliveryFailed || s == DeliveryRejected
}

func (s DeliveryStatus) IsPending() bool {
	return s == DeliveryEnqueued
}

func (s DeliveryStatus) IsUnconfirmed() bool {
	return s == DeliveryUnconfirmed
}

func (s DeliveryStatus) IsEnqueued() bool {
	return s.IsPending()
}

// DurableResult is the result of durable storage performed after delivery to
// the intended backend failed.
type DurableResult struct {
	Backend string
	Kind    DurableKind
	Status  DurableStatus
	Err     error
}

type DurableKind string

const (
	DurableArchive    DurableKind = "archive"
	DurableDeadLetter DurableKind = "dead_letter"
	DurableLocalFile  DurableKind = "local_file"
)

func (k DurableKind) String() string {
	return string(k)
}

type DurableStatus string

const (
	DurableSucceeded DurableStatus = "succeeded"
	DurableFailed    DurableStatus = "failed"
	DurableSkipped   DurableStatus = "skipped"
	DurableUnknown   DurableStatus = "unknown"
)

func (s DurableStatus) String() string {
	return string(s)
}

func (s DurableStatus) IsSucceeded() bool {
	return s == DurableSucceeded
}

// EvacuationResult is the result of an evacuation outside the failure store, or
// of the action taken under overload.
type EvacuationResult struct {
	Kind   EvacuationKind
	Status EvacuationStatus
	Err    error
}

// EvacuationKind is the action that an EvacuationResult reports.
type EvacuationKind string

const (
	// EvacuationSpilled reports that the entries were written to an evacuation
	// destination, by a direct evacuation or by the degrade overload policy.
	EvacuationSpilled EvacuationKind = "spilled"
	// EvacuationDropped reports that the drop overload policy discarded the
	// entries. Applying the policy is reported as EvacuationSucceeded, so use
	// DeliveryReport.HasDrop to detect discarded entries.
	EvacuationDropped EvacuationKind = "dropped"
)

func (k EvacuationKind) String() string {
	return string(k)
}

type EvacuationStatus string

const (
	EvacuationSucceeded EvacuationStatus = "succeeded"
	EvacuationFailed    EvacuationStatus = "failed"
	EvacuationSkipped   EvacuationStatus = "skipped"
	EvacuationUnknown   EvacuationStatus = "unknown"
)

func (s EvacuationStatus) String() string {
	return string(s)
}

func (s EvacuationStatus) IsSucceeded() bool {
	return s == EvacuationSucceeded
}

// FailedTargets returns only the destinations that explicitly failed or were
// rejected. DeliveryUnconfirmed and DeliverySkipped are not registered, but
// they are not explicit failures, so they are not included.
func (r DeliveryReport) FailedTargets() []DeliveryResult {
	targets := make([]DeliveryResult, 0)
	for _, target := range r.Targets {
		if target.Status.IsFailed() {
			targets = append(targets, target)
		}
	}
	return targets
}

// SkippedTargets returns only the destinations that were not targets of the
// send.
func (r DeliveryReport) SkippedTargets() []DeliveryResult {
	targets := make([]DeliveryResult, 0)
	for _, target := range r.Targets {
		if target.Status == DeliverySkipped {
			targets = append(targets, target)
		}
	}
	return targets
}

// HasUnregisteredTarget returns true when registration at an intended backend
// has not been confirmed. To fail closed, it also returns true when Targets is
// empty. DeliveryEnqueued, DeliveryUnconfirmed and DeliverySkipped are not
// confirmed successes, so it returns true for them.
func (r DeliveryReport) HasUnregisteredTarget() bool {
	if len(r.Targets) == 0 {
		return true
	}

	for _, target := range r.Targets {
		if !target.Status.IsRegistered() {
			return true
		}
	}

	return false
}

// HasRegisteredTarget returns true when registration or output was confirmed
// for at least one destination. It tells whether any destination is registered
// even when some destinations were skipped.
func (r DeliveryReport) HasRegisteredTarget() bool {
	for _, target := range r.Targets {
		if target.Status.IsRegistered() {
			return true
		}
	}
	return false
}

// HasTargetFailure returns true when registration at an intended backend
// explicitly failed. DeliveryEnqueued, DeliveryUnconfirmed and DeliverySkipped
// are not successes, but they are not explicit failures either, so it returns
// false for them. A report with empty Targets arises only from an internal
// inconsistency, because a configuration without outputs is rejected at
// construction. Just as TargetError returns ErrNoDeliveryTargets, it returns
// true for such a report, as a failure without destinations. Use
// HasUnregisteredTarget for a fail-closed check for unregistered destinations.
func (r DeliveryReport) HasTargetFailure() bool {
	if len(r.Targets) == 0 {
		return true
	}
	for _, target := range r.Targets {
		if target.Status.IsFailed() {
			return true
		}
	}

	return false
}

func (r DeliveryReport) AllTargetsRegistered() bool {
	return !r.HasUnregisteredTarget()
}

func (r DeliveryReport) HasDurable() bool {
	return len(r.Durables) > 0
}

func (r DeliveryReport) HasDurableFailure() bool {
	for _, durable := range r.Durables {
		if !durable.Status.IsSucceeded() {
			return true
		}
	}
	return false
}

func (r DeliveryReport) HasEvacuation() bool {
	return len(r.Evacuations) > 0
}

func (r DeliveryReport) HasEvacuationFailure() bool {
	for _, evacuation := range r.Evacuations {
		if !evacuation.Status.IsSucceeded() {
			return true
		}
	}
	return false
}

func (r DeliveryReport) HasDrop() bool {
	for _, evacuation := range r.Evacuations {
		if evacuation.Kind == EvacuationDropped {
			return true
		}
	}
	return false
}

// HasAnyFailure returns true when any target result is an explicit failure or
// any durable or evacuation result has not succeeded. A report with empty
// Targets is also a failure, as with HasTargetFailure. HasAnyFailure returns
// true exactly when Error returns a non-nil error. To decide whether to stop
// for audit logs, normally use HasUnregisteredTarget.
func (r DeliveryReport) HasAnyFailure() bool {
	return r.HasTargetFailure() || r.HasDurableFailure() || r.HasEvacuationFailure()
}

// TargetError returns only the explicit registration failures of targets, as
// an error. It does not include DeliveryEnqueued, DeliveryUnconfirmed or
// DeliverySkipped. For a report with empty Targets, it returns
// ErrNoDeliveryTargets, consistent with HasTargetFailure.
func (r DeliveryReport) TargetError() error {
	if len(r.Targets) == 0 {
		return ErrNoDeliveryTargets
	}

	errs := make([]error, 0, len(r.Targets))
	for _, target := range r.Targets {
		if !target.Status.IsFailed() {
			continue
		}
		errs = append(errs, deliveryResultError(target))
	}

	return errors.Join(errs...)
}

// DurableError returns the durable storage results that have not succeeded
// combined into one error. The success or failure of durable storage is
// independent of the result of delivery to the intended backend.
func (r DeliveryReport) DurableError() error {
	errs := make([]error, 0, len(r.Durables))
	for _, durable := range r.Durables {
		if durable.Status.IsSucceeded() {
			continue
		}
		errs = append(errs, durableResultError(durable))
	}

	return errors.Join(errs...)
}

// EvacuationError returns the evacuation results that have not succeeded
// combined into one error. The success or failure of an evacuation is
// independent of the result of delivery to the intended backend.
func (r DeliveryReport) EvacuationError() error {
	errs := make([]error, 0, len(r.Evacuations))
	for _, evacuation := range r.Evacuations {
		if evacuation.Status.IsSucceeded() {
			continue
		}
		errs = append(errs, evacuationResultError(evacuation))
	}

	return errors.Join(errs...)
}

// Error returns the explicit failures of targets and the durable storage and
// evacuation results that have not succeeded combined into one error. Use
// HasUnregisteredTarget for a fail-closed check of backend registration.
func (r DeliveryReport) Error() error {
	return errors.Join(
		r.TargetError(),
		r.DurableError(),
		r.EvacuationError(),
	)
}

func deliveryResultError(result DeliveryResult) error {
	if result.Err != nil {
		return fmt.Errorf(
			"%w: backend=%q status=%s: %w",
			ErrTargetDeliveryNotRegistered,
			result.Backend,
			result.Status,
			result.Err,
		)
	}
	return fmt.Errorf(
		"%w: backend=%q status=%s",
		ErrTargetDeliveryNotRegistered,
		result.Backend,
		result.Status,
	)
}

func durableResultError(result DurableResult) error {
	if result.Err != nil {
		return fmt.Errorf(
			"durable store failed: backend=%q kind=%s status=%s: %w",
			result.Backend,
			result.Kind,
			result.Status,
			result.Err,
		)
	}
	return fmt.Errorf(
		"durable store failed: backend=%q kind=%s status=%s",
		result.Backend,
		result.Kind,
		result.Status,
	)
}

func evacuationResultError(result EvacuationResult) error {
	if result.Err != nil {
		return fmt.Errorf(
			"evacuation failed: kind=%s status=%s: %w",
			result.Kind,
			result.Status,
			result.Err,
		)
	}
	return fmt.Errorf(
		"evacuation failed: kind=%s status=%s",
		result.Kind,
		result.Status,
	)
}
