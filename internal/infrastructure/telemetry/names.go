// Package telemetryは、loggerが記録するmetricsとspan属性の記録の共通処理をまとめます。
// 名前と属性の値はinternal/application/metricnamesで定義し、ここでは同じ名前で参照できる
// ようにします。
package telemetry

import "github.com/go-jappe/heterolog/internal/application/metricnames"

// metricnamesの名前と値です。説明はmetricnamesにあります。
const (
	FileDroppedEntries             = metricnames.FileDroppedEntries
	BackendRecords                 = metricnames.BackendRecords
	BackendRequestDuration         = metricnames.BackendRequestDuration
	BackendFlushRecords            = metricnames.BackendFlushRecords
	BackendFlushBytes              = metricnames.BackendFlushBytes
	BackendRetries                 = metricnames.BackendRetries
	FailureRecords                 = metricnames.FailureRecords
	CircuitBreakerRejectedRequests = metricnames.CircuitBreakerRejectedRequests
	CircuitBreakerTransitions      = metricnames.CircuitBreakerTransitions
	ObjectStorageUploads           = metricnames.ObjectStorageUploads
	ObjectStorageUploadDuration    = metricnames.ObjectStorageUploadDuration
	AsyncRejectedEntries           = metricnames.AsyncRejectedEntries
	AsyncBackendFailures           = metricnames.AsyncBackendFailures
	MemoryPressureRatio            = metricnames.MemoryPressureRatio
	MemoryPressureCritical         = metricnames.MemoryPressureCritical
	SpoolReplayRecords             = metricnames.SpoolReplayRecords
	TraceSpans                     = metricnames.TraceSpans
	TraceExportDuration            = metricnames.TraceExportDuration
	TraceExportRetries             = metricnames.TraceExportRetries
	MetricPoints                   = metricnames.MetricPoints
	MetricExportDuration           = metricnames.MetricExportDuration
	MetricExportRetries            = metricnames.MetricExportRetries
	InternalLogs                   = metricnames.InternalLogs
	AttrBackend                    = metricnames.AttrBackend
	AttrOutcome                    = metricnames.AttrOutcome
	AttrReason                     = metricnames.AttrReason
	AttrState                      = metricnames.AttrState
	AttrAction                     = metricnames.AttrAction
	AttrLevel                      = metricnames.AttrLevel
	AttrTarget                     = metricnames.AttrTarget
	AttrExporter                   = metricnames.AttrExporter
	ActionSpooled                  = metricnames.ActionSpooled
	ActionArchived                 = metricnames.ActionArchived
	ActionDegraded                 = metricnames.ActionDegraded
	ActionDropped                  = metricnames.ActionDropped
	ActionFailed                   = metricnames.ActionFailed
	ReasonInvalid                  = metricnames.ReasonInvalid
	ReasonClosed                   = metricnames.ReasonClosed
	ReasonError                    = metricnames.ReasonError
	ReasonTimeout                  = metricnames.ReasonTimeout
	ReasonPanic                    = metricnames.ReasonPanic
	StateOpen                      = metricnames.StateOpen
	StateClosed                    = metricnames.StateClosed
	OutcomeSuccess                 = metricnames.OutcomeSuccess
	OutcomeFailure                 = metricnames.OutcomeFailure
	OutcomeRetryable               = metricnames.OutcomeRetryable
	OutcomeDead                    = metricnames.OutcomeDead
	OutcomeQuarantine              = metricnames.OutcomeQuarantine
	OutcomeDropped                 = metricnames.OutcomeDropped
	OutcomePermanent               = metricnames.OutcomePermanent
	SpanAttrRecords                = metricnames.SpanAttrRecords
	SpanAttrBytes                  = metricnames.SpanAttrBytes
	SpanAttrAttempt                = metricnames.SpanAttrAttempt
	SpanAttrBackend                = metricnames.SpanAttrBackend
	SpanAttrTarget                 = metricnames.SpanAttrTarget
	SpanAttrDestination            = metricnames.SpanAttrDestination
	SpanAttrLevel                  = metricnames.SpanAttrLevel
)
