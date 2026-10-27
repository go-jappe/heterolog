package logger

import "strings"

// LoggerContractKind classifies a top-level MultiLogger exposed to the
// application as sync, async best-effort or delivery, by its contract for how it
// is called and what it returns. It does not represent the kind of a child
// Logger, such as File, Loki or OpenSearch, or of an output backend. The value
// is a canonical ID used to build diagnostic sources and the ScopeLabel of a
// process-local directory Lease. It is neither a display name for humans nor an
// ID that uniquely identifies an individual MultiLogger instance.
type LoggerContractKind string

const (
	// LoggerContractSync is the contract under which the caller waits for each
	// child Logger to finish its work and receives the errors.
	LoggerContractSync LoggerContractKind = "logger"
	// LoggerContractAsyncBestEffort is the contract that runs child Loggers in
	// separate goroutines and does not return their errors to the caller. It
	// does not mean that a child is dropped because of the concurrency limit.
	LoggerContractAsyncBestEffort LoggerContractKind = "async_logger"
	// LoggerContractDelivery is the contract that returns the delivery result
	// for each destination in a DeliveryReport.
	LoggerContractDelivery LoggerContractKind = "delivery_logger"
)

// String returns the canonical ID of the LoggerContractKind.
func (k LoggerContractKind) String() string {
	return string(k)
}

// DisplayName returns the contract name to embed in text that humans read, such
// as error messages. It leaves the canonical ID itself unchanged and only
// replaces the underscores that separate words with spaces, so display names are
// not maintained twice as separate constants.
func (k LoggerContractKind) DisplayName() string {
	return strings.ReplaceAll(k.String(), "_", " ")
}
