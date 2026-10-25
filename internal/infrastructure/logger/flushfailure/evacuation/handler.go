// Package evacuation provides destination writers shared by direct degradation
// and durable-store overload handling.
package evacuation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

var ErrUnsupportedSpillDestination = errors.New("unsupported spill destination")

// UnsupportedSpillDestinationErrorは、対応していない退避先を指定したことを表します。
// Destinationは指定された退避先です。errors.Is(err, ErrUnsupportedSpillDestination)でも判定できます。
type UnsupportedSpillDestinationError struct {
	Destination pkgdurable.SpillDestination
}

func (e *UnsupportedSpillDestinationError) Error() string {
	return fmt.Sprintf(
		"%v: %q; only %q is implemented",
		ErrUnsupportedSpillDestination,
		e.Destination,
		pkgdurable.SpillDestinationStdout,
	)
}

func (e *UnsupportedSpillDestinationError) Is(target error) bool {
	return target == ErrUnsupportedSpillDestination
}

// stdoutBatchMu is shared by every stdoutHandler instance. A SendResult is
// encoded as one meta row followed by zero or more record rows, so the whole
// batch must stay contiguous even when different backends evacuate failures at
// the same time.
//
// The lock is package-global, so tests must not let a testing/synctest bubble
// contend for it with goroutines outside that bubble (for example a parallel
// test or another bubble): a waiter queued inside a bubble is released through
// a channel created in that bubble, and releasing it from outside makes synctest
// panic ("deadlock: all goroutines in bubble are blocked"). Tests that spill to
// stdout inside a bubble therefore must not run in parallel with other users.
var stdoutBatchMu infrafsstore.ContextMutex

type Handler interface {
	Handle(context.Context, loggerfailure.SendResult) error
}

func NewHandler(destination pkgdurable.SpillDestination) (Handler, error) {
	return NewHandlerWithStdout(destination, nil)
}

// NewHandlerWithStdoutは、stdoutへの退避にos.Stdoutの代わりにwriterを使うHandlerを構築します。
// writerがnilならos.Stdoutへ出力します。一回分の退避を連続して出力するlockは、
// 出力先にかかわらず全Handlerで共有します。
func NewHandlerWithStdout(destination pkgdurable.SpillDestination, stdout io.Writer) (Handler, error) {
	switch destination {
	case pkgdurable.SpillDestinationStdout:
		if stdout == nil {
			stdout = os.Stdout
		}
		return newStdoutHandler(stdout), nil
	default:
		return nil, &UnsupportedSpillDestinationError{Destination: destination}
	}
}

type stdoutHandler struct {
	writer io.Writer
}

// newStdoutHandler keeps the production destination fixed at os.Stdout while
// allowing tests to observe output without replacing the process-global file.
func newStdoutHandler(writer io.Writer) *stdoutHandler {
	return &stdoutHandler{writer: writer}
}

func (h *stdoutHandler) Handle(ctx context.Context, result loggerfailure.SendResult) error {
	if h == nil || h.writer == nil {
		return errors.New("stdout evacuation writer is nil")
	}

	if err := stdoutBatchMu.LockContext(ctx); err != nil {
		return err
	}
	defer stdoutBatchMu.Unlock()

	enc := json.NewEncoder(h.writer)
	enc.SetEscapeHTML(false)

	meta := map[string]any{
		"type":                  "meta",
		"handler":               "stdout_degrade",
		"backend":               result.Meta.Backend,
		"requestID":             result.Meta.RequestID,
		"appName":               result.Meta.AppName,
		"hostName":              result.Meta.HostName,
		"environment":           result.Meta.Environment,
		"runtimeType":           result.Meta.RuntimeType,
		"executionUnitID":       result.Meta.ExecutionUnitID,
		"version":               result.Meta.Version,
		"traceID":               result.Meta.TraceID,
		"attemptedAt":           result.Meta.AttemptedAt,
		"inputCount":            result.Meta.InputCount,
		"failedCount":           result.FailedCount(),
		"successCount":          result.SuccessCount(),
		"retryableRecordCount":  len(result.RetryableRecords),
		"deadRecordCount":       len(result.DeadRecords),
		"quarantineRecordCount": len(result.QuarantineRecords),
	}

	if result.TerminalError != nil {
		meta["terminalError"] = result.TerminalError.Error()
	}

	if err := enc.Encode(meta); err != nil {
		return fmt.Errorf("failed to write failure degrade stdout meta: %w", err)
	}

	for _, group := range []struct {
		kind    string
		records []loggerfailure.FailedRecord
	}{
		{kind: "retryable", records: result.RetryableRecords},
		{kind: "dead", records: result.DeadRecords},
		{kind: "quarantine", records: result.QuarantineRecords},
	} {
		for _, rec := range group.records {
			row := map[string]any{
				"type":          "record",
				"kind":          group.kind,
				"backend":       result.Meta.Backend,
				"requestID":     result.Meta.RequestID,
				"traceID":       result.Meta.TraceID,
				"failedAt":      rec.FailedAt,
				"operation":     rec.Operation,
				"statusCode":    rec.StatusCode,
				"failureCode":   rec.FailureCode,
				"failureReason": rec.FailureReason,
				"payload":       rec.Payload,
			}

			if err := enc.Encode(row); err != nil {
				return fmt.Errorf("failed to write failure degrade stdout %s record: %w", group.kind, err)
			}
		}
	}

	return nil
}
