// Package evacuationは、直接のdegradationと、durable storeのoverload処理とで共有する
// 退避先のwriterを提供します。
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

// stdoutBatchMuは、すべてのstdoutHandler instanceが共有します。SendResultは1行の
// meta行と、それに続く0行以上のrecord行としてencodeされるため、異なるbackendが
// 同時に失敗を退避する場合でも、batch全体が連続していなければなりません。
//
// このlockはpackage全体で1つなので、testでは、testing/synctestのbubbleと、その
// bubbleの外のgoroutine（たとえば並列のtestや別のbubble）とにこのlockを奪い合わせては
// いけません。bubble内で順番を待つwaiterはそのbubble内で作られたchannelを通じて
// 解放され、bubbleの外から解放するとsynctestがpanicします
// （"deadlock: all goroutines in bubble are blocked"）。そのため、bubble内でstdoutへ
// 退避するtestは、このlockを使う他の処理と並列に実行してはいけません。
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

// newStdoutHandlerは、writerへ出力するstdoutHandlerを作ります。本番ではNewHandlerWithStdoutが
// os.Stdoutを渡し、testはprocess全体で共有するos.Stdoutを置き換えずに出力を観測できます。
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
