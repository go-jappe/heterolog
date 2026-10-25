package logging

import (
	"context"
	"slices"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// BackendLogEntryHandlerは、MultiLoggerが一度生成したbackend非依存の
// logical LogEntryを、通常backendが受け取るためのinfra内部契約です。
// application向けのDebug / Infoなどは要求しません。
type BackendLogEntryHandler interface {
	HandleLogEntry(context.Context, *LogEntry) error
}

// DeliveryBackendLogEntryHandlerは、DeliveryMultiLoggerが一度生成した
// backend非依存のlogical LogEntryをDelivery backendが受け取るための
// infra内部契約です。送達結果も返します。
type DeliveryBackendLogEntryHandler interface {
	HandleLogEntry(context.Context, *LogEntry) (pkglogger.DeliveryReport, error)
}

// CloneLogEntryは、並行実行される保存先ごとに独立して加工できるcopyを返します。
// sliceとFields mapは複製しますが、ErrとFields内の値はimmutableとして共有します。
func CloneLogEntry(entry *LogEntry) *LogEntry {
	if entry == nil {
		return nil
	}

	cloned := *entry
	cloned.stackPCs = slices.Clone(entry.stackPCs)
	cloned.File = slices.Clone(entry.File)
	cloned.Line = slices.Clone(entry.Line)
	cloned.FuncName = slices.Clone(entry.FuncName)
	if entry.Fields != nil {
		cloned.Fields = make(map[string]any, len(entry.Fields))
		for key, value := range entry.Fields {
			cloned.Fields[key] = value
		}
	}
	return &cloned
}
