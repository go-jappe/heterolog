package slack

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func (s *SlackLogger) InternalDebugEntry(msg, requestId string) *logging.LogEntry {
	return &logging.LogEntry{
		RequestID:  requestId,
		OccurredAt: logging.ResolveOccurredAt(s.Clock),
		Level:      pkglogger.LogLevelDebug,
		Message:    msg,
		Internal:   true,
		Source:     common.SourceNameSlackLogger,
	}
}

func (s *SlackLogger) InternalInfoEntry(msg, requestId string) *logging.LogEntry {
	return &logging.LogEntry{
		RequestID:  requestId,
		OccurredAt: logging.ResolveOccurredAt(s.Clock),
		Level:      pkglogger.LogLevelInfo,
		Message:    msg,
		Internal:   true,
		Source:     common.SourceNameSlackLogger,
	}
}

func (s *SlackLogger) InternalWarnEntry(msg, requestId string) *logging.LogEntry {
	return &logging.LogEntry{
		RequestID:  requestId,
		OccurredAt: logging.ResolveOccurredAt(s.Clock),
		Level:      pkglogger.LogLevelWarn,
		Message:    msg,
		Internal:   true,
		Source:     common.SourceNameSlackLogger,
	}
}

func (s *SlackLogger) InternalErrorEntry(msg, requestId string, err error) *logging.LogEntry {

	return &logging.LogEntry{
		RequestID:  requestId,
		OccurredAt: logging.ResolveOccurredAt(s.Clock),
		Level:      pkglogger.LogLevelError,
		Message:    msg,
		Err:        err,
		Internal:   true,
		Source:     common.SourceNameSlackLogger,
	}
}

func (s *SlackLogger) InternalErrorEntryWithStack(msg, requestId string, err error, depth int) *logging.LogEntry {
	// 呼び出し元をdepthの数の深さまで取得
	var funcNames, sourceFiles []string
	var lines []int

	if depth > 0 {
		funcNames, sourceFiles, lines = logging.CollectCallerDetails(depth)
	}

	return &logging.LogEntry{
		RequestID:  requestId,
		OccurredAt: logging.ResolveOccurredAt(s.Clock),
		Level:      pkglogger.LogLevelError,
		Message:    msg,
		FuncName:   funcNames,
		File:       sourceFiles,
		Line:       lines,
		Err:        err,
		Internal:   true,
		Source:     common.SourceNameSlackLogger,
	}
}
