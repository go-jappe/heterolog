package stdout

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func (s *StdoutLogger) InternalErrorEntry(msg, requestId string, err error) *logging.LogEntry {

	return &logging.LogEntry{
		RequestID:  requestId,
		OccurredAt: logging.ResolveOccurredAt(s.Clock),
		Level:      pkglogger.LogLevelError,
		Message:    msg,
		Err:        err,
		Internal:   true,
		Source:     common.SourceNameStdoutLogger,
	}
}
