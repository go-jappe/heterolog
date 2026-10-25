package opensearch

import "github.com/go-jappe/heterolog/internal/infrastructure/logger/opensearch/errors"

func (l *LoggerBase) wrapErrorByLogger(err error, op string) error {
	return &errors.OpenSearchLoggerInternalError{Op: op, Err: err}
}
