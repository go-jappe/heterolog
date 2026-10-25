package file

import "github.com/go-jappe/heterolog/internal/infrastructure/logger/file/errors"

func (l *FileLogger) wrapErrorByLogger(err error, op string) error {
	switch l.logger {
	case DefaultLogger:
		return &errors.DefaultLoggerInternalError{Op: op, Err: err}
	case DebugLogger:
		return &errors.DebugLoggerInternalError{Op: op, Err: err}
	case ErrorLogger:
		return &errors.ErrorLoggerInternalError{Op: op, Err: err}
	default:
		return &errors.DefaultLoggerInternalError{Op: op, Err: err}
	}
}
