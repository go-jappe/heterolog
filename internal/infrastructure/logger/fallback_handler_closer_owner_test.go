package logger

import (
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"

	"github.com/stretchr/testify/require"
)

func TestFallbackHandlerCloserOwnerReportsSourceTargetAndLevel(t *testing.T) {
	closeErr := errors.New("close failed")
	opensearchCloser := &fallbackHandlerTestCloser{err: closeErr}
	lokiCloser := &fallbackHandlerTestCloser{err: closeErr}
	owner := newFallbackHandlerCloserOwner()

	owner.Register("opensearch", handlerStruct{
		OnError: []logging.NamedLoggerErrorHandler{{
			Name:   "clickhouse",
			Closer: opensearchCloser,
		}},
	})
	owner.Register("loki", handlerStruct{
		OnError: []logging.NamedLoggerErrorHandler{{
			Name:   "clickhouse",
			Closer: lokiCloser,
		}},
	})

	err := owner.Close()

	require.ErrorIs(t, err, closeErr)
	// 登録と逆の順序で閉じるため、後に登録したlokiのhandlerから報告されます。
	closeErrs := joinedErrorsOfType[*FallbackHandlerCloseError](err)
	require.Len(t, closeErrs, 2)
	for index, want := range []FallbackHandlerCloseError{
		{Source: "loki", Target: "clickhouse", Level: "error", Err: closeErr},
		{Source: "opensearch", Target: "clickhouse", Level: "error", Err: closeErr},
	} {
		require.Equal(t, want, *closeErrs[index])
	}
	require.Equal(t, 1, opensearchCloser.calls)
	require.Equal(t, 1, lokiCloser.calls)
}

type fallbackHandlerTestCloser struct {
	calls int
	err   error
}

func (c *fallbackHandlerTestCloser) Close() error {
	c.calls++
	return c.err
}
