package wrapper

import (
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/pkg/clickhouse"
	"github.com/stretchr/testify/require"
)

func TestClickHouseResponseReadFailureDoesNotRetryButTransportEOFDoes(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		var cause error = io.ErrUnexpectedEOF
		if uncertain {
			cause = &clickhouse.ResponseDecodeError{Err: cause}
		}
		client := &recordingClickHouseClient{}
		client.insertLogsWithOptions = func(context.Context, []clickhouse.LogRecord, clickhouse.InsertOptions) (*clickhouse.InsertResponse, error) {
			if len(client.calls) == 1 {
				return nil, cause
			}
			return &clickhouse.InsertResponse{StatusCode: http.StatusOK}, nil
		}
		retrying := newRetryingClient(client, fastRetryConfig(2), observer.NopTracer{})
		_, err := retrying.InsertLogsWithOptions(context.Background(), []clickhouse.LogRecord{{Message: "keep"}}, clickhouse.InsertOptions{})
		if uncertain {
			require.ErrorIs(t, err, cause)
			require.Len(t, client.calls, 1)
		} else {
			require.NoError(t, err)
			require.Len(t, client.calls, 2)
		}
	}
}
