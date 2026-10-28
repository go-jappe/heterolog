package wrapper

import (
	"context"
	"io"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/pkg/opensearch"
	"github.com/stretchr/testify/require"
)

func TestOpenSearchResponseReadFailureDoesNotRetryButTransportEOFDoes(t *testing.T) {
	for _, bulk := range []bool{false, true} {
		for _, uncertain := range []bool{false, true} {
			var cause error = io.ErrUnexpectedEOF
			if uncertain {
				cause = &opensearch.ResponseDecodeError{Operation: "send", Err: cause}
			}
			client := &recordingOpenSearchClient{}
			client.indexDocument = func(context.Context, opensearch.DocumentWriteRequest) (*opensearch.IndexResponse, error) {
				if len(client.documentCalls) == 1 {
					return nil, cause
				}
				return &opensearch.IndexResponse{}, nil
			}
			client.indexDocuments = func(context.Context, []opensearch.DocumentWriteRequest) (*opensearch.BulkResponse, error) {
				if len(client.bulkCalls) == 1 {
					return nil, cause
				}
				return &opensearch.BulkResponse{}, nil
			}
			retrying := newRetryingClient(client, fastRetryConfig(2), observer.NopTracer{}, nil)
			var err error
			if bulk {
				_, err = retrying.IndexDocuments(context.Background(), []opensearch.DocumentWriteRequest{{Index: "logs", ID: "fixed"}})
			} else {
				_, err = retrying.IndexDocument(context.Background(), opensearch.DocumentWriteRequest{Index: "logs", ID: "fixed"})
			}
			calls := len(client.documentCalls) + len(client.bulkCalls)
			if uncertain {
				require.ErrorIs(t, err, cause)
				require.Equal(t, 1, calls)
			} else {
				require.NoError(t, err)
				require.Equal(t, 2, calls)
			}
		}
	}
}
