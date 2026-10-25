package wrapper

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/loki"
	"github.com/stretchr/testify/require"
)

func TestRetryClientsUseHTTPStatusDespiteDisconnectWordsInResponse(t *testing.T) {
	for _, selection := range []pkglogger.RetryClientSelection{
		{Mode: pkglogger.RetryClientModeRetry, UseRetryingClient: true},
		{Mode: pkglogger.RetryClientModeRetryPolicy, UseRetryPolicyClient: true},
	} {
		for _, code := range []int{http.StatusBadRequest, http.StatusTooManyRequests, http.StatusServiceUnavailable} {
			for _, body := range []string{"request rejected", "upstream connection refused", "unexpected EOF"} {
				t.Run(fmt.Sprintf("%s/%d/%s", selection.Mode, code, body), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						started := time.Now()
						streams := []loki.Stream{{
							Labels:  map[string]string{"app": "service"},
							Entries: []loki.Entry{{Timestamp: started, Line: "original record"}},
						}}
						statusErr := &loki.HTTPStatusError{Code: code, Body: []byte(body), RetryAfterDuration: 3 * time.Second}
						var attemptedAt []time.Time
						raw := &recordingLokiClient{push: func(_ context.Context, got []loki.Stream) error {
							attemptedAt = append(attemptedAt, time.Now())
							require.Equal(t, streams, got, "retries must preserve the original record")
							if code == http.StatusBadRequest || len(attemptedAt) < 3 {
								return statusErr
							}
							return nil
						}}
						client, err := BuildClientChain(raw, chainRetryConfig{
							staticRetryConfig: staticRetryConfig{maxAttempts: 3, baseDelay: time.Millisecond, maxDelay: 5 * time.Second},
							selection:         selection,
						}, nil, nil)
						require.NoError(t, err)

						err = client.Push(context.Background(), streams)
						if code == http.StatusBadRequest {
							require.ErrorIs(t, err, statusErr)
							require.Equal(t, []time.Time{started}, attemptedAt, "permanent rejection must not retry or back off")
							require.Zero(t, time.Since(started))
							return
						}
						require.NoError(t, err, "the third allowed attempt succeeds")
						require.Equal(t, []time.Time{started, started.Add(3 * time.Second), started.Add(6 * time.Second)}, attemptedAt,
							"HTTP retry budget and Retry-After must not depend on response wording")
					})
				})
			}
		}
	}
}
