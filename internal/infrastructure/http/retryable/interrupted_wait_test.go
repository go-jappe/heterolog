package retryable_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	chclassifier "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse/retryclassifier"
	cwclassifier "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/retryclassifier"
	lokiclassifier "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki/retryclassifier"
	osclassifier "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch/retryclassifier"
	"github.com/go-jappe/heterolog/pkg/clickhouse"
	"github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	"github.com/go-jappe/heterolog/pkg/loki"
	"github.com/go-jappe/heterolog/pkg/opensearch"
	"github.com/stretchr/testify/require"
)

func TestInterruptedWaitPreservesBackendClassificationAndBothCauses(t *testing.T) {
	chClassify := func(ctx context.Context, err error) retryable.RetryDecision {
		return chclassifier.ClassifyDurableInsertFailure(ctx, err, 1)
	}
	for _, stop := range []error{context.Canceled, context.DeadlineExceeded} {
		for _, tt := range []struct {
			name      string
			last      error
			classify  func(context.Context, error) retryable.RetryDecision
			wantRetry bool
			wantCode  int
		}{
			{"Loki temporary refusal", &loki.HTTPStatusError{Code: 503}, lokiclassifier.ClassifyDurableRequestFailure, true, 503},
			{"CloudWatch HTTP400 throttle", &cloudwatchlogs.HTTPStatusError{Code: 400, AwsErrorType: "ThrottlingException"}, cwclassifier.ClassifyDurableRequestFailure, true, 400},
			{"ClickHouse permanent HTTP500", &clickhouse.HTTPStatusError{Code: 500, Body: []byte("Code: 60. DB::Exception: unknown table. (UNKNOWN_TABLE)")}, chClassify, false, 500},
			{"OpenSearch temporary refusal", &opensearch.HTTPStatusError{Code: 503}, osclassifier.ClassifyDurableRequestFailure, true, 503},
		} {
			t.Run(tt.name+"/"+stop.Error(), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				err := fmt.Errorf("client operation: %w", retryable.InterruptedWait(tt.last, stop))
				require.ErrorIs(t, err, tt.last)
				require.ErrorIs(t, err, stop)
				// 即時retryは、それでも止まらなければなりません。durable分類は完了したrequestに
				// ついての別の判断であり、今すぐ続行してよいという許可ではありません。
				require.False(t, retryable.ClassifyRetry(ctx, err).Retry)
				got := retryable.ClassifyDurableRequestOutcome(ctx, err, tt.classify)
				require.Equal(t, tt.wantRetry, got.Retry)
				require.Equal(t, tt.wantCode, got.Code)
				require.False(t, got.Quarantine)
			})
		}
	}
}

func TestInterruptedWaitDoesNotInventConfirmedResponseEvidence(t *testing.T) {
	statusErr := &loki.HTTPStatusError{Code: 503}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tt := range []struct {
		name     string
		err      error
		classify func(context.Context, error) retryable.RetryDecision
	}{
		{"in-flight cancellation", context.Canceled, lokiclassifier.ClassifyDurableRequestFailure},
		{"ordinary joined HTTP and cancellation", errors.Join(statusErr, context.Canceled), lokiclassifier.ClassifyDurableRequestFailure},
		{"wait joined with another unknown request", errors.Join(retryable.InterruptedWait(statusErr, context.Canceled), io.EOF), lokiclassifier.ClassifyDurableRequestFailure},
		{"EOF before wait", retryable.InterruptedWait(io.EOF, context.Canceled), lokiclassifier.ClassifyDurableRequestFailure},
		{"timeout before wait", retryable.InterruptedWait(context.DeadlineExceeded, context.Canceled), lokiclassifier.ClassifyDurableRequestFailure},
		{"HTTP joined with unknown transport result", retryable.InterruptedWait(errors.Join(statusErr, io.EOF), context.Canceled), lokiclassifier.ClassifyDurableRequestFailure},
		{"unclassified error", retryable.InterruptedWait(errors.New("unconfirmed"), context.Canceled), lokiclassifier.ClassifyDurableRequestFailure},
		{"CloudWatch uncertain response wrapping status", retryable.InterruptedWait(&cloudwatchlogs.ResponseDecodeError{Err: statusErr}, context.Canceled), cwclassifier.ClassifyDurableRequestFailure},
		{"OpenSearch uncertain response wrapping status", retryable.InterruptedWait(&opensearch.ResponseDecodeError{Err: statusErr}, context.Canceled), osclassifier.ClassifyDurableRequestFailure},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := retryable.ClassifyDurableRequestOutcome(ctx, tt.err, tt.classify)
			require.False(t, got.Retry)
			require.True(t, got.Quarantine)
		})
	}
}
