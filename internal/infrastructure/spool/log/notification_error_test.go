package logspool_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	logspool "github.com/go-jappe/heterolog/internal/infrastructure/spool/log"
	"github.com/stretchr/testify/require"
)

func TestNotificationErrorKeepsFailuresJoinedWithCancellation(t *testing.T) {
	stoppedCtx, cancel := context.WithCancel(context.Background())
	cancel()
	failure := errors.New("filesystem failed")
	err := logspool.NotificationError(stoppedCtx, errors.Join(fmt.Errorf("stop: %w", context.Canceled), failure))
	require.ErrorIs(t, err, failure)
	require.NotErrorIs(t, err, context.Canceled)
	for _, err := range []error{nil, context.Canceled, fmt.Errorf("stop: %w", context.Canceled)} {
		require.NoError(t, logspool.NotificationError(stoppedCtx, err))
	}
	for _, ctx := range []context.Context{context.Background(), stoppedCtx} {
		require.ErrorIs(t, logspool.NotificationError(ctx, context.DeadlineExceeded), context.DeadlineExceeded)
	}
	require.ErrorIs(t, logspool.NotificationError(context.Background(), context.Canceled), context.Canceled)
}

// causeLessJoinErrorは原因を一つも返さないmulti-error実装です。
type causeLessJoinError struct{}

func (causeLessJoinError) Error() string   { return "multi error without causes" }
func (causeLessJoinError) Unwrap() []error { return nil }

// Unwrapがnilや空の原因を返すerrorも、中止要求ではない障害として通知します。
// runtimeが稼働中かどうかにかかわらず、原因を辿れないことを理由に捨ててはいけません。
func TestNotificationErrorKeepsErrorsWithoutCause(t *testing.T) {
	stoppedCtx, cancel := context.WithCancel(context.Background())
	cancel()
	for name, ctx := range map[string]context.Context{"running": context.Background(), "stopped": stoppedCtx} {
		t.Run(name, func(t *testing.T) {
			pathErr := &os.PathError{Op: "open", Path: "active.hlogspool"}
			require.Nil(t, pathErr.Unwrap())
			require.Same(t, pathErr, logspool.NotificationError(ctx, pathErr))

			wrapped := fmt.Errorf("claim spool file: %w", pathErr)
			require.Same(t, wrapped, logspool.NotificationError(ctx, wrapped))

			joined := logspool.NotificationError(ctx, errors.Join(fmt.Errorf("stop: %w", context.Canceled), pathErr))
			require.ErrorIs(t, joined, pathErr)

			var causeLess error = causeLessJoinError{}
			require.Equal(t, causeLess, logspool.NotificationError(ctx, causeLess))
		})
	}
}
