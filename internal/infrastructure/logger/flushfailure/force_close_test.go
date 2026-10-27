package flushfailure

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

type forceFailureHandler struct {
	entered, finish chan struct{}
	forced          int
	shutdown        int
}

func (h *forceFailureHandler) HandleFailure(context.Context, loggerfailure.SendResult) error {
	close(h.entered)
	<-h.finish
	return nil
}

func (h *forceFailureHandler) ForceClose() error              { h.forced++; return pkgshutdown.ErrForcedClose }
func (h *forceFailureHandler) Shutdown(context.Context) error { h.shutdown++; return nil }

func TestCompositeForceCloseForwardsWithoutReleasingActiveHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		finish := make(chan struct{})
		var finishOnce sync.Once
		finishChild := func() { finishOnce.Do(func() { close(finish) }) }
		// 試験が途中で失敗しても、止めたchildを終えてから終了します。
		t.Cleanup(finishChild)
		child := &forceFailureHandler{entered: make(chan struct{}), finish: finish}
		composite, err := NewCompositeFailureHandler([]FailureHandler{child}, 1)
		require.NoError(t, err)
		handled := make(chan error, 1)
		go func() { handled <- composite.HandleFailure(context.Background(), loggerfailure.SendResult{}) }()
		<-child.entered
		drained := make(chan error, 1)
		go func() { drained <- composite.Drain(context.Background()) }()
		synctest.Wait()
		require.ErrorIs(t, composite.ForceClose(), pkgshutdown.ErrForcedClose)
		require.Equal(t, 1, child.forced)
		require.Zero(t, child.shutdown)
		require.Empty(t, drained)
		require.ErrorIs(t, composite.HandleFailure(context.Background(), loggerfailure.SendResult{}), ErrFailureHandlerNotAccepting)
		finishChild()
		require.NoError(t, <-handled)
		require.NoError(t, <-drained)
		require.Equal(t, 1, child.shutdown)
	})
}
