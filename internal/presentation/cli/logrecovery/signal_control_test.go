package logrecovery

import (
	"context"
	"os"
	"testing"
	"time"
)

// stopSignalHangTimeoutは、終了要求の通知が届かない停止（hang）を検出するまで待つ上限です。
// 正常な処理に必要な時間の見積もりではありません。
const stopSignalHangTimeout = 10 * time.Second

func waitForStopSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(stopSignalHangTimeout):
		t.Fatalf("stop request was not delivered within %s", stopSignalHangTimeout)
	}
}

func TestSignalsRequestGracefulThenForcedStop(t *testing.T) {
	signals := make(chan os.Signal, 3)
	control := NewSignalControl(context.Background(), signals)
	defer control.Close()
	signals <- os.Interrupt
	waitForStopSignal(t, control.Context().Done())
	select {
	case <-control.ForceStop():
		t.Fatal("first signal must not force stop")
	default:
	}
	signals <- os.Interrupt
	waitForStopSignal(t, control.ForceStop())
	// 追加signalが到着してもchannelの二重closeやprocess終了は起こしません。
	signals <- os.Interrupt
	control.Close()
	control.Close()
}

func TestSignalAfterParentCancellationForcesWaitingShutdown(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 1)
	control := NewSignalControl(parent, signals)
	defer control.Close()
	cancel()
	waitForStopSignal(t, control.Context().Done())
	signals <- os.Interrupt
	waitForStopSignal(t, control.ForceStop())
}

func TestSignalControlCloseEndsIdleReceptionWithoutForceRequest(t *testing.T) {
	control := NewSignalControl(context.Background(), make(chan os.Signal))
	control.Close()
	waitForStopSignal(t, control.Context().Done())
	select {
	case <-control.stopped:
	default:
		t.Fatal("signal goroutine is still running after Close")
	}
	select {
	case <-control.ForceStop():
		t.Fatal("cleanup must not request a forced recovery stop")
	default:
	}
}
