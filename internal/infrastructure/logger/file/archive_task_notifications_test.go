package file

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeferredNotificationsRunOnceAndContinueAfterHandlerPanic(t *testing.T) {
	var notifications backgroundTaskNotificationQueue
	var received []string
	notifications.enqueue(func() {
		received = append(received, "first")
		panic("notification handler failed")
	})
	notifications.enqueue(func() {
		received = append(received, "second")
	})
	require.Empty(t, received, "通知はファイル処理の所有者が保留した通知を実行するまで溜める")

	require.NotPanics(t, notifications.runDeferredNotifications)
	require.Equal(t, []string{"first", "second"}, received)
	require.Empty(t, notifications.notifications)

	notifications.runDeferredNotifications()
	require.Equal(t, []string{"first", "second"}, received, "通知先がpanicしても同じ通知を繰り返さない")
}
