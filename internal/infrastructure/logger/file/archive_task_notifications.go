package file

// taskNotificationDeliveryは、通知を保留するか、その場で実行するかを呼び出し元が指定します。
// ファイル操作中はqueue.enqueueを渡し、lockを解放してからqueue.runDeferredNotificationsを呼びます。
// 保留が不要な経路はrunNotificationImmediatelyを渡します。nilは渡しません。
type taskNotificationDelivery func(notification func())

// backgroundTaskNotificationQueueは、一回のファイル処理で発生した内部ログ通知を保留します。
// 処理ごとに作成し、通知が発生するたびに直列に追加します。
// 呼び出し元はファイル操作・lockの解放を済ませてから保留した通知を実行し、
// 通知がすべて戻った後で処理をCloseの待機対象から外します。
type backgroundTaskNotificationQueue struct {
	notifications []func()
}

// archiveTaskNotificationQueueとcleanupTaskNotificationQueueは、rotation処理とcleanup処理で
// 同じ通知の保留・実行規則を用途別の名前で使うための別名です。
type archiveTaskNotificationQueue = backgroundTaskNotificationQueue
type cleanupTaskNotificationQueue = backgroundTaskNotificationQueue

// enqueueは通知を保留します。この呼び出しでは通知先を実行しません。
func (q *backgroundTaskNotificationQueue) enqueue(notification func()) {
	if notification == nil {
		return
	}
	q.notifications = append(q.notifications, notification)
}

// runNotificationImmediatelyは通知をその場で実行します。通知先のpanicは呼び出し元へ伝わります。
// 通知先から同じ所有者を終了する場合はRequestCloseを使い、通知自身の完了を待たないようにします。
// notificationには実行する通知関数を必ず渡してください。
func runNotificationImmediately(notification func()) {
	notification()
}

// runDeferredNotificationsは、処理中に保留した通知を登録順に実行します。
// 呼び出し元はファイル操作とlockの解放を終えてから呼び、通知完了まで終了待ちの件数を維持します。
// rotation・cleanupは、このメソッドが戻った後にendBackgroundOperationで件数を減らします。
// このメソッド自体は件数を変更しません。通知先が終了を要求する場合はRequestCloseを使います。
// 各通知先のpanicを回収し、残りの通知も続けて実行します。
func (q *backgroundTaskNotificationQueue) runDeferredNotifications() {
	if q == nil {
		return
	}
	notifications := q.notifications
	q.notifications = nil
	for _, notification := range notifications {
		func() {
			defer func() {
				_ = recover()
			}()
			notification()
		}()
	}
}
