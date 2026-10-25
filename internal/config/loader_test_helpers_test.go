package config

import (
	"fmt"
	"path/filepath"
	"runtime/debug"
	"testing"
	"testing/synctest"
	"time"
)

func mustLoadConfig[T any](t *testing.T, load func() (T, error)) T {
	t.Helper()

	cfg, err := load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return cfg
}

// resolvedTempDirForTestは、symbolic linkを解決したt.TempDir()を返します。macOSの一時
// directoryは/var（/private/varへのlink）配下にあり、保存先の実体pathを解決する処理の
// 結果と比べる期待値を、入力側で同じ実体pathにそろえるために使います。
func resolvedTempDirForTest(t *testing.T) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve temporary directory: %v", err)
	}
	return directory
}

// synctestHangLimitは、synctestの試験が偽の時刻で終わるはずの処理を、実時間で待つ上限です。
// 判定には使わず、bubble内のgoroutineが止まった場合の停止検出だけに使います。
const synctestHangLimit = 2 * time.Minute

// synctestWithHangWatchdogは、synctest.Testの完了を実時間で監視します。bubble内の
// goroutineがsync.Mutexなど偽の時刻を進められない待ちで止まると、試験は既定のtimeoutまで
// 停止するため、上限を過ぎた時点で全goroutineのstackを出力して失敗させます。
func synctestWithHangWatchdog(t *testing.T, f func(*testing.T)) {
	t.Helper()
	name := t.Name()
	watchdog := time.AfterFunc(synctestHangLimit, func() {
		debug.SetTraceback("all")
		panic(fmt.Sprintf(
			"%s: synctest bubble did not finish within %s of real time; a goroutine is blocked on an operation that does not advance fake time (for example sync.Mutex)",
			name,
			synctestHangLimit,
		))
	})
	defer watchdog.Stop()
	synctest.Test(t, f)
}
