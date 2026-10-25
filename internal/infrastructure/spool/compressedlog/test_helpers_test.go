package compressedlog

import (
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	compressedlogworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/worker"
)

// hangDetectionTimeoutは、背景のgoroutineの進行を実時間で待つ試験が、処理が止まったと判断する
// までの上限です。synctestのbubble内では仮想時間の上限になります。
const hangDetectionTimeout = 10 * time.Second

func newReplayTestWorker(
	t *testing.T,
	store *objectstoragespool.Store,
	routes compressedlogworker.RouteRegistry,
) *compressedlogworker.Worker {
	t.Helper()
	worker, err := compressedlogworker.NewWorker(store, routes, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

// resolvedTempDirForTestは、symlinkを解決したtemporary directoryを返します。
// macOSの/var→/private/varのようにOSのtemporary directoryがsymlinkを経由しても、
// productionが返す物理pathと試験が組み立てるpathを同じ表記で比較できます。
func resolvedTempDirForTest(t testing.TB) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve temp dir: %v", err)
	}
	return dir
}

// synctestFromWallClockForTestは、synctestの仮想時計を開始時点の実時刻まで進めてからfを実行します。
// 実際のfilesystemが付けた更新時刻と、bubble内のtime.Nowによる時刻の前後関係を揃えます。
func synctestFromWallClockForTest(t *testing.T, f func(*testing.T)) {
	t.Helper()
	start := time.Now()
	synctest.Test(t, func(t *testing.T) {
		time.Sleep(time.Until(start))
		f(t)
	})
}
