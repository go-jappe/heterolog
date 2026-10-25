//go:build !windows

package retryafter

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil"
)

// markerとそのdirectoryは、他のspool fileと同じく所有者だけに公開する。
// 置き換えた後のmarkerも同じ権限を保つ。
func TestStoreCreatesOwnerOnlyMarkerOnUnix(t *testing.T) {
	t.Parallel()
	logSpool, err := NewLogSpoolStore(resolvedTempDirForTest(t), "opensearch")
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(resolvedTempDirForTest(t), "s3-a")
	if err := os.Mkdir(destination, 0o750); err != nil {
		t.Fatal(err)
	}
	objectStorage, err := NewObjectStorageStore(destination, "s3-a")
	if err != nil {
		t.Fatal(err)
	}
	for name, store := range map[string]*Store{"log spool": logSpool, "object storage": objectStorage} {
		t.Run(name, func(t *testing.T) {
			for _, wait := range []time.Duration{time.Minute, time.Hour} {
				if err := store.Save(State{NotBefore: testNow.Add(wait)}); err != nil {
					t.Fatal(err)
				}
				testutil.RequirePrivatePermissions(t, store.Path(), 0o600)
				testutil.RequirePrivatePermissions(t, filepath.Dir(store.Path()), 0o700)
			}
		})
	}
}
