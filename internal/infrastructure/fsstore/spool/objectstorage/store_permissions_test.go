package objectstoragespool

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestStoreKeepsPrivatePermissionsAcrossStateMoves(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	source := writeTestSource(t, t.TempDir(), "app_20260911_1.log.gz", "compressed payload")
	file, err := store.SaveRetryableFailure(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	// 新規directoryはdirectoryModeとprocess umaskで作成します。umaskが追加で外したbitは許容し、
	// directoryModeを超える権限が付かないことを確認します。
	createdMode := createdDirectoryModeForTest(t, directoryMode)
	for _, directory := range []string{
		store.Root().ActiveDir(), store.Root().ProcessingDir(), store.Root().DoneDir(),
		store.Root().DeadDir(), store.Root().QuarantineDir(), store.Root().stagingDir(), store.Root().preservedDir(),
	} {
		testutil.RequirePrivatePermissions(t, directory, createdMode)
	}
	testutil.RequirePrivatePermissions(t, file.Path, payloadMode)
	claimed, ok, err := store.ClaimOldestActive(ctx)
	if err != nil || !ok {
		t.Fatalf("claim = %t, error = %v", ok, err)
	}
	testutil.RequirePrivatePermissions(t, claimed.Path, payloadMode)
	done, err := store.TransitionProcessing(ctx, claimed, pkgspool.StateDone)
	if err != nil {
		t.Fatal(err)
	}
	testutil.RequirePrivatePermissions(t, done.Path, payloadMode)
	if data, err := os.ReadFile(done.Path); err != nil || string(data) != "compressed payload" {
		t.Fatalf("stored bytes = %q, error = %v", data, err)
	}
	// 同一bytesの再handoffでも、既存payloadのprivate設定を維持します。
	adopted, err := store.SaveRetryableFailure(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	testutil.RequirePrivatePermissions(t, adopted.Path, payloadMode)
	assertDirectoryEmpty(t, store.Root().stagingDir())
}

// createdDirectoryModeForTestは、process umaskを変更せずに、modeで新規作成した
// directoryの実際の権限を観測します。並行する試験のumaskにも影響しません。
// WindowsのRequirePrivatePermissionsはmodeではなくACLを検査します。
func createdDirectoryModeForTest(t *testing.T, mode os.FileMode) os.FileMode {
	t.Helper()
	control := filepath.Join(t.TempDir(), "creation-mode-control")
	if err := os.Mkdir(control, mode); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(control)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode().Perm()
}
