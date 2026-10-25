//go:build !windows

package testutil

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// DenyFileRemovalは、pathの通常ファイルを読めるまま、そのファイルの削除を権限エラー
// （errors.Is(err, os.ErrPermission)）で失敗させます。返却関数またはtest cleanupで元の
// 権限へ戻し、戻した後は削除できます。全OSで保証するのはこの対象ファイルについての
// 動作だけです。同じdirectoryの他のentryの作成・削除・renameへの影響はOSごとに異なるため、
// 呼び出し側は依存しないでください。
//
// POSIXでは親directoryの書込権限を外すため、同じdirectoryへの作成・削除・renameも
// 失敗します。fixtureは呼び出し前に作成してください。root権限などで書込権限の制限が
// 効かない環境では、確認用fileを削除できるため呼び出し元の試験をskipします。
// 確認用fileは返却前に削除します。
func DenyFileRemoval(t testing.TB, path string) func() {
	t.Helper()
	directory := filepath.Dir(path)
	info, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	original := info.Mode().Perm()
	// 権限を外した後は作成できないため、確認用fileを先に作ります。
	probe, err := os.CreateTemp(directory, ".heterolog-removal-probe-")
	if err != nil {
		t.Fatalf("create file removal probe: %v", err)
	}
	probePath := probe.Name()
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	restored := false
	restore := func() {
		t.Helper()
		if restored {
			return
		}
		if err := os.Chmod(directory, original); err != nil {
			t.Fatalf("restore file removal fixture permissions: %v", err)
		}
		restored = true
	}
	t.Cleanup(restore)
	deny := func() {
		t.Helper()
		if err := os.Chmod(directory, original&^0o222); err != nil {
			t.Fatalf("deny file removal: %v", err)
		}
	}
	deny()
	if err := os.Remove(probePath); err == nil {
		restore()
		t.Skip("current user or filesystem bypasses file removal permissions")
	} else if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("probe denied file removal: %v", err)
	}
	// 確認用fileが残るとdirectoryの走査結果が変わるため、権限を一時的に戻して削除します。
	if err := os.Chmod(directory, original); err != nil {
		t.Fatalf("restore permissions to remove file removal probe: %v", err)
	}
	if err := os.Remove(probePath); err != nil {
		t.Fatalf("remove file removal probe: %v", err)
	}
	deny()
	return restore
}
