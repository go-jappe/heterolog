package fsstore

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil"
)

func TestGetFilesInfoExcludesNonRegularPaths(t *testing.T) {
	for _, kind := range []string{"file link", "directory link", "dangling link", "loop link", "directory"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			regular := filepath.Join(root, "empty.log")
			if err := os.WriteFile(regular, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			nonRegular := filepath.Join(root, "entry.log")
			makeNonRegularPurgeEntry(t, nonRegular, kind)
			files := GetFilesInfo([]string{nonRegular, regular})
			if len(files) != 1 || files[0].Path != regular || files[0].Size != 0 {
				t.Fatalf("files = %+v, want only the empty regular file", files)
			}
		})
	}
}

func TestPurgeIgnoresLinkedFileMetadata(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	for _, policy := range []string{"age", "generation", "directory size"} {
		t.Run(policy, func(t *testing.T) {
			root := t.TempDir()
			regular := writePurgeTestFile(t, root, "app_20260920_1.log", now.Add(-24*time.Hour))
			external := filepath.Join(t.TempDir(), "external.log")
			contents := bytes.Repeat([]byte("valuable external data"), 1024)
			if err := os.WriteFile(external, contents, 0o600); err != nil {
				t.Fatal(err)
			}
			// 容量・世代purgeでは正常ログより新しいリンク先を使い、旧Stat実装なら
			// 正常ログが先に削除される状況にします。ageではリンク自体の削除も防ぎます。
			mtime := now
			if policy == "age" {
				mtime = now.Add(-10 * 24 * time.Hour)
			}
			if err := os.Chtimes(external, mtime, mtime); err != nil {
				t.Fatal(err)
			}
			link := filepath.Join(root, "app_20260921_1.log")
			testutil.Symlink(t, external, link)
			files := GetFilesInfo([]string{regular.Path, link})
			var result PurgeResult
			switch policy {
			case "age":
				result = PurgeByFileNameDateIfNeeded(files, 3, now, time.Time{})
			case "generation":
				result = PurgeByGenerationsFromFileNameIfNeeded(files, 1)
			case "directory size":
				result = PurgeByDirectorySizeFromFileNameIfNeeded(files, regular.Size+1, nil)
			}
			if len(result.RemovalResults) != 0 || len(result.Remaining) != 1 || result.Remaining[0].Path != regular.Path {
				t.Fatalf("purge result = %+v, want the regular file retained without deletion", result)
			}
			if _, err := os.Lstat(regular.Path); err != nil {
				t.Fatalf("regular log was removed: %v", err)
			}
			if _, err := os.Lstat(link); err != nil {
				t.Fatalf("link was removed: %v", err)
			}
			got, err := os.ReadFile(external)
			if err != nil || !bytes.Equal(got, contents) {
				t.Fatalf("external file changed: %v", err)
			}
		})
	}
}

func TestRemoveObservedFileRejectsNonRegularPaths(t *testing.T) {
	for _, kind := range []string{"file link", "directory link", "dangling link", "loop link", "directory"} {
		t.Run(kind, func(t *testing.T) {
			observed := writePurgeTestFile(t, t.TempDir(), "observed.log", time.Now())
			// 走査後・削除前に別種のentryになった場合も、元snapshotを削除成功扱いしません。
			if err := os.Remove(observed.Path); err != nil {
				t.Fatal(err)
			}
			makeNonRegularPurgeEntry(t, observed.Path, kind)
			result := removeObservedFile(observed)
			if result.Err == nil || !reflect.DeepEqual(result.File, observed) {
				t.Fatalf("removal = %+v, want non-regular rejection with original snapshot", result)
			}
			if _, err := os.Lstat(observed.Path); err != nil {
				t.Fatalf("non-regular path was removed: %v", err)
			}
		})
	}
}

func makeNonRegularPurgeEntry(t *testing.T, path, kind string) {
	t.Helper()
	switch kind {
	case "file link":
		target := filepath.Join(t.TempDir(), "external.log")
		if err := os.WriteFile(target, []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
		testutil.Symlink(t, target, path)
	case "directory link":
		testutil.Symlink(t, t.TempDir(), path)
	case "dangling link":
		testutil.Symlink(t, filepath.Join(t.TempDir(), "missing"), path)
	case "loop link":
		testutil.Symlink(t, filepath.Base(path), path)
	case "directory":
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown non-regular test kind %q", kind)
	}
}
