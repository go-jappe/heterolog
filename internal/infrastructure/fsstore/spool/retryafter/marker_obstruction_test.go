package retryafter

import (
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// markerのdirectory（.retry-after）がリンクやfileに置き換えられていても、読み込み・一時fileの掃除・
// 保存のどれもその先を辿りません。読み込みは壊れたmarkerとして、markerがない状態と警告を返し、
// 保存は失敗します。リンク先のmarker・保存途中の一時file・その他のentryと、置き換えたentry自身は
// 変更しません。
func TestStoreDoesNotFollowObstructedMarkerDirectory(t *testing.T) {
	t.Parallel()
	stores := map[string]func(owner, destinationID string) (*Store, error){
		"log spool":      NewLogSpoolStore,
		"object storage": NewObjectStorageStore,
	}
	for kind, newStore := range stores {
		for _, obstruction := range []string{"symlink", "file"} {
			t.Run(kind+"/"+obstruction, func(t *testing.T) {
				t.Parallel()
				owner := resolvedTempDirForTest(t)
				store, err := newStore(owner, "opensearch")
				if err != nil {
					t.Fatal(err)
				}
				directory := filepath.Dir(store.Path())
				// リンク先は、期限が残るmarkerと保存途中の一時fileを持つmarkerのdirectoryに見えます。
				outside := resolvedTempDirForTest(t)
				pending, err := json.Marshal(markerDocument{
					Version: markerVersion, DestinationID: "opensearch", NotBefore: testNow.Add(time.Hour),
				})
				if err != nil {
					t.Fatal(err)
				}
				for name, body := range map[string][]byte{
					"opensearch.json":         pending,
					".opensearch.json.42.tmp": []byte("stale temporary marker"),
					"notes.txt":               []byte("unrelated entry"),
				} {
					if err := os.WriteFile(filepath.Join(outside, name), body, 0o600); err != nil {
						t.Fatal(err)
					}
				}
				switch obstruction {
				case "symlink":
					testutil.Symlink(t, outside, directory)
				default:
					if err := os.WriteFile(directory, []byte("not a marker directory"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				outsideBefore := directoryEntriesForTest(t, outside)
				ownerBefore := directoryEntriesForTest(t, owner)

				state, clamp, err := store.Load(testNow, 2*time.Hour)
				requireMarkerError(t, err, MarkerOperationLoad, ErrInvalidMarker)
				requireObstructedMarkerDirectory(t, err, directory)
				if state != (State{}) || clamp != nil {
					t.Fatalf("Load() = %+v/%+v, want no marker for an obstructed marker directory", state, clamp)
				}

				err = store.Save(State{NotBefore: testNow.Add(time.Minute)})
				requireMarkerError(t, err, MarkerOperationSave, nil)
				requireObstructedMarkerDirectory(t, err, directory)

				if got := directoryEntriesForTest(t, outside); !maps.Equal(got, outsideBefore) {
					t.Fatalf("link target entries = %v, want unchanged %v", got, outsideBefore)
				}
				if got := directoryEntriesForTest(t, owner); !maps.Equal(got, ownerBefore) {
					t.Fatalf("owner directory entries = %v, want unchanged %v", got, ownerBefore)
				}
			})
		}
	}
}

// requireObstructedMarkerDirectoryは、errがmarkerのdirectoryの位置にあるリンク・fileを拒否の原因として
// 示すことを確認します。保存先の種類により、管理directoryの検証かdirectoryの準備のどちらかが拒否します。
// Windowsの短縮名や大小文字の違いに依存しないよう、同じentryを指すかをos.SameFileで比べます。
func requireObstructedMarkerDirectory(t *testing.T, err error, directory string) {
	t.Helper()
	var reported string
	var managed *pathidentity.ManagedDirectoryObstructionError
	var obstruction *infrafsstore.DirectoryObstructionError
	switch {
	case errors.As(err, &managed):
		reported = managed.Path
	case errors.As(err, &obstruction):
		reported = obstruction.Path
	default:
		t.Fatalf("error = %v, want the obstructed marker directory %q as the cause", err, directory)
	}
	want, wantErr := os.Lstat(directory)
	got, gotErr := os.Lstat(reported)
	if wantErr != nil || gotErr != nil || !os.SameFile(want, got) {
		t.Fatalf("obstruction path = %q (%v), want the marker directory %q (%v)", reported, gotErr, directory, wantErr)
	}
}

// directoryEntriesForTestは、directory直下の各entryを、通常fileは内容、リンクはリンク先、
// directoryは種類で表した一覧を返します。
func directoryEntriesForTest(t *testing.T, directory string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	contents := make(map[string]string, len(entries))
	for _, entry := range entries {
		path := filepath.Join(directory, entry.Name())
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				t.Fatal(err)
			}
			contents[entry.Name()] = "symlink to " + target
		case info.Mode().IsRegular():
			body, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			contents[entry.Name()] = "file: " + string(body)
		default:
			contents[entry.Name()] = "entry of type " + info.Mode().Type().String()
		}
	}
	return contents
}
