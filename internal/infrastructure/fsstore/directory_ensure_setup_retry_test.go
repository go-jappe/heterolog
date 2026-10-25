package fsstore

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestDirectoryCreationRetriesSyncFailureAcrossPolicies(t *testing.T) {
	for _, strictFirst := range []bool{false, true} {
		name := "checkpoint then strict store"
		if strictFirst {
			name = "strict store then checkpoint"
		}
		t.Run(name, func(t *testing.T) {
			firstPolicy := directorySetupPolicy{rejectSymlinks: strictFirst, setDestinationPermissions: !strictFirst}
			secondPolicy := directorySetupPolicy{rejectSymlinks: !strictFirst, setDestinationPermissions: strictFirst}
			// リンクを拒否する方針でも使えるよう、macOSの/varなど上位のリンクを解決します。
			root := resolvedTempDirForTest(t)
			directory := filepath.Join(root, "state", "target")
			failure := errors.New("parent sync unavailable")
			var initial []string
			err := ensureDirectoryWithPolicy(context.Background(), directory, 0o700, firstPolicy, func(path string) error {
				initial = append(initial, path)
				if path == filepath.Join(root, "state") {
					return failure
				}
				return SyncDirectory(path)
			})
			if !errors.Is(err, failure) || !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
				t.Fatalf("initial creation error = %v, want unconfirmed durability and cause", err)
			}
			if want := []string{root, filepath.Join(root, "state")}; !reflect.DeepEqual(initial, want) {
				t.Fatalf("initial parent syncs = %v, want %v", initial, want)
			}
			// 親の同期失敗は記録し、リンク検証や権限設定の方針が異なる次回の初期化でも、
			// 既存の保存先を成功扱いにする前に再試行します。同期できる親は再同期しません。
			for attempt := 0; attempt < 2; attempt++ {
				var retried []string
				err := ensureDirectoryWithPolicy(context.Background(), directory, 0o700, secondPolicy, func(path string) error {
					retried = append(retried, path)
					return failure
				})
				var syncErr *DirectorySyncError
				if !errors.Is(err, failure) || !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) || !errors.As(err, &syncErr) {
					t.Fatalf("existing directory initialization %d error = %v, want recorded parent sync failure", attempt, err)
				}
				if syncErr.Directory != filepath.Join(root, "state") {
					t.Fatalf("existing directory initialization %d failed parent = %q", attempt, syncErr.Directory)
				}
				if want := []string{filepath.Join(root, "state")}; !reflect.DeepEqual(retried, want) {
					t.Fatalf("recorded parent syncs retried = %v, want %v", retried, want)
				}
			}
			if err := ensureDirectoryWithPolicy(context.Background(), directory, 0o700, secondPolicy, SyncDirectory); err != nil {
				t.Fatal(err)
			}
			if err := ensureDirectoryWithPolicy(context.Background(), directory, 0o700, firstPolicy, func(path string) error {
				t.Errorf("existing directory parent was synced after recovery: %q", path)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
