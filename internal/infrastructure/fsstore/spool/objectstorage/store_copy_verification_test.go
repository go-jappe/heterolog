package objectstoragespool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestStoreRejectsChangedCopyBeforePublication(t *testing.T) {
	for _, state := range []pkgspool.State{pkgspool.StateActive, pkgspool.StateDead} {
		for _, extension := range []string{"gz", "custom"} {
			t.Run(string(state)+"/"+extension, func(t *testing.T) {
				store := newTestStore(t)
				if err := store.EnsureLayout(context.Background()); err != nil {
					t.Fatal(err)
				}
				// 形式を解釈できなくても比較できることを、独自形式と複数bufferの内容で確認します。
				body := strings.Repeat("\x00\xfforiginal", 32*1024) + "last"
				source := writeTestSource(t, t.TempDir(), "app_20260925_1.log."+extension, body)
				injected := false
				ctx := &publishObservedContext{Context: context.Background(), beforeErr: func() {
					if injected {
						return
					}
					entries, err := os.ReadDir(store.root.stagingDir())
					if err != nil {
						t.Fatal(err)
					}
					if len(entries) != 1 {
						return
					}
					path := filepath.Join(store.root.stagingDir(), entries[0].Name())
					info, err := os.Stat(path)
					if err != nil {
						t.Fatal(err)
					}
					if info.Size() != int64(len(body)) {
						return
					}
					// copy完了後、次のctx確認時に同じgoroutineで破損を注入します。
					// サイズとmtimeは変えず、既存のmetadata確認では検出できない状態にします。
					file, err := os.OpenFile(path, os.O_WRONLY, 0)
					if err != nil {
						t.Fatal(err)
					}
					_, writeErr := file.WriteAt([]byte("!"), info.Size()-1)
					closeErr := file.Close()
					if writeErr != nil || closeErr != nil {
						t.Fatalf("inject copy corruption: %v, %v", writeErr, closeErr)
					}
					if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
						t.Fatal(err)
					}
					injected = true
				}}
				save := store.SaveRetryableFailure
				if state == pkgspool.StateDead {
					save = store.SavePermanentFailure
				}
				result, err := save(ctx, source)
				if !injected || !errors.Is(err, infrafsstore.ErrCopyDiffersFromSource) || result.Path != "" {
					t.Fatalf("save changed copy = %+v, %v; injected=%t", result, err, injected)
				}
				for _, directory := range []string{store.root.ActiveDir(), store.root.DeadDir(), store.root.stagingDir()} {
					assertDirectoryEmpty(t, directory)
				}
				if got, err := os.ReadFile(source); err != nil || string(got) != body {
					t.Fatalf("source was not preserved: %v", err)
				}
				// 保存失敗後も同じ元ファイルから再試行でき、codecなしで完全一致のコピーを公開します。
				result, err = save(context.Background(), source)
				if err != nil || result.State != state {
					t.Fatalf("save retry = %+v, %v", result, err)
				}
				if got, err := os.ReadFile(result.Path); err != nil || string(got) != body {
					t.Fatalf("saved content differs: %v", err)
				}
			})
		}
	}
}
