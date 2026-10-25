package compressedlogfailure

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/retryafter"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgs3 "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
)

// destinationIDTestStoreTypesは、宛先IDを名前に使う退避先の種類です。
var destinationIDTestStoreTypes = map[string][]failurestore.DurableType{
	"spool":        {failurestore.DurableTypeSpool},
	"file archive": {failurestore.DurableTypeFileArchive},
}

// newDestinationIDTestConfigは、baseの下に退避先を置く設定を返します。
func newDestinationIDTestConfig(t *testing.T, base string, types []failurestore.DurableType) ManagedUploaderConfig {
	t.Helper()
	return ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), types)),
		SpoolRuntimes:                       newTestSpoolRuntimes(t, filepath.Join(base, "spool")),
		FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
	}
}

// 宛先IDは宛先別spool・file archiveのdirectoryとRetry-Afterのmarker（.retry-after/<宛先ID>.json）の
// 名前になるため、markerと同じ規則に合わない宛先IDは、どの退避先も作る前に構築時の設定エラーにします。
// 規則に合わない宛先が後にあっても、先の宛先の退避先を作りません。
func TestNewManagedUploaderRejectsDestinationIDsUnusableForFailureStores(t *testing.T) {
	for _, id := range []objectstorage.DestinationID{
		"_backup", "-x", "con", "nul", "com1", "lpt9",
		objectstorage.DestinationID(strings.Repeat("a", 129)),
		"Destination-A", "a.b", "a b", "日本",
	} {
		for storeName, types := range destinationIDTestStoreTypes {
			t.Run(storeName+"/"+string(id)[:min(len(id), 16)], func(t *testing.T) {
				base := t.TempDir()
				remote := &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a", id}}
				cfg := newDestinationIDTestConfig(t, base, types)
				created, err := NewManagedUploader(cfg, remote)
				if created != nil {
					if managed, ok := created.(*ManagedUploader); ok {
						_ = managed.Close()
					}
				}
				var invalid *InvalidDestinationIDError
				if created != nil || !errors.As(err, &invalid) || invalid.Destination != id {
					t.Fatalf("NewManagedUploader() = %T, %v; want InvalidDestinationIDError for the destination at index 1", created, err)
				}
				if !errors.Is(err, ErrInvalidConfig) || !errors.Is(err, retryafter.ErrInvalidDestinationID) {
					t.Fatalf("NewManagedUploader() error = %v, want a configuration error with the destination id rule as the cause", err)
				}
				if _, statErr := os.Lstat(filepath.Join(base, "archive")); !errors.Is(statErr, os.ErrNotExist) {
					t.Fatalf("rejected configuration created the failure archive: %v", statErr)
				}
				requireNoDestinationSpoolDirectory(t, cfg.SpoolRuntimes)
				if remote.callCount() != 0 {
					t.Fatalf("rejected configuration uploaded %d times", remote.callCount())
				}
			})
		}
	}
}

// requireNoDestinationSpoolDirectoryは、spoolのservice root（宛先別spoolの親directory）に、
// 先の宛先・規則に合わない宛先のどちらの宛先別spoolのdirectoryも作られていないことを確認します。
// service rootは最初の宛先を登録するときに作るため、存在しなければ宛先別spoolもありません。
func requireNoDestinationSpoolDirectory(t *testing.T, runtimes compressedlog.RootRuntimes) {
	t.Helper()
	for service, runtime := range runtimes {
		root := runtime.Root().Path()
		entries, err := os.ReadDir(root)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatalf("read the %s spool service root %q: %v", service, root, err)
		}
		for _, entry := range entries {
			if entry.IsDir() {
				t.Fatalf("rejected configuration created the destination spool %q under %q", entry.Name(), root)
			}
		}
	}
}

// S3の宛先ID（s3-<SHA-256の16進数>）と、規則の範囲内の宛先IDは受け付けます。
func TestNewManagedUploaderAcceptsDestinationIDsUsableForFailureStores(t *testing.T) {
	s3ID, err := pkgs3.ResolveDestinationID(objectstorage.S3Config{Region: "ap-northeast-1", Bucket: "logs"})
	if err != nil {
		t.Fatal(err)
	}
	ids := []objectstorage.DestinationID{s3ID, "old_bucket", "0", objectstorage.DestinationID(strings.Repeat("a", 128))}
	for storeName, types := range destinationIDTestStoreTypes {
		t.Run(storeName, func(t *testing.T) {
			managed := newTestManagedUploader(t, newDestinationIDTestConfig(t, t.TempDir(), types), &testDestinationUploader{ids: ids})
			if got := managed.DestinationIDs(); len(got) != len(ids) {
				t.Fatalf("DestinationIDs() = %v, want %v", got, ids)
			}
		})
	}
}
