package compressedlogarchive

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// base extensionの大小文字は、OS名ではなく保存先directoryの規則で比較します。
// 規則を指定して両方をどのOSでも確認し、保存名のsuffixは元の表記を保ちます。
func TestCompressedLogFileSuffixCaseFollowsDirectoryRuleAndPreservesSuffix(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	for _, test := range []struct {
		name        string
		wantSuffix  string
		foldingOnly bool
	}{
		{name: "app_20260911_1.log.gz", wantSuffix: ".log.gz"},
		{name: "app_20260911_1.log.GZ", wantSuffix: ".log.GZ"},
		{name: "app_20260911_1.LOG.GZ", wantSuffix: ".LOG.GZ", foldingOnly: true},
		{name: "app_20260911_1.LoG.Custom", wantSuffix: ".LoG.Custom", foldingOnly: true},
		{name: "app_20260911_1.log.log", wantSuffix: ".log.log"},
		{name: "app_20260911_1.jsonl.gz", wantSuffix: ".jsonl.gz"},
		{name: "app_20260911_1.jsonl.custom_format-v2", wantSuffix: ".jsonl.custom_format-v2"},
		{name: "app_20260911_1.JSONL.GZ", wantSuffix: ".JSONL.GZ", foldingOnly: true},
		{name: "app_20260911_1.jsonl"},
		{name: "app_20260911_1.jsonl.gz.tmp"},
		{name: ".jsonl.gz"},
		{name: "app_20260911_1.LOG"},
		{name: "app_20260911_1.LOG.GZ.TMP"},
		{name: "app_20260911_1.LOG.TMP"},
		{name: ".LOG.GZ"},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, nameCase := range []pathidentity.NameCase{pathidentity.NameCaseSensitive, pathidentity.NameCaseInsensitive} {
				wantSuffix := test.wantSuffix
				if test.foldingOnly && nameCase != pathidentity.NameCaseInsensitive {
					wantSuffix = ""
				}
				got, err := compressedLogFileSuffixForNameCase(test.name, nameCase)
				if wantSuffix == "" {
					if got != "" || !errors.Is(err, ErrUnsupportedCompressedLog) {
						t.Fatalf("compressedLogFileSuffixForNameCase(%q, %d) = %q, %v, want unsupported", test.name, nameCase, got, err)
					}
				} else if err != nil || got != wantSuffix {
					t.Fatalf("compressedLogFileSuffixForNameCase(%q, %d) = %q, %v, want %q", test.name, nameCase, got, err, wantSuffix)
				}
				if validErr := validateArchivedFileNameForNameCase(test.name, nameCase); (validErr == nil) != (wantSuffix != "") {
					t.Fatalf("validateArchivedFileNameForNameCase(%q, %d) = %v, want valid=%t", test.name, nameCase, validErr, wantSuffix != "")
				}
			}
			// directoryを渡す検証は、ComparisonKeyと共用する保存先directoryの判定を使います。
			wantSuffix, wantErr := compressedLogFileSuffixForNameCase(test.name, pathidentity.DirectoryNameCase(directory))
			got, err := compressedLogFileSuffix(directory, test.name)
			if got != wantSuffix || (err == nil) != (wantErr == nil) {
				t.Fatalf("compressedLogFileSuffix(%q) = %q, %v, want directory rule result %q, %v", test.name, got, err, wantSuffix, wantErr)
			}
		})
	}
}

// Storeは保存先archive rootの規則で保存する名前を検証します。
func TestStoreValidatesArchivedNameWithRootNameCase(t *testing.T) {
	store := newTestStore(t, pkglogger.LoggerContractSync)
	sourcePath := filepath.Join(t.TempDir(), "app_20260911_1.LOG.GZ")
	if err := os.WriteFile(sourcePath, []byte("compressed-log"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := store.Save(context.Background(), SaveInput{
		FailureCategory: uploader.FailureCategoryRetryable,
		SourcePath:      sourcePath,
	})
	if pathidentity.DirectoryNameCase(store.Root().Path()) == pathidentity.NameCaseInsensitive {
		if err != nil {
			t.Fatalf("Save() error = %v, want accepted in a case-insensitive root", err)
		}
		return
	}
	if !errors.Is(err, ErrUnsupportedCompressedLog) {
		t.Fatalf("Save() error = %v, want ErrUnsupportedCompressedLog in a case-sensitive root", err)
	}
}
