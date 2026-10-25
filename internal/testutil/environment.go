package testutil

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// RepositoryRootは、テストの作業ディレクトリからgo.modのある場所を探します。
func RepositoryRoot(t testing.TB) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatalf("could not find repo root from %q", directory)
		}
		directory = parent
	}
}

// PreserveEnvironmentは現在の環境変数を保存し、テスト終了時に復元します。
// os.Clearenvを使うテスト向けです。呼び出し側は並列実行しないでください。
func PreserveEnvironment(t testing.TB) {
	t.Helper()
	snapshot := os.Environ()
	t.Cleanup(func() {
		os.Clearenv()
		for _, entry := range snapshot {
			key, value, ok := splitEnvironmentEntry(entry)
			if ok {
				if err := os.Setenv(key, value); err != nil {
					t.Errorf("restore environment variable %q: %v", key, err)
				}
			}
		}
	})
}

// runtimeEnvironmentKeysは、ClearEnvironmentが消去後も元の値を残すOSの実行環境の変数です。
// WindowsのSystemRootはWinsockのprovider読込に、LOCALAPPDATA・APPDATA・USERPROFILEと
// macOS・Linux等のHOME、LinuxのXDG_STATE_HOMEは既定の保存先とos.UserHomeDirに、
// TEMP・TMP・TMPDIRはos.TempDirに使われます。どれも設定loaderが読む設定項目ではありません。
var runtimeEnvironmentKeys = []string{
	"SystemRoot", "LOCALAPPDATA", "APPDATA", "USERPROFILE", "HOME", "XDG_STATE_HOME", "TEMP", "TMP", "TMPDIR",
}

// ClearEnvironmentは、テスト終了時に環境全体を元へ戻すよう登録してからprocess environmentを
// 消去し、runtimeEnvironmentKeysの変数だけを元の値へ戻します。設定値の隔離にos.Clearenvを
// 使うと、Windowsでは消去後のsocket作成や既定の保存先の解決がOSの実行環境の不足で失敗します。
// 呼び出し側は並列実行しないでください。
func ClearEnvironment(t testing.TB) {
	t.Helper()
	PreserveEnvironment(t)
	kept := make(map[string]string, len(runtimeEnvironmentKeys))
	for _, key := range runtimeEnvironmentKeys {
		if value, ok := os.LookupEnv(key); ok {
			kept[key] = value
		}
	}
	os.Clearenv()
	for key, value := range kept {
		if err := os.Setenv(key, value); err != nil {
			t.Fatalf("keep runtime environment variable %q: %v", key, err)
		}
	}
}

// splitEnvironmentEntryは環境変数の名前と値を分けます。
// Windowsの「=C:」のような名前も扱うため、区切りの「=」は2文字目から探します。
func splitEnvironmentEntry(entry string) (key, value string, ok bool) {
	if len(entry) < 2 {
		return "", "", false
	}
	separator := strings.IndexByte(entry[1:], '=')
	if separator < 0 {
		return "", "", false
	}
	separator++
	return entry[:separator], entry[separator+1:], true
}
