package testutil

import (
	"os"
	"runtime"
	"slices"
	"strings"
	"testing"
)

func TestSplitEnvironmentEntry(t *testing.T) {
	for _, test := range []struct {
		name  string
		entry string
		key   string
		value string
		valid bool
	}{
		{name: "ordinary", entry: "SETTING=value", key: "SETTING", value: "value", valid: true},
		{name: "empty value", entry: "SETTING=", key: "SETTING", valid: true},
		{name: "value contains equals", entry: "SETTING=a=b=c", key: "SETTING", value: "a=b=c", valid: true},
		{name: "single character name", entry: "X=value", key: "X", value: "value", valid: true},
		{name: "windows drive", entry: `=C:=C:\work`, key: "=C:", value: `C:\work`, valid: true},
		{name: "windows drive empty value", entry: "=D:=", key: "=D:", valid: true},
		{name: "windows drive value contains equals", entry: `=C:=C:\a=b`, key: "=C:", value: `C:\a=b`, valid: true},
		{name: "empty entry"},
		{name: "missing separator", entry: "SETTING"},
		{name: "empty name", entry: "=value"},
	} {
		t.Run(test.name, func(t *testing.T) {
			key, value, valid := splitEnvironmentEntry(test.entry)
			if key != test.key || value != test.value || valid != test.valid {
				t.Fatalf("splitEnvironmentEntry(%q) = (%q, %q, %t), want (%q, %q, %t)",
					test.entry, key, value, valid, test.key, test.value, test.valid)
			}
		})
	}
}

func TestPreserveEnvironmentRestoresSnapshot(t *testing.T) {
	const existing = "TESTUTIL_ENVIRONMENT_EXISTING"
	const empty = "TESTUTIL_ENVIRONMENT_EMPTY"
	const added = "TESTUTIL_ENVIRONMENT_ADDED"
	t.Setenv(existing, "original=value")
	t.Setenv(empty, "")
	t.Setenv(added, "temporary")
	if err := os.Unsetenv(added); err != nil {
		t.Fatal(err)
	}
	want := os.Environ()

	t.Run("change environment", func(t *testing.T) {
		PreserveEnvironment(t)
		os.Clearenv()
		if err := os.Setenv(existing, "changed"); err != nil {
			t.Fatal(err)
		}
		if err := os.Setenv(added, "new"); err != nil {
			t.Fatal(err)
		}
	})

	got := os.Environ()
	slices.Sort(want)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		// 環境変数には認証情報が含まれ得るため、差分の内容は出力しません。
		t.Fatal("environment was not restored to the original snapshot")
	}
}

// 設定値を消去しても、socket作成や既定の保存先の解決に使うOSの実行環境の変数は残します。
func TestClearEnvironmentKeepsOnlyRuntimeVariables(t *testing.T) {
	const setting = "TESTUTIL_ENVIRONMENT_SETTING"
	t.Setenv(setting, "configured")
	type value struct {
		text string
		set  bool
	}
	// OSによって未設定の変数もあるため、元の有無と値をそのまま保つことを確認します。
	kept := make(map[string]value, len(runtimeEnvironmentKeys))
	for _, key := range runtimeEnvironmentKeys {
		text, set := os.LookupEnv(key)
		kept[key] = value{text: text, set: set}
	}
	want := os.Environ()

	t.Run("clear environment", func(t *testing.T) {
		ClearEnvironment(t)
		if _, found := os.LookupEnv(setting); found {
			t.Fatal("setting variable was not cleared")
		}
		for key, original := range kept {
			if text, set := os.LookupEnv(key); set != original.set || text != original.text {
				// 環境変数には利用者の情報が含まれ得るため、値は出力しません。
				t.Fatalf("runtime variable %s was not kept", key)
			}
		}
		for _, entry := range os.Environ() {
			key, _, ok := splitEnvironmentEntry(entry)
			if ok && !slices.ContainsFunc(runtimeEnvironmentKeys, func(runtimeKey string) bool {
				// Windowsの環境変数名は大文字小文字を区別しません。
				return runtimeKey == key || runtime.GOOS == "windows" && strings.EqualFold(runtimeKey, key)
			}) {
				t.Fatalf("variable %s remained after clearing", key)
			}
		}
	})

	got := os.Environ()
	slices.Sort(want)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatal("environment was not restored to the original snapshot")
	}
}
