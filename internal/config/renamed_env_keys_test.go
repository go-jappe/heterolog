package config

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/config/renamedkey"
)

// TestRenamedEnvironmentKeyCoversEveryPublicRenamedKeyは、公開例に掲載した綴り修正後の
// 全keyについて、旧名を新しいkey名へ対応付けられることを検証します。現在のkey名は
// 旧名として扱いません。旧名は検索で正しい綴りと混同しないよう、renamedkey.Oldで組み立てます。
func TestRenamedEnvironmentKeyCoversEveryPublicRenamedKey(t *testing.T) {
	counts := map[string]int{}
	for key := range readPublicEnvExample(t) {
		if renamed, found := renamedkey.Current(key); found {
			t.Errorf("current key %s is treated as renamed to %s", key, renamed)
		}

		var kind string
		switch {
		case key == "HETEROLOG_MEMORY_CRITICAL_RATIO":
			kind = "ratio"
		case strings.HasSuffix(key, "_SPOOL_SCANNED_CACHE_USAGE_COUNT"):
			kind = "scanned"
		case strings.HasSuffix(key, "_HANDLER"):
			kind = "handler"
		default:
			continue
		}
		oldKey, found := renamedkey.Old(key)
		if !found || oldKey == key {
			t.Errorf("renamedkey.Old(%s) = %q, %t; want a misspelled name", key, oldKey, found)
			continue
		}
		counts[kind]++
		if renamed, found := renamedkey.Current(oldKey); !found || renamed != key {
			t.Errorf("renamedkey.Current(%s) = %q, %t, want %q", oldKey, renamed, found, key)
		}
	}
	for _, kind := range []string{"handler", "ratio", "scanned"} {
		if counts[kind] == 0 {
			t.Errorf("public example has no renamed %s key", kind)
		}
	}
}

// TestLoadConfigRejectsRenamedEnvironmentKeysは、旧名のkeyを新しいkeyの別名として読まず、
// 環境設定fileと環境変数のどちらに残っていても、新しいkey名を示す設定エラーにすることを
// 検証します。値が空でも設定が失われる可能性を示すため拒否します。
func TestLoadConfigRejectsRenamedEnvironmentKeys(t *testing.T) {
	// 旧名は、綴りを修正する前のkey名にHETEROLOG_を付けた名前を、規則ごとに一つ試験側で固定します。
	// renamedkeyの対応表から組み立てると、表の旧綴りを誤っても検出できないためです。
	// 検索で正しい綴りと混同しないよう、旧名は文字列を分けて連結します。
	tests := []struct {
		name, oldKey, newKey, value string
	}{
		{
			name:   "handler routing",
			oldKey: "HETEROLOG_STDOUT_LOGGER_ERROR_HAND" + "ER", newKey: "HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER", value: "file",
		},
		{
			name:   "empty handler routing",
			oldKey: "HETEROLOG_DELIVERY_FILE_LOGGER_DEBUG_HAND" + "ER", newKey: "HETEROLOG_DELIVERY_FILE_LOGGER_DEBUG_HANDLER",
		},
		{
			name:   "memory critical ratio",
			oldKey: "HETEROLOG_MEMORY_CRITICAL_RAI" + "TO", newKey: "HETEROLOG_MEMORY_CRITICAL_RATIO", value: "0.8",
		},
		{
			name:   "spool scanned cache usage count",
			oldKey: "HETEROLOG_MAX_ASYNC_LOKI_SPOOL_SCAN" + "ED_CACHE_USAGE_COUNT", newKey: "HETEROLOG_MAX_ASYNC_LOKI_SPOOL_SCANNED_CACHE_USAGE_COUNT",
			value: "10",
		},
	}
	for _, tt := range tests {
		oldKey := tt.oldKey
		if got, ok := renamedkey.Old(tt.newKey); !ok || got != oldKey {
			t.Errorf("renamedkey.Old(%s) = %q, %t; want the pre-rename spelling %q", tt.newKey, got, ok, oldKey)
		}
		assertRenamedKeyError := func(t *testing.T, err error) {
			t.Helper()
			var renamedErr *RenamedEnvironmentKeyError
			if !errors.As(err, &renamedErr) || renamedErr.Old != oldKey || renamedErr.New != tt.newKey {
				t.Fatalf("LoadConfig() error = %v, want %s renamed to %s", err, oldKey, tt.newKey)
			}
		}
		t.Run(tt.name+"/env file", func(t *testing.T) {
			_, err := loadConfigErrorFromTempEnv(t, storagePathValidationEnv(t, oldKey+"="+tt.value))
			assertRenamedKeyError(t, err)
		})
		t.Run(tt.name+"/environment", func(t *testing.T) {
			workDirectory := t.TempDir()
			restoreProcessEnvAfterTest(t)
			clearProcessEnvForTest(t)
			t.Chdir(workDirectory)
			if err := os.Setenv(oldKey, tt.value); err != nil {
				t.Fatalf("set %s: %v", oldKey, err)
			}
			_, err := LoadConfig()
			assertRenamedKeyError(t, err)
		})
	}
}

// TestLoadConfigLeavesApplicationKeysMatchingRenamedKeyRulesは、HETEROLOG_で始まらない名前を、
// 旧名の規則に一致しても設定エラーにしないことを検証します。利用元のアプリは同じ環境変数や
// 環境設定fileに自分のkeyを置くため、heterologの旧名と取り違えないためです。旧名は
// process environmentと環境設定fileの両方のkey名で同じ規則により確認するため、環境変数に
// 置いた場合も同じです。
func TestLoadConfigLeavesApplicationKeysMatchingRenamedKeyRules(t *testing.T) {
	_, err := loadConfigErrorFromTempEnv(t, storagePathValidationEnv(t,
		"STDOUT_LOGGER_ERROR_HAND"+"ER=file",
		"MEMORY_CRITICAL_RAI"+"TO=0.8",
		"MAX_ASYNC_LOKI_SPOOL_SCAN"+"ED_CACHE_USAGE_COUNT=10",
	))
	if err != nil {
		t.Fatalf("LoadConfig() with application keys matching the renamed-key rules = %v, want nil", err)
	}
}

func TestLoadConfigReadsMemoryCriticalRatio(t *testing.T) {
	cfg, err := loadConfigErrorFromTempEnv(t, storagePathValidationEnv(t, "HETEROLOG_MEMORY_CRITICAL_RATIO=0.75"))
	if err != nil {
		t.Fatalf("LoadConfig() with HETEROLOG_MEMORY_CRITICAL_RATIO: %v", err)
	}
	if got := cfg.RuntimeConfig.Health.Memory.CriticalRatio; got != 0.75 {
		t.Fatalf("CriticalRatio = %v, want 0.75", got)
	}
}
