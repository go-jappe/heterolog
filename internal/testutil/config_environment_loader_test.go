package testutil_test

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-jappe/heterolog/internal/config"
	"github.com/go-jappe/heterolog/internal/testutil"
)

// 開発者の環境に旧名のkeyが残っていても、設定項目を外した後の設定読込は
// 改名の設定エラーにならないことを、実際のloaderで確認する。
func TestUnsetConfigEnvironmentLetsLoaderIgnoreShellRenamedKeys(t *testing.T) {
	// 旧名は、綴りを修正する前のkey名を、HETEROLOG_の接頭辞付きで規則ごとに固定する。検索で
	// 正しい綴りと混同しないよう、文字列を分けて連結する。
	renamed := []string{
		"HETEROLOG_STDOUT_LOGGER_ERROR_HAND" + "ER",
		"HETEROLOG_MEMORY_CRITICAL_RAI" + "TO",
		"HETEROLOG_MAX_OPENSEARCH_SPOOL_SCAN" + "ED_CACHE_USAGE_COUNT",
	}
	envPath := filepath.Join(t.TempDir(), "empty.env")
	if err := os.WriteFile(envPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	// 実行中のprocessの設定項目に左右されないよう、旧名だけを残した環境で比較する。
	testutil.UnsetConfigEnvironment(t)
	for _, key := range renamed {
		t.Setenv(key, "configured")
	}
	t.Setenv("HETEROLOG_ENV_PATH", envPath)
	// 旧名が残る間は、loaderが各旧名を新しいkey名とともに拒否する。
	_, err := config.LoadConfig()
	reported := renamedEnvironmentKeys(err)
	slices.Sort(reported)
	want := slices.Clone(renamed)
	slices.Sort(want)
	if !slices.Equal(reported, want) {
		t.Fatalf("LoadConfig() renamed key errors = %v (error %v), want %v", reported, err, want)
	}

	t.Run("after unsetting config keys", func(t *testing.T) {
		testutil.UnsetConfigEnvironment(t)
		t.Setenv("HETEROLOG_ENV_PATH", envPath)
		if _, err := config.LoadConfig(); err != nil {
			t.Fatalf("LoadConfig() after UnsetConfigEnvironment = %v", err)
		}
	})
}

// renamedEnvironmentKeysは、errに含まれる全てのRenamedEnvironmentKeyErrorの旧名を返します。
// errors.Asは最初の一件だけを返すため、errors.Joinでまとめたerrorを辿って全件を集めます。
func renamedEnvironmentKeys(err error) []string {
	var keys []string
	var walk func(error)
	walk = func(err error) {
		switch node := err.(type) {
		case nil:
		case *config.RenamedEnvironmentKeyError:
			keys = append(keys, node.Old)
		case interface{ Unwrap() []error }:
			for _, child := range node.Unwrap() {
				walk(child)
			}
		case interface{ Unwrap() error }:
			walk(node.Unwrap())
		}
	}
	walk(err)
	return keys
}
