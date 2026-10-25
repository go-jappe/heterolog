package testutil

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestUnsetConfigEnvironmentKeepsRuntimeEnvironment(t *testing.T) {
	configValues := map[string]string{
		"HETEROLOG_LOG_TYPE":       "slack",                 // 公開設定例の項目
		"HETEROLOG_SYSTEM_LOG_DIR": "/production/logs",      // 既定値をコメントで示す項目
		"AWS_ACCESS_KEY_ID":        "production-access-key", // loaderが既定値として読む項目
		"HETEROLOG_ENV_PATH":       "/production/application.env",
	}
	runtimeValues := map[string]string{
		"TESTUTIL_RUNTIME_SETTING": "kept",
		"HTTPS_PROXY":              "http://proxy.invalid:8080",
		"SSL_CERT_FILE":            filepath.Join(t.TempDir(), "ca.pem"),
	}
	for key, value := range configValues {
		t.Setenv(key, value)
	}
	for key, value := range runtimeValues {
		t.Setenv(key, value)
	}
	before := os.Environ()
	configKeys := make(map[string]bool)
	for _, key := range configEnvironmentKeys(t) {
		configKeys[key] = true
	}

	t.Run("unset config keys", func(t *testing.T) {
		UnsetConfigEnvironment(t)
		for key := range configValues {
			if _, found := os.LookupEnv(key); found {
				t.Errorf("config variable %s was not unset", key)
			}
		}
		for key, want := range runtimeValues {
			if got := os.Getenv(key); got != want {
				t.Errorf("runtime variable %s = %q, want %q", key, got, want)
			}
		}
		// PATHやHOMEなど、設定項目以外の変数は値を変えずに残します。開発者のshellに
		// 残る旧名の設定keyも設定項目として外すため、比較の対象から除きます。
		for _, entry := range before {
			key, value, ok := splitEnvironmentEntry(entry)
			if !ok || configKeys[key] || isRenamedConfigKey(key) {
				continue
			}
			if got, found := os.LookupEnv(key); !found || got != value {
				// 環境変数には認証情報が含まれ得るため、値は出力しません。
				t.Errorf("runtime variable %s was changed", key)
			}
		}
	})

	after := os.Environ()
	slices.Sort(before)
	slices.Sort(after)
	if !slices.Equal(before, after) {
		t.Fatal("environment was not restored to the original snapshot")
	}
}

// configKeyLiteralは、環境変数keyの形をした文字列literalです。
var configKeyLiteral = regexp.MustCompile(`^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+$`)

// 設定loaderが名前を固定して読む項目は、すべて隔離の対象に含めます。
// 公開設定例への記載漏れがあると、実行中のprocessの値が試験の設定へ混入するためです。
// env.GetEnv*等へ直接渡すkeyだけでなく、読込helperへ引数や一覧で渡すkeyも対象にするため、
// loaderのsource内でkeyの形をした全ての文字列literalを調べます。
func TestConfigEnvironmentKeysCoverConfigLoader(t *testing.T) {
	keys := make(map[string]bool)
	for _, key := range configEnvironmentKeys(t) {
		keys[key] = true
	}
	sources, err := filepath.Glob(filepath.Join(RepositoryRoot(t), "internal", "config", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	literals := make(map[string]bool)
	files := token.NewFileSet()
	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(files, source, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err == nil && configKeyLiteral.MatchString(value) {
				literals[value] = true
			}
			return true
		})
	}
	// env.GetEnv*等へ直接渡すkeyも含まれていることを確認し、走査方法の誤りで
	// 対象が空になったまま合格しないようにします。
	for _, directlyRead := range []string{"HETEROLOG_LOG_TYPE", "HETEROLOG_SYSTEM_LOG_DIR", "HETEROLOG_MIN_AVAILABLE_DISK_SPACE_GB"} {
		if !literals[directlyRead] {
			t.Fatalf("config loader literal %s was not found; update the source scan", directlyRead)
		}
	}
	// 読込helperへ引数で渡すkeyの例です。
	for _, helperRead := range []string{"HETEROLOG_OPENSEARCH_DEGRADE_TYPES", "HETEROLOG_ENABLE_OPENSEARCH_DEGRADE"} {
		if !literals[helperRead] {
			t.Fatalf("config loader literal %s passed to a loader helper was not found; update the source scan", helperRead)
		}
	}
	var missing []string
	for literal := range literals {
		if !keys[literal] {
			missing = append(missing, literal)
		}
	}
	sort.Strings(missing)
	if len(missing) != 0 {
		t.Fatalf("config keys read by the loader are not isolated (document them in .env.example or configLoaderOnlyKeys): %v", missing)
	}
}

// 設定loaderが改名を案内して拒否する旧名のkeyも外すことを確認する。旧名は、綴りを修正する
// 前に公開していたkey名そのものを規則ごとに試験側で固定する。renamedkeyの対応表から
// 組み立てると、表の旧綴りを誤っても開発者のshellに残る旧名を外せないことを検出できないため。
// 検索で正しい綴りと混同しないよう、旧名は文字列を分けて連結する。
func TestUnsetConfigEnvironmentRemovesRenamedKeys(t *testing.T) {
	renamed := []string{
		"HETEROLOG_STDOUT_LOGGER_ERROR_HAND" + "ER",
		"HETEROLOG_ASYNC_LOG_INTERNAL_LOG_DEBUG_HAND" + "ER",
		"HETEROLOG_MEMORY_CRITICAL_RAI" + "TO",
		"HETEROLOG_MAX_ASYNC_OPENSEARCH_SPOOL_SCAN" + "ED_CACHE_USAGE_COUNT",
	}
	// 改名規則に一致しない名前は、設定項目以外の実行環境として残す。
	kept := []string{
		"_WARN_HAND" + "ER",                       // 接頭辞のない旧綴り
		"MAX_SPOOL_SCAN" + "ED_CACHE_USAGE_COUNT", // backend名のない旧綴り
		"TESTUTIL_ERROR_HANDLING",
		"STDOUT_LOGGER_ERROR_HAND" + "ER", // HETEROLOG_で始まらない、利用元のアプリの変数
	}
	for _, key := range append(slices.Clone(renamed), kept...) {
		t.Setenv(key, "configured")
	}

	t.Run("unset renamed keys", func(t *testing.T) {
		UnsetConfigEnvironment(t)
		for _, key := range renamed {
			if _, found := os.LookupEnv(key); found {
				t.Errorf("renamed config variable %s was not unset", key)
			}
		}
		for _, key := range kept {
			if _, found := os.LookupEnv(key); !found {
				t.Errorf("variable %s outside the renamed keys was removed", key)
			}
		}
	})
	for _, key := range renamed {
		if os.Getenv(key) != "configured" {
			t.Errorf("renamed config variable %s was not restored", key)
		}
	}
}
