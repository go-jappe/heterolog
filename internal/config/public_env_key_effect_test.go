package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/joho/godotenv"

	"github.com/go-jappe/heterolog/internal/testutil"
)

// publicEnvExampleKeyは、公開例に記載したkeyと、その記載値（既定値をコメントで示すkeyは
// コメント内の値）です。
type publicEnvExampleKey struct {
	key     string
	example string
}

// commentedPublicEnvAssignmentは、既定値をコメントで示す公開例のkeyです。値を引用符で
// 囲んだ代入だけを対象にし、説明文中の「KEY=値」は含めません。
var commentedPublicEnvAssignment = regexp.MustCompile(`^#\s*([A-Z][A-Z0-9_]*)="([^"]*)"`)

// publicEnvExampleKeysは、公開例の全keyをkey名の順で返します。
func publicEnvExampleKeys(t *testing.T) []publicEnvExampleKey {
	t.Helper()
	values := readPublicEnvExample(t)
	contents, err := os.ReadFile(publicEnvExamplePath(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(contents), "\n") {
		if match := commentedPublicEnvAssignment.FindStringSubmatch(strings.TrimSpace(line)); match != nil {
			if _, exists := values[match[1]]; !exists {
				values[match[1]] = match[2]
			}
		}
	}
	keys := make([]publicEnvExampleKey, 0, len(values))
	for key, value := range values {
		keys = append(keys, publicEnvExampleKey{key: key, example: value})
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].key < keys[j].key })
	return keys
}

// TestEveryPublicEnvExampleKeyChangesLoadedConfigは、公開例に記載した全keyと、公開例に
// 記載せずloaderが既定値として読むkeyについて、有効な非既定値を与えるとLoadConfigの結果と、
// その結果をmapperで変換した実行時の設定（To{Logger,AsyncBestEffortMultiLogger,DeliveryLogger}Config、
// NewSpoolConfig、NewOtelResourceConfig）の両方が変わることを検証します。keyの読込漏れや、
// loaderのkey名の誤り（改名・綴り違い）があると、値を与えても結果が既定値のままになり、
// 読み込んだ値をmapperが落とすと、実行時の設定が既定値のままになります。名前にASYNC_・
// DELIVERY_を含むkeyは、そのLoggerContractKindの実行時の設定が変わることも求めます。
// 読込結果のうちLoggerContractKindごとの設定が変わった場合は、変わった全てのLoggerContractKindの
// 実行時の設定が変わることも求めます。sync・asyncのstdout設定（HETEROLOG_STDOUT_LOG_LEVELなど）や
// 3契約のstdoutが読むHETEROLOG_ENABLE_STDOUT_OTEL_FORMATのように複数のLoggerContractKindが共有するkeyは、
// 一つの使用箇所へ届くだけで実行時の設定が変わるため、一部のLoggerContractKindでmapperが
// 落とす誤りをこれで検出します。
//
// 各keyは、そのkeyだけを未設定にした環境と、keyの型・値の一覧に応じた候補値を与えた
// 環境で読み込んだ結果を比べます。試験側では既定値を特定しないため、既定値になり得る値も
// 含めて候補を並べ、いずれかの候補で結果が変わることを求めます。候補はLoadConfigが
// 受け付ける値に限ります。他のkeyは、全出力先・全退避先・object storage upload・internal logの
// 全委譲先を有効にした共通の環境に固定し、条件付きで読む設定も読み込ませます。
//
// 実装済みの値が既定値だけのkeyは、未対応の値も読込結果と実行時の設定へ届くこと（後段の
// 検証で拒否できること）を確認します。LoadConfigが読まない試験専用のkeyと、環境設定fileを
// 選ぶHETEROLOG_ENV_PATHは理由とともに除外します。mapperへ届かないkeyはpublicEnvKeyMapperExemptionsに
// 理由とともに記載し、実行時の設定が変わらないことを確認します。LoggerContractKindの区分に
// 属さない共有のkey（HETEROLOG_APP_NAME・HETEROLOG_APP_STATE_DIRなど）と、loaderが与える固定値が全ての使用箇所へ
// 届くことは、TestMapperPropagatesSharedSettingsToEveryConsumerが、そこに列挙したkeyと固定値に
// ついて確認します。
func TestEveryPublicEnvExampleKeyChangesLoadedConfig(t *testing.T) {
	base := publicEnvKeyEffectBaseEnvironment(t)
	envPath := filepath.Join(t.TempDir(), "empty.env")
	if err := os.WriteFile(envPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	// 一時directoryを作成してから設定項目だけを外します。TMP・TEMPなどの実行環境は残ります。
	testutil.UnsetConfigEnvironment(t)
	t.Setenv("HETEROLOG_ENV_PATH", envPath)
	for key, value := range base {
		if err := os.Setenv(key, value); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}

	// 同じ環境を二度読み込んだ結果が等しいことを先に確認し、関数値などを含むため比較が
	// 常に不一致になる状態で、全keyを合格させないようにします。
	first, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() with the activated public environment: %v", err)
	}
	second, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() with the activated public environment again: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("LoadConfig() results for the same environment differ; the key effect comparison would be vacuous")
	}

	keys := publicEnvExampleKeys(t)
	documented := make(map[string]bool, len(keys))
	for _, entry := range keys {
		documented[entry.key] = true
	}
	// 公開例に記載せず、loaderが既定値として読むkeyも同じ規則で確認します。
	for _, key := range testutil.ConfigLoaderOnlyKeys() {
		if !documented[key] {
			keys = append(keys, publicEnvExampleKey{key: key})
		}
	}
	for _, entry := range keys {
		t.Run(entry.key, func(t *testing.T) {
			spec := publicEnvKeyEffectSpecFor(entry)
			if spec.exemptReason != "" {
				t.Skipf("excluded from the key effect check: %s", spec.exemptReason)
			}
			restore := applyPublicEnvKeyEffectEnv(t, spec.activation)
			defer restore()
			restoreKey := applyPublicEnvKeyEffectEnv(t, map[string]*string{entry.key: nil})
			defer restoreKey()
			unset, err := LoadConfig()
			if err != nil {
				t.Fatalf("LoadConfig() without %s: %v", entry.key, err)
			}

			if spec.unsupported != "" {
				// 実装済みの値は既定値だけなので、未対応の値が読込結果に届くことで
				// keyを読んでいることを確認します。値の拒否は後段の検証が行います。
				if err := os.Setenv(entry.key, spec.unsupported); err != nil {
					t.Fatal(err)
				}
				cfg, err := LoadConfig()
				if err != nil {
					t.Fatalf("LoadConfig() with unsupported %s=%q: %v", entry.key, spec.unsupported, err)
				}
				if reflect.DeepEqual(unset, cfg) {
					t.Fatalf("LoadConfig() ignores %s=%q (%s); the key is not read", entry.key, spec.unsupported, spec.singleValueReason)
				}
				if problem := publicEnvKeyMapperProblem(entry.key, unset, cfg); problem != "" {
					t.Fatalf("LoadConfig() reads %s=%q, but %s", entry.key, spec.unsupported, problem)
				}
				return
			}

			if len(spec.candidates) == 0 {
				t.Fatalf("no candidate value for %s; add one or document why the key cannot change LoadConfig", entry.key)
			}
			var loadErrs, mapperProblems []string
			for _, candidate := range spec.candidates {
				if err := os.Setenv(entry.key, candidate); err != nil {
					t.Fatal(err)
				}
				cfg, err := LoadConfig()
				if err != nil {
					loadErrs = append(loadErrs, strconv.Quote(candidate)+": "+err.Error())
					continue
				}
				if reflect.DeepEqual(unset, cfg) {
					continue
				}
				problem := publicEnvKeyMapperProblem(entry.key, unset, cfg)
				if problem == "" {
					return
				}
				mapperProblems = append(mapperProblems, strconv.Quote(candidate)+": "+problem)
			}
			if len(mapperProblems) != 0 {
				t.Fatalf("LoadConfig() reads %s, but %s", entry.key, strings.Join(mapperProblems, "; "))
			}
			if len(loadErrs) == len(spec.candidates) {
				t.Fatalf("LoadConfig() rejected every candidate for %s: %s", entry.key, strings.Join(loadErrs, "; "))
			}
			t.Fatalf("LoadConfig() result does not change for %s=%q; the key is not read or every accepted candidate equals its default",
				entry.key, spec.candidates)
		})
	}
}

// publicEnvKeyMapperProblemは、keyの値を変えた読込結果cfgが、mapperで変換した実行時の設定へ
// 届いているかを確かめ、届いていなければその内容を返します。名前にASYNC_・DELIVERY_を含むkeyは、
// そのLoggerContractKindの実行時の設定が変わることを求めます。読込結果が変わった各
// LoggerContractKindについても、その実行時の設定が変わることを求めます。
// publicEnvKeyMapperExemptionsのkeyは、記載した理由のとおり実行時の設定が変わらないことを確認します。
func publicEnvKeyMapperProblem(key string, unset, cfg AppConfig) string {
	loaded, mapped := changedContractEnvSections(unset, cfg)
	if reason, exempt := publicEnvKeyMapperExemptions[key]; exempt {
		if len(mapped) != 0 {
			return fmt.Sprintf("the mapped %v config also changes; remove the mapper exemption (%s)", mapped, reason)
		}
		return ""
	}
	if len(mapped) == 0 {
		return "no mapped config changes; the mapper drops the setting"
	}
	if contract := contractEnvKeySection(key); contract != contractEnvSync {
		if want := contractEnvMappedSections[contract]; !slices.Contains(mapped, want) {
			return fmt.Sprintf("only the mapped %v config changes, want the %s config", mapped, want)
		}
	}
	if missing := unmappedLoadedSections(loaded, mapped); len(missing) != 0 {
		return fmt.Sprintf("the loaded %v config changes but not the %v config (mapped %v); the mapper drops the setting for that contract",
			loaded, missing, mapped)
	}
	return ""
}

// applyPublicEnvKeyEffectEnvはenvの値をprocess environmentへ設定し、元へ戻す関数を返します。
// 値がnilのkeyは外します。
func applyPublicEnvKeyEffectEnv(t *testing.T, env map[string]*string) func() {
	t.Helper()
	type previous struct {
		value   string
		existed bool
	}
	saved := make(map[string]previous, len(env))
	for key, value := range env {
		old, existed := os.LookupEnv(key)
		saved[key] = previous{value: old, existed: existed}
		var err error
		if value == nil {
			err = os.Unsetenv(key)
		} else {
			err = os.Setenv(key, *value)
		}
		if err != nil {
			t.Fatalf("change %s: %v", key, err)
		}
	}
	return func() {
		for key, old := range saved {
			if old.existed {
				_ = os.Setenv(key, old.value)
			} else {
				_ = os.Unsetenv(key)
			}
		}
	}
}

// publicEnvKeyEffectBaseEnvironmentは、公開例から生成したfixtureの.envに、条件付きで
// 読む設定を読み込ませるための有効化を加えた環境を返します。保存先は一時directoryです。
func publicEnvKeyEffectBaseEnvironment(t *testing.T) map[string]string {
	t.Helper()
	root := configFixtureRootForTest(t)
	env, err := godotenv.Read(filepath.Join(root, ".env"))
	if err != nil {
		t.Fatalf("read config fixture: %v", err)
	}
	storage := t.TempDir()
	env["HETEROLOG_SYSTEM_LOG_DIR"] = filepath.Join(storage, "logs")
	env["HETEROLOG_SYSTEM_LIB_DIR"] = filepath.Join(storage, "lib")
	env["HETEROLOG_APP_STATE_DIR"] = filepath.Join(storage, "state")
	for key, value := range publicEnvKeyEffectBaseActivation {
		env[key] = value
	}
	// object storage uploadは各logger・failure archiveの有効化keyで読む設定が決まるため、
	// 全て有効にしてS3とupload失敗の保存先の設定も読み込ませます。
	for key := range env {
		if strings.HasPrefix(key, "HETEROLOG_ENABLE_") && strings.HasSuffix(key, "OBJECT_STORAGE_UPLOAD") &&
			!strings.Contains(key, "_DEFAULT_OBJECT_STORAGE") && !strings.Contains(key, "_ERROR_OBJECT_STORAGE") &&
			!strings.Contains(key, "_DEBUG_OBJECT_STORAGE") {
			env[key] = "true"
		}
	}
	return env
}

// publicEnvKeyEffectAllDestinationsは、全出力先です。
const publicEnvKeyEffectAllDestinations = "file,stdout,slack,opensearch,loki,cloudwatchlogs,clickhouse"

// publicEnvKeyEffectBaseActivationは、全keyの試験で共通に有効化する出力先・退避先です。
var publicEnvKeyEffectBaseActivation = map[string]string{
	"HETEROLOG_ENABLE_LOGGING":                   "true",
	"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING": "true",
	"HETEROLOG_ENABLE_DELIVERY_LOGGING":          "true",
	"HETEROLOG_LOG_TYPE":                         publicEnvKeyEffectAllDestinations,
	"HETEROLOG_ASYNC_LOG_TYPE":                   publicEnvKeyEffectAllDestinations,
	"HETEROLOG_DELIVERY_LOG_TYPE":                publicEnvKeyEffectAllDestinations,
	"HETEROLOG_ENABLE_OBSERVABILITY":             "true",
	// 公開例のOpenSearchはspoolだけを選ぶため、failure archiveの設定も読み込ませます。
	"HETEROLOG_OPENSEARCH_DURABLE_TYPES":       "spool,fileArchive",
	"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_TYPES": "spool,fileArchive",
	// internal logの宛先を上書きするkey（*LOG_INTERNAL_LOG_*）は、その宛先をinternal logの
	// 委譲先に選んだ場合だけmapperが使います。各LoggerContractKindのstdout loggerのerror通知を
	// 全ての宛先へ委譲し、上書きkeyも実行時の設定まで届くことを確認します。Delivery loggerの
	// internal log batch設定も、委譲先に使うbackendだけ読みます。
	"HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER":          publicEnvKeyEffectInternalLogTargets,
	"HETEROLOG_ASYNC_STDOUT_LOGGER_ERROR_HANDLER":    publicEnvKeyEffectInternalLogTargets,
	"HETEROLOG_DELIVERY_STDOUT_LOGGER_ERROR_HANDLER": publicEnvKeyEffectInternalLogTargets,
}

// publicEnvKeyEffectInternalLogTargetsは、stdout以外の全出力先をinternal logの委譲先に選ぶ値です。
const publicEnvKeyEffectInternalLogTargets = "file,slack,opensearch,loki,cloudwatchlogs,clickhouse"

// publicEnvKeyEffectSpecは、一つのkeyの試験方法です。
type publicEnvKeyEffectSpec struct {
	// activationは、共通の環境では読まないkeyを読ませるための追加の設定です。nilは外します。
	activation map[string]*string
	// candidatesは、LoadConfigが受け付ける値の候補です。既定値を含んでも構いません。
	candidates []string
	// unsupportedは、実装済みの値が既定値だけのkeyに与える未対応の値です。
	unsupported       string
	singleValueReason string
	// exemptReasonは、値の変更をLoadConfigの結果で確認できないkeyの理由です。
	exemptReason string
}

// publicEnvKeyEffectExemptionsは、値の変更をLoadConfigの結果で確認できないkeyと理由です。
var publicEnvKeyEffectExemptions = map[string]string{
	"HETEROLOG_ENV_PATH":            "selects the environment file; this test sets it to an empty file",
	"HETEROLOG_OTEL_COLLECTOR_ADDR": "test-only variable for the opt-in OpenTelemetry Collector integration test",
}

// publicEnvKeyMapperExemptionsは、LoadConfigが読みますがmapperの変換結果へ届かないkeyと理由です。
// 理由が実装と合わなくなった場合に気付けるよう、試験はこれらのkeyで実行時の設定が変わらない
// ことも確認します。契約試験のassertEveryContractEnvLineChangesItsContractも同じ一覧を使います。
var publicEnvKeyMapperExemptions = func() map[string]string {
	const (
		timeoutBudget = "the timeout budget policy is not part of the mapped logger config; " +
			"TestTimeoutBudgetPolicyKeyAppliesOnlyToItsContract checks that it governs ValidateApplicationConfig for its contract"
		streamRouting = "CloudWatch Logs stream routing is passed to the logger constructor separately from the mapped LoggerConfig; " +
			"Map*LoggerConfig only validates it"
		unusedClient = "it only makes LoadConfig read and syntax-check the OpenSearch client settings when OpenSearch is not selected; " +
			"the mapper uses the client only for a selected OpenSearch output"
		observability = "it is loaded into AppConfig.ObservabilityCfg for the tracer (internal/application/trace/tracer, on hold); " +
			"no logger mapper uses it"
	)
	exemptions := map[string]string{
		"HETEROLOG_ENABLE_OPENSEARCH":       unusedClient,
		"HETEROLOG_ENABLE_ASYNC_OPENSEARCH": unusedClient,
	}
	for _, prefix := range publicEnvKeyEffectContractPrefixes {
		exemptions["HETEROLOG_"+prefix+"LOG_TIMEOUT_BUDGET_POLICY"] = timeoutBudget
		exemptions["HETEROLOG_"+prefix+"CLOUDWATCH_LOG_STREAM_ROTATION"] = streamRouting
	}
	for _, key := range []string{
		"HETEROLOG_ENABLE_OBSERVABILITY", "HETEROLOG_TRACING_ENABLED", "HETEROLOG_TRACING_PROVIDER", "HETEROLOG_TRACING_EXPORTER_TYPE",
		"HETEROLOG_OTEL_SPAN_ENDPOINT", "HETEROLOG_OTEL_SPAN_HEADERS", "HETEROLOG_OTEL_SPAN_INSECURE", "HETEROLOG_OTEL_SPAN_PROTOCOL",
	} {
		exemptions[key] = observability
	}
	return exemptions
}()

// publicEnvKeyEffectContractPrefixesは、sync・async・Deliveryの各loggerのkeyの接頭辞です。
var publicEnvKeyEffectContractPrefixes = []string{"ASYNC_", "DELIVERY_", ""}

func publicEnvKeyEffectSpecFor(entry publicEnvExampleKey) publicEnvKeyEffectSpec {
	key := entry.key
	if reason, exempt := publicEnvKeyEffectExemptions[key]; exempt {
		return publicEnvKeyEffectSpec{exemptReason: reason}
	}
	spec := publicEnvKeyEffectSpec{activation: publicEnvKeyEffectActivation(key)}
	switch {
	case strings.HasSuffix(key, "_DEGRADE_TYPES"):
		// DURABLE_TYPES=noneでdegradeを有効にした場合だけ読みます。s3・nfsは予約値です。
		scope := strings.TrimSuffix(key, "_DEGRADE_TYPES")
		spec.activation[scope+"_DURABLE_TYPES"] = publicEnvKeyEffectValue("none")
		spec.activation["HETEROLOG_ENABLE_"+strings.TrimPrefix(scope, "HETEROLOG_")+"_DEGRADE"] = publicEnvKeyEffectValue("true")
		spec.unsupported = "s3"
		spec.singleValueReason = "stdout is the only implemented degrade destination and the default"
		return spec
	case strings.HasSuffix(key, "_SECONDARY_DESTINATION"):
		spec.unsupported = "s3"
		spec.singleValueReason = "stdout is the only implemented secondary destination and the default"
		return spec
	}
	spec.candidates = publicEnvKeyEffectCandidates(entry)
	return spec
}

func publicEnvKeyEffectValue(value string) *string { return &value }

// publicEnvKeyEffectActivationは、共通の環境では読まないkeyを読ませる追加の設定を返します。
func publicEnvKeyEffectActivation(key string) map[string]*string {
	activation := map[string]*string{}
	for _, prefix := range publicEnvKeyEffectContractPrefixes {
		// OpenSearchの認証情報は、選択した認証方式の項目だけを読みます。
		authKey := "HETEROLOG_" + prefix + "OPENSEARCH_AUTH_TYPE"
		for _, rule := range []struct{ suffix, authType string }{
			{"OPENSEARCH_API_KEY_ID", "apikey"},
			{"OPENSEARCH_API_KEY_SECRET", "apikey"},
			{"OPENSEARCH_AWS_REGION", "awsiam"},
			{"OPENSEARCH_AWS_ACCESS_KEY_ID", "awsiam"},
			{"OPENSEARCH_AWS_SECRET_ACCESS_KEY", "awsiam"},
			{"OPENSEARCH_AWS_SESSION_TOKEN", "awsiam"},
			{"OPENSEARCH_SAML_TOKEN", "saml"},
			{"OPENSEARCH_OIDC_TOKEN", "oidc"},
			{"OPENSEARCH_CERT_FILE", "certificate"},
			{"OPENSEARCH_KEY_FILE", "certificate"},
			{"OPENSEARCH_CA_FILE", "certificate"},
		} {
			if key == "HETEROLOG_"+prefix+rule.suffix {
				activation[authKey] = publicEnvKeyEffectValue(rule.authType)
			}
		}
	}
	// AWSの共通の認証情報・regionは、S3・CloudWatch Logsの個別の値が空の場合だけ使います。
	if overriding, ok := publicEnvKeyEffectAWSFallbacks[key]; ok {
		for _, overridingKey := range overriding {
			activation[overridingKey] = nil
		}
	}
	// internal log専用fileのdebug保存先は、debugの個別directoryを有効にした場合だけ使います。
	// 切り出したprefixには、HETEROLOG_とLoggerContractKindの接頭辞が含まれます。
	if prefix, ok := strings.CutSuffix(key, "LOG_INTERNAL_LOG_FILE_DEBUG_LOG_DIR"); ok {
		activation[prefix+"LOG_INTERNAL_LOG_FILE_SEPARATE_DEBUG_LOG_DIR"] = publicEnvKeyEffectValue("true")
	}
	// HETEROLOG_ENABLE_OPENSEARCH・HETEROLOG_ENABLE_ASYNC_OPENSEARCHは、出力先にOpenSearchを含まない場合でも
	// client設定を読ませるkeyです。共通の環境では出力先とinternal logの委譲先に含むため、
	// 両方から外して読みます。
	switch key {
	case "HETEROLOG_ENABLE_OPENSEARCH":
		activation["HETEROLOG_LOG_TYPE"] = publicEnvKeyEffectValue("file,stdout,slack,loki,cloudwatchlogs,clickhouse")
		activation["HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER"] = publicEnvKeyEffectValue("file,slack,loki,cloudwatchlogs,clickhouse")
	case "HETEROLOG_ENABLE_ASYNC_OPENSEARCH":
		activation["HETEROLOG_ASYNC_LOG_TYPE"] = publicEnvKeyEffectValue("file,stdout,slack,loki,cloudwatchlogs,clickhouse")
		activation["HETEROLOG_ASYNC_STDOUT_LOGGER_ERROR_HANDLER"] = publicEnvKeyEffectValue("file,slack,loki,cloudwatchlogs,clickhouse")
	}
	return activation
}

// publicEnvKeyEffectAWSFallbacksは、AWSの共通のkeyと、それより優先する個別のkeyです。
var publicEnvKeyEffectAWSFallbacks = func() map[string][]string {
	fallbacks := map[string][]string{}
	for awsKey, suffixes := range map[string][2]string{
		"AWS_ACCESS_KEY_ID":     {"ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"},
		"AWS_SECRET_ACCESS_KEY": {"SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY"},
		"AWS_SESSION_TOKEN":     {"SESSION_TOKEN", "AWS_SESSION_TOKEN"},
		"AWS_REGION":            {"REGION", "REGION"},
	} {
		keys := []string{"HETEROLOG_OBJECT_STORAGE_S3_" + suffixes[0]}
		for _, prefix := range publicEnvKeyEffectContractPrefixes {
			keys = append(keys, "HETEROLOG_"+prefix+"CLOUDWATCH_"+suffixes[1])
		}
		fallbacks[awsKey] = keys
	}
	return fallbacks
}()

// publicEnvKeyEffectSuffixCandidatesは、値の一覧から選ぶkeyの候補です。既定値になり得る
// 値も含め、一覧の要素を並べます。
var publicEnvKeyEffectSuffixCandidates = []struct {
	suffix     string
	candidates []string
}{
	{"_UPLOAD_FAILURE_DURABLE_TYPES", []string{"fileArchive", "spool", "fileArchive,spool"}},
	{"_LOG_STREAM_ROTATION", []string{"daily", "hourly", "monthly", "none"}},
	{"_TIMEOUT_BUDGET_POLICY", []string{"may_cutoff", "unbounded", "must_fit"}},
	{"_RETRY_CLIENT_MODE", []string{"retry", "none", "circuit_breaker", "retry_with_breaker", "retry_policy_with_breaker", "retry_policy"}},
	{"_AUTH_TYPE", []string{"none", "apikey", "basic"}},
	{"_OVERLOAD_POLICY", []string{"degrade", "drop"}},
	{"_LIFECYCLE_MODE", []string{"runner", "disable", "inline"}},
	{"_COMPRESS_FORMAT", []string{"xz", "lz4", "zstd", "gzip"}},
	{"_ROTATION", []string{"weekly", "monthly", "daily"}},
	{"_LOG_LEVEL", []string{"ERROR", "WARN", "INFO", "DEBUG"}},
	{"_UPLOAD_TYPE", []string{"s3"}},
	{"_S3_DESTINATIONS", []string{`[{"bucket":"key-effect-bucket"}]`}},
	{"_INTERVALS", []string{"2,4,8", "3,9"}},
	{"_HANDLER", []string{"file", "slack,stdout"}},
	{"_HEADERS", []string{`{"X-Key-Effect":"enabled"}`}},
	{"_LABELS", []string{`{"key_effect":"enabled"}`}},
	{"_ENDPOINT", []string{"http://key-effect.invalid:18081"}},
	{"_ADDR", []string{"127.0.0.1:14318"}},
	{"_REGION", []string{"eu-west-1", "us-west-2"}},
}

// publicEnvKeyEffectCandidateOverridesは、名前と公開例の値から候補を決められないkeyの候補です。
var publicEnvKeyEffectCandidateOverrides = map[string][]string{
	"HETEROLOG_LOG_TYPE":              {"file,stdout", "stdout"},
	"HETEROLOG_ASYNC_LOG_TYPE":        {"file,stdout", "stdout"},
	"HETEROLOG_DELIVERY_LOG_TYPE":     {"file,stdout", "stdout"},
	"HETEROLOG_MEMORY_CRITICAL_RATIO": {"0.5", "0.75"},
	"HETEROLOG_APP_ENV":               {"prod", "stg", "dev"},
	"HETEROLOG_RUNTIME_TYPE":          {"k8s", "ecs", "local"},
	"HETEROLOG_TRACING_PROVIDER":      {"noop", "otel"},
	"HETEROLOG_TRACING_EXPORTER_TYPE": {"stdout", "none", "OTLP"},
	"HETEROLOG_OTEL_SPAN_PROTOCOL":    {"http", "grpc"},
	"HETEROLOG_TIMEZONE_LOG":          {"America/New_York", "Asia/Tokyo"},
}

// publicEnvKeyEffectIntegerSuffixesは、公開例の値が空でも整数を受け付けるkeyの語尾です。
var publicEnvKeyEffectIntegerSuffixes = []string{
	"_SECOND", "_MILLISECOND", "_MINUTE", "_DAYS", "_AGE", "_BACKUPS", "_MB", "_GB", "_BYTES",
	"_COUNT", "_RATE", "_ENTRIES", "_EVENTS", "_DOCUMENTS", "_CONCURRENT", "_PARALLEL",
}

// publicEnvKeyEffectCandidatesは、keyの公開例の値と名前から、有効な値の候補を返します。
func publicEnvKeyEffectCandidates(entry publicEnvExampleKey) []string {
	key, value := entry.key, entry.example
	if candidates, ok := publicEnvKeyEffectCandidateOverrides[key]; ok {
		return candidates
	}
	// 公開例の値が真偽値・整数のkeyは、名前の語尾より値の型を優先します。
	if value == "true" || value == "false" {
		return []string{"true", "false"}
	}
	if number, err := strconv.Atoi(value); err == nil {
		return []string{strconv.Itoa(number + 7), strconv.Itoa(number + 13)}
	}
	for _, rule := range publicEnvKeyEffectSuffixCandidates {
		if strings.HasSuffix(key, rule.suffix) {
			return rule.candidates
		}
	}
	if strings.HasSuffix(key, "_DURABLE_TYPES") {
		if strings.HasPrefix(key, "HETEROLOG_DELIVERY_") {
			return []string{"none", "fileArchive"}
		}
		return []string{"fileArchive", "spool", "none", "spool,fileArchive"}
	}
	if value == "" && publicEnvKeyEffectBooleanName(key) {
		return []string{"true", "false"}
	}
	if value == "" && publicEnvKeyEffectIntegerName(key) {
		return []string{"17", "29"}
	}
	switch {
	case strings.HasSuffix(key, "_CHANNEL"):
		return []string{"C0KEYEFFECT"}
	case strings.HasSuffix(key, "_DIR"):
		return []string{"key_effect_dir"}
	case value == "":
		return []string{"key-effect-value"}
	}
	return []string{value + "-key-effect"}
}

func publicEnvKeyEffectBooleanName(key string) bool {
	return strings.HasPrefix(key, "HETEROLOG_ENABLE_") || strings.HasPrefix(key, "HETEROLOG_USE_") ||
		strings.HasSuffix(key, "_ENABLED") || strings.HasSuffix(key, "_SEPARATE_DEBUG_LOG_DIR")
}

func publicEnvKeyEffectIntegerName(key string) bool {
	if strings.HasPrefix(key, "HETEROLOG_MAX_") || strings.HasPrefix(key, "HETEROLOG_MIN_") {
		return true
	}
	for _, suffix := range publicEnvKeyEffectIntegerSuffixes {
		if strings.HasSuffix(key, suffix) {
			return true
		}
	}
	return false
}
