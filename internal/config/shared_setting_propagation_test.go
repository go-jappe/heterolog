package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	goruntime "runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/config/app"
	"github.com/go-jappe/heterolog/internal/config/fsstoreconfig"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	configslack "github.com/go-jappe/heterolog/internal/config/slack"
	"github.com/go-jappe/heterolog/internal/config/spool/spoolsupervisor"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	loggerdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/otel"
	"github.com/go-jappe/heterolog/pkg/path/appstatepath"
	pkgslack "github.com/go-jappe/heterolog/pkg/slack"
)

// TestMapperPropagatesSharedSettingsToEveryConsumerは、LoggerContractKindの区分に属さず、複数の
// 出力先・LoggerContractKindで共有する環境変数（HETEROLOG_APP_NAME・HETEROLOG_TARGET・HETEROLOG_ENABLE_LOCAL_TIME_LOG・HETEROLOG_TIMEZONE_LOG・
// HETEROLOG_MIN_AVAILABLE_DISK_SPACE_GB・memory監視・実行環境の情報・HETEROLOG_APP_STATE_DIR・HETEROLOG_SYSTEM_LOG_DIR・HETEROLOG_SYSTEM_LIB_DIR）と、
// loaderが固定値として与える設定（log本文の時刻表記とerror traceの深さ・送信遅延の警告閾値・file操作の
// 再試行・file名の書式と接頭辞・file書込みと圧縮の遅延閾値・Slackの最大文字数・OpenSearchのbulk item
// 再送・spool runnerの再起動待機）が、mapperで変換した全ての使用箇所へ届くことを検証します。共有するkeyは一つの使用箇所へ
// 届くだけで読込結果も実行時の設定も変わるため、TestEveryPublicEnvExampleKeyChangesLoadedConfigでは
// 一部の使用箇所の取りこぼしを検出できません。sync・asyncのstdout設定のように特定の
// LoggerContractKindの区分に属する共有keyは、TestEveryPublicEnvExampleKeyChangesLoadedConfigが
// 読み込んだ全てのLoggerContractKindへ届くことを確認します。
//
// 使用箇所は名前で列挙せず、公開設定型の型で変換結果から探します。全出力先・全退避先・internal logの
// 全宛先を有効にし、rootごとに通常の使用箇所とinternal log専用の宛先の複製の件数が想定と一致する
// ことも確認します。使用箇所の追加・削除や型の変更で件数が変わり、確認の対象から外れた使用箇所や、
// 宛先ごと落ちた設定に気付けます。
//
// IsDebugとweekly rotationの曜日は、loaderの固定値が型の初期値（false・time.Sunday）と同じため、
// 変換で落としても実行時の値は変わらず、この試験の対象外です。
func TestMapperPropagatesSharedSettingsToEveryConsumer(t *testing.T) {
	const (
		appName  = "propagation-app"
		target   = "propagation-target"
		timeZone = "America/New_York"
	)
	storage := t.TempDir()
	stateDir := filepath.Join(storage, "propagation-state")
	logDir := filepath.Join(storage, "propagation-logs")
	libDir := filepath.Join(storage, "propagation-lib")
	env := publicEnvKeyEffectBaseEnvironment(t)
	for key, value := range map[string]string{
		"HETEROLOG_APP_NAME":                         appName,
		"HETEROLOG_TARGET":                           target,
		"HETEROLOG_ENABLE_LOCAL_TIME_LOG":            "true",
		"HETEROLOG_TIMEZONE_LOG":                     timeZone,
		"HETEROLOG_MIN_AVAILABLE_DISK_SPACE_GB":      "37",
		"HETEROLOG_MEMORY_CRITICAL_RATIO":            "0.61",
		"HETEROLOG_MIN_AVAILABLE_MEMORY_BYTES_MB":    "333",
		"HETEROLOG_MEMORY_CRITICAL_CONSECUTIVE_HITS": "7",
		"HETEROLOG_APP_ENV":                          "stg",
		"HETEROLOG_RUNTIME_TYPE":                     "ecs",
		"HETEROLOG_INSTANCE_ID":                      "propagation-instance",
		"HETEROLOG_CLOUD_PROVIDER":                   "aws",
		"HETEROLOG_CLOUD_PROVIDER_REGION":            "ap-northeast-1",
		"HETEROLOG_APP_STATE_DIR":                    stateDir,
		"HETEROLOG_SYSTEM_LOG_DIR":                   logDir,
		"HETEROLOG_SYSTEM_LIB_DIR":                   libDir,
	} {
		env[key] = value
	}
	// internal log専用の宛先も通常の出力先と同じ共有設定を継承することを確認するため、
	// 各LoggerContractKindで全ての宛先を上書きします。委譲先はpublicEnvKeyEffectBaseEnvironmentが
	// stdout loggerのerror通知に設定済みです。
	for _, prefix := range publicEnvKeyEffectContractPrefixes {
		env["HETEROLOG_"+prefix+"LOG_INTERNAL_LOG_FILE_LOG_DIR"] = "internal-propagation"
		env["HETEROLOG_"+prefix+"LOG_INTERNAL_LOG_SLACK_CHANNEL"] = "#internal-propagation"
		env["HETEROLOG_"+prefix+"LOG_INTERNAL_LOG_OPENSEARCH_INDEX"] = "internal-propagation"
		env["HETEROLOG_"+prefix+"LOG_INTERNAL_LOG_LOKI_LABELS"] = `{"scope":"internal-propagation"}`
		env["HETEROLOG_"+prefix+"LOG_INTERNAL_LOG_CLOUDWATCH_LOG_GROUP_NAME"] = "internal-propagation"
		env["HETEROLOG_"+prefix+"LOG_INTERNAL_LOG_CLICKHOUSE_TABLE"] = "internal_propagation"
	}
	envPath := filepath.Join(t.TempDir(), "empty.env")
	if err := os.WriteFile(envPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	// 一時directoryを作成してから設定項目だけを外します。TMP・TEMPなどの実行環境は残ります。
	testutil.UnsetConfigEnvironment(t)
	t.Setenv("HETEROLOG_ENV_PATH", envPath)
	for key, value := range env {
		if err := os.Setenv(key, value); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() with every output enabled: %v", err)
	}
	roots := map[string]any{
		"sync":     ToLoggerConfig(cfg),
		"async":    ToAsyncBestEffortMultiLoggerConfig(cfg),
		"delivery": ToDeliveryLoggerConfig(cfg),
	}

	hostName, _ := os.Hostname()
	diskSpace := uint64(37) * BytesPerMegabyte * 1024
	memory := uint64(333) * BytesPerMegabyte
	const (
		// outputsは全LoggerContractKindで有効にした出力先（file・stdout・slack・opensearch・loki・
		// cloudwatchlogs・clickhouse）の数、internalTargetsはinternal log専用の宛先（stdout以外）の数です。
		outputs, internalTargets = 7, 6
		// backendsは送信失敗を保存するbackend（opensearch・loki・cloudwatchlogs・clickhouse）の数です。
		backends = 4
		// fileRolesは、log fileのupload先を持つdefault・error・debugの数です。
		fileRoles = 3
	)

	t.Run("clock", func(t *testing.T) {
		want := pkglogger.TimeConfig{IsLocalTimeEnabled: true, TimeZone: timeZone}
		requireEveryMappedValue(t, roots, everyContractValues(outputs, internalTargets), func(got pkglogger.TimeConfig) string {
			return mismatch(got, want)
		})
	})
	t.Run("runtime", func(t *testing.T) {
		// Delivery loggerはSlackだけがRuntimeConfigを使い、他の出力先はDeliveryRuntimeConfigです。
		requireEveryMappedValue(t, roots, mappedValueCounts{
			"sync": {outputs, internalTargets}, "async": {outputs, internalTargets}, "delivery": {1, 1},
		}, func(got pkglogger.RuntimeConfig) string {
			return mismatch(struct{ AppName, Target string }{got.AppName, got.Target}, struct{ AppName, Target string }{appName, target})
		})
		requireEveryMappedValue(t, roots, deliveryValues(outputs-1, internalTargets-1), func(got pkglogger.DeliveryRuntimeConfig) string {
			return mismatch(struct{ AppName, Target string }{got.AppName, got.Target}, struct{ AppName, Target string }{appName, target})
		})
	})
	t.Run("resource", func(t *testing.T) {
		wantResource := pkglogger.Resource{
			ServiceName: appName, ServiceVersion: app.AppVersion, HostName: hostName, OSType: goruntime.GOOS,
			Environment: "stg", RuntimeType: "ecs",
			Cloud: &pkglogger.CloudResourceInfo{RuntimeInstanceID: "propagation-instance", Provider: "aws", Region: "ap-northeast-1"},
		}
		requireEveryMappedValue(t, roots, everyContractValues(backends, backends), func(got pkglogger.Resource) string {
			return mismatch(got, wantResource)
		})
		wantOTel := otel.Resource{
			ServiceName: appName, ServiceVersion: app.AppVersion, CloudProvider: "aws", HostName: hostName,
			OSType: goruntime.GOOS, Environment: "stg",
		}
		requireEveryMappedValue(t, roots, everyContractValues(1, 0), func(got otel.Resource) string {
			return mismatch(got, wantOTel)
		})
	})
	t.Run("log body", func(t *testing.T) {
		type logBody struct {
			dateFormat    string
			normal, debug int
		}
		want := logBody{logconfig.LogLineDateFormat, logconfig.DefaultErrorTraceDepth, logconfig.DefaultErrorTraceDepthDebugMode}
		requireEveryMappedValue(t, roots, everyContractValues(outputs, internalTargets), func(got pkglogger.LogBodyConfig) string {
			return mismatch(logBody{got.LogLineDateFormat, got.ErrorTraceDepth, got.ErrorTraceDepthDebugMode}, want)
		})
		type depths struct{ normal, debug int }
		wantDepths := depths{logconfig.DefaultErrorTraceDepth, logconfig.DefaultErrorTraceDepthDebugMode}
		requireEveryMappedValue(t, roots, syncAndAsyncValues(backends, backends), func(got loggerdurable.FileArchiveConfig) string {
			return mismatch(depths{got.ErrorTraceDepth, got.ErrorTraceDepthDebugMode}, wantDepths)
		})
		requireEveryMappedValue(t, roots, deliveryValues(backends, backends), func(got loggerdurable.DeliveryFileArchiveConfig) string {
			return mismatch(depths{got.ErrorTraceDepth, got.ErrorTraceDepthDebugMode}, wantDepths)
		})
	})
	t.Run("send latency thresholds", func(t *testing.T) {
		want := [2]time.Duration{logconfig.PostLogWarnThresholdDuration, logconfig.PostLogErrorThresholdDuration}
		requireEveryMappedValue(t, roots, syncAndAsyncValues(1, 1), func(got pkglogger.OpenSearchFlushManagerConfig) string {
			return mismatch([2]time.Duration{got.PostWarnThresholdDuration, got.PostErrorThresholdDuration}, want)
		})
		requireEveryMappedValue(t, roots, deliveryValues(1, 1), func(got pkglogger.DeliveryOpenSearchFlushManagerConfig) string {
			return mismatch([2]time.Duration{got.PostWarnThresholdDuration, got.PostErrorThresholdDuration}, want)
		})
		requireEveryMappedValue(t, roots, syncAndAsyncValues(1, 1), func(got pkglogger.LokiFlushManagerConfig) string {
			return mismatch([2]time.Duration{got.PostWarnThresholdDuration, got.PostErrorThresholdDuration}, want)
		})
		requireEveryMappedValue(t, roots, deliveryValues(1, 1), func(got pkglogger.DeliveryLokiFlushManagerConfig) string {
			return mismatch([2]time.Duration{got.PostWarnThresholdDuration, got.PostErrorThresholdDuration}, want)
		})
		requireEveryMappedValue(t, roots, syncAndAsyncValues(1, 1), func(got pkglogger.CloudWatchLogsFlushManagerConfig) string {
			return mismatch([2]time.Duration{got.PostWarnThresholdDuration, got.PostErrorThresholdDuration}, want)
		})
		requireEveryMappedValue(t, roots, deliveryValues(1, 1), func(got pkglogger.DeliveryCloudWatchLogsFlushManagerConfig) string {
			return mismatch([2]time.Duration{got.PostWarnThresholdDuration, got.PostErrorThresholdDuration}, want)
		})
		requireEveryMappedValue(t, roots, syncAndAsyncValues(1, 1), func(got pkglogger.ClickHouseFlushManagerConfig) string {
			return mismatch([2]time.Duration{got.PostWarnThresholdDuration, got.PostErrorThresholdDuration}, want)
		})
		requireEveryMappedValue(t, roots, deliveryValues(1, 1), func(got pkglogger.DeliveryClickHouseFlushManagerConfig) string {
			return mismatch([2]time.Duration{got.PostWarnThresholdDuration, got.PostErrorThresholdDuration}, want)
		})
		wantSlack := [2]time.Duration{configslack.SlackPostWarnThresholdDuration, configslack.SlackPostErrorThresholdDuration}
		requireEveryMappedValue(t, roots, syncAndAsyncValues(1, 1), func(got pkglogger.SlackLoggerConfig) string {
			return mismatch([2]time.Duration{got.SlackPostWarnThresholdDuration, got.SlackPostErrorThresholdDuration}, wantSlack)
		})
		requireEveryMappedValue(t, roots, deliveryValues(1, 1), func(got pkglogger.DeliverySlackLoggerConfig) string {
			return mismatch([2]time.Duration{got.SlackPostWarnThresholdDuration, got.SlackPostErrorThresholdDuration}, wantSlack)
		})
	})
	t.Run("file operation retry", func(t *testing.T) {
		type retry struct {
			attempts int
			wait     time.Duration
		}
		want := retry{fsstoreconfig.MaxFileOperationRetries, fsstoreconfig.FileOperationRetryWaitTime}
		requireEveryMappedValue(t, roots, everyContractValues(1, 1), func(got pkgfsstore.RetryConfig) string {
			return mismatch(retry{got.MaxWriteRetries, got.FileOperationRetryWaitTime}, want)
		})
		requireEveryMappedValue(t, roots, syncAndAsyncValues(backends, backends), func(got loggerdurable.FileArchiveConfig) string {
			return mismatch(retry{got.MaxWriteRetries, got.FileOperationRetryWaitTime}, want)
		})
		requireEveryMappedValue(t, roots, deliveryValues(backends, backends), func(got loggerdurable.DeliveryFileArchiveConfig) string {
			return mismatch(retry{got.MaxWriteRetries, got.FileOperationRetryWaitTime}, want)
		})
	})
	t.Run("target", func(t *testing.T) {
		requireEveryMappedValue(t, roots, everyContractValues(1, 1), func(got pkgfsstore.FsstoreConfig) string {
			return mismatch(got.Target, target)
		})
		requireEveryMappedValue(t, roots, syncAndAsyncValues(backends, backends), func(got loggerdurable.FileArchiveConfig) string {
			return mismatch(got.Target, target)
		})
		requireEveryMappedValue(t, roots, deliveryValues(backends, backends), func(got loggerdurable.DeliveryFileArchiveConfig) string {
			return mismatch(got.Target, target)
		})
		requireEveryMappedValue(t, roots, syncAndAsyncValues(backends, backends), func(got loggerdurable.SpoolConfig) string {
			return mismatch(got.Target, target)
		})
	})
	t.Run("storage paths", func(t *testing.T) {
		requireEveryMappedValue(t, roots, everyContractValues(1, 1), func(got pkgfsstore.FsstoreConfig) string {
			return mismatch(got.LogPath, logDir)
		})
		requireEveryMappedValue(t, roots, syncAndAsyncValues(backends, backends), func(got loggerdurable.SpoolConfig) string {
			return mismatch(got.SpoolPath, libDir)
		})
		requireEveryMappedValue(t, roots, syncAndAsyncValues(backends, backends), func(got loggerdurable.FileArchiveConfig) string {
			return mismatch(got.FailureFileArchivePath, stateDir)
		})
		requireEveryMappedValue(t, roots, deliveryValues(backends, backends), func(got loggerdurable.DeliveryFileArchiveConfig) string {
			return mismatch(got.FailureFileArchivePath, stateDir)
		})
		// archive checkpointはlog fileとfailure archiveごとに、HETEROLOG_APP_STATE_DIRとHETEROLOG_TARGETから決めます。
		wantCheckpoint := appstatepath.ArchiveCheckpointDirectory(stateDir, target)
		requireEveryMappedValue(t, roots, everyContractValues(1+backends, 1+backends), func(got pkgfsstore.ArchiveCheckpointConfig) string {
			return mismatch(got.Directory, wantCheckpoint)
		})
		// upload失敗の退避先は、log fileのroleごとの送信先とfailure archiveの送信先ごとに持ちます。
		requireEveryMappedValue(t, roots, everyContractValues(fileRoles+backends, fileRoles+backends),
			func(got objectstorage.CompressedLogFileUploadFailureArchiveConfig) string {
				return mismatch(filepath.Dir(got.RootPath), filepath.Join(stateDir, target))
			})
	})
	t.Run("file name formats and write thresholds", func(t *testing.T) {
		type layout struct{ nameFormat, dateFormat, errorPrefix, debugPrefix string }
		requireEveryMappedValue(t, roots, everyContractValues(1, 1), func(got pkgfsstore.FsstoreConfig) string {
			return mismatch(
				layout{got.FileNameFormat, got.FileDateFormat, got.SeparateErrorLogsPrefix, got.SeparateDebugLogsPrefix},
				layout{pkgfsstore.FileNameFormat, pkgfsstore.FileDateFormat, logconfig.SeparateErrorLogsPrefix, logconfig.SeparateDebugLogsPrefix},
			)
		})
		requireEveryMappedValue(t, roots, syncAndAsyncValues(backends, backends), func(got loggerdurable.FileArchiveConfig) string {
			return mismatch(got.FileDateFormat, pkgfsstore.FileDateFormat)
		})
		requireEveryMappedValue(t, roots, deliveryValues(backends, backends), func(got loggerdurable.DeliveryFileArchiveConfig) string {
			return mismatch(got.FileDateFormat, pkgfsstore.FileDateFormat)
		})
		wantWrite := [2]time.Duration{fsstoreconfig.WriteLogFileWarnThresholdDuration, fsstoreconfig.WriteLogFileErrorThresholdDuration}
		requireEveryMappedValue(t, roots, everyContractValues(1, 1), func(got pkgfsstore.DurationConfig) string {
			return mismatch([2]time.Duration{got.WriteFileWarnThresholdDuration, got.WriteFileErrorThresholdDuration}, wantWrite)
		})
		// 圧縮はsync・asyncのfile書込み設定と、Deliveryのlifecycle runner設定が持ちます。
		wantCompress := [2]time.Duration{fsstoreconfig.CompressWarnThresholdDuration, fsstoreconfig.CompressErrorThresholdDuration}
		syncAndAsyncRoots := map[string]any{"sync": roots["sync"], "async": roots["async"]}
		requireEveryMappedValue(t, syncAndAsyncRoots, syncAndAsyncValues(1, 1), func(got pkgfsstore.DurationConfig) string {
			return mismatch([2]time.Duration{got.CompressWarnThresholdDuration, got.CompressErrorThresholdDuration}, wantCompress)
		})
		requireEveryMappedValue(t, roots, deliveryValues(1, 1), func(got pkglogger.DeliveryFileLifecycleRunnerSettings) string {
			return mismatch([2]time.Duration{got.CompressWarnThresholdDuration, got.CompressErrorThresholdDuration}, wantCompress)
		})
	})
	t.Run("backend fixed limits", func(t *testing.T) {
		requireEveryMappedValue(t, roots, syncAndAsyncValues(1, 1), func(got pkglogger.SlackLoggerConfig) string {
			return mismatch(got.MaxMessageLength, pkgslack.MaxMessageLength)
		})
		requireEveryMappedValue(t, roots, deliveryValues(1, 1), func(got pkglogger.DeliverySlackLoggerConfig) string {
			return mismatch(got.MaxMessageLength, pkgslack.MaxMessageLength)
		})
		type bulkRetry struct {
			attempts int
			wait     time.Duration
		}
		requireEveryMappedValue(t, roots, syncAndAsyncValues(1, 1), func(got pkglogger.OpenSearchFlushManagerConfig) string {
			return mismatch(bulkRetry{got.MaxItemRetries, got.RetryWaitTime},
				bulkRetry{logconfig.MaxOpenSearchLoggerItemRetries, logconfig.OpenSearchRetryWaitTime})
		})
		// spool runnerの再起動待機はloaderの固定値で、spool設定（NewSpoolConfig）へ変換します。
		if problem := mismatch(NewSpoolConfig(&cfg).SupervisorConfig.RestartDelay, spoolsupervisor.DefaultRestartDelay); problem != "" {
			t.Errorf("NewSpoolConfig SupervisorConfig.RestartDelay: %s", problem)
		}
	})
	t.Run("disk and memory thresholds", func(t *testing.T) {
		requireEveryMappedValue(t, roots, syncAndAsyncValues(1, 1), func(got pkgfsstore.FileArchiveOverloadPolicyConfig) string {
			return mismatch(got.MinAvailableDiskSpace, diskSpace)
		})
		requireEveryMappedValue(t, roots, everyContractValues(backends, backends), func(got loggerdurable.FileArchiveOverloadPolicyConfig) string {
			return mismatch(got.MinAvailableDiskSpace, diskSpace)
		})
		type spoolThresholds struct {
			disk, memory uint64
			ratio        float64
			hits         int
		}
		want := spoolThresholds{disk: diskSpace, memory: memory, ratio: 0.61, hits: 7}
		requireEveryMappedValue(t, roots, syncAndAsyncValues(backends, backends), func(got loggerdurable.SpoolOverloadPolicyConfig) string {
			return mismatch(spoolThresholds{
				disk: got.MinAvailableDiskSpace, memory: got.MinAvailableMemoryBytes,
				ratio: got.MemoryCriticalRatio, hits: got.MemoryCriticalConsecutiveHits,
			}, want)
		})
	})
}

// mismatchは、gotがwantと異なる場合にその内容を返します。
func mismatch(got, want any) string {
	if reflect.DeepEqual(got, want) {
		return ""
	}
	return fmt.Sprintf("got %+v, want %+v", got, want)
}

// mappedValueCountは、一つのrootにある値の件数を、通常の使用箇所とinternal log専用の宛先の
// 複製（InternalLogDestinations）に分けたものです。
type mappedValueCount struct{ normal, internal int }

// mappedValueCountsは、rootの名前ごとのmappedValueCountです。記載しないrootは0件を求めます。
type mappedValueCounts map[string]mappedValueCount

// everyContractValuesは、sync・async・Deliveryの各rootに同じ件数があることを表します。
func everyContractValues(normal, internal int) mappedValueCounts {
	return mappedValueCounts{"sync": {normal, internal}, "async": {normal, internal}, "delivery": {normal, internal}}
}

// syncAndAsyncValuesは、sync・asyncの各rootにだけ値があることを表します。
func syncAndAsyncValues(normal, internal int) mappedValueCounts {
	return mappedValueCounts{"sync": {normal, internal}, "async": {normal, internal}}
}

// deliveryValuesは、Deliveryのrootにだけ値があることを表します。
func deliveryValues(normal, internal int) mappedValueCounts {
	return mappedValueCounts{"delivery": {normal, internal}}
}

// requireEveryMappedValueは、rootsから型Tの値を全て探し、rootごとに通常の使用箇所と
// internal log専用の宛先の複製の件数がwantと一致することと、全ての値でcheckが空文字を返すことを
// 確認します。checkは不一致の内容を返します。
func requireEveryMappedValue[T any](t *testing.T, roots map[string]any, want mappedValueCounts, check func(T) string) {
	t.Helper()
	names := make([]string, 0, len(roots))
	for name := range roots {
		names = append(names, name)
	}
	for name := range want {
		if _, ok := roots[name]; !ok {
			t.Fatalf("want counts for %s, which is not a mapped root", name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		var got mappedValueCount
		for _, value := range mappedValuesOfType[T](roots[name], name) {
			if strings.Contains(value.path, ".InternalLogDestinations.") {
				got.internal++
			} else {
				got.normal++
			}
			if problem := check(value.value); problem != "" {
				t.Errorf("%s (%v): %s", value.path, reflect.TypeFor[T](), problem)
			}
		}
		if got != want[name] {
			t.Errorf("found %d %v values in the mapped %s config and %d in its internal-log destinations, want %d and %d",
				got.normal, reflect.TypeFor[T](), name, got.internal, want[name].normal, want[name].internal)
		}
	}
}

// mappedValueは、変換結果から型で探した値と、診断用の場所です。
type mappedValue[T any] struct {
	path  string
	value T
}

// mappedValuesOfTypeは、rootから公開field・pointer・interface・slice・array・mapを辿って到達できる
// 型Tの値を全て返します。同じpointerは一度だけ辿ります。
func mappedValuesOfType[T any](root any, rootPath string) []mappedValue[T] {
	type visitKey struct {
		typ     reflect.Type
		address uintptr
	}
	want := reflect.TypeFor[T]()
	visited := make(map[visitKey]bool)
	var found []mappedValue[T]
	var visit func(reflect.Value, string)
	visit = func(value reflect.Value, path string) {
		if !value.IsValid() {
			return
		}
		if value.Type() == want {
			found = append(found, mappedValue[T]{path: path, value: value.Interface().(T)})
		}
		switch value.Kind() {
		case reflect.Pointer:
			if value.IsNil() {
				return
			}
			key := visitKey{typ: value.Type(), address: value.Pointer()}
			if visited[key] {
				return
			}
			visited[key] = true
			visit(value.Elem(), path)
		case reflect.Interface:
			if !value.IsNil() {
				visit(value.Elem(), path)
			}
		case reflect.Struct:
			for index := range value.NumField() {
				if field := value.Type().Field(index); field.IsExported() {
					visit(value.Field(index), path+"."+field.Name)
				}
			}
		case reflect.Slice, reflect.Array:
			for index := range value.Len() {
				visit(value.Index(index), fmt.Sprintf("%s[%d]", path, index))
			}
		case reflect.Map:
			iterator := value.MapRange()
			for iterator.Next() {
				visit(iterator.Value(), fmt.Sprintf("%s[%v]", path, iterator.Key()))
			}
		}
	}
	visit(reflect.ValueOf(root), rootPath)
	return found
}
