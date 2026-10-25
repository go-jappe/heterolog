package config

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
)

// contractEnvSectionは、LoadConfigの結果のうちLoggerContractKindごとの設定とそれ以外、
// およびmapperで変換した実行時の設定のうちLoggerContractKindごとの設定とそれ以外です。
type contractEnvSection string

const (
	contractEnvSync     contractEnvSection = "sync"
	contractEnvAsync    contractEnvSection = "async"
	contractEnvDelivery contractEnvSection = "delivery"
	contractEnvOther    contractEnvSection = "other"

	contractEnvMappedSync     contractEnvSection = "mapped sync"
	contractEnvMappedAsync    contractEnvSection = "mapped async"
	contractEnvMappedDelivery contractEnvSection = "mapped delivery"
	contractEnvMappedOther    contractEnvSection = "mapped other"
)

// contractEnvMappedSectionsは、読込結果の区分と、同じLoggerContractKindの実行時の設定の区分です。
var contractEnvMappedSections = map[contractEnvSection]contractEnvSection{
	contractEnvSync:     contractEnvMappedSync,
	contractEnvAsync:    contractEnvMappedAsync,
	contractEnvDelivery: contractEnvMappedDelivery,
	contractEnvOther:    contractEnvMappedOther,
}

// contractEnvSectionsは、cfgをLoggerContractKindごとの設定とそれ以外の設定へ分けます。
// 読込結果の各LoggerContractKindには、logger設定とbackend clientの設定を含めます。
// mapperで変換した実行時の設定も、LoggerContractKindごとの区分として別に含めます。
// 読込結果だけを比べると、mapperが別のLoggerContractKindの値を使う取り違えや、
// 読み込んだ値をmapperが落とす誤りを検出できないためです。
func contractEnvSections(cfg AppConfig) map[contractEnvSection]any {
	other := cfg
	other.Logger = logconfig.LoggerConfig{}
	other.AsyncBestEffortMultiLoggerConfig = logconfig.AsyncBestEffortMultiLoggerConfig{}
	other.DeliveryLogger = logconfig.DeliveryLoggerConfig{}
	other.OpenSearchClient, other.AsyncOpenSearchClient, other.DeliveryOpenSearchClient =
		configopensearchClientZero, configopensearchClientZero, configopensearchClientZero
	other.LokiClient, other.AsyncLokiClient, other.DeliveryLokiClient =
		configlokiClientZero, configlokiClientZero, configlokiClientZero
	other.CloudWatchLogsClient, other.AsyncCloudWatchLogsClient, other.DeliveryCloudWatchLogsClient =
		configcloudwatchClientZero, configcloudwatchClientZero, configcloudwatchClientZero
	other.ClickHouseClient, other.AsyncClickHouseClient, other.DeliveryClickHouseClient =
		configclickhouseClientZero, configclickhouseClientZero, configclickhouseClientZero
	return map[contractEnvSection]any{
		contractEnvSync: []any{
			cfg.Logger, cfg.OpenSearchClient, cfg.LokiClient, cfg.CloudWatchLogsClient, cfg.ClickHouseClient,
		},
		contractEnvAsync: []any{
			cfg.AsyncBestEffortMultiLoggerConfig, cfg.AsyncOpenSearchClient, cfg.AsyncLokiClient,
			cfg.AsyncCloudWatchLogsClient, cfg.AsyncClickHouseClient,
		},
		contractEnvDelivery: []any{
			cfg.DeliveryLogger, cfg.DeliveryOpenSearchClient, cfg.DeliveryLokiClient,
			cfg.DeliveryCloudWatchLogsClient, cfg.DeliveryClickHouseClient,
		},
		contractEnvOther: other,

		contractEnvMappedSync:     ToLoggerConfig(cfg),
		contractEnvMappedAsync:    ToAsyncBestEffortMultiLoggerConfig(cfg),
		contractEnvMappedDelivery: ToDeliveryLoggerConfig(cfg),
		contractEnvMappedOther:    []any{NewSpoolConfig(&cfg), NewOtelResourceConfig(&cfg)},
	}
}

var (
	configopensearchClientZero = AppConfig{}.OpenSearchClient
	configlokiClientZero       = AppConfig{}.LokiClient
	configcloudwatchClientZero = AppConfig{}.CloudWatchLogsClient
	configclickhouseClientZero = AppConfig{}.ClickHouseClient
)

// changedContractEnvSectionsは、二つの読込結果で異なる読込結果の区分と、実行時の設定の区分を
// それぞれ名前の順で返します。
func changedContractEnvSections(before, after AppConfig) (loaded, mapped []contractEnvSection) {
	beforeSections := contractEnvSections(before)
	afterSections := contractEnvSections(after)
	for section, value := range beforeSections {
		if reflect.DeepEqual(value, afterSections[section]) {
			continue
		}
		if strings.HasPrefix(string(section), "mapped ") {
			mapped = append(mapped, section)
		} else {
			loaded = append(loaded, section)
		}
	}
	sort.Slice(loaded, func(i, j int) bool { return loaded[i] < loaded[j] })
	sort.Slice(mapped, func(i, j int) bool { return mapped[i] < mapped[j] })
	return loaded, mapped
}

// unmappedLoadedSectionsは、読込結果が変わったLoggerContractKindのうち、mapperで変換した
// 実行時の設定が変わらなかったものについて、実行時の設定の区分を返します。複数の
// LoggerContractKindが共有するkey（sync・asyncのstdout設定など）は、一つの使用箇所へ届くだけで
// 実行時の設定が変わるため、読み込んだ全てのLoggerContractKindへ届くことをこれで確かめます。
// LoggerContractKindに属さない読込結果（contractEnvOther）は、区分から使用箇所を特定できないため
// 対象外です。その共有keyはTestMapperPropagatesSharedSettingsToEveryConsumerが確認します。
func unmappedLoadedSections(loaded, mapped []contractEnvSection) []contractEnvSection {
	var missing []contractEnvSection
	for _, section := range loaded {
		if section == contractEnvOther {
			continue
		}
		if want := contractEnvMappedSections[section]; !slices.Contains(mapped, want) {
			missing = append(missing, want)
		}
	}
	return missing
}

// contractEnvKeySectionは、keyの名前に含まれる接頭辞からLoggerContractKindを判定します。
func contractEnvKeySection(key string) contractEnvSection {
	switch {
	case strings.Contains(key, "DELIVERY_"):
		return contractEnvDelivery
	case strings.Contains(key, "ASYNC_"):
		return contractEnvAsync
	}
	return contractEnvSync
}

// assertEveryContractEnvLineChangesItsContractは、checkedの各行を一つずつlinesから外して
// 読み込み、その行のkeyが属するLoggerContractKindの設定だけが、読込結果とmapperで変換した
// 実行時の設定の両方で変わることを確認します。keyを読まない実装や、既定値と同じ値を
// 与えた行では結果が変わらず、別のLoggerContractKindの値として読む・変換する実装では
// 別の設定が変わり、読み込んだ値をmapperが落とす実装では実行時の設定が変わらないため、
// 契約の分離を全ての行について確かめられます。sharedは複数のLoggerContractKindに効くkeyと、
// その理由です。sharedのkeyは、読込結果が変わった各LoggerContractKindの実行時の設定も
// 変わることを確認します。mapperへ届かないkeyはpublicEnvKeyMapperExemptionsに理由とともに
// 記載し、実行時の設定が変わらないことを確認します。
func assertEveryContractEnvLineChangesItsContract(
	t *testing.T,
	lines []string,
	checked []string,
	shared map[string]string,
) {
	t.Helper()
	full, err := loadContractEnvForEffect(t, lines)
	if err != nil {
		t.Fatalf("LoadConfig() with every contract line: %v", err)
	}
	for _, line := range checked {
		key, _, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("invalid contract env line %q", line)
		}
		without := make([]string, 0, len(lines)-1)
		removed := false
		for _, candidate := range lines {
			if !removed && candidate == line {
				removed = true
				continue
			}
			without = append(without, candidate)
		}
		if !removed {
			t.Fatalf("contract env line %q is not part of the loaded environment", line)
		}
		cfg, err := loadContractEnvForEffect(t, without)
		if err != nil {
			// 外すと読込自体が失敗するkeyは、値を読んでいます。
			continue
		}
		loaded, mapped := changedContractEnvSections(full, cfg)
		if len(loaded) == 0 {
			t.Errorf("%s: removing the line does not change LoadConfig(); the key is not read or %q equals its default", key, line)
			continue
		}
		reason, mapperExempt := publicEnvKeyMapperExemptions[key]
		switch {
		case mapperExempt && len(mapped) != 0:
			t.Errorf("%s: removing the line changes the mapped %v config; remove the mapper exemption (%s)", key, mapped, reason)
			continue
		case !mapperExempt && len(mapped) == 0:
			t.Errorf("%s: removing the line changes the loaded %v config but no mapped config; the mapper drops the setting", key, loaded)
			continue
		}
		if reason, ok := shared[key]; ok {
			if reason == "" {
				t.Errorf("%s: shared key has no documented reason", key)
			}
			if missing := unmappedLoadedSections(loaded, mapped); !mapperExempt && len(missing) != 0 {
				t.Errorf("%s: removing the line changes the loaded %v config but not the %v config (mapped %v); the mapper drops the shared setting",
					key, loaded, missing, mapped)
			}
			continue
		}
		want := contractEnvKeySection(key)
		if len(loaded) != 1 || loaded[0] != want {
			t.Errorf("%s: removing the line changes %v, want only the %s contract", key, loaded, want)
		}
		if wantMapped := contractEnvMappedSections[want]; !mapperExempt && (len(mapped) != 1 || mapped[0] != wantMapped) {
			t.Errorf("%s: removing the line changes the %v config, want only the %s config", key, mapped, wantMapped)
		}
	}
}

// loadContractEnvForEffectは、linesだけを環境設定fileとして読み込みます。process environmentは
// 試験終了時に元へ戻します。
func loadContractEnvForEffect(t *testing.T, lines []string) (AppConfig, error) {
	t.Helper()
	envPath := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(envPath, []byte(strings.Join(lines, "\n")), 0o600); err != nil {
		t.Fatalf("write contract env: %v", err)
	}
	restoreProcessEnvAfterTest(t)
	clearProcessEnvForTest(t)
	if err := os.Setenv("HETEROLOG_ENV_PATH", envPath); err != nil {
		t.Fatalf("set HETEROLOG_ENV_PATH: %v", err)
	}
	return LoadConfig()
}
