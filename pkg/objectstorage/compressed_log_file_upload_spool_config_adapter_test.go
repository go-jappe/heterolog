package objectstorage

import (
	"reflect"
	"testing"
	"time"
)

// providerは構築時の設定値をそのまま返し、既定値の補完や検証を行いません。
// 0のMaxRetryAfterなどの既定値は利用側が解決するため、未指定のまま返します。
func TestCompressedLogFileUploadSpoolConfigAdapterReturnsConstructionSettings(t *testing.T) {
	populated := CompressedLogFileUploadSpoolConfig{
		SpoolPath:                  "/var/lib/example/spool",
		Target:                     "example",
		LoggerContract:             "delivery_logger",
		RunnerInterval:             3 * time.Second,
		LifecycleInterval:          2 * time.Minute,
		CircuitBreakerOpenCooldown: 7 * time.Minute,
		MaxRetryAfter:              45 * time.Minute,
		Done:                       CompressedLogFileUploadSpoolPurgeConfig{Enabled: true, MaxFileAgeDays: 11, MaxDirSize: 1200},
		Dead:                       CompressedLogFileUploadSpoolPurgeConfig{Enabled: true, MaxFileAgeDays: 23, MaxDirSize: 3400},
		Quarantine:                 CompressedLogFileUploadSpoolPurgeConfig{Enabled: true, MaxFileAgeDays: 37, MaxDirSize: 5600},
	}
	// 設定項目を追加した場合も、providerのgetterと下のhelperを確認できるよう全fieldを設定します。
	assertEveryFieldSet(t, reflect.ValueOf(populated), "CompressedLogFileUploadSpoolConfig")

	for _, test := range []struct {
		name   string
		config CompressedLogFileUploadSpoolConfig
	}{
		{name: "configured values", config: populated},
		{name: "zero values remain unspecified"},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := NewCompressedLogFileUploadSpoolConfigAdapter(test.config)
			if got := spoolConfigFromProvider(t, provider); got != test.config {
				t.Fatalf("provider settings = %+v, want %+v", got, test.config)
			}
		})
	}
}

// spoolConfigFromProviderは、providerの全getterからCompressedLogFileUploadSpoolConfigを
// 組み立て直します。
func spoolConfigFromProvider(t *testing.T, provider CompressedLogFileUploadSpoolConfigProvider) CompressedLogFileUploadSpoolConfig {
	t.Helper()
	purge := func(name string, config CompressedLogFileUploadSpoolPurgeConfigProvider) CompressedLogFileUploadSpoolPurgeConfig {
		t.Helper()
		if config == nil {
			t.Fatalf("%s purge settings are nil", name)
		}
		return CompressedLogFileUploadSpoolPurgeConfig{
			Enabled: config.Enabled(), MaxFileAgeDays: config.MaxFileAgeDays(), MaxDirSize: config.MaxDirSize(),
		}
	}
	return CompressedLogFileUploadSpoolConfig{
		SpoolPath:                  provider.SpoolPath(),
		Target:                     provider.Target(),
		LoggerContract:             provider.LoggerContract(),
		RunnerInterval:             provider.RunnerInterval(),
		LifecycleInterval:          provider.LifecycleInterval(),
		CircuitBreakerOpenCooldown: provider.CircuitBreakerOpenCooldown(),
		MaxRetryAfter:              provider.MaxRetryAfter(),
		Done:                       purge("done", provider.DonePurge()),
		Dead:                       purge("dead", provider.DeadPurge()),
		Quarantine:                 purge("quarantine", provider.QuarantinePurge()),
	}
}

// assertEveryFieldSetは、test fixtureがstructの全fieldへ0以外の値を設定していることを
// 再帰的に確認します。fieldの追加時にgetterの対応漏れを検出できるようにします。
func assertEveryFieldSet(t *testing.T, value reflect.Value, path string) {
	t.Helper()
	if value.Kind() != reflect.Struct {
		if value.IsZero() {
			t.Fatalf("test fixture field %s is zero; set it so the provider getter is verified", path)
		}
		return
	}
	for i := range value.NumField() {
		assertEveryFieldSet(t, value.Field(i), path+"."+value.Type().Field(i).Name)
	}
}
