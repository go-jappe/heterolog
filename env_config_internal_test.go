package heterolog

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	appconfig "github.com/go-jappe/heterolog/internal/config"
	"github.com/go-jappe/heterolog/internal/testutil"
)

func loadEnvConfigFromLinesForTest(t *testing.T, lines ...string) (*EnvConfig, error) {
	t.Helper()
	testutil.UnsetConfigEnvironment(t)
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	path := filepath.Join(t.TempDir(), "heterolog.env")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return LoadEnvConfig(path)
}

// Config・AsyncConfigのoptionは、環境設定から読んだspoolの再送設定とCloudWatch Logsの
// stream分割を渡します。Delivery loggerはspoolを再送しないため、WithSpoolを含めません。
func TestEnvConfigOptionsCarryTheSpoolAndStreamRouting(t *testing.T) {
	// stream分割はCloudWatch Logsを出力先に選んだ場合に読むため、試験用の認証情報で選びます。
	var lines []string
	for _, contract := range []struct{ prefix, routing string }{
		{"", "daily"}, {"ASYNC_", "hourly"}, {"DELIVERY_", "monthly"},
	} {
		lines = append(lines,
			"HETEROLOG_"+contract.prefix+"LOG_TYPE=stdout,cloudwatchlogs",
			"HETEROLOG_"+contract.prefix+"CLOUDWATCH_LOG_STREAM_ROTATION="+contract.routing,
			"HETEROLOG_"+contract.prefix+"CLOUDWATCH_AWS_ACCESS_KEY_ID=env-config-test-access-key",
			"HETEROLOG_"+contract.prefix+"CLOUDWATCH_AWS_SECRET_ACCESS_KEY=env-config-test-secret-key",
		)
	}
	loaded, err := loadEnvConfigFromLinesForTest(t, lines...)
	if err != nil {
		t.Fatalf("LoadEnvConfig: %v", err)
	}
	resolve := func(t *testing.T, opts []Option, err error) options {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		o, err := resolveOptions(opts)
		if err != nil {
			t.Fatal(err)
		}
		return o
	}

	_, syncOpts, err := loaded.Config()
	syncOptions := resolve(t, syncOpts, err)
	_, asyncOpts, err := loaded.AsyncConfig()
	asyncOptions := resolve(t, asyncOpts, err)
	_, deliveryOpts, err := loaded.DeliveryConfig()
	deliveryOptions := resolve(t, deliveryOpts, err)

	for name, o := range map[string]options{"sync": syncOptions, "async": asyncOptions} {
		if o.spool == nil || !reflect.DeepEqual(*o.spool, loaded.app.SpoolConfig) {
			t.Errorf("%s options spool = %+v, want the loaded spool config", name, o.spool)
		}
	}
	if deliveryOptions.spool != nil {
		t.Errorf("delivery options include WithSpool: %+v", deliveryOptions.spool)
	}
	for name, tt := range map[string]struct {
		got, want StreamRouting
	}{
		"sync":     {syncOptions.routing, StreamRoutingDaily},
		"async":    {asyncOptions.routing, StreamRoutingHourly},
		"delivery": {deliveryOptions.routing, StreamRoutingMonthly},
	} {
		if tt.got != tt.want {
			t.Errorf("%s stream routing = %q, want %q", name, tt.got, tt.want)
		}
	}
}

// 保存先の重複のように、loggerを組み合わせて初めて分かる不整合もLoadEnvConfigが返します。
func TestLoadEnvConfigRejectsLoggersSharingADirectory(t *testing.T) {
	base := t.TempDir()
	_, err := loadEnvConfigFromLinesForTest(t,
		"HETEROLOG_SYSTEM_LOG_DIR="+base,
		"HETEROLOG_LOG_TYPE=file",
		"HETEROLOG_ASYNC_LOG_TYPE=file",
		"HETEROLOG_LOG_DIR=/shared",
		"HETEROLOG_ASYNC_LOG_DIR=/shared",
	)
	var collision *appconfig.StorageDirectoryCollisionError
	if !errors.As(err, &collision) {
		t.Fatalf("LoadEnvConfig error = %v, want a StorageDirectoryCollisionError", err)
	}
}
