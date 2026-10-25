package config

import (
	"reflect"
	"testing"

	configfailure "github.com/go-jappe/heterolog/internal/config/failure"
	"github.com/go-jappe/heterolog/internal/config/internalerror"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestFailureArchiveHandlerRoutingEnvKeys(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		load   func() (configfailure.FailureHandlerConfig, error)
	}{
		{name: "opensearch", prefix: "OPENSEARCH", load: fromProcessEnvironment(loadOpenSearchFailureHandlerConfig)},
		{name: "async opensearch", prefix: "ASYNC_OPENSEARCH", load: fromProcessEnvironment(loadAsyncOpenSearchFailureHandlerConfig)},
		{name: "loki", prefix: "LOKI", load: fromProcessEnvironment(loadLokiFailureHandlerConfig)},
		{name: "async loki", prefix: "ASYNC_LOKI", load: fromProcessEnvironment(loadAsyncLokiFailureHandlerConfig)},
		{name: "cloudwatch", prefix: "CLOUDWATCH", load: fromProcessEnvironment(loadCloudWatchLogsFailureHandlerConfig)},
		{name: "async cloudwatch", prefix: "ASYNC_CLOUDWATCH", load: fromProcessEnvironment(loadAsyncCloudWatchLogsFailureHandlerConfig)},
		{name: "clickhouse", prefix: "CLICKHOUSE", load: fromProcessEnvironment(loadClickHouseFailureHandlerConfig)},
		{name: "async clickhouse", prefix: "ASYNC_CLICKHOUSE", load: fromProcessEnvironment(loadAsyncClickHouseFailureHandlerConfig)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HETEROLOG_"+tt.prefix+"_DURABLE_TYPES", "fileArchive")
			t.Setenv("HETEROLOG_"+tt.prefix+"_DURABLE_FILE_ERROR_HANDLER", "stdout,slack")
			t.Setenv("HETEROLOG_"+tt.prefix+"_DURABLE_FILE_WARN_HANDLER", "file")
			t.Setenv("HETEROLOG_"+tt.prefix+"_DURABLE_FILE_INFO_HANDLER", "stdout")
			t.Setenv("HETEROLOG_"+tt.prefix+"_DURABLE_FILE_DEBUG_HANDLER", "slack,file")

			loaded, err := tt.load()
			if err != nil {
				t.Fatalf("load failure handler config: %v", err)
			}
			got := loaded.Fsstore.FileArchiveConfig.HandlerConfig
			want := internalerror.RoutingConfig{
				OnErrorConfig: []string{"stdout", "slack"},
				OnWarnConfig:  []string{"file"},
				OnInfoConfig:  []string{"stdout"},
				OnDebugConfig: []string{"slack", "file"},
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("handler routing = %#v, want %#v", got, want)
			}
		})
	}
}

func TestFailureArchiveHandlerRoutingMapper(t *testing.T) {
	fh := configfailure.FailureHandlerConfig{
		Fsstore: configfailure.FailureFileSystemStoreConfig{
			FileArchiveConfig: configfailure.FileArchiveConfig{
				HandlerConfig: internalerror.RoutingConfig{
					OnErrorConfig: []string{"stdout", "slack"},
					OnWarnConfig:  []string{"file"},
					OnInfoConfig:  []string{"stdout"},
					OnDebugConfig: []string{"slack"},
				},
			},
		},
	}

	got := newFailureStoreConfig(
		&AppConfig{},
		checkpointSourceLoggerOpenSearch,
		pkglogger.LoggerContractSync,
		fh,
		logconfig.LogBodyConfig{},
	).DurableStoreConfig.FileArchiveConfig.HandlerConfig
	if !reflect.DeepEqual(got.OnErrorConfig, []string{"stdout", "slack"}) ||
		!reflect.DeepEqual(got.OnWarnConfig, []string{"file"}) ||
		!reflect.DeepEqual(got.OnInfoConfig, []string{"stdout"}) ||
		!reflect.DeepEqual(got.OnDebugConfig, []string{"slack"}) {
		t.Fatalf("mapped handler routing = %#v", got)
	}
}
