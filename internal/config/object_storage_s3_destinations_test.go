package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/uploader/compose"
	s3builder "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/uploader/s3"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	objectstoragevalidation "github.com/go-jappe/heterolog/pkg/objectstorage/validation"
)

func TestParseS3DestinationsInheritsOnlyOmittedFields(t *testing.T) {
	defaults := objectstorage.S3Config{
		Region: "ap-northeast-1", Endpoint: "https://storage.example.test", Bucket: "default",
		Prefix: "logs", AccessKeyID: "shared-key", SecretAccessKey: "shared-secret",
		SessionToken: "shared-token", ForcePathStyle: true, Timeout: 35 * time.Second,
	}
	configs, err := parseS3Destinations(`[
		{"bucket":"primary"},
		{"bucket":"backup","region":"us-east-1","endpoint":"","prefix":"", "access_key_id":"backup-key",
		 "secret_access_key":"backup-secret","session_token":"","force_path_style":false,"timeout_seconds":0}
	]`, defaults)
	require.NoError(t, err)
	require.Len(t, configs, 2)
	wantFirst := defaults
	wantFirst.Bucket = "primary"
	require.Equal(t, wantFirst, configs[0])
	require.Equal(t, objectstorage.S3Config{
		Region: "us-east-1", Bucket: "backup", AccessKeyID: "backup-key", SecretAccessKey: "backup-secret",
	}, configs[1])
	configs, err = parseS3Destinations(`[{"timeout_seconds":12}]`, defaults)
	require.NoError(t, err)
	require.Equal(t, 12*time.Second, configs[0].Timeout)
	for _, raw := range []string{"", "  "} {
		configs, err = parseS3Destinations(raw, defaults)
		require.NoError(t, err)
		require.Nil(t, configs)
	}
}

func TestParseS3DestinationsRejectsInvalidJSONWithoutExposingSecrets(t *testing.T) {
	for _, raw := range []string{
		`[]`, `null`, `{}`, `[null]`, `[1]`, `[{"bucket":"a"}] {}`,
		`[{"secret_access_key":"sensitive-value",}]`,
		`[{"sensitive-value":true}]`, `[{"bucket":true}]`,
		`[{"timeout_seconds":-1}]`, `[{"timeout_seconds":9223372037}]`, `[{"timeout_seconds":0.5}]`,
	} {
		_, err := parseS3Destinations(raw, objectstorage.S3Config{})
		require.ErrorIs(t, err, ErrInvalidS3Destinations)
		// 認証情報を表示しないことが要件なので、表示文に値が含まれないことを確認します。
		require.NotContains(t, err.Error(), "sensitive-value")
	}
}

// TestLoadConfigRejectsInvalidS3DestinationsWithoutExposingSecretsは、利用者へ返るLoadConfigの
// errorでも、*_S3_DESTINATIONSの一覧に含まれ得る認証情報を表示しないことを検証します。
// failure archiveとlog fileの一覧は別の読込経路で包むため、両方を確認します。
func TestLoadConfigRejectsInvalidS3DestinationsWithoutExposingSecrets(t *testing.T) {
	const secret = "s3-destinations-secret-value"
	for _, invalid := range []struct{ name, raw string }{
		{name: "syntax", raw: `[{"bucket":"archive","secret_access_key":"` + secret + `",}]`},
		{name: "unknown field", raw: `[{"bucket":"archive","` + secret + `":true}]`},
	} {
		for _, route := range []struct {
			name  string
			key   string
			setup []string
		}{
			{
				name: "failure archive",
				key:  "HETEROLOG_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_DESTINATIONS",
				setup: []string{
					"HETEROLOG_ENABLE_LOGGING=true",
					"HETEROLOG_LOG_TYPE=loki",
					"HETEROLOG_LOKI_DURABLE_TYPES=fileArchive",
					"HETEROLOG_ENABLE_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD=true",
					"HETEROLOG_LOKI_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE=s3",
				},
			},
			{
				name: "log file",
				key:  "HETEROLOG_LOG_FILE_OBJECT_STORAGE_S3_DESTINATIONS",
				setup: []string{
					"HETEROLOG_ENABLE_LOGGING=true",
					"HETEROLOG_LOG_TYPE=file",
					"HETEROLOG_ENABLE_LOG_FILE_OBJECT_STORAGE_UPLOAD=true",
					"HETEROLOG_LOG_FILE_OBJECT_STORAGE_UPLOAD_TYPE=s3",
				},
			},
		} {
			t.Run(route.name+"/"+invalid.name, func(t *testing.T) {
				lines := append(append([]string(nil), route.setup...), route.key+"='"+invalid.raw+"'")
				_, err := loadConfigErrorFromTempEnv(t, storagePathValidationEnv(t, lines...))
				require.ErrorIs(t, err, ErrInvalidS3Destinations)
				// 値を表示しないこと自体が要件なので、表示文と型付きerrorのValueなど、error treeの
				// どこにも認証情報が含まれないことを確認します。
				requireNoValueInErrorTree(t, err, secret)
			})
		}
	}
}

func TestLogFileEnvironmentBuildsAllS3Destinations(t *testing.T) {
	restoreProcessEnvAfterTest(t)
	clearProcessEnvForTest(t)
	setTestS3CommonEnvironment(t)
	for _, tc := range []struct {
		prefix string
		load   func() (objectstorage.LogFileUploadConfig, error)
	}{
		{"", fromProcessEnvironment(loadLogFileObjectStorageUploadConfig)},
		{"ASYNC_", fromProcessEnvironment(loadAsyncLogFileObjectStorageUploadConfig)},
		{"DELIVERY_", fromProcessEnvironment(loadDeliveryLogFileObjectStorageUploadConfig)},
	} {
		t.Run(tc.prefix+"log", func(t *testing.T) {
			prefix := tc.prefix + "LOG_FILE_"
			t.Setenv("HETEROLOG_ENABLE_"+prefix+"OBJECT_STORAGE_UPLOAD", "true")
			t.Setenv("HETEROLOG_"+prefix+"OBJECT_STORAGE_UPLOAD_TYPE", "s3")
			t.Setenv("HETEROLOG_"+prefix+"OBJECT_STORAGE_S3_DESTINATIONS", `[{"bucket":"primary"},{"bucket":"backup"}]`)
			t.Setenv("HETEROLOG_"+prefix+"OBJECT_STORAGE_S3_PREFIX", "normal")
			t.Setenv("HETEROLOG_"+prefix+"ERROR_OBJECT_STORAGE_S3_PREFIX", "errors")
			t.Setenv("HETEROLOG_"+prefix+"DEBUG_OBJECT_STORAGE_S3_DESTINATIONS", `[{"bucket":"debug","access_key_id":"own-key","secret_access_key":"own-secret"}]`)
			cfg, err := tc.load()
			require.NoError(t, err)
			require.NoError(t, objectstoragevalidation.ValidateLogFileUploadConfig("test", cfg, true, true))
			require.Len(t, cfg.Default.S3Destinations, 2)
			require.Len(t, cfg.Error.S3Destinations, 2)
			require.Len(t, cfg.Debug.S3Destinations, 1)
			require.Equal(t, "normal", cfg.Default.S3Destinations[0].Prefix)
			require.Equal(t, "errors", cfg.Error.S3Destinations[0].Prefix)
			require.Equal(t, "own-key", cfg.Debug.S3Destinations[0].AccessKeyID)
			assertConfiguredS3Destinations(t, cfg.Default, 2)
			assertConfiguredS3Destinations(t, cfg.Error, 2)
			assertConfiguredS3Destinations(t, cfg.Debug, 1)
			cfg.Error.S3Destinations[0].Bucket = "changed"
			require.Equal(t, "primary", cfg.Default.S3Destinations[0].Bucket)

			t.Setenv("HETEROLOG_ENABLE_"+prefix+"DEBUG_OBJECT_STORAGE_UPLOAD", "false")
			t.Setenv("HETEROLOG_"+prefix+"DEBUG_OBJECT_STORAGE_S3_DESTINATIONS", "malformed")
			_, err = tc.load()
			require.NoError(t, err)
			t.Setenv("HETEROLOG_ENABLE_"+prefix+"OBJECT_STORAGE_UPLOAD", "false")
			t.Setenv("HETEROLOG_"+prefix+"OBJECT_STORAGE_S3_DESTINATIONS", "malformed")
			_, err = tc.load()
			require.NoError(t, err)
		})
	}
}

func TestFailureArchiveEnvironmentBuildsAllS3Destinations(t *testing.T) {
	restoreProcessEnvAfterTest(t)
	clearProcessEnvForTest(t)
	setTestS3CommonEnvironment(t)
	for _, tc := range []struct {
		prefix string
		load   func() (objectstorage.UploadTargetConfig, error)
	}{
		{"OPENSEARCH", fromProcessEnvironment(loadOpenSearchFailureArchiveObjectStorageUploadConfig)},
		{"ASYNC_OPENSEARCH", fromProcessEnvironment(loadAsyncOpenSearchFailureArchiveObjectStorageUploadConfig)},
		{"DELIVERY_OPENSEARCH", fromProcessEnvironment(loadDeliveryOpenSearchFailureArchiveObjectStorageUploadConfig)},
		{"LOKI", fromProcessEnvironment(loadLokiFailureArchiveObjectStorageUploadConfig)},
		{"ASYNC_LOKI", fromProcessEnvironment(loadAsyncLokiFailureArchiveObjectStorageUploadConfig)},
		{"DELIVERY_LOKI", fromProcessEnvironment(loadDeliveryLokiFailureArchiveObjectStorageUploadConfig)},
		{"CLOUDWATCH", fromProcessEnvironment(loadCloudWatchLogsFailureArchiveObjectStorageUploadConfig)},
		{"ASYNC_CLOUDWATCH", fromProcessEnvironment(loadAsyncCloudWatchLogsFailureArchiveObjectStorageUploadConfig)},
		{"DELIVERY_CLOUDWATCH", fromProcessEnvironment(loadDeliveryCloudWatchLogsFailureArchiveObjectStorageUploadConfig)},
		{"CLICKHOUSE", fromProcessEnvironment(loadClickHouseFailureArchiveObjectStorageUploadConfig)},
		{"ASYNC_CLICKHOUSE", fromProcessEnvironment(loadAsyncClickHouseFailureArchiveObjectStorageUploadConfig)},
		{"DELIVERY_CLICKHOUSE", fromProcessEnvironment(loadDeliveryClickHouseFailureArchiveObjectStorageUploadConfig)},
	} {
		t.Run(tc.prefix, func(t *testing.T) {
			prefix := tc.prefix + "_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_"
			t.Setenv("HETEROLOG_ENABLE_"+prefix+"UPLOAD", "true")
			t.Setenv("HETEROLOG_"+prefix+"UPLOAD_TYPE", "s3")
			t.Setenv("HETEROLOG_"+prefix+"S3_DESTINATIONS", `[{"bucket":"primary"},{"bucket":"backup","timeout_seconds":9}]`)
			cfg, err := tc.load()
			require.NoError(t, err)
			assertConfiguredS3Destinations(t, cfg, 2)
			require.Equal(t, 9*time.Second, cfg.S3Destinations[1].Timeout)
			t.Setenv("HETEROLOG_"+prefix+"S3_DESTINATIONS", "malformed")
			_, err = tc.load()
			requireEnvironmentKeyError(t, err, "HETEROLOG_"+prefix+"S3_DESTINATIONS", ErrInvalidS3Destinations)
			t.Setenv("HETEROLOG_ENABLE_"+prefix+"UPLOAD", "false")
			_, err = tc.load()
			require.NoError(t, err)
		})
	}
}

func TestS3DestinationListValidationAndSingleFallback(t *testing.T) {
	base := objectstorage.S3Config{Region: "ap-northeast-1", Bucket: "one", AccessKeyID: "test", SecretAccessKey: "test"}
	equivalent := base
	equivalent.AccessKeyID, equivalent.SecretAccessKey = "other-key", "other-secret"
	equivalent.Prefix = "/"
	for _, tc := range []struct {
		name    string
		configs []objectstorage.S3Config
		valid   bool
	}{
		{"single shorthand", nil, true},
		{"explicit empty", []objectstorage.S3Config{}, false},
		{"one", []objectstorage.S3Config{base}, true},
		{"duplicate", []objectstorage.S3Config{base, base}, false},
		{"same destination with different credentials", []objectstorage.S3Config{base, equivalent}, false},
		{"invalid later entry", []objectstorage.S3Config{base, {}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := objectstorage.UploadTargetConfig{Enabled: true, Targets: []objectstorage.UploadTarget{"s3"}, S3: base, S3Destinations: tc.configs}
			validationErr := objectstoragevalidation.ValidateUploadTargetConfig("test", cfg)
			targetConfig := objectstorage.NewUploadTargetConfigAdapter(cfg)
			_, resolveErr := compose.DestinationIDsFromTargets(targetConfig, s3builder.DestinationIDResolvers())
			_, buildErr := compose.FromTargets(targetConfig, s3builder.Builders())
			for _, err := range []error{validationErr, resolveErr, buildErr} {
				if tc.valid {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
			}
		})
	}
}

func TestDotEnvMultipleS3DestinationsReachMappedLoggerConfig(t *testing.T) {
	for _, prefix := range []string{"", "ASYNC_", "DELIVERY_"} {
		t.Run(prefix+"logger", func(t *testing.T) {
			flags := []string{"HETEROLOG_ENABLE_LOGGING=false", "HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false", "HETEROLOG_ENABLE_DELIVERY_LOGGING=false"}
			switch prefix {
			case "":
				flags[0] = "HETEROLOG_ENABLE_LOGGING=true"
			case "ASYNC_":
				flags[1] = "HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true"
			case "DELIVERY_":
				flags[2] = "HETEROLOG_ENABLE_DELIVERY_LOGGING=true"
			}
			cfg := loadConfigFromTempEnv(t, append(flags,
				"HETEROLOG_"+prefix+"LOG_TYPE=file",
				"HETEROLOG_"+prefix+"LOG_FILE_LIFECYCLE_MODE=runner",
				"HETEROLOG_ENABLE_"+prefix+"LOG_FILE_OBJECT_STORAGE_UPLOAD=true",
				"HETEROLOG_"+prefix+"LOG_FILE_OBJECT_STORAGE_UPLOAD_TYPE=s3",
				"HETEROLOG_OBJECT_STORAGE_S3_ACCESS_KEY_ID=test-key",
				"HETEROLOG_OBJECT_STORAGE_S3_SECRET_ACCESS_KEY=test-secret",
				"HETEROLOG_"+prefix+`LOG_FILE_OBJECT_STORAGE_S3_DESTINATIONS='[{"bucket":"primary"},{"bucket":"backup","prefix":"copy"}]'`,
			))
			var upload objectstorage.LogFileUploadConfig
			switch prefix {
			case "":
				mapped, err := MapLoggerConfig(cfg)
				require.NoError(t, err)
				require.NotNil(t, mapped.File)
				upload = mapped.File.ObjectStorageUploadConfig
			case "ASYNC_":
				mapped, err := MapAsyncLoggerConfig(cfg)
				require.NoError(t, err)
				require.NotNil(t, mapped.File)
				upload = mapped.File.ObjectStorageUploadConfig
			case "DELIVERY_":
				mapped, err := MapDeliveryLoggerConfig(cfg)
				require.NoError(t, err)
				require.NotNil(t, mapped.File)
				upload = mapped.File.LifecycleRunnerConfig().ObjectStorageUploadConfig
			}
			assertConfiguredS3Destinations(t, upload.Default, 2)
			require.Equal(t, "primary", upload.Default.S3Destinations[0].Bucket)
			require.Equal(t, "backup", upload.Default.S3Destinations[1].Bucket)
		})
	}
}

func setTestS3CommonEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("HETEROLOG_OBJECT_STORAGE_S3_REGION", "ap-northeast-1")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_S3_ACCESS_KEY_ID", "test-key")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_S3_SECRET_ACCESS_KEY", "test-secret")
}

func assertConfiguredS3Destinations(t *testing.T, cfg objectstorage.UploadTargetConfig, count int) {
	t.Helper()
	require.NoError(t, objectstoragevalidation.ValidateUploadTargetConfig("test", cfg))
	targetConfig := objectstorage.NewUploadTargetConfigAdapter(cfg)
	ids, err := compose.DestinationIDsFromTargets(targetConfig, s3builder.DestinationIDResolvers())
	require.NoError(t, err)
	require.Len(t, ids, count)
	remote, err := compose.FromTargets(targetConfig, s3builder.Builders())
	require.NoError(t, err)
	require.Equal(t, ids, remote.(uploader.DestinationUploader).DestinationIDs())
}
