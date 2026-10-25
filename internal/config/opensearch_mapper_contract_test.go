package config

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	configopensearch "github.com/go-jappe/heterolog/internal/config/opensearch"
	configruntime "github.com/go-jappe/heterolog/internal/config/runtime"
	opensearchlogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/opensearch"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/opensearch"
)

func TestOpenSearchClientMappersPreserveRoleConfigAndAuth(t *testing.T) {
	tests := []struct {
		name        string
		authType    opensearch.AuthType
		wantAuth    func(string) any
		missingAuth bool
		wantErr     error
	}{
		{
			name: "basic", authType: opensearch.AuthTypeBasic,
			wantAuth: func(role string) any {
				return opensearch.BasicAuthConfig{Username: role + "-user", Password: role + "-password"}
			},
		},
		{
			name: "apikey", authType: opensearch.AuthTypeAPIKey,
			wantAuth: func(role string) any {
				return opensearch.APIKeyAuthConfig{ID: role + "-id", Secret: role + "-secret"}
			},
		},
		{
			name: "awsiam", authType: opensearch.AuthTypeAWSIAM,
			wantAuth: func(role string) any {
				return opensearch.AWSIAMAuthConfig{
					Region: role + "-region", AccessKeyID: role + "-access-key",
					SecretAccessKey: role + "-secret-key", SessionToken: role + "-session-token",
				}
			},
		},
		{
			name: "saml", authType: opensearch.AuthTypeSAML,
			wantAuth: func(role string) any {
				return opensearch.SAMLAuthConfig{Token: role + "-saml-token"}
			},
		},
		{
			name: "oidc", authType: opensearch.AuthTypeOIDC,
			wantAuth: func(role string) any {
				return opensearch.OIDCAuthConfig{Token: role + "-oidc-token"}
			},
		},
		{
			name: "certificate", authType: opensearch.AuthTypeCertificate,
			wantAuth: func(role string) any {
				return opensearch.CertificateAuthConfig{
					CertFile: role + "-cert.pem", KeyFile: role + "-key.pem", CAFile: role + "-ca.pem",
				}
			},
		},
		{name: "none", authType: opensearch.AuthTypeNone},
		{name: "unsupported", authType: "unsupported", wantErr: opensearch.ErrUnsupportedAuthType},
		{name: "missing type", wantErr: opensearch.ErrUnsupportedAuthType},
	}
	for _, tt := range tests {
		if tt.wantAuth != nil {
			tt.name += " missing config"
			tt.wantAuth = nil
			tt.missingAuth = true
			tt.wantErr = opensearch.ErrInvalidAuthConfigType
			tests = append(tests, tt)
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newClient := func(role string, timeout time.Duration, skipVerify, compress bool) configopensearch.OpenSearchClientConfig {
				cfg := configopensearch.OpenSearchClientConfig{
					Endpoint: "https://" + role + ".example:9200", Timeout: timeout,
					SkipVerifySSL: skipVerify, CompressRequest: compress,
					Auth: configopensearch.OpenSearchAuthConfig{
						Type: tt.authType,
						Basic: &configopensearch.BasicAuthConfig{
							Username: role + "-user", Password: role + "-password",
						},
						APIKey: &configopensearch.APIKeyAuthConfig{
							ID: role + "-id", Secret: role + "-secret",
						},
						AWSIAM: &configopensearch.AWSIAMAuthConfig{
							Region: role + "-region", AccessKeyID: role + "-access-key",
							SecretAccessKey: role + "-secret-key", SessionToken: role + "-session-token",
						},
						SAML: &configopensearch.SAMLAuthConfig{Token: role + "-saml-token"},
						OIDC: &configopensearch.OIDCAuthConfig{Token: role + "-oidc-token"},
						Certificate: &configopensearch.CertificateAuthConfig{
							CertFile: role + "-cert.pem", KeyFile: role + "-key.pem", CAFile: role + "-ca.pem",
						},
					},
				}
				if tt.missingAuth {
					cfg.Auth = configopensearch.OpenSearchAuthConfig{Type: tt.authType}
				}
				return cfg
			}
			cfg := AppConfig{
				OpenSearchClient:         newClient("sync", 11*time.Second, false, true),
				AsyncOpenSearchClient:    newClient("async", 22*time.Second, true, false),
				DeliveryOpenSearchClient: newClient("delivery", 33*time.Second, false, false),
			}
			roles := []struct {
				name       string
				mapClient  func(*AppConfig) *opensearch.Config
				timeout    time.Duration
				skipVerify bool
				compress   bool
			}{
				{"sync", NewOpenSearchClientConfig, 11 * time.Second, false, true},
				{"async", NewAsyncOpenSearchClientConfig, 22 * time.Second, true, false},
				{"delivery", NewDeliveryOpenSearchClientConfig, 33 * time.Second, false, false},
			}
			for _, role := range roles {
				t.Run(role.name, func(t *testing.T) {
					want := opensearch.Config{
						Endpoint: "https://" + role.name + ".example:9200", Timeout: role.timeout,
						SkipVerifySSL: role.skipVerify, CompressRequest: role.compress,
						AuthConfig: opensearch.AuthConfig{Type: tt.authType},
					}
					if tt.wantAuth != nil {
						want.AuthConfig.Config = tt.wantAuth(role.name)
					}
					got := role.mapClient(&cfg)
					if got == nil || *got != want {
						t.Fatalf("mapped client = %+v, want %+v", got, want)
					}
					// 不正な認証方式や設定の値は、上のmapped clientの比較で確認済みです。
					err := opensearch.ValidateConfig(*got)
					if tt.wantErr == nil {
						if err != nil {
							t.Fatalf("ValidateConfig() error = %v", err)
						}
					} else if !errors.Is(err, tt.wantErr) {
						t.Fatalf("ValidateConfig() error = %v, want %v", err, tt.wantErr)
					}
				})
			}
		})
	}
}

// TestLoadConfigMapsOpenSearchFailureSettingsFromMatchingEnvContract は、HETEROLOG_ENV_PATHで指定した
// 実ファイルの.envからLoadConfig、logger DTOへのmapperまでを一続きで検証します。
// sync/asyncに意図的に異なる値を与え、別LoggerContractKindの値を参照しても偶然passしないようにします。
func TestLoadConfigMapsOpenSearchFailureSettingsFromMatchingEnvContract(t *testing.T) {
	root := t.TempDir()
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_APP_NAME=opensearch-env-contract-test",
		"HETEROLOG_TARGET=opensearch-env-contract-target",
		"HETEROLOG_SYSTEM_LOG_DIR=" + filepath.Join(root, "log"),
		"HETEROLOG_SYSTEM_LIB_DIR=" + filepath.Join(root, "lib"),
		"HETEROLOG_APP_STATE_DIR=" + filepath.Join(root, "state"),
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_LOG_TYPE=opensearch",
		"HETEROLOG_ASYNC_LOG_TYPE=opensearch",
		"HETEROLOG_FILE_LOGGER_ERROR_HANDLER=slack",
		"HETEROLOG_FILE_LOGGER_WARN_HANDLER=slack",
		"HETEROLOG_FILE_LOGGER_INFO_HANDLER=slack",
		"HETEROLOG_FILE_LOGGER_DEBUG_HANDLER=slack",
		"HETEROLOG_OPENSEARCH_LOGGER_ERROR_HANDLER=stdout,slack",
		"HETEROLOG_OPENSEARCH_LOGGER_WARN_HANDLER=stdout",
		"HETEROLOG_OPENSEARCH_LOGGER_INFO_HANDLER=stdout",
		"HETEROLOG_OPENSEARCH_LOGGER_DEBUG_HANDLER=stdout",
		"HETEROLOG_OPENSEARCH_BUFFER_FLUSH_BYTES=5001",
		"HETEROLOG_MAX_OPENSEARCH_BULK_REQUEST_BYTES=15001",
		"HETEROLOG_ASYNC_OPENSEARCH_LOGGER_ERROR_HANDLER=file,stdout",
		"HETEROLOG_ASYNC_OPENSEARCH_LOGGER_WARN_HANDLER=file",
		"HETEROLOG_ASYNC_OPENSEARCH_LOGGER_INFO_HANDLER=file",
		"HETEROLOG_ASYNC_OPENSEARCH_LOGGER_DEBUG_HANDLER=file",
		"HETEROLOG_ASYNC_OPENSEARCH_BUFFER_FLUSH_BYTES=6001",
		"HETEROLOG_MAX_ASYNC_OPENSEARCH_BULK_REQUEST_BYTES=16001",
		"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_OPENSEARCH_DURABLE_TYPES=spool,fileArchive",
		"HETEROLOG_OPENSEARCH_FAILURE_ARCHIVE_DIR=sync-archive",
		"HETEROLOG_OPENSEARCH_FAILURE_SPOOL_DIR=sync-spool",
		"HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY=drop",
		"HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION=stdout",
		"HETEROLOG_MAX_OPENSEARCH_SPOOL_SCANNED_CACHE_USAGE_COUNT=32",
		"HETEROLOG_OPENSEARCH_SPOOL_CACHE_LIFETIME_SECOND=33",
		"HETEROLOG_OPENSEARCH_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS=31",
		"HETEROLOG_USE_MEMORY_PRESSURE_OPENSEARCH_SPOOL_OVERLOAD=false",
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_TYPES=spool,fileArchive",
		"HETEROLOG_ASYNC_OPENSEARCH_FAILURE_ARCHIVE_DIR=async-archive",
		"HETEROLOG_ASYNC_OPENSEARCH_FAILURE_SPOOL_DIR=async-spool",
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY=degrade",
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION=s3",
		"HETEROLOG_MAX_ASYNC_OPENSEARCH_SPOOL_SCANNED_CACHE_USAGE_COUNT=42",
		"HETEROLOG_ASYNC_OPENSEARCH_SPOOL_CACHE_LIFETIME_SECOND=43",
		"HETEROLOG_ASYNC_OPENSEARCH_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS=41",
		"HETEROLOG_USE_MEMORY_PRESSURE_ASYNC_OPENSEARCH_SPOOL_OVERLOAD=true",
	})

	syncLoaded := cfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig
	asyncLoaded := cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig
	assertOpenSearchMapperValue(t, "loaded sync buffer flush bytes", cfg.Logger.OpenSearchConfig.BufferFlushBytes, 5001)
	assertOpenSearchMapperValue(t, "loaded sync max bulk request bytes", cfg.Logger.OpenSearchConfig.MaxBulkRequestBytes, 15001)
	assertOpenSearchMapperValue(t, "loaded async buffer flush bytes", cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.BufferFlushBytes, 6001)
	assertOpenSearchMapperValue(t, "loaded async max bulk request bytes", cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.MaxBulkRequestBytes, 16001)
	if got, want := syncLoaded.Fsstore.SpoolConfig.OverloadPolicyConfig.MaxCallsSinceScan, uint64(32); got != want {
		t.Fatalf("loaded sync spool MaxCallsSinceScan = %d, want %d", got, want)
	}
	if got, want := syncLoaded.Fsstore.SpoolConfig.OverloadPolicyConfig.StatsScanInterval, 33*time.Second; got != want {
		t.Fatalf("loaded sync spool StatsScanInterval = %s, want %s", got, want)
	}
	if got, want := asyncLoaded.Fsstore.SpoolConfig.OverloadPolicyConfig.MaxCallsSinceScan, uint64(42); got != want {
		t.Fatalf("loaded async spool MaxCallsSinceScan = %d, want %d", got, want)
	}
	if got, want := asyncLoaded.Fsstore.SpoolConfig.OverloadPolicyConfig.StatsScanInterval, 43*time.Second; got != want {
		t.Fatalf("loaded async spool StatsScanInterval = %s, want %s", got, want)
	}
	assertOpenSearchFailureContract(
		t,
		"loaded sync",
		openSearchFailureContractValues{
			archiveDir:                      syncLoaded.Fsstore.FileArchiveConfig.Dir,
			spoolDir:                        syncLoaded.Fsstore.SpoolConfig.Dir,
			overloadPolicy:                  syncLoaded.Fsstore.FileArchiveConfig.OverloadPolicyConfig.OverloadPolicy,
			spillDestination:                syncLoaded.Fsstore.FileArchiveConfig.OverloadPolicyConfig.SpillDestination,
			spoolActiveBacklogWarnAfterDays: syncLoaded.Fsstore.SpoolConfig.OverloadPolicyConfig.ActiveBacklogWarnAfterDays,
			useMemoryPressure:               syncLoaded.Fsstore.SpoolConfig.OverloadPolicyConfig.UseMemoryPressureForOverload,
		},
		openSearchFailureContractValues{
			archiveDir:                      "sync-archive",
			spoolDir:                        "sync-spool",
			overloadPolicy:                  pkgdurable.OverloadPolicyDrop,
			spillDestination:                pkgdurable.SpillDestinationStdout,
			spoolActiveBacklogWarnAfterDays: 31,
			useMemoryPressure:               false,
		},
	)
	assertOpenSearchStringSlice(t, "loaded sync error handlers", cfg.Logger.OpenSearchConfig.HandlerConfig.OnErrorConfig, []string{"stdout", "slack"})
	assertOpenSearchStringSlice(t, "loaded sync debug handlers", cfg.Logger.OpenSearchConfig.HandlerConfig.OnDebugConfig, []string{"stdout"})
	assertOpenSearchStringSlice(t, "loaded async error handlers", cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.HandlerConfig.OnErrorConfig, []string{"file", "stdout"})
	assertOpenSearchStringSlice(t, "loaded async debug handlers", cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.HandlerConfig.OnDebugConfig, []string{"file"})
	assertOpenSearchFailureContract(
		t,
		"loaded async",
		openSearchFailureContractValues{
			archiveDir:                      asyncLoaded.Fsstore.FileArchiveConfig.Dir,
			spoolDir:                        asyncLoaded.Fsstore.SpoolConfig.Dir,
			overloadPolicy:                  asyncLoaded.Fsstore.FileArchiveConfig.OverloadPolicyConfig.OverloadPolicy,
			spillDestination:                asyncLoaded.Fsstore.FileArchiveConfig.OverloadPolicyConfig.SpillDestination,
			spoolActiveBacklogWarnAfterDays: asyncLoaded.Fsstore.SpoolConfig.OverloadPolicyConfig.ActiveBacklogWarnAfterDays,
			useMemoryPressure:               asyncLoaded.Fsstore.SpoolConfig.OverloadPolicyConfig.UseMemoryPressureForOverload,
		},
		openSearchFailureContractValues{
			archiveDir:                      "async-archive",
			spoolDir:                        "async-spool",
			overloadPolicy:                  pkgdurable.OverloadPolicyDegrade,
			spillDestination:                pkgdurable.SpillDestinationS3,
			spoolActiveBacklogWarnAfterDays: 41,
			useMemoryPressure:               true,
		},
	)

	syncMapped := ToLoggerConfig(cfg)
	if syncMapped.OpenSearch == nil {
		t.Fatal("ToLoggerConfig() OpenSearch = nil")
	}
	asyncMapped := ToAsyncBestEffortMultiLoggerConfig(cfg)
	if asyncMapped.OpenSearch == nil {
		t.Fatal("ToAsyncBestEffortMultiLoggerConfig() OpenSearch = nil")
	}
	assertOpenSearchMapperValue(t, "mapped sync buffer flush bytes", syncMapped.OpenSearch.BufferFlushBytes, 5001)
	assertOpenSearchMapperValue(t, "mapped sync max bulk request bytes", syncMapped.OpenSearch.MaxBulkRequestBytes, 15001)
	assertOpenSearchMapperValue(t, "mapped async buffer flush bytes", asyncMapped.OpenSearch.BufferFlushBytes, 6001)
	assertOpenSearchMapperValue(t, "mapped async max bulk request bytes", asyncMapped.OpenSearch.MaxBulkRequestBytes, 16001)

	syncLoggerAdapter := opensearchlogger.NewOpenSearchLoggerConfigAdapter(
		syncMapped.OpenSearch.OpenSearchLoggerBaseConfig,
	)
	assertOpenSearchMapperValue(t, "sync adapter buffer flush bytes", syncLoggerAdapter.BufferFlushBytes(), 5001)
	assertOpenSearchMapperValue(t, "sync adapter max bulk request bytes", syncLoggerAdapter.MaxBulkRequestBytes(), 15001)

	syncStore := syncMapped.OpenSearch.FailureStoreConfig.DurableStoreConfig
	assertOpenSearchFailureContract(
		t,
		"mapped sync",
		openSearchFailureContractValues{
			archiveDir:                      syncStore.FileArchiveConfig.Dir,
			spoolDir:                        syncStore.SpoolConfig.Dir,
			overloadPolicy:                  syncStore.FileArchiveConfig.OverloadPolicyConfig.OverloadPolicy,
			spillDestination:                syncStore.FileArchiveConfig.OverloadPolicyConfig.SpillDestination,
			spoolActiveBacklogWarnAfterDays: syncStore.SpoolConfig.OverloadPolicyConfig.ActiveBacklogWarnAfterDays,
			useMemoryPressure:               syncStore.SpoolConfig.OverloadPolicyConfig.UseMemoryPressureForOverload,
		},
		openSearchFailureContractValues{
			archiveDir:                      "sync-archive",
			spoolDir:                        "sync-spool",
			overloadPolicy:                  pkgdurable.OverloadPolicyDrop,
			spillDestination:                pkgdurable.SpillDestinationStdout,
			spoolActiveBacklogWarnAfterDays: 31,
			useMemoryPressure:               false,
		},
	)
	asyncStore := asyncMapped.OpenSearch.FailureStoreConfig.DurableStoreConfig
	if got, want := syncStore.SpoolConfig.OverloadPolicyConfig.MaxCallsSinceScan, uint64(32); got != want {
		t.Fatalf("mapped sync spool MaxCallsSinceScan = %d, want %d", got, want)
	}
	if got, want := syncStore.SpoolConfig.OverloadPolicyConfig.StatsScanInterval, 33*time.Second; got != want {
		t.Fatalf("mapped sync spool StatsScanInterval = %s, want %s", got, want)
	}
	if got, want := asyncStore.SpoolConfig.OverloadPolicyConfig.MaxCallsSinceScan, uint64(42); got != want {
		t.Fatalf("mapped async spool MaxCallsSinceScan = %d, want %d", got, want)
	}
	if got, want := asyncStore.SpoolConfig.OverloadPolicyConfig.StatsScanInterval, 43*time.Second; got != want {
		t.Fatalf("mapped async spool StatsScanInterval = %s, want %s", got, want)
	}
	assertOpenSearchFailureContract(
		t,
		"mapped async",
		openSearchFailureContractValues{
			archiveDir:                      asyncStore.FileArchiveConfig.Dir,
			spoolDir:                        asyncStore.SpoolConfig.Dir,
			overloadPolicy:                  asyncStore.FileArchiveConfig.OverloadPolicyConfig.OverloadPolicy,
			spillDestination:                asyncStore.FileArchiveConfig.OverloadPolicyConfig.SpillDestination,
			spoolActiveBacklogWarnAfterDays: asyncStore.SpoolConfig.OverloadPolicyConfig.ActiveBacklogWarnAfterDays,
			useMemoryPressure:               asyncStore.SpoolConfig.OverloadPolicyConfig.UseMemoryPressureForOverload,
		},
		openSearchFailureContractValues{
			archiveDir:                      "async-archive",
			spoolDir:                        "async-spool",
			overloadPolicy:                  pkgdurable.OverloadPolicyDegrade,
			spillDestination:                pkgdurable.SpillDestinationS3,
			spoolActiveBacklogWarnAfterDays: 41,
			useMemoryPressure:               true,
		},
	)
	assertOpenSearchStringSlice(t, "mapped sync error handlers", syncMapped.OpenSearch.HandlerConfig.OnErrorConfig, []string{"stdout", "slack"})
	assertOpenSearchStringSlice(t, "mapped sync debug handlers", syncMapped.OpenSearch.HandlerConfig.OnDebugConfig, []string{"stdout"})
	assertOpenSearchStringSlice(t, "mapped async error handlers", asyncMapped.OpenSearch.HandlerConfig.OnErrorConfig, []string{"file", "stdout"})
	assertOpenSearchStringSlice(t, "mapped async debug handlers", asyncMapped.OpenSearch.HandlerConfig.OnDebugConfig, []string{"file"})
}

func TestOpenSearchMappersKeepSyncAndAsyncContractsSeparated(t *testing.T) {
	var cfg AppConfig
	cfg.AppConfig.AppName = "mapper-contract-test"
	cfg.AppConfig.AppVersion = "test-version"
	cfg.RuntimeConfig.Target = "mapper-target"
	cfg.RuntimeEnv.CoreInfo.HostName = "mapper-host"
	cfg.RuntimeEnv.CoreInfo.OSType = "linux"
	cfg.RuntimeEnv.CoreInfo.Environment = "test"
	cfg.RuntimeEnv.CoreInfo.RuntimeType = "unit-test"
	cfg.RuntimeEnv.CoreInfo.RuntimeInstanceID = "mapper-instance"
	cfg.RuntimeEnv.Cloud = &configruntime.CloudInfo{Provider: "test-cloud", Region: "test-region"}
	cfg.FsstoreConfig.AppStatePath = "/state/mapper-target"
	cfg.FsstoreConfig.SpoolPath = "/spool"
	cfg.FsstoreConfig.FileNameFormat = "failure-%d.jsonl"
	cfg.FsstoreConfig.FileDateFormat = "20060102"
	cfg.FsstoreConfig.MinAvailableDiskSpace = 99

	syncConfig := &cfg.Logger.OpenSearchConfig
	syncConfig.ClockConfig.IsLocalTimeEnabled = true
	syncConfig.ClockConfig.TimeZone = "sync-zone"
	syncConfig.PostWarnThresholdDuration = 11 * time.Millisecond
	syncConfig.PostErrorThresholdDuration = 12 * time.Millisecond
	syncConfig.IndexLatencyEnabled = false
	syncConfig.MaxRetryAttempts = 13
	syncConfig.RetryWaitTime = 14 * time.Millisecond
	syncConfig.LogBodyConfig.ErrorTraceDepth = 15
	syncConfig.LogBodyConfig.ErrorTraceDepthDebugMode = 16
	syncFailure := &syncConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig
	syncFailure.Fsstore.FileArchiveConfig.Dir = "sync-archive"
	syncFailure.Fsstore.FileArchiveConfig.ObjectStorageUploadConfig.S3.Bucket = "sync-bucket"
	syncFailure.Fsstore.FileArchiveConfig.OverloadPolicyConfig.OverloadPolicy = pkgdurable.OverloadPolicyDrop
	syncFailure.Fsstore.FileArchiveConfig.OverloadPolicyConfig.SpillDestination = pkgdurable.SpillDestinationStdout
	syncFailure.Fsstore.SpoolConfig.Dir = "sync-spool"
	syncFailure.Fsstore.SpoolConfig.OverloadPolicyConfig.UseMemoryPressureForOverload = false

	asyncConfig := &cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig
	asyncConfig.ClockConfig.IsLocalTimeEnabled = false
	asyncConfig.ClockConfig.TimeZone = "async-zone"
	asyncConfig.PostWarnThresholdDuration = 21 * time.Millisecond
	asyncConfig.PostErrorThresholdDuration = 22 * time.Millisecond
	asyncConfig.IndexLatencyEnabled = true
	asyncConfig.MaxRetryAttempts = 23
	asyncConfig.RetryWaitTime = 24 * time.Millisecond
	asyncConfig.LogBodyConfig.ErrorTraceDepth = 25
	asyncConfig.LogBodyConfig.ErrorTraceDepthDebugMode = 26
	asyncFailure := &asyncConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig
	asyncFailure.Fsstore.FileArchiveConfig.Dir = "async-archive"
	asyncFailure.Fsstore.FileArchiveConfig.ObjectStorageUploadConfig.S3.Bucket = "async-bucket"
	asyncFailure.Fsstore.FileArchiveConfig.OverloadPolicyConfig.OverloadPolicy = pkgdurable.OverloadPolicyDegrade
	asyncFailure.Fsstore.FileArchiveConfig.OverloadPolicyConfig.SpillDestination = pkgdurable.SpillDestinationS3
	asyncFailure.Fsstore.SpoolConfig.Dir = "async-spool"
	asyncFailure.Fsstore.SpoolConfig.OverloadPolicyConfig.UseMemoryPressureForOverload = true

	syncMapped := NewOpenSearchFlushManagerConfig(&cfg)
	asyncMapped := NewAsyncOpenSearchFlushManagerConfig(&cfg)
	asyncLoggerMapped := NewAsyncOpenSearchLoggerConfig(&cfg)

	assertOpenSearchMapperValue(t, "sync post warn threshold", syncMapped.PostWarnThresholdDuration, 11*time.Millisecond)
	assertOpenSearchMapperValue(t, "sync archive overload policy", syncMapped.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.OverloadPolicyConfig.OverloadPolicy, pkgdurable.OverloadPolicyDrop)
	assertOpenSearchMapperValue(t, "sync archive spill destination", syncMapped.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.OverloadPolicyConfig.SpillDestination, pkgdurable.SpillDestinationStdout)
	assertOpenSearchMapperValue(t, "sync archive directory", syncMapped.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.Dir, "sync-archive")
	assertOpenSearchMapperValue(t, "sync archive object storage bucket", syncMapped.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig.S3.Bucket, "sync-bucket")

	assertOpenSearchMapperValue(t, "async post warn threshold", asyncMapped.PostWarnThresholdDuration, 21*time.Millisecond)
	assertOpenSearchMapperValue(t, "async post error threshold", asyncMapped.PostErrorThresholdDuration, 22*time.Millisecond)
	assertOpenSearchMapperValue(t, "async index latency enabled", asyncMapped.IndexLatencyEnabled, true)
	assertOpenSearchMapperValue(t, "async max retry attempts", asyncMapped.MaxRetryAttempts, 23)
	assertOpenSearchMapperValue(t, "async retry wait time", asyncMapped.RetryWaitTime, 24*time.Millisecond)
	assertOpenSearchMapperValue(t, "async archive trace depth", asyncMapped.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ErrorTraceDepth, 25)
	assertOpenSearchMapperValue(t, "async archive debug trace depth", asyncMapped.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ErrorTraceDepthDebugMode, 26)
	assertOpenSearchMapperValue(t, "async archive overload policy", asyncMapped.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.OverloadPolicyConfig.OverloadPolicy, pkgdurable.OverloadPolicyDegrade)
	assertOpenSearchMapperValue(t, "async archive spill destination", asyncMapped.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.OverloadPolicyConfig.SpillDestination, pkgdurable.SpillDestinationS3)
	assertOpenSearchMapperValue(t, "async archive directory", asyncMapped.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.Dir, "async-archive")
	assertOpenSearchMapperValue(t, "async archive object storage bucket", asyncMapped.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig.S3.Bucket, "async-bucket")
	assertOpenSearchMapperValue(t, "async spool directory", asyncMapped.FailureStoreConfig.DurableStoreConfig.SpoolConfig.Dir, "async-spool")
	assertOpenSearchMapperValue(t, "async spool memory pressure", asyncMapped.FailureStoreConfig.DurableStoreConfig.SpoolConfig.OverloadPolicyConfig.UseMemoryPressureForOverload, true)
	assertOpenSearchMapperValue(t, "async resource service name", asyncMapped.Resource.ServiceName, "mapper-contract-test")
	assertOpenSearchMapperValue(t, "async resource host name", asyncMapped.Resource.HostName, "mapper-host")
	if asyncMapped.Resource.Cloud == nil {
		t.Fatal("async resource cloud = nil")
	}
	assertOpenSearchMapperValue(t, "async resource cloud provider", asyncMapped.Resource.Cloud.Provider, "test-cloud")
	assertOpenSearchMapperValue(t, "async clock local time", asyncLoggerMapped.TimeConfig.IsLocalTimeEnabled, false)
	assertOpenSearchMapperValue(t, "async clock time zone", asyncLoggerMapped.TimeConfig.TimeZone, "async-zone")
}

func assertOpenSearchMapperValue[T comparable](t *testing.T, name string, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

func assertOpenSearchStringSlice(t *testing.T, name string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s = %v, want %v", name, got, want)
		}
	}
}

type openSearchFailureContractValues struct {
	archiveDir                      string
	spoolDir                        string
	overloadPolicy                  pkgdurable.OverloadPolicy
	spillDestination                pkgdurable.SpillDestination
	spoolActiveBacklogWarnAfterDays int
	useMemoryPressure               bool
}

func assertOpenSearchFailureContract(
	t *testing.T,
	contractLabel string,
	got, want openSearchFailureContractValues,
) {
	t.Helper()
	assertOpenSearchMapperValue(t, contractLabel+" archive directory", got.archiveDir, want.archiveDir)
	assertOpenSearchMapperValue(t, contractLabel+" spool directory", got.spoolDir, want.spoolDir)
	assertOpenSearchMapperValue(t, contractLabel+" archive overload policy", got.overloadPolicy, want.overloadPolicy)
	assertOpenSearchMapperValue(t, contractLabel+" archive spill destination", got.spillDestination, want.spillDestination)
	assertOpenSearchMapperValue(t, contractLabel+" spool active backlog warn after days", got.spoolActiveBacklogWarnAfterDays, want.spoolActiveBacklogWarnAfterDays)
	assertOpenSearchMapperValue(t, contractLabel+" spool memory pressure", got.useMemoryPressure, want.useMemoryPressure)
}
