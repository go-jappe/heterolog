package logrecovery

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestRecoverySpecSharesAllServiceRuntimesAcrossBuilds(t *testing.T) {
	cfg := objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(objectstorage.CompressedLogFileUploadSpoolConfig{
		SpoolPath: t.TempDir(), Target: "service", LoggerContract: pkglogger.LoggerContractSync.String(),
		RunnerInterval: time.Hour, LifecycleInterval: time.Hour, CircuitBreakerOpenCooldown: time.Minute,
	})
	services := []objectstorage.UploadTarget{objectstorage.UploadTargetS3, "another-backend"}
	configs := compressedlog.NewConfigAdapters(cfg, services)
	owner := compressedlog.NewRootRuntimeBuilder()
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	var received []compressedlog.RootRuntimes
	buildFailure := errors.New("test lifecycle construction failure")
	spec := recoverySpec{spoolConfigs: configs,
		newLifecycles: func(runtimes compressedlog.RootRuntimes, _ logfilelifecycle.RunReporter, _ ...logfilelifecycle.RecoveryOption) ([]*logfilelifecycle.DirectoryLifecycle, error) {
			require.Len(t, runtimes, len(services))
			for _, service := range services {
				runtime, exists := runtimes[service]
				require.True(t, exists, "missing spool runtime for service %q", service)
				require.NotNil(t, runtime)
				require.Equal(t, service, runtime.Root().Service())
			}
			received = append(received, runtimes)
			if len(received) == 1 {
				return nil, buildFailure
			}
			return nil, nil
		}}
	_, err := spec.build(owner, nil)
	require.ErrorIs(t, err, buildFailure)
	_, err = spec.build(owner, nil)
	require.NoError(t, err)
	for _, service := range services {
		require.Same(t, received[0][service], received[1][service], "retry must reuse the caller-owned runtime")
	}
	// 別系列も同じBuilderを使うため、同じrootを取り直さず共有できます。
	other := spec
	_, err = other.build(owner, nil)
	require.NoError(t, err)
	for _, service := range services {
		require.Same(t, received[0][service], received[2][service])
	}
	require.NoError(t, owner.Close())
	reopened, err := compressedlog.NewRootRuntimes(configs...)
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}

func TestRecoverBuildsAndReleasesRequiredSpoolRuntimes(t *testing.T) {
	for _, test := range []struct {
		name     string
		prefix   string
		contract pkglogger.LoggerContractKind
		archive  bool
	}{
		{name: "file", contract: pkglogger.LoggerContractSync},
		{name: "async-file", prefix: "ASYNC_", contract: pkglogger.LoggerContractAsyncBestEffort},
		{name: "delivery-file", prefix: "DELIVERY_", contract: pkglogger.LoggerContractDelivery},
		{name: "failure-archive", contract: pkglogger.LoggerContractSync, archive: true},
	} {
		for _, retry := range []bool{false, true} {
			mode := "once"
			if retry {
				mode = "retry"
			}
			t.Run(test.name+"/"+mode, func(t *testing.T) {
				values := map[string]string{
					"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES":          "spool",
					"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES": "spool",
					"HETEROLOG_OBJECT_STORAGE_S3_REGION":                                                 "ap-northeast-1", "HETEROLOG_OBJECT_STORAGE_S3_ENDPOINT": "http://127.0.0.1:1",
					"HETEROLOG_OBJECT_STORAGE_S3_ACCESS_KEY_ID": "test-key", "HETEROLOG_OBJECT_STORAGE_S3_SECRET_ACCESS_KEY": "test-secret",
				}
				scope := RecoveryScopeFileLogger
				uploadPrefix := test.prefix + "LOG_FILE_"
				if test.archive {
					scope = RecoveryScopeLogBackendFailureArchive
					values["HETEROLOG_LOG_TYPE"] = "opensearch"
					// 認証情報は暗黙に補完されないため、送信しない復旧試験では認証なしを明示します。
					values["HETEROLOG_OPENSEARCH_AUTH_TYPE"] = "none"
					values["HETEROLOG_OPENSEARCH_DURABLE_TYPES"] = "fileArchive"
					uploadPrefix = "OPENSEARCH_DURABLE_FILE_ARCHIVE_"
				}
				values["HETEROLOG_ENABLE_"+uploadPrefix+"OBJECT_STORAGE_UPLOAD"] = "true"
				values["HETEROLOG_"+uploadPrefix+"OBJECT_STORAGE_UPLOAD_TYPE"] = "s3"
				values["HETEROLOG_"+uploadPrefix+"OBJECT_STORAGE_S3_BUCKET"] = "recovery-test"
				cfg := loadRecoveryTestConfig(t, values)
				opts := Options{LoggerContract: test.contract, Scope: scope, Retry: retry, RetryInterval: time.Millisecond}
				specs, err := recoverySpecs(cfg, opts)
				require.NoError(t, err)
				require.Len(t, specs, 1)
				require.Len(t, specs[0].spoolConfigs, 1)
				report, err := Recover(context.Background(), cfg, opts)
				require.NoError(t, err)
				require.NotEmpty(t, report.Directories)
				// routeだけでなく、CLIが所有したrootも終了時に解放されています。
				reopened, err := compressedlog.NewRootRuntimes(specs[0].spoolConfigs...)
				require.NoError(t, err)
				require.NoError(t, reopened.Close())
			})
		}
	}
}

func TestRecoverReleasesSpoolRuntimeAfterLifecycleConstructionFailure(t *testing.T) {
	cfg := loadRecoveryTestConfig(t, map[string]string{
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES": "spool",
		"HETEROLOG_ENABLE_LOG_FILE_OBJECT_STORAGE_UPLOAD":                           "true", "HETEROLOG_LOG_FILE_OBJECT_STORAGE_UPLOAD_TYPE": "s3",
		"HETEROLOG_OBJECT_STORAGE_S3_REGION": "ap-northeast-1", "HETEROLOG_OBJECT_STORAGE_S3_ENDPOINT": "http://127.0.0.1:1",
		"HETEROLOG_OBJECT_STORAGE_S3_ACCESS_KEY_ID": "test-key", "HETEROLOG_OBJECT_STORAGE_S3_SECRET_ACCESS_KEY": "test-secret",
		"HETEROLOG_LOG_FILE_OBJECT_STORAGE_S3_BUCKET": "recovery-test",
	})
	opts := Options{LoggerContract: pkglogger.LoggerContractSync, Scope: RecoveryScopeFileLogger}
	specs, err := recoverySpecs(cfg, opts)
	require.NoError(t, err)
	require.Len(t, specs, 1)
	require.Len(t, specs[0].spoolConfigs, 1)
	// runtimeを取得した後のlifecycle構築を、ログ保存先の競合で失敗させます。
	directory := filepath.Join(cfg.FsstoreConfig.LogPath, "service", "sync")
	require.NoError(t, os.MkdirAll(filepath.Dir(directory), 0o750))
	require.NoError(t, os.WriteFile(directory, []byte("blocked"), 0o600))
	_, err = Recover(context.Background(), cfg, opts)
	require.Error(t, err)
	reopened, err := compressedlog.NewRootRuntimes(specs[0].spoolConfigs...)
	require.NoError(t, err, "failed lifecycle construction must not retain the CLI's root ownership")
	require.NoError(t, reopened.Close())
}
