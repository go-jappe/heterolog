package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	fsstorefailurearchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/pkg/env"
	objectstoragevalidation "github.com/go-jappe/heterolog/pkg/objectstorage/validation"
)

func TestLoadConfigRejectsUnreadableExplicitEnvPath(t *testing.T) {
	restoreProcessEnvAfterTest(t)
	envPath := filepath.Join(t.TempDir(), "missing.env")
	clearProcessEnvForTest(t)

	if err := os.Setenv("HETEROLOG_ENV_PATH", envPath); err != nil {
		t.Fatalf("set HETEROLOG_ENV_PATH: %v", err)
	}

	_, err := LoadConfig()
	var envFileErr *env.EnvFileError
	if !errors.As(err, &envFileErr) || envFileErr.Path != envPath || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("LoadConfig() error = %v, want missing explicit HETEROLOG_ENV_PATH %q", err, envPath)
	}
}

func TestLoadConfigAllowsAbsentDefaultEnvFile(t *testing.T) {
	restoreProcessEnvAfterTest(t)
	root := t.TempDir()
	clearProcessEnvForTest(t)
	t.Chdir(root)

	if _, err := LoadConfig(); err != nil {
		t.Fatalf("LoadConfig() without default .env: %v", err)
	}
}

func TestLoadConfigRejectsExplicitInvalidTypedEnvironmentValue(t *testing.T) {
	_, err := loadConfigErrorFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=false",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_MEMORY_CRITICAL_CONSECUTIVE_HITS=several",
	})
	requireEnvironmentKeyError(t, err, "HETEROLOG_MEMORY_CRITICAL_CONSECUTIVE_HITS", env.ErrNotInteger)
}

func TestLoadConfigRejectsExplicitInvalidBooleanEnvironmentValue(t *testing.T) {
	_, err := loadConfigErrorFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=perhaps",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
	})
	requireEnvironmentKeyError(t, err, "HETEROLOG_ENABLE_LOGGING", env.ErrNotBoolean)
}

func TestLoadConfigAggregatesEnvironmentErrorsAcrossLogicalLoaders(t *testing.T) {
	_, err := loadConfigErrorFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=false",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_MEMORY_CRITICAL_CONSECUTIVE_HITS=several",
		"HETEROLOG_APPENDLOG_RUNNER_INTERVAL_SECOND=eventually",
	})
	requireEnvironmentKeyErrors(t, err, "HETEROLOG_MEMORY_CRITICAL_CONSECUTIVE_HITS", "HETEROLOG_APPENDLOG_RUNNER_INTERVAL_SECOND")
}

func TestLoadConfigDoesNotRetainEnvironmentReadErrorsAcrossCalls(t *testing.T) {
	restoreProcessEnvAfterTest(t)
	envPath := filepath.Join(t.TempDir(), ".env")
	clearProcessEnvForTest(t)

	contents := strings.Join([]string{
		"HETEROLOG_ENABLE_LOGGING=false",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_MEMORY_CRITICAL_CONSECUTIVE_HITS=not-an-integer",
	}, "\n")
	if err := os.WriteFile(envPath, []byte(contents), 0o600); err != nil {
		t.Fatalf("write temp env: %v", err)
	}
	if err := os.Setenv("HETEROLOG_ENV_PATH", envPath); err != nil {
		t.Fatalf("set HETEROLOG_ENV_PATH: %v", err)
	}

	_, err := LoadConfig()
	requireEnvironmentKeyError(t, err, "HETEROLOG_MEMORY_CRITICAL_CONSECUTIVE_HITS", env.ErrNotInteger)

	// process environmentの値は環境設定fileより優先するため、2回目の読み込みでは
	// 同じkeyの正しい値をprocess environmentへ設定します。
	if err := os.Setenv("HETEROLOG_MEMORY_CRITICAL_CONSECUTIVE_HITS", "3"); err != nil {
		t.Fatalf("replace HETEROLOG_MEMORY_CRITICAL_CONSECUTIVE_HITS: %v", err)
	}
	if _, err := LoadConfig(); err != nil {
		t.Fatalf("second LoadConfig() retained the previous read error: %v", err)
	}
}

// TestLoadConfigRejectsExplicitInvalidCompressionFormatは、fileを出力先に選んだ場合だけ
// HETEROLOG_LOG_COMPRESS_FORMATを読み込み、不正値を設定エラーにすることを検証します。
// HETEROLOG_LOG_TYPEの既定値はstdoutだけなので、未設定時のfile設定は起動を妨げません。
func TestLoadConfigRejectsExplicitInvalidCompressionFormat(t *testing.T) {
	for _, tt := range []struct {
		name      string
		selection []string
		wantErr   bool
	}{
		{name: "file selected", selection: []string{"HETEROLOG_LOG_TYPE=file"}, wantErr: true},
		{name: "file selected with stdout", selection: []string{"HETEROLOG_LOG_TYPE=stdout,file"}, wantErr: true},
		{name: "default stdout only"},
		{name: "explicit stdout only", selection: []string{"HETEROLOG_LOG_TYPE=stdout"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lines := []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
				"HETEROLOG_LOG_COMPRESS_FORMAT=unknown-codec",
			}
			cfg, err := loadConfigErrorFromTempEnv(t, storagePathValidationEnv(t, append(lines, tt.selection...)...))
			if tt.wantErr {
				requireEnvironmentKeyError(t, err, "HETEROLOG_LOG_COMPRESS_FORMAT", logconfig.ErrUnsupportedLogCompressFormat)
				if valueErr := environmentKeyError(err, "HETEROLOG_LOG_COMPRESS_FORMAT").(*env.InvalidEnvironmentValueError); valueErr.Value != "unknown-codec" {
					t.Fatalf("HETEROLOG_LOG_COMPRESS_FORMAT error value = %q, want unknown-codec", valueErr.Value)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadConfig() with unselected file config error = %v, want nil", err)
			}
			if cfg.Logger.LogOutputDestinations.Has(logconfig.LogOutputFile) {
				t.Fatalf("HETEROLOG_LOG_TYPE selected file implicitly: %v", cfg.Logger.LogOutputDestinations)
			}
		})
	}
}

func TestFailureStoreBasePathUsesFinalRuntimeTarget(t *testing.T) {
	basePath := filepath.Join(t.TempDir(), "state")
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_TARGET=env-target",
		"HETEROLOG_APP_STATE_DIR=" + basePath,
		"HETEROLOG_ENABLE_LOGGING=false",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
	})

	if got := cfg.FsstoreConfig.AppStatePath; got != basePath {
		t.Fatalf("AppStatePath = %q, want base path %q", got, basePath)
	}

	cfg.RuntimeConfig.Target = "cli-target"
	mapped := NewOpenSearchFlushManagerConfig(&cfg)
	archive := mapped.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig
	resolved := fsstorefailurearchive.ResolveDirectoryPath(
		archive.FailureFileArchivePath,
		archive.Target,
		archive.Dir,
		archive.DirCaseRetryable,
	)
	want := filepath.Join(basePath, "cli-target")
	if resolved != want {
		t.Fatalf("resolved failure archive path = %q, want %q", resolved, want)
	}
}

func TestObjectStorageUploadTargetMeaningIsValidatedAfterMapping(t *testing.T) {
	tests := []struct {
		name        string
		targetLine  string
		wantTargets []string
		// wantTargetErrの種類で拒否したerrorを期待します。一覧内のtargetを拒否する種類では、
		// wantIndex番目のtargetを拒否したことも確認します。
		wantTargetErr error
		wantIndex     int
	}{
		{
			name:          "missing target remains empty",
			wantTargetErr: objectstoragevalidation.ErrUploadTargetsRequired,
		},
		{
			name:          "empty target remains empty",
			targetLine:    "HETEROLOG_LOG_FILE_OBJECT_STORAGE_UPLOAD_TYPE=",
			wantTargetErr: objectstoragevalidation.ErrUploadTargetsRequired,
		},
		{
			name:          "unsupported target is preserved for mapper validation",
			targetLine:    "HETEROLOG_LOG_FILE_OBJECT_STORAGE_UPLOAD_TYPE=future-storage",
			wantTargets:   []string{"future-storage"},
			wantTargetErr: objectstoragevalidation.ErrUnsupportedUploadTarget,
		},
		{
			name:          "empty element is preserved for mapper validation",
			targetLine:    "HETEROLOG_LOG_FILE_OBJECT_STORAGE_UPLOAD_TYPE=s3, ,s3",
			wantTargets:   []string{"s3", "", "s3"},
			wantTargetErr: objectstoragevalidation.ErrUploadTargetRequired,
			wantIndex:     1,
		},
		{
			name:          "duplicate targets are preserved for mapper validation",
			targetLine:    "HETEROLOG_LOG_FILE_OBJECT_STORAGE_UPLOAD_TYPE=s3,s3",
			wantTargets:   []string{"s3", "s3"},
			wantTargetErr: objectstoragevalidation.ErrDuplicateUploadTarget,
			wantIndex:     1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lines := []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
				"HETEROLOG_LOG_TYPE=file",
				"HETEROLOG_ENABLE_LOG_FILE_OBJECT_STORAGE_UPLOAD=true",
			}
			if tt.targetLine != "" {
				lines = append(lines, tt.targetLine)
			}

			cfg := loadConfigFromTempEnv(t, lines)
			targets := cfg.Logger.FileConfig.
				FileSystemStoreConfig.
				ObjectStorageUploadConfig.
				Default.
				Targets
			if len(targets) != len(tt.wantTargets) {
				t.Fatalf("loaded targets = %v, want %v", targets, tt.wantTargets)
			}
			for index, want := range tt.wantTargets {
				if string(targets[index]) != want {
					t.Fatalf("loaded targets = %v, want %v", targets, tt.wantTargets)
				}
			}

			_, err := MapLoggerConfig(cfg)
			switch tt.wantTargetErr {
			case objectstoragevalidation.ErrUploadTargetsRequired:
				if !errors.Is(err, objectstoragevalidation.ErrUploadTargetsRequired) {
					t.Fatalf("MapLoggerConfig() error = %v, want %v", err, objectstoragevalidation.ErrUploadTargetsRequired)
				}
			case objectstoragevalidation.ErrUnsupportedUploadTarget:
				var unsupportedErr *objectstoragevalidation.UnsupportedUploadTargetError
				if !errors.As(err, &unsupportedErr) || unsupportedErr.Target != "future-storage" {
					t.Fatalf("MapLoggerConfig() error = %v, want unsupported upload target %q", err, "future-storage")
				}
			default:
				var targetErr *objectstoragevalidation.UploadTargetError
				if !errors.As(err, &targetErr) || !errors.Is(targetErr, tt.wantTargetErr) ||
					targetErr.Index != tt.wantIndex || string(targetErr.Target) != tt.wantTargets[tt.wantIndex] {
					t.Fatalf("MapLoggerConfig() error = %v, want %v at index %d", err, tt.wantTargetErr, tt.wantIndex)
				}
			}
			// targetが不正なら、S3を暗黙に選択してS3設定を検証してはいけません。
			if errors.Is(err, objectstoragevalidation.ErrInvalidS3Config) {
				t.Fatalf("MapLoggerConfig() error = %v, must not select s3 implicitly", err)
			}
		})
	}
}

func TestDisabledObjectStorageUploadDoesNotValidateOptionalTarget(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_LOG_TYPE=file",
		"HETEROLOG_ENABLE_LOG_FILE_OBJECT_STORAGE_UPLOAD=false",
		"HETEROLOG_LOG_FILE_OBJECT_STORAGE_UPLOAD_TYPE=s3, ,future-storage",
	})

	if _, err := MapLoggerConfig(cfg); err != nil {
		t.Fatalf("MapLoggerConfig() with disabled optional upload: %v", err)
	}
}
