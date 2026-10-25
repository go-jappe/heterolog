package config

import (
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestLoadLogRotationParsesValueWithoutReadingEnvironment(t *testing.T) {
	t.Setenv("TEST_FILE_ROTATION", string(fsstore.FileRotationWeekly))
	for _, tc := range []struct {
		value string
		want  fsstore.FileRotationType
	}{
		{"daily", fsstore.FileRotationDaily},
		{"weekly", fsstore.FileRotationWeekly},
		{"monthly", fsstore.FileRotationMonthly},
		{"hourly", fsstore.FileRotationDaily},
		{"", fsstore.FileRotationDaily},
		// 環境変数名を渡しても、その値を取得せず不正な設定値として扱います。
		{"TEST_FILE_ROTATION", fsstore.FileRotationDaily},
	} {
		t.Run(tc.value, func(t *testing.T) {
			if got := loadLogRotation(tc.value, fsstore.FileRotationDaily); got != tc.want {
				t.Fatalf("loadLogRotation(%q) = %q, want %q", tc.value, got, tc.want)
			}
		})
	}
}

func TestLoadConfigReadsFileRotationWithDailyDefault(t *testing.T) {
	for _, tc := range []struct {
		name  string
		entry string
		want  fsstore.FileRotationType
	}{
		{"unspecified", "", fsstore.FileRotationDaily},
		{"empty", "HETEROLOG_LOG_FILE_ROTATION=", fsstore.FileRotationDaily},
		{"invalid", "HETEROLOG_LOG_FILE_ROTATION=hourly", fsstore.FileRotationDaily},
		{"daily", "HETEROLOG_LOG_FILE_ROTATION=daily", fsstore.FileRotationDaily},
		{"weekly", "HETEROLOG_LOG_FILE_ROTATION=weekly", fsstore.FileRotationWeekly},
		{"monthly", "HETEROLOG_LOG_FILE_ROTATION=monthly", fsstore.FileRotationMonthly},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			lines := []string{
				"HETEROLOG_APP_NAME=file-rotation-fallback-test",
				"HETEROLOG_TARGET=file-rotation-fallback-target",
				"HETEROLOG_SYSTEM_LOG_DIR=" + filepath.Join(root, "log"),
				"HETEROLOG_SYSTEM_LIB_DIR=" + filepath.Join(root, "lib"),
				"HETEROLOG_APP_STATE_DIR=" + filepath.Join(root, "state"),
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
				"HETEROLOG_LOG_TYPE=file",
			}
			if tc.entry != "" {
				lines = append(lines, tc.entry)
			}
			cfg := loadConfigFromTempEnv(t, lines)
			if got := cfg.Logger.FileConfig.FileSystemStoreConfig.RotationType; got != tc.want {
				t.Fatalf("LoadConfig() HETEROLOG_LOG_FILE_ROTATION = %q, want %q", got, tc.want)
			}
		})
	}
}
