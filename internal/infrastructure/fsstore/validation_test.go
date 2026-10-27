package fsstore

import (
	"errors"
	"testing"
	"time"
)

type lifecycleValidationConfig struct{}

func (lifecycleValidationConfig) MaxFileBackups() int { return MinAllowedMaxFileBackups }
func (lifecycleValidationConfig) MaxFileAge() int     { return MinAllowedMaxFileAge }
func (lifecycleValidationConfig) MaxFileSize() uint64 { return MinAllowedFileSizeBytes }
func (lifecycleValidationConfig) MaxDirSize() uint64  { return MinAllowedMaxDirSize }

func TestValidateFileLifecycleConfigProviderAcceptsValidLimits(t *testing.T) {
	if err := ValidateFileLifecycleConfigProvider(lifecycleValidationConfig{}); err != nil {
		t.Fatalf("ValidateFileLifecycleConfigProvider() error = %v", err)
	}
}

// retentionValidationConfigは、各項目を個別に変更できる検証用の保持設定です。
type retentionValidationConfig struct {
	maxFileBackups int
	maxFileAge     int
	maxFileSize    uint64
	maxDirSize     uint64
}

func (c retentionValidationConfig) MaxFileBackups() int { return c.maxFileBackups }
func (c retentionValidationConfig) MaxFileAge() int     { return c.maxFileAge }
func (c retentionValidationConfig) MaxFileSize() uint64 { return c.maxFileSize }
func (c retentionValidationConfig) MaxDirSize() uint64  { return c.maxDirSize }

func minimumRetentionValidationConfig() retentionValidationConfig {
	return retentionValidationConfig{
		maxFileBackups: MinAllowedMaxFileBackups,
		maxFileAge:     MinAllowedMaxFileAge,
		maxFileSize:    MinAllowedFileSizeBytes,
		maxDirSize:     MinAllowedMaxDirSize,
	}
}

// 通常ログの保持上限は従来どおり0を受け付けません。failure archive向けの0（上限なし）は
// 通常ログへ広げず、0では削除しない設定を通常ログで受け付けないことを確認します。
func TestValidateFileLifecycleConfigProviderStillRejectsZeroRetentionLimits(t *testing.T) {
	for _, test := range []struct {
		name    string
		mutate  func(*retentionValidationConfig)
		wantErr []error
	}{
		{
			name:    "backups",
			mutate:  func(cfg *retentionValidationConfig) { cfg.maxFileBackups = 0 },
			wantErr: []error{ErrInvalidMaxFileBackups},
		},
		{
			name:    "age",
			mutate:  func(cfg *retentionValidationConfig) { cfg.maxFileAge = 0 },
			wantErr: []error{ErrInvalidMaxFileAge},
		},
		{
			// 0はディレクトリ最大サイズの最小値も、ファイル最大サイズ以上という条件も満たしません。
			name:    "dir size",
			mutate:  func(cfg *retentionValidationConfig) { cfg.maxDirSize = 0 },
			wantErr: []error{ErrInvalidMaxDirSize, ErrMaxDirSizeBelowMaxFileSize},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := minimumRetentionValidationConfig()
			test.mutate(&cfg)
			err := ValidateFileLifecycleConfigProvider(cfg)
			requireOnlyValidationErrors(t, err, test.wantErr...)
		})
	}
}

// failure archiveでは保持世代数・保持日数・ディレクトリ最大サイズの0を上限なしとして受け付けます。
// 0以外は通常ログと同じ最小値ちょうどまで受け付けます。
func TestValidateFailureArchiveRetentionConfigProviderAcceptsZeroAsUnlimited(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*retentionValidationConfig)
	}{
		{name: "minimum non-zero limits", mutate: func(*retentionValidationConfig) {}},
		{name: "zero backups", mutate: func(cfg *retentionValidationConfig) { cfg.maxFileBackups = 0 }},
		{name: "zero age", mutate: func(cfg *retentionValidationConfig) { cfg.maxFileAge = 0 }},
		{name: "zero dir size", mutate: func(cfg *retentionValidationConfig) { cfg.maxDirSize = 0 }},
		{
			// ディレクトリ上限がない場合、ファイル最大サイズとの大小関係は検証しません。
			name: "zero dir size with large file size",
			mutate: func(cfg *retentionValidationConfig) {
				cfg.maxFileSize = 2 * MinAllowedMaxDirSize
				cfg.maxDirSize = 0
			},
		},
		{name: "all retention limits zero", mutate: func(cfg *retentionValidationConfig) {
			cfg.maxFileBackups = 0
			cfg.maxFileAge = 0
			cfg.maxDirSize = 0
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := minimumRetentionValidationConfig()
			test.mutate(&cfg)
			if err := ValidateFailureArchiveRetentionConfigProvider(cfg); err != nil {
				t.Fatalf("ValidateFailureArchiveRetentionConfigProvider() error = %v, want nil", err)
			}
		})
	}
}

// 0以外の保持上限は最小値を満たす必要があります。負数も0（上限なし）としては扱いません。
// ファイル最大サイズは保持上限ではないため、0を受け付けません。
func TestValidateFailureArchiveRetentionConfigProviderRejectsNonZeroBelowMinimum(t *testing.T) {
	for _, test := range []struct {
		name    string
		mutate  func(*retentionValidationConfig)
		wantErr error
	}{
		{
			name:    "one backup",
			mutate:  func(cfg *retentionValidationConfig) { cfg.maxFileBackups = MinAllowedMaxFileBackups - 1 },
			wantErr: ErrInvalidMaxFileBackups,
		},
		{
			name:    "negative backups",
			mutate:  func(cfg *retentionValidationConfig) { cfg.maxFileBackups = -1 },
			wantErr: ErrInvalidMaxFileBackups,
		},
		{
			name:    "negative age",
			mutate:  func(cfg *retentionValidationConfig) { cfg.maxFileAge = -1 },
			wantErr: ErrInvalidMaxFileAge,
		},
		{
			name:    "dir size below minimum",
			mutate:  func(cfg *retentionValidationConfig) { cfg.maxDirSize = MinAllowedMaxDirSize - 1 },
			wantErr: ErrInvalidMaxDirSize,
		},
		{
			name: "dir size below file size",
			mutate: func(cfg *retentionValidationConfig) {
				cfg.maxFileSize = MinAllowedMaxDirSize + 1
				cfg.maxDirSize = MinAllowedMaxDirSize
			},
			wantErr: ErrMaxDirSizeBelowMaxFileSize,
		},
		{
			name: "zero file size",
			mutate: func(cfg *retentionValidationConfig) {
				cfg.maxFileSize = 0
				cfg.maxFileBackups = 0
				cfg.maxFileAge = 0
				cfg.maxDirSize = 0
			},
			wantErr: ErrInvalidMaxFileSize,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := minimumRetentionValidationConfig()
			test.mutate(&cfg)
			err := ValidateFailureArchiveRetentionConfigProvider(cfg)
			requireOnlyValidationErrors(t, err, test.wantErr)
		})
	}
}

// validationSentinelsは、検証が返し得る項目別のsentinelです。
var validationSentinels = []error{
	ErrInvalidMaxFileSize, ErrInvalidMaxFileBackups, ErrInvalidMaxFileAge, ErrInvalidMaxDirSize,
	ErrMaxDirSizeBelowMaxFileSize,
	ErrInvalidCompressWarnThreshold, ErrInvalidCompressErrorThreshold,
	ErrInvalidWriteFileWarnThreshold, ErrInvalidWriteFileErrorThreshold,
	ErrCompressErrorThresholdBelowWarn, ErrWriteFileErrorThresholdBelowWarn,
	ErrInvalidMaxWriteRetries, ErrInvalidFileOperationRetryWaitTime,
}

// requireOnlyValidationErrorsは、errが指定した項目のsentinelだけをwrapすることを確認します。
func requireOnlyValidationErrors(t *testing.T, err error, want ...error) {
	t.Helper()
	if err == nil {
		t.Fatalf("validation error = nil, want %v", want)
	}
	for _, sentinel := range validationSentinels {
		wanted := false
		for _, target := range want {
			wanted = wanted || target == sentinel
		}
		if got := errors.Is(err, sentinel); got != wanted {
			t.Fatalf("errors.Is(%v, %v) = %t, want %t", err, sentinel, got, wanted)
		}
	}
}

// durationValidationConfigは、処理時間の閾値を個別に変更できる検証用の設定です。
type durationValidationConfig struct {
	compressWarn, compressError, writeWarn, writeError time.Duration
}

func (c durationValidationConfig) CompressWarnThresholdDuration() time.Duration {
	return c.compressWarn
}
func (c durationValidationConfig) CompressErrorThresholdDuration() time.Duration {
	return c.compressError
}
func (c durationValidationConfig) WriteFileWarnThresholdDuration() time.Duration { return c.writeWarn }
func (c durationValidationConfig) WriteFileErrorThresholdDuration() time.Duration {
	return c.writeError
}

func TestValidateDurationConfigProviderIdentifiesRejectedThresholds(t *testing.T) {
	valid := durationValidationConfig{
		compressWarn: MinCompressWarnThresholdDuration, compressError: MinCompressErrorThresholdDuration,
		writeWarn: MinWriteFileWarnThresholdDuration, writeError: MinWriteFileErrorThresholdDuration,
	}
	if err := ValidateDurationConfigProvider(valid); err != nil {
		t.Fatalf("ValidateDurationConfigProvider() error = %v", err)
	}
	for _, test := range []struct {
		name    string
		mutate  func(*durationValidationConfig)
		wantErr []error
	}{
		{name: "compress warn", mutate: func(cfg *durationValidationConfig) { cfg.compressWarn = 0 }, wantErr: []error{ErrInvalidCompressWarnThreshold}},
		{name: "compress error", mutate: func(cfg *durationValidationConfig) { cfg.compressError = 0 },
			wantErr: []error{ErrInvalidCompressErrorThreshold, ErrCompressErrorThresholdBelowWarn}},
		{name: "write warn", mutate: func(cfg *durationValidationConfig) { cfg.writeWarn = 0 }, wantErr: []error{ErrInvalidWriteFileWarnThreshold}},
		{name: "write error", mutate: func(cfg *durationValidationConfig) { cfg.writeError = 0 },
			wantErr: []error{ErrInvalidWriteFileErrorThreshold, ErrWriteFileErrorThresholdBelowWarn}},
		{name: "compress error below warn", mutate: func(cfg *durationValidationConfig) { cfg.compressWarn = time.Second },
			wantErr: []error{ErrCompressErrorThresholdBelowWarn}},
		{name: "write error below warn", mutate: func(cfg *durationValidationConfig) { cfg.writeWarn = time.Second },
			wantErr: []error{ErrWriteFileErrorThresholdBelowWarn}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid
			test.mutate(&cfg)
			requireOnlyValidationErrors(t, ValidateDurationConfigProvider(cfg), test.wantErr...)
		})
	}
}

type retryValidationConfig struct {
	retries int
	wait    time.Duration
}

func (c retryValidationConfig) MaxWriteRetries() int                      { return c.retries }
func (c retryValidationConfig) FileOperationRetryWaitTime() time.Duration { return c.wait }

func TestValidateRetryConfigProviderIdentifiesRejectedSettings(t *testing.T) {
	// 再試行0回は不正な値ではなく、1回だけ操作します。負の回数だけを拒否します。
	if err := ValidateRetryConfigProvider(retryValidationConfig{retries: 0, wait: MinFileOperationRetryWaitTime}); err != nil {
		t.Fatalf("ValidateRetryConfigProvider() error = %v", err)
	}
	requireOnlyValidationErrors(t, ValidateRetryConfigProvider(retryValidationConfig{retries: -1, wait: MinFileOperationRetryWaitTime}), ErrInvalidMaxWriteRetries)
	requireOnlyValidationErrors(t, ValidateRetryConfigProvider(retryValidationConfig{retries: 0}), ErrInvalidFileOperationRetryWaitTime)
}
