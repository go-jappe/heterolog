package logger

import (
	"testing"
)

func TestValidateOpenSearchBulkByteLimits(t *testing.T) {
	tests := []struct {
		name                string
		bufferFlushBytes    int
		maxBulkRequestBytes int
		// wantは不正として報告される項目です。Errがnilなら成功を求めます。
		want ConfigFieldError
	}{
		{
			name:                "valid",
			bufferFlushBytes:    5 * 1024 * 1024,
			maxBulkRequestBytes: 15 * 1024 * 1024,
		},
		{
			name:                "zero buffer threshold",
			bufferFlushBytes:    0,
			maxBulkRequestBytes: 15 * 1024 * 1024,
			want:                ConfigFieldError{Field: ConfigFieldBufferFlushBytes, Value: 0, Err: ErrNonPositiveSetting},
		},
		{
			name:                "negative bulk request limit",
			bufferFlushBytes:    5 * 1024 * 1024,
			maxBulkRequestBytes: -1,
			want: ConfigFieldError{
				Field: ConfigFieldMaxBulkRequestBytes, Value: -1, Err: ErrNonPositiveSetting,
			},
		},
		{
			name:                "buffer threshold equals request limit",
			bufferFlushBytes:    5 * 1024 * 1024,
			maxBulkRequestBytes: 5 * 1024 * 1024,
			want: ConfigFieldError{
				Field: ConfigFieldBufferFlushBytes, Value: 5 * 1024 * 1024, Limit: 5 * 1024 * 1024,
				Err: ErrInconsistentSetting,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateOpenSearchBulkByteLimits(
				tt.bufferFlushBytes,
				tt.maxBulkRequestBytes,
			)
			if tt.want.Err == nil {
				if err != nil {
					t.Fatalf("ValidateOpenSearchBulkByteLimits() error = %v, want nil", err)
				}
				return
			}
			fieldErr := findFieldError(err, tt.want.Field, tt.want.Err)
			if fieldErr == nil || fieldErr.Value != tt.want.Value || fieldErr.Limit != tt.want.Limit {
				t.Fatalf(
					"ValidateOpenSearchBulkByteLimits() error = %v, want %s value=%v limit=%v of kind %v",
					err, tt.want.Field, tt.want.Value, tt.want.Limit, tt.want.Err,
				)
			}
		})
	}
}
