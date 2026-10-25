package logger

import (
	"errors"
	"testing"

	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
)

func TestValidateLokiStaticLabels(t *testing.T) {
	const source = "logger internal log destination"
	tests := []struct {
		name   string
		labels map[string]string
		// wantErrがfalseなら成功を求めます。wantInvalidは不正として報告されるlabel名です。
		wantErr     bool
		wantInvalid string
	}{
		{name: "nil additional labels"},
		{name: "empty additional labels", labels: map[string]string{}},
		{name: "valid", labels: map[string]string{"event_scope": "internal"}},
		{name: "empty value is valid", labels: map[string]string{"event_scope": ""}},
		{
			name:   "reserved labels remain accepted",
			labels: map[string]string{"appName": "ignored", "loglevel": "ignored", "env": "ignored"},
		},
		{
			name:        "invalid name",
			labels:      map[string]string{"bad-key": "internal"},
			wantErr:     true,
			wantInvalid: "bad-key",
		},
		{
			name:    "empty name",
			labels:  map[string]string{"": "internal"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateLokiStaticLabels(source, tt.labels)
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("validateLokiStaticLabels() error = %v, want nil", err)
				}
				return
			}
			fieldErr := findFieldError(err, ConfigFieldStaticLabels, ErrInvalidSetting)
			var nameErr *pkgloki.InvalidLabelNameError
			if fieldErr == nil || fieldErr.Source != source || fieldErr.Backend != LogOutputLoki ||
				!errors.As(err, &nameErr) || nameErr.Name != tt.wantInvalid {
				t.Fatalf("validateLokiStaticLabels() error = %v, want %s loki static label name %q", err, source, tt.wantInvalid)
			}
		})
	}
}

func TestValidateLokiPushLimits(t *testing.T) {
	tests := []struct {
		name            string
		maxLineBytes    int
		maxRequestBytes int
		// wantは不正として報告される項目です。zero値なら成功を求めます。
		want ConfigFieldError
	}{
		{
			name:            "valid",
			maxLineBytes:    256 * 1024,
			maxRequestBytes: 4 * 1024 * 1024,
		},
		{
			name:            "line limit is not positive",
			maxLineBytes:    0,
			maxRequestBytes: 1024,
			want:            ConfigFieldError{Field: ConfigFieldMaxLineBytes, Value: 0, Err: ErrNonPositiveSetting},
		},
		{
			name:            "request limit is not positive",
			maxLineBytes:    1,
			maxRequestBytes: -1,
			want:            ConfigFieldError{Field: ConfigFieldMaxPushRequestBytes, Value: -1, Err: ErrNonPositiveSetting},
		},
		{
			name:            "line limit equals request limit",
			maxLineBytes:    1024,
			maxRequestBytes: 1024,
			want: ConfigFieldError{
				Field: ConfigFieldMaxLineBytes, Value: 1024, Limit: 1024, Err: ErrInconsistentSetting,
			},
		},
		{
			name:            "line limit exceeds request limit",
			maxLineBytes:    2048,
			maxRequestBytes: 1024,
			want: ConfigFieldError{
				Field: ConfigFieldMaxLineBytes, Value: 2048, Limit: 1024, Err: ErrInconsistentSetting,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateLokiPushLimits(tt.maxLineBytes, tt.maxRequestBytes)
			if tt.want.Err == nil {
				if err != nil {
					t.Fatalf("ValidateLokiPushLimits() error = %v, want nil", err)
				}
				return
			}
			fieldErr := findFieldError(err, tt.want.Field, tt.want.Err)
			if fieldErr == nil || fieldErr.Value != tt.want.Value || fieldErr.Limit != tt.want.Limit {
				t.Fatalf("ValidateLokiPushLimits() error = %v, want %s value=%v limit=%v of kind %v",
					err, tt.want.Field, tt.want.Value, tt.want.Limit, tt.want.Err)
			}
		})
	}
}
