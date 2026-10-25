package validation

import (
	"errors"
	"slices"
	"testing"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
	s3uploader "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
)

func TestValidateUploadTargets(t *testing.T) {
	tests := []struct {
		name    string
		targets []objectstorage.UploadTarget
		// wantErrorは報告される不正です。Errがnilなら成功を求めます。
		wantError UploadTargetError
	}{
		{
			name: "empty slice is left to caller",
		},
		{
			name:    "canonical targets are valid",
			targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3, "custom"},
		},
		{
			name:      "blank target is invalid",
			targets:   []objectstorage.UploadTarget{" "},
			wantError: UploadTargetError{Index: 0, Target: " ", Err: ErrUploadTargetRequired},
		},
		{
			name:    "surrounding whitespace is not normalized",
			targets: []objectstorage.UploadTarget{" s3 "},
			wantError: UploadTargetError{
				Index: 0, Target: " s3 ", Canonical: objectstorage.UploadTargetS3, Err: ErrNonCanonicalUploadTarget,
			},
		},
		{
			name:    "uppercase is not normalized",
			targets: []objectstorage.UploadTarget{"S3"},
			wantError: UploadTargetError{
				Index: 0, Target: "S3", Canonical: objectstorage.UploadTargetS3, Err: ErrNonCanonicalUploadTarget,
			},
		},
		{
			name: "duplicate target is invalid",
			targets: []objectstorage.UploadTarget{
				objectstorage.UploadTargetS3,
				objectstorage.UploadTargetS3,
			},
			wantError: UploadTargetError{Index: 1, Target: objectstorage.UploadTargetS3, Err: ErrDuplicateUploadTarget},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateUploadTargets(tt.targets)
			if tt.wantError.Err == nil {
				if err != nil {
					t.Fatalf("ValidateUploadTargets() error = %v", err)
				}
				return
			}
			var targetErr *UploadTargetError
			if !errors.As(err, &targetErr) || *targetErr != tt.wantError || !errors.Is(err, tt.wantError.Err) {
				t.Fatalf("ValidateUploadTargets() error = %#v, want %#v", err, tt.wantError)
			}
		})
	}
}

func TestValidateLogFileUploadConfigOnlyValidatesPhysicalRoles(t *testing.T) {
	cfg := objectstorage.LogFileUploadConfig{
		Enabled: true,
		Default: objectstorage.UploadTargetConfig{
			Enabled: false,
		},
		Error: objectstorage.UploadTargetConfig{
			Enabled: true,
			Targets: []objectstorage.UploadTarget{"unsupported"},
		},
		Debug: objectstorage.UploadTargetConfig{
			Enabled: true,
			Targets: []objectstorage.UploadTarget{"unsupported"},
		},
	}

	if err := ValidateLogFileUploadConfig("logger file", cfg, false, false); err != nil {
		t.Fatalf("unsplit role validation error = %v, want nil", err)
	}

	err := ValidateLogFileUploadConfig("logger file", cfg, true, true)
	if !errors.Is(err, ErrUnsupportedUploadTarget) {
		t.Fatalf("split role validation error = %v, want unsupported target errors", err)
	}
	want := []UnsupportedUploadTargetError{
		{Scope: "logger file error", Target: "unsupported"},
		{Scope: "logger file debug", Target: "unsupported"},
	}
	if got := unsupportedUploadTargetErrors(err); !slices.Equal(got, want) {
		t.Fatalf("split role validation errors = %v, want %v", got, want)
	}
}

// unsupportedUploadTargetErrorsは、errのerror treeに含まれるUnsupportedUploadTargetErrorを
// 出現順に返します。
func unsupportedUploadTargetErrors(err error) []UnsupportedUploadTargetError {
	var found []UnsupportedUploadTargetError
	var visit func(error)
	visit = func(err error) {
		if targetErr, ok := err.(*UnsupportedUploadTargetError); ok {
			found = append(found, *targetErr)
		}
		switch wrapped := err.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range wrapped.Unwrap() {
				visit(child)
			}
		case interface{ Unwrap() error }:
			visit(wrapped.Unwrap())
		}
	}
	visit(err)
	return found
}

// UploadTargetErrorの表示形式は、利用者が直すtargetの位置と値を読むerror文字列なので、
// 型ごとに一度だけ文字列で固定します。
func TestUploadTargetErrorFormat(t *testing.T) {
	for _, tt := range []struct {
		err  *UploadTargetError
		want string
	}{
		{
			err:  &UploadTargetError{Index: 0, Target: " ", Err: ErrUploadTargetRequired},
			want: "object storage upload target is required at index 0",
		},
		{
			err:  &UploadTargetError{Index: 2, Target: " S3", Canonical: "s3", Err: ErrNonCanonicalUploadTarget},
			want: `object storage upload target " S3" at index 2 must use canonical form "s3"`,
		},
		{
			err:  &UploadTargetError{Index: 1, Target: "s3", Err: ErrDuplicateUploadTarget},
			want: `duplicate object storage upload target "s3" at index 1`,
		},
	} {
		if got := tt.err.Error(); got != tt.want {
			t.Fatalf("Error() = %q, want %q", got, tt.want)
		}
	}
}

// UnsupportedUploadTargetErrorの表示形式は、利用者がどの設定のtargetを直すかを読む
// error文字列なので、型ごとに一度だけ文字列で固定します。
func TestUnsupportedUploadTargetErrorFormat(t *testing.T) {
	for _, tt := range []struct {
		err  *UnsupportedUploadTargetError
		want string
	}{
		{
			err:  &UnsupportedUploadTargetError{Scope: "logger file default", Target: "gcs"},
			want: `logger file default unsupported object storage upload target: "gcs"`,
		},
		{
			err:  &UnsupportedUploadTargetError{Target: "gcs"},
			want: `unsupported object storage upload target: "gcs"`,
		},
	} {
		if got := tt.err.Error(); got != tt.want {
			t.Fatalf("Error() = %q, want %q", got, tt.want)
		}
		if !errors.Is(tt.err, ErrUnsupportedUploadTarget) {
			t.Fatalf("errors.Is(%v, ErrUnsupportedUploadTarget) = false", tt.err)
		}
	}
}

func TestValidateUploadTargetConfigUsesS3Validation(t *testing.T) {
	err := ValidateUploadTargetConfig("logger file default", objectstorage.UploadTargetConfig{
		Enabled: true,
		Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
		S3: objectstorage.S3Config{
			AccessKeyID:     "access-key",
			SecretAccessKey: "secret-key",
			Bucket:          "archive",
		},
	})
	// S3 clientの検証errorをwrapして返し、呼び出し側がsentinelで欠落を判別できます。
	if !errors.Is(err, s3uploader.ErrRegionRequired) {
		t.Fatalf("ValidateUploadTargetConfig() error = %v, want errors.Is(..., ErrRegionRequired)", err)
	}
}
