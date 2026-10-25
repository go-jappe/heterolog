package durable

import (
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/pkg/failurestore"
)

// DurableTypeが変換を要する別型にならないことをコンパイル時に確認します。
var _ []failurestore.DurableType = []DurableType(nil)

func TestValidateFailureHandlerMaxConcurrent(t *testing.T) {
	for _, tt := range []struct {
		name          string
		maxConcurrent int
		want          error
	}{
		{name: "positive", maxConcurrent: 1},
		{name: "zero", maxConcurrent: 0, want: ErrInvalidFailureHandlerMaxConcurrent},
		{name: "negative", maxConcurrent: -1, want: ErrInvalidFailureHandlerMaxConcurrent},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assertValidationError(t, ValidateFailureHandlerMaxConcurrent(tt.maxConcurrent), tt.want)
		})
	}
}

func TestValidateSpoolActiveBacklogWarnAfterDays(t *testing.T) {
	for _, tt := range []struct {
		name          string
		warnAfterDays int
		want          error
	}{
		{name: "positive", warnAfterDays: 1},
		{name: "zero disables stale backlog warning", warnAfterDays: 0},
		{name: "negative", warnAfterDays: -1, want: ErrNegativeSpoolActiveBacklogWarnAfterDays},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assertValidationError(t, ValidateSpoolActiveBacklogWarnAfterDays(tt.warnAfterDays), tt.want)
		})
	}
}

func TestValidateFailureStoreDurableTypes(t *testing.T) {
	tests := []struct {
		name  string
		types []DurableType
		// wantは報告される不正です。Errがnilなら成功を求めます。
		want DurableTypeError
	}{
		{
			name:  "spool",
			types: []DurableType{DurableTypeSpool},
		},
		{
			name: "file archive and spool",
			types: []DurableType{
				DurableTypeFileArchive,
				DurableTypeSpool,
			},
		},
		{
			name: "empty",
			want: DurableTypeError{Scope: "failure store", Err: ErrDurableTypesRequired},
		},
		{
			name:  "none selects no durable store",
			types: []DurableType{DurableTypeNone},
		},
		{
			name:  "unsupported",
			types: []DurableType{"unknown"},
			want:  DurableTypeError{Scope: "failure store", Type: "unknown", Err: ErrUnsupportedDurableType},
		},
		{
			name: "duplicate",
			types: []DurableType{
				DurableTypeSpool,
				DurableTypeSpool,
			},
			want: DurableTypeError{Scope: "failure store", Type: DurableTypeSpool, Err: ErrDuplicateDurableType},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertDurableTypeError(t, ValidateFailureStoreDurableTypes(tt.types), tt.want)
		})
	}
}

func TestValidateDeliveryFailureStoreDurableTypes(t *testing.T) {
	tests := []struct {
		name  string
		types []DurableType
		// wantは報告される不正です。Errがnilなら成功を求めます。
		want DurableTypeError
	}{
		{
			name:  "file archive",
			types: []DurableType{DurableTypeFileArchive},
		},
		{
			name: "empty",
			want: DurableTypeError{Scope: "delivery failure store", Err: ErrDurableTypesRequired},
		},
		{
			name:  "none selects no durable store",
			types: []DurableType{DurableTypeNone},
		},
		{
			name:  "spool",
			types: []DurableType{DurableTypeSpool},
			want: DurableTypeError{
				Scope: "delivery failure store", Type: DurableTypeSpool, Err: ErrUnsupportedDurableType,
			},
		},
		{
			name: "none mixed with file archive",
			types: []DurableType{
				DurableTypeNone,
				DurableTypeFileArchive,
			},
			want: DurableTypeError{
				Scope: "delivery failure store", Type: DurableTypeNone, Err: ErrDurableTypeNoneNotAlone,
			},
		},
		{
			name:  "unsupported",
			types: []DurableType{"unknown"},
			want:  DurableTypeError{Scope: "delivery failure store", Type: "unknown", Err: ErrUnsupportedDurableType},
		},
		{
			name: "duplicate",
			types: []DurableType{
				DurableTypeFileArchive,
				DurableTypeFileArchive,
			},
			want: DurableTypeError{
				Scope: "delivery failure store", Type: DurableTypeFileArchive, Err: ErrDuplicateDurableType,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertDurableTypeError(t, ValidateDeliveryFailureStoreDurableTypes(tt.types), tt.want)
		})
	}
}

func TestValidateDirectDegradeSpillTypes(t *testing.T) {
	tests := []struct {
		name  string
		types []SpillType
		// wantは報告される不正です。Errがnilなら成功を求めます。
		want SpillTypeError
	}{
		{
			name:  "stdout",
			types: []SpillType{"stdout"},
		},
		{
			name: "empty",
			want: SpillTypeError{Err: ErrSpillTypesRequired},
		},
		{
			name:  "none while enabled",
			types: []SpillType{SpillTypeNone},
			want:  SpillTypeError{Type: SpillTypeNone, Err: ErrSpillTypeNoneWithDegrade},
		},
		{
			name:  "none before destination",
			types: []SpillType{SpillTypeNone, "stdout"},
			want:  SpillTypeError{Type: SpillTypeNone, Err: ErrSpillTypeNoneNotAlone},
		},
		{
			name:  "none after destination",
			types: []SpillType{"stdout", SpillTypeNone},
			want:  SpillTypeError{Type: SpillTypeNone, Err: ErrSpillTypeNoneNotAlone},
		},
		{
			name:  "none after unsupported value",
			types: []SpillType{"future-store", SpillTypeNone},
			want:  SpillTypeError{Type: SpillTypeNone, Err: ErrSpillTypeNoneNotAlone},
		},
		{
			name:  "none after duplicate destinations",
			types: []SpillType{"stdout", "stdout", SpillTypeNone},
			want:  SpillTypeError{Type: SpillTypeNone, Err: ErrSpillTypeNoneNotAlone},
		},
		{
			name:  "duplicate none",
			types: []SpillType{SpillTypeNone, SpillTypeNone},
			want:  SpillTypeError{Type: SpillTypeNone, Err: ErrSpillTypeNoneNotAlone},
		},
		{
			name:  "empty value",
			types: []SpillType{""},
			want:  SpillTypeError{Type: "", Err: ErrUnsupportedSpillType},
		},
		{
			name:  "reserved s3",
			types: []SpillType{"s3"},
			want:  SpillTypeError{Type: "s3", Err: ErrUnsupportedSpillType},
		},
		{
			name:  "reserved nfs",
			types: []SpillType{"nfs"},
			want:  SpillTypeError{Type: "nfs", Err: ErrUnsupportedSpillType},
		},
		{
			name:  "unknown",
			types: []SpillType{"future-store"},
			want:  SpillTypeError{Type: "future-store", Err: ErrUnsupportedSpillType},
		},
		{
			name: "duplicate",
			types: []SpillType{
				"stdout",
				"stdout",
			},
			want: SpillTypeError{Type: "stdout", Err: ErrDuplicateSpillType},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateDirectDegradeSpillTypes(tt.types)
			if tt.want.Err == nil {
				if err != nil {
					t.Fatalf("validation error = %v, want nil", err)
				}
				return
			}
			var spillTypeErr *SpillTypeError
			if !errors.As(err, &spillTypeErr) || *spillTypeErr != tt.want || !errors.Is(err, tt.want.Err) {
				t.Fatalf("validation error = %#v, want %#v", err, tt.want)
			}
		})
	}
}

// assertValidationErrorは、wantがnilならerrなし、そうでなければerrがwantを持つことを確認します。
func assertValidationError(t *testing.T, err, want error) {
	t.Helper()
	if want == nil {
		if err != nil {
			t.Fatalf("validation error = %v, want nil", err)
		}
		return
	}
	if !errors.Is(err, want) {
		t.Fatalf("validation error = %v, want %v", err, want)
	}
}

// assertDurableTypeErrorは、want.Errがnilならerrなし、そうでなければerrがwantと同じ
// DurableTypeErrorであることを確認します。
func assertDurableTypeError(t *testing.T, err error, want DurableTypeError) {
	t.Helper()
	if want.Err == nil {
		if err != nil {
			t.Fatalf("validation error = %v, want nil", err)
		}
		return
	}
	var durableTypeErr *DurableTypeError
	if !errors.As(err, &durableTypeErr) || *durableTypeErr != want || !errors.Is(err, want.Err) {
		t.Fatalf("validation error = %#v, want %#v", err, want)
	}
}

// SpillTypeErrorの表示形式は、利用者が直す退避先の値を読むerror文字列なので、型ごとに
// 一度だけ文字列で固定します。
func TestSpillTypeErrorFormat(t *testing.T) {
	for _, tt := range []struct {
		err  *SpillTypeError
		want string
	}{
		{err: &SpillTypeError{Err: ErrSpillTypesRequired}, want: "direct degrade spill types are required"},
		{
			err:  &SpillTypeError{Type: SpillTypeNone, Err: ErrSpillTypeNoneNotAlone},
			want: `direct degrade spill type "none" must be specified alone`,
		},
		{
			err:  &SpillTypeError{Type: SpillTypeNone, Err: ErrSpillTypeNoneWithDegrade},
			want: `direct degrade spill type "none" requires degrade to be disabled`,
		},
		{
			err:  &SpillTypeError{Type: "s3", Err: ErrUnsupportedSpillType},
			want: `unsupported direct degrade spill type: "s3"`,
		},
		{
			err:  &SpillTypeError{Type: "stdout", Err: ErrDuplicateSpillType},
			want: `duplicate direct degrade spill type: "stdout"`,
		},
	} {
		if got := tt.err.Error(); got != tt.want {
			t.Fatalf("Error() = %q, want %q", got, tt.want)
		}
	}
}

// durable package経由のDurableTypeErrorとsentinelは、failurestoreのものと同一です。
func TestDurableTypeErrorAliasesFailureStore(t *testing.T) {
	err := ValidateDeliveryFailureStoreDurableTypes([]DurableType{DurableTypeSpool})
	var storeErr *failurestore.DurableTypeError
	if !errors.As(err, &storeErr) || !errors.Is(err, failurestore.ErrUnsupportedDurableType) ||
		ErrUnsupportedDurableType != failurestore.ErrUnsupportedDurableType {
		t.Fatalf("validation error = %v, want failurestore.DurableTypeError of ErrUnsupportedDurableType", err)
	}
}
