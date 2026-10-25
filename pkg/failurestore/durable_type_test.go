package failurestore

import (
	"errors"
	"testing"
)

func TestDurableTypeValues(t *testing.T) {
	tests := []struct {
		name string
		got  DurableType
		want string
	}{
		{name: "none", got: DurableTypeNone, want: "none"},
		{name: "file archive", got: DurableTypeFileArchive, want: "fileArchive"},
		{name: "spool", got: DurableTypeSpool, want: "spool"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if string(test.got) != test.want {
				t.Fatalf("DurableType = %q, want %q", test.got, test.want)
			}
		})
	}
}

func TestParseDurableType(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  DurableType
	}{
		{name: "spool", value: "  SPOOL ", want: DurableTypeSpool},
		{name: "file archive", value: "FILEARCHIVE", want: DurableTypeFileArchive},
		{name: "none", value: " NONE ", want: DurableTypeNone},
		{name: "unknown is preserved", value: " Unsupported-Store ", want: "unsupported-store"},
		{name: "empty is preserved", value: " ", want: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := ParseDurableType(test.value); got != test.want {
				t.Fatalf("ParseDurableType(%q) = %q, want %q", test.value, got, test.want)
			}
		})
	}
}

func TestValidateDurableTypes(t *testing.T) {
	const scope = "compressed log file upload failure store"
	tests := []struct {
		name      string
		types     []DurableType
		supported []DurableType
		// wantErrorは報告される不正です。Errがnilなら成功を求めます。
		wantError DurableTypeError
	}{
		{
			name:      "none alone",
			types:     []DurableType{DurableTypeNone},
			supported: []DurableType{DurableTypeFileArchive, DurableTypeSpool},
		},
		{
			name:      "file archive once",
			types:     []DurableType{DurableTypeFileArchive},
			supported: []DurableType{DurableTypeFileArchive, DurableTypeSpool},
		},
		{
			name:      "spool once",
			types:     []DurableType{DurableTypeSpool},
			supported: []DurableType{DurableTypeFileArchive, DurableTypeSpool},
		},
		{
			name:      "file archive and spool",
			types:     []DurableType{DurableTypeFileArchive, DurableTypeSpool},
			supported: []DurableType{DurableTypeFileArchive, DurableTypeSpool},
		},
		{
			name:      "empty",
			supported: []DurableType{DurableTypeFileArchive, DurableTypeSpool},
			wantError: DurableTypeError{Scope: scope, Err: ErrDurableTypesRequired},
		},
		{
			name:      "none mixed with a store",
			types:     []DurableType{DurableTypeNone, DurableTypeSpool},
			supported: []DurableType{DurableTypeFileArchive, DurableTypeSpool},
			wantError: DurableTypeError{Scope: scope, Type: DurableTypeNone, Err: ErrDurableTypeNoneNotAlone},
		},
		{
			name:      "unsupported",
			types:     []DurableType{DurableTypeSpool},
			supported: []DurableType{DurableTypeFileArchive},
			wantError: DurableTypeError{Scope: scope, Type: DurableTypeSpool, Err: ErrUnsupportedDurableType},
		},
		{
			name:      "duplicate",
			types:     []DurableType{DurableTypeSpool, DurableTypeSpool},
			supported: []DurableType{DurableTypeFileArchive, DurableTypeSpool},
			wantError: DurableTypeError{Scope: scope, Type: DurableTypeSpool, Err: ErrDuplicateDurableType},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateDurableTypes(scope, test.types, test.supported...)
			if test.wantError.Err == nil {
				if err != nil {
					t.Fatalf("ValidateDurableTypes() error = %v", err)
				}
				return
			}
			var durableTypeErr *DurableTypeError
			if !errors.As(err, &durableTypeErr) || *durableTypeErr != test.wantError ||
				!errors.Is(err, test.wantError.Err) {
				t.Fatalf("ValidateDurableTypes() error = %#v, want %#v", err, test.wantError)
			}
		})
	}
}

// DurableTypeErrorの表示形式は、利用者がどの設定のdurable typeを直すかを読むerror文字列
// なので、型ごとに一度だけ文字列で固定します。
func TestDurableTypeErrorFormat(t *testing.T) {
	const scope = "delivery failure store"
	for _, test := range []struct {
		err  *DurableTypeError
		want string
	}{
		{
			err:  &DurableTypeError{Scope: scope, Err: ErrDurableTypesRequired},
			want: "delivery failure store durable types are required",
		},
		{
			err:  &DurableTypeError{Scope: scope, Type: DurableTypeNone, Err: ErrDurableTypeNoneNotAlone},
			want: `delivery failure store durable type "none" must be specified alone`,
		},
		{
			err:  &DurableTypeError{Scope: scope, Type: DurableTypeSpool, Err: ErrUnsupportedDurableType},
			want: `unsupported delivery failure store durable type: "spool"`,
		},
		{
			err:  &DurableTypeError{Scope: scope, Type: DurableTypeFileArchive, Err: ErrDuplicateDurableType},
			want: `duplicate delivery failure store durable type: "fileArchive"`,
		},
	} {
		if got := test.err.Error(); got != test.want {
			t.Fatalf("Error() = %q, want %q", got, test.want)
		}
	}
}
