package env

import (
	"reflect"
	"testing"
)

func TestGetEnvSliceString(t *testing.T) {
	tests := []struct {
		name         string
		set          bool
		raw          string
		defaultValue []string
		want         []string
	}{
		{
			name:         "missing returns default unchanged",
			defaultValue: []string{" fallback "},
			want:         []string{" fallback "},
		},
		{
			name:         "empty returns default unchanged",
			set:          true,
			defaultValue: []string{" fallback "},
			want:         []string{" fallback "},
		},
		{
			name:         "whitespace only returns default unchanged",
			set:          true,
			raw:          " \t ",
			defaultValue: []string{" fallback "},
			want:         []string{" fallback "},
		},
		{
			name: "configured elements are trimmed",
			set:  true,
			raw:  " first,second , third ",
			want: []string{"first", "second", "third"},
		},
		{
			name: "empty elements are preserved",
			set:  true,
			raw:  "first, ,second,",
			want: []string{"first", "", "second", ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := "GET_ENV_SLICE_STRING_TEST"
			if tt.set {
				t.Setenv(key, tt.raw)
			}

			got := GetEnvSliceString(key, tt.defaultValue)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("GetEnvSliceString() = %#v, want %#v", got, tt.want)
			}
		})
	}
}
