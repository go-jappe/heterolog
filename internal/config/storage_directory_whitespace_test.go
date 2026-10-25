package config

import (
	"runtime"
	"testing"
)

func TestStorageDirectoryClaimsPreserveLiteralWhitespace(t *testing.T) {
	for _, tt := range []struct {
		name, first, second string
		conflict            bool
		trailingSpace       bool
	}{
		{name: "distinct ordinary directories", first: "logs", second: "other"},
		{name: "ordinary parent", first: "logs", second: "logs/child", conflict: true},
		{name: "ordinary child", first: "logs/child", second: "logs", conflict: true},
		{name: "leading space is distinct", first: " logs", second: "logs"},
		{name: "interior space is distinct", first: "log files", second: "logfiles"},
		{name: "trailing space is distinct", first: "logs ", second: "logs", trailingSpace: true},
		{name: "trailing space parent", first: "logs ", second: "logs /child", conflict: true, trailingSpace: true},
		{name: "trailing space child", first: "logs /child", second: "logs ", conflict: true, trailingSpace: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.trailingSpace && runtime.GOOS == "windows" {
				t.Skip("Windows ordinary paths do not preserve trailing-space directory names")
			}
			root := t.TempDir()
			first, err := newFileLogDirectoryClaims("first", root, "app", tt.first, "", "", false, false)
			if err != nil {
				t.Fatal(err)
			}
			second, err := newFileLogDirectoryClaims("second", root, "app", tt.second, "", "", false, false)
			if err != nil {
				t.Fatal(err)
			}
			err = validateStorageDirectoryClaims(append(first, second...))
			if (err != nil) != tt.conflict {
				t.Fatalf("directory collision = %v, want conflict %v", err, tt.conflict)
			}
		})
	}
}
