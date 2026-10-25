package logpattern

import (
	"strconv"
	"testing"
)

func TestParseCanonicalGenerationFilePath(t *testing.T) {
	t.Parallel()
	maxIntIndex := uint64(^uint(0) >> 1)
	maxIntIndexText := strconv.FormatUint(maxIntIndex, 10)
	overIntIndexText := strconv.FormatUint(maxIntIndex+1, 10)

	pattern := FileNamePattern{
		Prefix:    "error_",
		Target:    "app",
		Extension: "log",
	}
	tests := []struct {
		name string
		path string
		want CanonicalGenerationFile
		ok   bool
	}{
		{
			name: "raw",
			path: "/logs/error_app_20260829_1.log",
			want: CanonicalGenerationFile{Period: "20260829", Index: 1, Kind: GenerationFileKindUncompressed},
			ok:   true,
		},
		{
			name: "compressed",
			path: "/logs/error_app_20260829_42.log.gz",
			want: CanonicalGenerationFile{Period: "20260829", Index: 42, Kind: GenerationFileKindCompressed},
			ok:   true,
		},
		{
			name: "user defined compression",
			path: "error_app_20260829_2.log.custom-format",
			want: CanonicalGenerationFile{Period: "20260829", Index: 2, Kind: GenerationFileKindCompressed},
			ok:   true,
		},
		{
			name: "writer maximum int index",
			path: "error_app_20260829_" + maxIntIndexText + ".log.gz",
			want: CanonicalGenerationFile{
				Period: "20260829",
				Index:  maxIntIndex,
				Kind:   GenerationFileKindCompressed,
			},
			ok: true,
		},
		{name: "different prefix", path: "app_20260829_1.log.gz"},
		{name: "prefix only memo", path: "error_app_memo_1.log.gz"},
		{name: "invalid calendar date", path: "error_app_20260230_1.log.gz"},
		{name: "zero index", path: "error_app_20260829_0.log.gz"},
		{name: "non canonical index", path: "error_app_20260829_01.log.gz"},
		{
			name: "index exceeds writer int range",
			path: "error_app_20260829_" + overIntIndexText + ".log.gz",
		},
		{name: "compression temporary", path: "error_app_20260829_1.log.gz.tmp"},
		{name: "raw temporary", path: "error_app_20260829_1.log.tmp"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, ok := ParseCanonicalGenerationFilePath(test.path, pattern)
			if ok != test.ok || got != test.want {
				t.Fatalf(
					"ParseCanonicalGenerationFilePath(%q) = (%+v, %t), want (%+v, %t)",
					test.path,
					got,
					ok,
					test.want,
					test.ok,
				)
			}
		})
	}
}
