package fsstore

import "testing"

func TestSplitBaseAndOptionalCompressionExtensionsReturnsGenerationSeriesExtension(t *testing.T) {
	tests := []struct {
		name          string
		fileName      string
		baseExtension string
		wantStem      string
		wantExtension string
		wantOK        bool
	}{
		{
			name:          "raw log",
			fileName:      "app_20260804_1.log",
			baseExtension: "log",
			wantStem:      "app_20260804_1",
			wantExtension: ".log",
			wantOK:        true,
		},
		{
			name:          "gzip jsonl",
			fileName:      "app_20260804_1.jsonl.gz",
			baseExtension: ".jsonl",
			wantStem:      "app_20260804_1",
			wantExtension: ".jsonl.gz",
			wantOK:        true,
		},
		{
			name:          "user-defined compression",
			fileName:      "app_20260804_1.log.custom_format-v2",
			baseExtension: "log",
			wantStem:      "app_20260804_1",
			wantExtension: ".log.custom_format-v2",
			wantOK:        true,
		},
		{
			name:          "compression temporary",
			fileName:      "app_20260804_1.log.gz.tmp",
			baseExtension: "log",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stem, extension, ok := SplitBaseAndOptionalCompressionExtensions(
				test.fileName,
				test.baseExtension,
			)
			if stem != test.wantStem || extension != test.wantExtension || ok != test.wantOK {
				t.Fatalf(
					"SplitBaseAndOptionalCompressionExtensions(%q, %q) = (%q, %q, %t), want (%q, %q, %t)",
					test.fileName,
					test.baseExtension,
					stem,
					extension,
					ok,
					test.wantStem,
					test.wantExtension,
					test.wantOK,
				)
			}
		})
	}
}
