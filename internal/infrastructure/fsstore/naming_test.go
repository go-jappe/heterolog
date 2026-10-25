package fsstore

import (
	"testing"
	"time"
)

func TestExtractDateFromFileNameSupportsLogAndFailureArchiveExtensions(t *testing.T) {
	want := time.Date(2026, 7, 12, 0, 0, 0, 0, time.UTC)
	for _, filename := range []string{
		"app_20260712_1.log",
		"test-app_20260712_1.jsonl",
		"example_service_20260712_12.jsonl",
	} {
		t.Run(filename, func(t *testing.T) {
			got, err := ExtractDateFromFileName(filename)
			if err != nil {
				t.Fatalf("ExtractDateFromFileName(%q): %v", filename, err)
			}
			if !got.Equal(want) {
				t.Fatalf("ExtractDateFromFileName(%q) = %s, want %s", filename, got, want)
			}
		})
	}
}
