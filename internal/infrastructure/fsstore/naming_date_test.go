package fsstore

import (
	"errors"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestValidateFileNameDateAndIndex(t *testing.T) {
	t.Parallel()

	for _, path := range []string{
		"app_20260920_1.log", "error_app_20240229_10.log.gz",
		filepath.Join("logs", "app_20260920_2.jsonl.zst"),
	} {
		if err := ValidateFileNameDateAndIndex(path); err != nil {
			t.Errorf("ValidateFileNameDateAndIndex(%q) = %v", path, err)
		}
	}
	overIntIndex := strconv.FormatUint(uint64(^uint(0)>>1)+1, 10)
	for _, path := range []string{
		"", "memo.log.gz", "app_20260229_1.log.gz", "app_20260920_0.log.gz",
		"app_20260920_01.log.gz", "app_20260920_1.log.gz.tmp",
		"app_20260920_" + overIntIndex + ".log.gz",
	} {
		var nameErr *FileNameDateAndIndexError
		if err := ValidateFileNameDateAndIndex(path); !errors.As(err, &nameErr) || nameErr.Path != path || !errors.Is(err, ErrInvalidFileNameDateAndIndex) {
			t.Errorf("ValidateFileNameDateAndIndex(%q) = %v, want error identifying the path", path, err)
		}
	}
}

func TestCompareFileNameDateIgnoresTimeAndIndex(t *testing.T) {
	t.Parallel()

	location := time.FixedZone("JST", 9*60*60)
	for _, reference := range []time.Time{
		time.Date(2026, time.September, 20, 0, 0, 0, 0, location),
		time.Date(2026, time.September, 20, 12, 30, 0, 0, location),
		time.Date(2026, time.September, 20, 23, 59, 59, 999999999, location),
	} {
		for _, test := range []struct {
			path string
			want int
		}{
			{path: "app_20260919_999.log.gz", want: -1},
			{path: "app_20260920_1.log"},
			{path: "error_app_20260920_10.log.zst"},
			{path: "app_20260920_999.jsonl.custom-format"},
			{path: filepath.Join("20260919", "app_20260920_2.log.gz")},
			{path: "app_20260921_1.log.gz", want: 1},
		} {
			got, err := CompareFileNameDate(test.path, reference)
			if err != nil || got != test.want {
				t.Fatalf("CompareFileNameDate(%q, %v) = %d, %v; want %d, nil", test.path, reference, got, err, test.want)
			}
		}
	}
}

func TestCompareFileNameDateUsesReferenceLocation(t *testing.T) {
	t.Parallel()

	instant := time.Date(2026, time.September, 20, 16, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name      string
		reference time.Time
		want      int
	}{
		{name: "UTC still previous day", reference: instant, want: 1},
		{name: "Tokyo next day", reference: instant.In(time.FixedZone("JST", 9*60*60)), want: 0},
		{name: "west of UTC still previous day", reference: instant.In(time.FixedZone("UTC-7", -7*60*60)), want: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := CompareFileNameDate("app_20260921_1.log.gz", test.reference)
			if err != nil || got != test.want {
				t.Fatalf("comparison = %d, %v; want %d, nil", got, err, test.want)
			}
		})
	}
}

func TestCompareFileNameDateCalendarBoundaries(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		fileDate      string
		referenceDate string
		want          int
	}{
		{fileDate: "20240228", referenceDate: "20240229", want: -1},
		{fileDate: "20240229", referenceDate: "20240229", want: 0},
		{fileDate: "20240301", referenceDate: "20240229", want: 1},
		{fileDate: "20240229", referenceDate: "20240301", want: -1},
		{fileDate: "20241231", referenceDate: "20250101", want: -1},
	} {
		reference, err := time.Parse("20060102", test.referenceDate)
		if err != nil {
			t.Fatal(err)
		}
		got, err := CompareFileNameDate("app_"+test.fileDate+"_1.log.gz", reference)
		if err != nil || got != test.want {
			t.Fatalf("compare %s with %s = %d, %v; want %d, nil", test.fileDate, test.referenceDate, got, err, test.want)
		}
	}
}

func TestCompareFileNameDateForMaxAgeDays(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 21, 23, 59, 0, 0, time.FixedZone("JST", 9*60*60))
	cutoff := now.AddDate(0, 0, -1)
	for _, test := range []struct {
		path    string
		expired bool
	}{
		{path: "app_20260919_10.log.gz", expired: true},
		{path: "app_20260920_1.log.gz"},
		{path: "app_20260921_1.log.gz"},
	} {
		order, err := CompareFileNameDate(test.path, cutoff)
		if err != nil {
			t.Fatal(err)
		}
		if expired := order < 0; expired != test.expired {
			t.Fatalf("expired(%q) = %t, want %t", test.path, expired, test.expired)
		}
	}
}

func TestCompareFileNameDateRejectsInvalidName(t *testing.T) {
	t.Parallel()

	reference := time.Date(2026, time.September, 20, 0, 0, 0, 0, time.UTC)
	overIntIndex := strconv.FormatUint(uint64(^uint(0)>>1)+1, 10)
	for _, path := range []string{
		"", "memo.log.gz", "app_20260229_1.log.gz", "app_20260920_0.log.gz",
		"app_20260920_01.log.gz", "app_20260920_1.log.gz.tmp",
		"app_20260920_" + overIntIndex + ".log.gz",
	} {
		t.Run(path, func(t *testing.T) {
			_, err := CompareFileNameDate(path, reference)
			var nameErr *FileNameDateAndIndexError
			if !errors.As(err, &nameErr) || nameErr.Path != path || !errors.Is(err, ErrInvalidFileNameDateAndIndex) {
				t.Fatalf("error = %v, want invalid path %q", err, path)
			}
		})
	}
}
