package logpattern

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

type rotationNameTestConfig struct {
	rotation       pkgfsstore.FileRotationType
	dayOfWeek      time.Weekday
	fileDateFormat string
}

func (c rotationNameTestConfig) RotationType() pkgfsstore.FileRotationType { return c.rotation }
func (c rotationNameTestConfig) RotationDayOfWeek() time.Weekday           { return c.dayOfWeek }
func (c rotationNameTestConfig) FileDateFormat() string {
	if c.fileDateFormat != "" {
		return c.fileDateFormat
	}
	return "Monday"
}
func (rotationNameTestConfig) FileNameFormat() string { return pkgfsstore.FileNameFormat }
func (rotationNameTestConfig) MaxFileSize() uint64    { return 1 }

func TestFindMaxIndexIgnoresNonCanonicalDecimalIndex(t *testing.T) {
	directory := t.TempDir()
	pattern := FileNamePattern{
		Dir:       directory,
		Target:    "heterolog",
		Date:      "20260802",
		Extension: "log",
	}
	for _, name := range []string{
		"heterolog_20260802_1.log",
		"heterolog_20260802_2.log",
		"heterolog_20260802_010.log",
	} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(name), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	index, err := FindMaxIndex(pattern)
	if err != nil {
		t.Fatalf("FindMaxIndex() error = %v", err)
	}
	if index != 2 {
		t.Fatalf("FindMaxIndex() = %d, want 2", index)
	}
}

func TestFindMaxIndexUsesDefaultWhenEveryMatchingIndexIsInvalid(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
	}{
		{name: "non-canonical decimal", fileName: "heterolog_20260802_010.log"},
		{name: "zero index", fileName: "heterolog_20260802_0.log"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			if err := os.WriteFile(filepath.Join(directory, tt.fileName), nil, 0o600); err != nil {
				t.Fatalf("write %s: %v", tt.fileName, err)
			}

			index, err := FindMaxIndex(FileNamePattern{
				Dir:       directory,
				Target:    "heterolog",
				Date:      "20260802",
				Extension: "log",
			})
			if err != nil {
				t.Fatalf("FindMaxIndex() error = %v", err)
			}
			if index != DefaultFileIndex {
				t.Fatalf("FindMaxIndex() = %d, want default %d", index, DefaultFileIndex)
			}
		})
	}
}

func TestFindMaxIndexIncludesCommittedCompressedGenerations(t *testing.T) {
	directory := t.TempDir()
	for _, name := range []string{
		"heterolog_20260829_2.log",
		"heterolog_20260829_5.log.gz",
		"heterolog_20260829_7.log.gz.tmp",
	} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(name), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	index, err := FindMaxIndex(FileNamePattern{
		Dir:       directory,
		Target:    "heterolog",
		Date:      "20260829",
		Extension: "log",
	})
	if err != nil {
		t.Fatalf("FindMaxIndex() error = %v", err)
	}
	if index != 5 {
		t.Fatalf("FindMaxIndex() = %d, want 5", index)
	}
}

func TestFindMaxIndexReservesCompressionSourceDeletePendingGeneration(t *testing.T) {
	directory := t.TempDir()
	for _, name := range []string{
		"heterolog_20260830_2.log",
		"heterolog_20260830_5.log" + CompressionSourceDeletePendingSuffix,
	} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(name), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	index, err := FindMaxIndex(FileNamePattern{
		Dir:       directory,
		Target:    "heterolog",
		Date:      "20260830",
		Extension: "log",
	})
	if err != nil {
		t.Fatalf("FindMaxIndex() error = %v", err)
	}
	if index != 5 {
		t.Fatalf("FindMaxIndex() = %d, want pending generation index 5", index)
	}
}

func TestResolveFilePathAtDoesNotReuseGenerationWithCompletionProof(t *testing.T) {
	tests := []struct {
		name     string
		existing []string
	}{
		{
			name:     "compressed only",
			existing: []string{"heterolog_20260829_1.log.gz"},
		},
		{
			name: "raw and compressed coexist",
			existing: []string{
				"heterolog_20260829_1.log",
				"heterolog_20260829_1.log.gz",
			},
		},
		{
			name: "compression source delete-pending only",
			existing: []string{
				"heterolog_20260829_1.log" + CompressionSourceDeletePendingSuffix,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			for _, name := range test.existing {
				if err := os.WriteFile(filepath.Join(directory, name), []byte(name), 0o600); err != nil {
					t.Fatalf("write %s: %v", name, err)
				}
			}
			pattern := FileNamePattern{
				Dir:       directory,
				Target:    "heterolog",
				Date:      "20260829",
				Extension: "log",
			}
			cfg := rotationNameTestConfig{
				rotation:       pkgfsstore.FileRotationDaily,
				fileDateFormat: "20060102",
			}
			at := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)

			path, err := ResolveFilePathAt(&pattern, cfg, at)
			if err != nil {
				t.Fatalf("ResolveFilePathAt() error = %v", err)
			}
			want := filepath.Join(directory, "heterolog_20260829_2.log")
			if path != want {
				t.Fatalf("ResolveFilePathAt() = %q, want %q", path, want)
			}
			if pattern.Index != 2 {
				t.Fatalf("FileNamePattern.Index = %d, want 2", pattern.Index)
			}
		})
	}
}

func TestSelectWritableFilePathDoesNotReuseSmallRawBeforePendingNextGeneration(t *testing.T) {
	directory := t.TempDir()
	const date = "20260830"
	currentPath := filepath.Join(directory, "heterolog_"+date+"_1.log")
	nextPath := filepath.Join(directory, "heterolog_"+date+"_2.log")
	if err := os.WriteFile(currentPath, nil, 0o600); err != nil {
		t.Fatalf("write current raw file: %v", err)
	}
	if err := os.WriteFile(CompressionSourceDeletePendingPath(nextPath), []byte("retired"), 0o600); err != nil {
		t.Fatalf("write next pending source: %v", err)
	}

	pattern := FileNamePattern{
		Dir:       directory,
		Target:    "heterolog",
		Date:      date,
		Extension: "log",
		Index:     1,
	}
	cfg := rotationNameTestConfig{
		rotation:       pkgfsstore.FileRotationDaily,
		fileDateFormat: "20060102",
	}
	path, err := selectWritableFilePathForDate(currentPath, &pattern, cfg, date)
	if err != nil {
		t.Fatalf("selectWritableFilePathForDate() error = %v", err)
	}
	want := filepath.Join(directory, "heterolog_"+date+"_3.log")
	if path != want {
		t.Fatalf("selectWritableFilePathForDate() = %q, want %q", path, want)
	}
	if pattern.Index != 3 {
		t.Fatalf("FileNamePattern.Index = %d, want 3", pattern.Index)
	}
}

func TestCompressionSourceDeletePendingPathExistsPreservesLstatSemantics(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		create func(*testing.T, string) error
		want   bool
	}{
		{name: "missing", want: false},
		{
			name: "regular pending file",
			create: func(t *testing.T, path string) error {
				return os.WriteFile(path, []byte("retired source"), 0o600)
			},
			want: true,
		},
		{
			name: "dangling symlink",
			create: func(t *testing.T, path string) error {
				testutil.Symlink(t, filepath.Join(filepath.Dir(path), "missing-target"), path)
				return nil
			},
			want: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			rawPath := filepath.Join(t.TempDir(), "heterolog_20260830_1.log")
			if test.create != nil {
				if err := test.create(t, CompressionSourceDeletePendingPath(rawPath)); err != nil {
					t.Fatalf("create pending path: %v", err)
				}
			}
			exists, err := compressionSourceDeletePendingPathExists(rawPath)
			if err != nil || exists != test.want {
				t.Fatalf("compressionSourceDeletePendingPathExists() = (%t, %v), want (%t, nil)", exists, err, test.want)
			}
		})
	}
}

func TestCompressionSourceDeletePendingPathExistsPreservesStatError(t *testing.T) {
	t.Parallel()

	// regular fileをparentにしたpathは、Windowsでは不存在に分類されます。
	// 不存在以外のerrorを伝播する契約は、両OSで確実に不正となるNULで検証します。
	rawPath := filepath.Join(t.TempDir(), "invalid\x00heterolog_20260830_1.log")
	pendingPath := CompressionSourceDeletePendingPath(rawPath)
	_, statErr := os.Lstat(pendingPath)
	var pathErr *os.PathError
	if !errors.As(statErr, &pathErr) || errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("Lstat() error = %v, want a non-not-exist PathError", statErr)
	}

	exists, err := compressionSourceDeletePendingPathExists(rawPath)
	if exists || !errors.Is(err, pathErr.Err) {
		t.Fatalf("compressionSourceDeletePendingPathExists() = (%t, %v), want (false, %v)", exists, err, pathErr.Err)
	}
	// 検査したpending pathのPathErrorを保持し、呼び出し箇所の文脈を付けてwrapします。
	var gotPathErr *os.PathError
	if !errors.As(err, &gotPathErr) || gotPathErr.Path != pendingPath || err == error(gotPathErr) {
		t.Fatalf("error context was lost: %v", err)
	}
}

func TestResolveFilePathAtUpdatesDateAndIndexesOnlyTheResolvedPeriod(t *testing.T) {
	directory := t.TempDir()
	for _, name := range []string{
		"heterolog_20260820_9.log",
		"heterolog_20260821_1.log",
		"heterolog_20260821_2.log",
	} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte("full"), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	pattern := FileNamePattern{
		Dir:       directory,
		Target:    "heterolog",
		Date:      "20260820",
		Extension: "log",
		Index:     9,
	}
	cfg := rotationNameTestConfig{
		rotation:       pkgfsstore.FileRotationDaily,
		fileDateFormat: "20060102",
	}
	resolvedAt := time.Date(2026, time.August, 21, 0, 30, 0, 0, time.FixedZone("JST", 9*60*60))

	path, err := ResolveFilePathAt(&pattern, cfg, resolvedAt)
	if err != nil {
		t.Fatalf("ResolveFilePathAt() error = %v", err)
	}
	if want := filepath.Join(directory, "heterolog_20260821_3.log"); path != want {
		t.Fatalf("ResolveFilePathAt() = %q, want %q", path, want)
	}
	if pattern.Date != "20260821" {
		t.Fatalf("FileNamePattern.Date = %q, want %q", pattern.Date, "20260821")
	}
	if pattern.Index != 3 {
		t.Fatalf("FileNamePattern.Index = %d, want 3", pattern.Index)
	}
}

func TestResolveFilePathAtKeepsNewerActivePeriodWhenClockMovesBackward(t *testing.T) {
	directory := t.TempDir()
	activePath := filepath.Join(directory, "heterolog_20260821_1.log")
	if err := os.WriteFile(activePath, nil, 0o600); err != nil {
		t.Fatalf("write active file: %v", err)
	}

	pattern := FileNamePattern{
		Dir:       directory,
		Target:    "heterolog",
		Date:      "20260821",
		Extension: "log",
		Index:     1,
	}
	cfg := rotationNameTestConfig{
		rotation:       pkgfsstore.FileRotationDaily,
		fileDateFormat: pkgfsstore.FileDateFormat,
	}
	backwardClockTime := time.Date(2026, time.August, 20, 23, 30, 0, 0, time.FixedZone("JST", 9*60*60))

	path, err := ResolveFilePathAt(&pattern, cfg, backwardClockTime)
	if err != nil {
		t.Fatalf("ResolveFilePathAt() error = %v", err)
	}
	if path != activePath {
		t.Fatalf("ResolveFilePathAt() = %q, want active path %q", path, activePath)
	}
	if pattern.Date != "20260821" || pattern.Index != 1 {
		t.Fatalf("FileNamePattern = date %q index %d, want date 20260821 index 1", pattern.Date, pattern.Index)
	}
}

func TestGetDateStringForFileNameUsesConfiguredWeeklyRotationDay(t *testing.T) {
	cfg := rotationNameTestConfig{
		rotation:  pkgfsstore.FileRotationWeekly,
		dayOfWeek: time.Thursday,
	}

	got, err := ResolveDateStringForFileName(cfg)
	if err != nil {
		t.Fatalf("ResolveDateStringForFileName() error = %v", err)
	}
	if want := "Thursday"; got != want {
		t.Fatalf("ResolveDateStringForFileName() = %q, want configured weekday %q", got, want)
	}
}

func TestResolveDateStringForFileNameUsesProvidedTimezoneForWeeklyPeriod(t *testing.T) {
	// UTCでは木曜日ですが、FileLoggerが渡すJSTでは金曜日です。
	now := time.Date(2026, time.August, 21, 0, 30, 0, 0, time.FixedZone("JST", 9*60*60))
	cfg := rotationNameTestConfig{
		rotation:       pkgfsstore.FileRotationWeekly,
		dayOfWeek:      time.Thursday,
		fileDateFormat: pkgfsstore.FileDateFormat,
	}

	got, err := ResolveDateStringForFileNameAt(cfg, now)
	if err != nil {
		t.Fatalf("ResolveDateStringForFileNameAt() error = %v", err)
	}
	if want := "20260820"; got != want {
		t.Fatalf("ResolveDateStringForFileNameAt() = %q, want weekly period %q", got, want)
	}
}

func TestResolveDateStringForFileNameUsesDailyForUnknownRotation(t *testing.T) {
	now := time.Date(2026, time.August, 16, 12, 0, 0, 0, time.FixedZone("JST", 9*60*60))
	cfg := rotationNameTestConfig{
		rotation:       pkgfsstore.FileRotationType("hourly"),
		dayOfWeek:      time.Sunday,
		fileDateFormat: "20060102",
	}

	got, err := ResolveDateStringForFileNameAt(cfg, now)
	if err != nil {
		t.Fatalf("ResolveDateStringForFileNameAt() error = %v, want nil", err)
	}
	if want := "20260816"; got != want {
		t.Fatalf("ResolveDateStringForFileNameAt() = %q, want daily fallback %q", got, want)
	}
}

func TestRotateByDateUsesDailyForUnknownRotation(t *testing.T) {
	now := time.Date(2026, time.August, 16, 12, 0, 0, 0, time.FixedZone("JST", 9*60*60))
	path := filepath.Join(t.TempDir(), "heterolog_20260815_1.log")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create active file: %v", err)
	}
	defer file.Close()

	rotationCalled := false
	rotated, err := rotateByDateAt(
		context.Background(),
		file,
		rotationNameTestConfig{
			rotation:       pkgfsstore.FileRotationType("hourly"),
			fileDateFormat: pkgfsstore.FileDateFormat,
		},
		&FileNamePattern{},
		func(context.Context) error {
			rotationCalled = true
			return nil
		},
		now,
	)
	if err != nil {
		t.Fatalf("rotateByDateAt() error = %v, want nil", err)
	}
	if !rotated {
		t.Fatal("rotateByDateAt() rotated = false, want true for daily fallback")
	}
	if !rotationCalled {
		t.Fatal("rotateByDateAt() did not invoke rotation for daily fallback")
	}
}

func TestRotateByDateAtReportsRotationOutcome(t *testing.T) {
	now := time.Date(2026, time.August, 16, 12, 0, 0, 0, time.UTC)
	rotationFailure := errors.New("rotation failed")

	tests := []struct {
		name        string
		fileDate    string
		rotate      func(context.Context) error
		wantRotated bool
		wantErr     error
	}{
		{
			name:     "condition not met",
			fileDate: "20260816",
			rotate: func(context.Context) error {
				t.Fatal("rotate was called when date condition was not met")
				return nil
			},
		},
		{
			name:        "rotation succeeded",
			fileDate:    "20260815",
			rotate:      func(context.Context) error { return nil },
			wantRotated: true,
		},
		{
			name:     "rotation failed",
			fileDate: "20260815",
			rotate: func(context.Context) error {
				return rotationFailure
			},
			wantErr: rotationFailure,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "heterolog_"+tt.fileDate+"_1.log")
			file, err := os.Create(path)
			if err != nil {
				t.Fatalf("create active file: %v", err)
			}
			t.Cleanup(func() { _ = file.Close() })

			rotated, err := rotateByDateAt(
				context.Background(),
				file,
				rotationNameTestConfig{
					rotation:       pkgfsstore.FileRotationDaily,
					fileDateFormat: pkgfsstore.FileDateFormat,
				},
				&FileNamePattern{},
				tt.rotate,
				now,
			)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("rotateByDateAt() error = %v, want %v", err, tt.wantErr)
			}
			if rotated != tt.wantRotated {
				t.Fatalf("rotateByDateAt() rotated = %t, want %t", rotated, tt.wantRotated)
			}
		})
	}
}

func TestCheckAndRotateAtDoesNotRotateBackwardWhenClockMovesBackward(t *testing.T) {
	path := filepath.Join(t.TempDir(), "heterolog_20260821_1.log")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create active file: %v", err)
	}
	t.Cleanup(func() { _ = file.Close() })

	fileName := FileNamePattern{Date: "20260821", Index: 1}
	rotations := 0
	err = CheckAndRotateAt(
		context.Background(),
		file,
		rotationNameTestConfig{
			rotation:       pkgfsstore.FileRotationDaily,
			fileDateFormat: pkgfsstore.FileDateFormat,
			// active file is empty, so size rotation is not selected either.
		},
		&fileName,
		time.Date(2026, time.August, 20, 23, 30, 0, 0, time.FixedZone("JST", 9*60*60)),
		func(context.Context) error {
			rotations++
			return nil
		},
	)
	if err != nil {
		t.Fatalf("CheckAndRotateAt() error = %v", err)
	}
	if rotations != 0 {
		t.Fatalf("rotation count = %d, want 0", rotations)
	}
	if fileName.Date != "20260821" || fileName.Index != 1 {
		t.Fatalf("FileNamePattern = date %q index %d, want date 20260821 index 1", fileName.Date, fileName.Index)
	}
}

func TestCheckAndRotateDoesNotStatOldFileAfterDateRotation(t *testing.T) {
	yesterday := time.Now().AddDate(0, 0, -1).Format("20060102")
	path := filepath.Join(t.TempDir(), "heterolog_"+yesterday+"_1.log")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create active file: %v", err)
	}
	t.Cleanup(func() { _ = file.Close() })

	rotations := 0
	err = CheckAndRotate(
		context.Background(),
		file,
		rotationNameTestConfig{
			rotation:       pkgfsstore.FileRotationDaily,
			fileDateFormat: "20060102",
		},
		&FileNamePattern{},
		func(context.Context) error {
			rotations++
			return file.Close()
		},
	)
	if err != nil {
		t.Fatalf("CheckAndRotate() error = %v, want nil after successful date rotation", err)
	}
	if rotations != 1 {
		t.Fatalf("rotation count = %d, want 1", rotations)
	}
}

// ResolveFilePathAtは、log directoryを列挙できない原因のfilesystem errorを
// errors.Asで辿れる形で返します。directoryがfileに置き換わった状態で確認します。
func TestResolveFilePathAtKeepsDirectoryListingErrorAsCause(t *testing.T) {
	notDirectory := filepath.Join(t.TempDir(), "logs")
	if err := os.WriteFile(notDirectory, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	pattern := FileNamePattern{Dir: notDirectory, Target: "app", Extension: "log"}
	cfg := rotationNameTestConfig{rotation: pkgfsstore.FileRotationDaily, fileDateFormat: pkgfsstore.FileDateFormat}
	at := time.Date(2026, time.September, 13, 12, 0, 0, 0, time.UTC)

	_, err := ResolveFilePathAt(&pattern, cfg, at)
	var pathErr *os.PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("ResolveFilePathAt() error = %v, want the directory listing *os.PathError as its cause", err)
	}
	if pathErr.Path != notDirectory {
		t.Fatalf("listing error path = %q, want %q", pathErr.Path, notDirectory)
	}
}
