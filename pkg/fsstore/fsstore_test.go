package fsstore

import (
	"errors"
	"testing"
	"time"
)

func TestCompressFormatExtensionSupportsUserDefinedFormat(t *testing.T) {
	if got, want := CompressFormat("br").Extension(), ".br"; got != want {
		t.Fatalf("CompressFormat(br).Extension() = %q, want %q", got, want)
	}
	if got, want := CompressFormat(".custom_format-v2").Extension(), ".custom_format-v2"; got != want {
		t.Fatalf("custom CompressFormat.Extension() = %q, want %q", got, want)
	}
}

func TestCompressFormatExtensionRejectsUnsafeUserDefinedFormat(t *testing.T) {
	for _, format := range []CompressFormat{
		"",
		".",
		"tmp",
		"tar.gz",
		"../br",
		"br*",
	} {
		t.Run(string(format), func(t *testing.T) {
			if got := format.Extension(); got != "" {
				t.Fatalf("CompressFormat(%q).Extension() = %q, want empty", format, got)
			}
		})
	}
}

func TestParseFileRotation(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    FileRotationType
		wantErr bool
	}{
		{name: "daily", input: "daily", want: FileRotationDaily},
		{name: "weekly", input: "weekly", want: FileRotationWeekly},
		{name: "monthly", input: "monthly", want: FileRotationMonthly},
		{name: "empty", input: "", wantErr: true},
		{name: "unknown", input: "hourly", wantErr: true},
		{name: "non canonical", input: "DAILY", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseFileRotation(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseFileRotation(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("ParseFileRotation(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestValidateFileRotationWeekday(t *testing.T) {
	tests := []struct {
		name      string
		rotation  FileRotationType
		dayOfWeek time.Weekday
		wantErr   bool
	}{
		{name: "daily", rotation: FileRotationDaily},
		{name: "monthly", rotation: FileRotationMonthly},
		{name: "weekly Sunday", rotation: FileRotationWeekly, dayOfWeek: time.Sunday},
		{name: "weekly Saturday", rotation: FileRotationWeekly, dayOfWeek: time.Saturday},
		{name: "empty rotation ignores weekday", dayOfWeek: time.Weekday(7)},
		{name: "unknown rotation ignores weekday", rotation: FileRotationType("hourly"), dayOfWeek: time.Weekday(7)},
		{name: "invalid weekly day", rotation: FileRotationWeekly, dayOfWeek: time.Weekday(7), wantErr: true},
		{name: "negative weekly day", rotation: FileRotationWeekly, dayOfWeek: time.Weekday(-1), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateFileRotationWeekday(tt.rotation, tt.dayOfWeek)
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("ValidateFileRotationWeekday() error = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, ErrInvalidFileRotationWeekday) {
				t.Fatalf("ValidateFileRotationWeekday() error = %v, want ErrInvalidFileRotationWeekday", err)
			}
		})
	}
}
