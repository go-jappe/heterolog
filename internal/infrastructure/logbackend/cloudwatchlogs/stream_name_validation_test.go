package cloudwatchlogs

import (
	"errors"
	"testing"

	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
)

func TestValidateApplicationLogStreamName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		streamName    string
		wantErr       error
		wantCharacter rune
	}{
		{name: "ASCII path", streamName: "app/container/task-1"},
		{name: "Japanese and emoji", streamName: "本番/アプリ/worker-1-✅"},
		{name: "embedded ASCII space", streamName: "app worker/task-1"},
		{name: "empty", streamName: "", wantErr: pkgcloudwatchlogs.ErrLogStreamNameRequired},
		{name: "whitespace only", streamName: " ", wantErr: pkgcloudwatchlogs.ErrLogStreamNameRequired},
		{name: "AWS forbidden character", streamName: "app:worker", wantErr: pkgcloudwatchlogs.ErrLogStreamNameInvalidCharacter},
		{name: "invalid UTF-8", streamName: string([]byte{'a', 0xff, 'b'}), wantErr: ErrLogStreamNameInvalidUTF8},
		{name: "line feed", streamName: "app\nworker", wantErr: ErrLogStreamNameControlCharacter, wantCharacter: '\n'},
		{name: "carriage return", streamName: "app\rworker", wantErr: ErrLogStreamNameControlCharacter, wantCharacter: '\r'},
		{name: "horizontal tab", streamName: "app\tworker", wantErr: ErrLogStreamNameControlCharacter, wantCharacter: '\t'},
		{name: "NUL", streamName: "app\x00worker", wantErr: ErrLogStreamNameControlCharacter, wantCharacter: 0},
		{name: "escape", streamName: "app\x1bworker", wantErr: ErrLogStreamNameControlCharacter, wantCharacter: 0x1b},
		{name: "Unicode line separator", streamName: "app\u2028worker", wantErr: ErrLogStreamNameNonASCIIWhitespace, wantCharacter: '\u2028'},
		{name: "non-breaking space", streamName: "app\u00a0worker", wantErr: ErrLogStreamNameNonASCIIWhitespace, wantCharacter: '\u00a0'},
		{name: "zero width space", streamName: "app\u200bworker", wantErr: ErrLogStreamNameFormatCharacter, wantCharacter: '\u200b'},
		{name: "bidirectional override", streamName: "app\u202eworker", wantErr: ErrLogStreamNameFormatCharacter, wantCharacter: '\u202e'},
		{name: "leading space", streamName: " app/worker", wantErr: ErrLogStreamNameSurroundingWhitespace},
		{name: "trailing space", streamName: "app/worker ", wantErr: ErrLogStreamNameSurroundingWhitespace},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateApplicationLogStreamName(tt.streamName)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("ValidateApplicationLogStreamName() error = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ValidateApplicationLogStreamName() error = %v, want %v", err, tt.wantErr)
			}
			var characterErr *LogStreamNameCharacterError
			if isCharacterRule(tt.wantErr) != errors.As(err, &characterErr) {
				t.Fatalf("ValidateApplicationLogStreamName() error = %#v, want character error %t", err, isCharacterRule(tt.wantErr))
			}
			if characterErr != nil && characterErr.Character != tt.wantCharacter {
				t.Fatalf("rejected character = %U, want %U", characterErr.Character, tt.wantCharacter)
			}
		})
	}
}

func isCharacterRule(err error) bool {
	return err == ErrLogStreamNameControlCharacter || err == ErrLogStreamNameFormatCharacter || err == ErrLogStreamNameNonASCIIWhitespace
}

// 拒否した文字を利用者が特定できるよう、messageへcode pointを含めます。
func TestLogStreamNameCharacterErrorNamesCodePoint(t *testing.T) {
	t.Parallel()

	err := &LogStreamNameCharacterError{Kind: ErrLogStreamNameControlCharacter, Character: '\n'}
	if want := "cloudwatch logs log stream name must not contain control character U+000A"; err.Error() != want {
		t.Fatalf("Error() = %q, want %q", err.Error(), want)
	}
}
