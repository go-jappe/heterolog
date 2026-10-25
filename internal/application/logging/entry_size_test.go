package logging

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestMeasureLogEntrySizeMatchesCanonicalLogicalEntry(t *testing.T) {
	payload, err := Any("payload", map[string]any{
		"z": []any{1, "two"},
		"a": true,
	})
	if err != nil {
		t.Fatalf("Any() error = %v", err)
	}

	got, err := MeasureLogEntrySize(
		"<line>\n世界",
		errors.New("boom & bad"),
		Field{},
		MustFieldKey("dup").String("one"),
		MustFieldKey("dup").Int(2),
		payload,
	)
	if err != nil {
		t.Fatalf("MeasureLogEntrySize() error = %v", err)
	}

	wantEntry := `{"message":"\u003cline\u003e\n世界","error":"boom \u0026 bad","fields":[{"key":"dup","value":"one"},{"key":"dup","value":2},{"key":"payload","value":{"a":true,"z":[1,"two"]}}]}`
	if got != len(wantEntry) {
		t.Fatalf("MeasureLogEntrySize() = %d, want %d for %s", got, len(wantEntry), wantEntry)
	}
}

func TestMeasureLogEntrySizeStringEncodingUsesCanonicalEscapes(t *testing.T) {
	message := "prefix" + string([]byte{0xff}) + "\u2028suffix"
	got, err := MeasureLogEntrySize(message, nil)
	if err != nil {
		t.Fatalf("MeasureLogEntrySize() error = %v", err)
	}

	// The canonical representation counts an invalid UTF-8 byte and U+2028 as
	// six-byte escapes, independently of the running encoding/json version.
	wantEntry := `{"message":"prefix\ufffd\u2028suffix"}`
	if got != len(wantEntry) {
		t.Fatalf("MeasureLogEntrySize() = %d, want %d for %s", got, len(wantEntry), wantEntry)
	}
}

// The canonical size bounds what an encoder writes: escaping choices may make
// the actual JSON shorter (encoding/json writes U+FFFD raw since Go 1.27), but
// never longer, so a measured entry within the limit also encodes within it.
func TestMeasureLogEntrySizeNeverUndercountsEncodingJSON(t *testing.T) {
	for _, message := range []string{
		"plain ascii",
		"<script>&amp;</script>",
		"\x00\x1f\t\n\r\b\f\"\\",
		"\u2028\u2029",
		"prefix" + string([]byte{0xff, 0xfe, 0x80}) + "suffix",
		"世界 \U0001F600",
		string([]byte{0xe4, 0xb8}),
	} {
		got, err := MeasureLogEntrySize(message, nil)
		if err != nil {
			t.Fatalf("MeasureLogEntrySize(%q) error = %v", message, err)
		}
		encoded, err := json.Marshal(struct {
			Message string `json:"message"`
		}{Message: message})
		if err != nil {
			t.Fatalf("json.Marshal(%q) error = %v", message, err)
		}
		if got < len(encoded) {
			t.Fatalf("MeasureLogEntrySize(%q) = %d, below the encoded %d bytes (%s)", message, got, len(encoded), encoded)
		}
	}
}

func TestMeasureLogEntrySizeUsesAnySnapshot(t *testing.T) {
	source := map[string]any{"state": "before"}
	field, err := Any("context", source)
	if err != nil {
		t.Fatalf("Any() error = %v", err)
	}

	before, err := MeasureLogEntrySize("message", nil, field)
	if err != nil {
		t.Fatalf("MeasureLogEntrySize() before mutation error = %v", err)
	}
	source["state"] = strings.Repeat("after", 100)
	source["new"] = true
	after, err := MeasureLogEntrySize("message", nil, field)
	if err != nil {
		t.Fatalf("MeasureLogEntrySize() after mutation error = %v", err)
	}
	if after != before {
		t.Fatalf("snapshot size after source mutation = %d, want %d", after, before)
	}
}

func TestValidateLogEntrySizeAcceptsLimitAndRejectsOnlyLargerEntry(t *testing.T) {
	field := MustFieldKey("attempt").Int(2)
	actual, err := MeasureLogEntrySize("message", errors.New("failure"), field)
	if err != nil {
		t.Fatalf("MeasureLogEntrySize() error = %v", err)
	}

	if err := ValidateLogEntrySize(actual, "message", errors.New("failure"), field); err != nil {
		t.Fatalf("ValidateLogEntrySize() at exact limit error = %v", err)
	}

	err = ValidateLogEntrySize(actual-1, "message", errors.New("failure"), field)
	if !errors.Is(err, ErrLogEntryTooLarge) {
		t.Fatalf("ValidateLogEntrySize() error = %v, want ErrLogEntryTooLarge", err)
	}
	var tooLarge *LogEntryTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("ValidateLogEntrySize() error type = %T, want *LogEntryTooLargeError", err)
	}
	if tooLarge.ActualBytes != actual || tooLarge.MaxBytes != actual-1 {
		t.Fatalf("LogEntryTooLargeError = %+v, want actual=%d max=%d", tooLarge, actual, actual-1)
	}
}

func TestValidateLogEntrySizeRejectsNonPositiveLimit(t *testing.T) {
	for _, limit := range []int{0, -1} {
		err := ValidateLogEntrySize(limit, "message", nil)
		if !errors.Is(err, pkglogger.ErrInvalidMaxLogEntryBytes) {
			t.Fatalf("ValidateLogEntrySize(%d) error = %v, want ErrInvalidMaxLogEntryBytes", limit, err)
		}
	}
}

type panickingLogError struct{}

func (panickingLogError) Error() string {
	panic("error text panic")
}

func TestMeasureLogEntrySizeConvertsPanickingErrorMethodToError(t *testing.T) {
	_, err := MeasureLogEntrySize("message", panickingLogError{})
	if !errors.Is(err, ErrLogErrorTextPanicked) {
		t.Fatalf("MeasureLogEntrySize() error = %v, want ErrLogErrorTextPanicked", err)
	}
}
