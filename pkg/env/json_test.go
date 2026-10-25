package env

import (
	"testing"
)

type jsonSettings struct {
	MaxEntries int `json:"max_entries"`
	Storage    struct {
		Enabled bool `json:"enabled"`
	} `json:"storage"`
}

func TestGetEnvJSONStrictUnknownFields(t *testing.T) {
	const key = "TEST_TYPED_JSON_SETTINGS"
	for _, test := range []struct {
		name    string
		raw     string
		unknown bool
	}{
		{"known fields", `{"max_entries":7,"storage":{"enabled":true}}`, false},
		{"unknown root field", `{"max_entries":7,"max_entrys":1000,"storage":{"enabled":true}}`, true},
		{"unknown nested field", `{"max_entries":7,"storage":{"enabled":true,"enabeld":false}}`, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(key, test.raw)
			for _, strict := range []bool{false, true} {
				fallback := jsonSettings{MaxEntries: 17}
				got, err := GetEnvJSON(key, fallback, strict)
				if strict && test.unknown {
					if got != fallback {
						t.Fatalf("strict JSON must reject unknown fields and return fallback: got %+v, err %v", got, err)
					}
					assertVariableError(t, err, key, test.raw, ErrInvalidJSON)
					continue
				}
				if err != nil || got.MaxEntries != 7 || !got.Storage.Enabled {
					t.Fatalf("strict=%v: got %+v, err %v", strict, got, err)
				}
			}
		})
	}
}

func TestGetEnvJSONRejectsTrailingTokensInBothModes(t *testing.T) {
	const key = "TEST_JSON_TRAILING_TOKENS"
	for _, raw := range []string{`{"max_entries":7} {}`, `{"max_entries":7} garbage`} {
		t.Setenv(key, raw)
		for _, strict := range []bool{false, true} {
			fallback := jsonSettings{MaxEntries: 17}
			got, err := GetEnvJSON(key, fallback, strict)
			if got != fallback {
				t.Fatalf("strict=%v accepted trailing tokens in %q: got %+v, err %v", strict, raw, got, err)
			}
			assertVariableError(t, err, key, raw, ErrInvalidJSON)
		}
	}
}
