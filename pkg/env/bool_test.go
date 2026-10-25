package env

import (
	"testing"
)

func TestBoolParsingPreservesEnvironmentRules(t *testing.T) {
	const key = "TEST_ENV_BOOLEAN_RULES"
	for _, tc := range []struct {
		value         string
		want, invalid bool
	}{
		{value: "true", want: true}, {value: "1", want: true},
		{value: "yes", want: true}, {value: "on", want: true},
		{value: "false"}, {value: "0"}, {value: "no"}, {value: "off"},
		{value: "TRUE", invalid: true}, {value: " true ", invalid: true},
		{value: " ", invalid: true}, {value: key, invalid: true},
	} {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv(key, "true")
			parsed, parseErr := ParseBool(tc.value)
			if (parseErr != nil) != tc.invalid || (!tc.invalid && parsed != tc.want) {
				t.Fatalf("ParseBool(%q) = %t, %v", tc.value, parsed, parseErr)
			}
			if tc.invalid {
				// ParseBoolは環境変数を参照しないため、Keyなしで値と種類を返します。
				assertVariableError(t, parseErr, "", tc.value, ErrNotBoolean)
			}
			t.Setenv(key, tc.value)
			for _, fallback := range []bool{false, true} {
				got, err := GetEnvBool(key, fallback)
				if tc.invalid {
					if got != fallback {
						t.Fatalf("GetEnvBool = %t, %v; want fallback %t", got, err, fallback)
					}
					assertVariableError(t, err, key, tc.value, ErrNotBoolean)
				} else if err != nil || got != tc.want {
					t.Fatalf("GetEnvBool = %t, %v; want %t", got, err, tc.want)
				}
			}
		})
	}
	if _, err := ParseBool(""); err == nil {
		t.Fatal("ParseBool must leave empty-input defaulting to its caller")
	}
}
