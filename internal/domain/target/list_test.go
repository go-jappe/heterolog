package target

import "testing"

func TestIsValidTarget(t *testing.T) {
	for _, name := range []string{"heterolog", "payments-api", "Billing.Service_2", "123", "COM10", "console"} {
		t.Run(name, func(t *testing.T) {
			if !IsValidTarget(name) {
				t.Errorf("valid application identifier %q was rejected", name)
			}
		})
	}
	for _, name := range []string{"", " ", ".", "..", "../logs", "logs/child", `logs\child`, "C:logs", "app:stream", "app.", "app ", " app", "app\x00", "app\n", "-app", "_app", "アプリ", "CON", "con.log", "PrN", "AUX", "nul.txt", "COM1", "com9.log", "LPT1", "lpt9"} {
		t.Run(name, func(t *testing.T) {
			if IsValidTarget(name) {
				t.Errorf("invalid application identifier %q was accepted", name)
			}
		})
	}
}
