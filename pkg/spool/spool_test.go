package spool

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// NotBeforeを持たない既存のspool fileは制限なしとして読み込み、zero valueは
// 保存形式へ追加しない。期限を保存した場合は再起動後も同じ時刻を復元する。
func TestSpoolRecordNotBeforeIsBackwardCompatible(t *testing.T) {
	occurredAt := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	legacyLine := `{"OccurredAt":"2026-10-21T00:00:00Z","FinalizedAt":"0001-01-01T00:00:00Z",` +
		`"FirstResendAttemptedAt":"0001-01-01T00:00:00Z","LastResendAttemptedAt":"0001-01-01T00:00:00Z",` +
		`"ResendAttemptCount":0,"RequestID":"request-1","Payload":{"message":"keep"}}`
	var legacy SpoolRecord
	if err := json.Unmarshal([]byte(legacyLine), &legacy); err != nil {
		t.Fatalf("Unmarshal legacy record returned error: %v", err)
	}
	if !legacy.NotBefore.IsZero() {
		t.Fatalf("legacy NotBefore = %s, want zero", legacy.NotBefore)
	}

	unrestricted, err := json.Marshal(SpoolRecord{OccurredAt: occurredAt, Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatalf("Marshal unrestricted record returned error: %v", err)
	}
	if strings.Contains(string(unrestricted), "NotBefore") {
		t.Fatalf("unrestricted record JSON = %s, want no NotBefore field", unrestricted)
	}

	notBefore := time.Date(2026, 10, 21, 0, 0, 30, 0, time.UTC)
	deferred, err := json.Marshal(SpoolRecord{OccurredAt: occurredAt, NotBefore: notBefore, Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatalf("Marshal deferred record returned error: %v", err)
	}
	var restored SpoolRecord
	if err := json.Unmarshal(deferred, &restored); err != nil {
		t.Fatalf("Unmarshal deferred record returned error: %v", err)
	}
	if !restored.NotBefore.Equal(notBefore) {
		t.Fatalf("restored NotBefore = %s, want %s", restored.NotBefore, notBefore)
	}
}
