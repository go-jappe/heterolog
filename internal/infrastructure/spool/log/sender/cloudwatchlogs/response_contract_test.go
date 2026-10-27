package cloudwatchlogs

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	pkgbackend "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	"github.com/go-jappe/heterolog/pkg/spool"
)

// default HTTP transportのgzip展開と実際のclientを動かし、checksumの失敗後に
// parseできるJSONの先頭部分でrecordが解放されないことを確認します。
func TestSpoolSenderVerifiesCompressedResponseBeforeSuccess(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		name := "valid checksum"
		if corrupt {
			name = "invalid checksum"
		}
		t.Run(name, func(t *testing.T) {
			var compressed bytes.Buffer
			writer := gzip.NewWriter(&compressed)
			if _, err := writer.Write([]byte(`{}`)); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			payload := compressed.Bytes()
			if corrupt {
				payload[len(payload)-8] ^= 0xff
			}
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Encoding", "gzip")
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(payload)
			}))
			t.Cleanup(server.Close)
			now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
			client, err := pkgbackend.New(pkgbackend.Config{Endpoint: server.URL, Region: "ap-northeast-1", LogGroupName: "/app/test", AccessKeyID: "test", SecretAccessKey: "test"})
			if err != nil {
				t.Fatal(err)
			}
			sender, err := NewCloudWatchLogsSpoolSender(fixedClock{now: now}, client)
			if err != nil {
				t.Fatal(err)
			}
			records := []spool.SpoolRecord{cloudWatchLogsSpoolRecord(t, "first", "first message"), cloudWatchLogsSpoolRecord(t, "second", "second message")}
			report, err := sender.Send(context.Background(), records)
			if err != nil {
				t.Fatal(err)
			}
			if err := report.Validate(len(records)); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 {
				t.Fatalf("HTTP attempts=%d, want 1", calls.Load())
			}
			for _, outcome := range report {
				want := spool.RecordOutcomeStatusSuccess
				if corrupt {
					want = spool.RecordOutcomeStatusInvalid
				}
				if outcome.Status != want || !outcome.AttemptedAt.Equal(now) {
					t.Fatalf("outcome=%+v, want %s at %s", outcome, want, now)
				}
				if corrupt {
					var decodeErr *pkgbackend.ResponseDecodeError
					if !errors.As(outcome.Err, &decodeErr) || !errors.Is(outcome.Err, gzip.ErrChecksum) {
						t.Fatalf("outcome error=%v, want ResponseDecodeError wrapping checksum failure", outcome.Err)
					}
				}
			}
		})
	}
}
