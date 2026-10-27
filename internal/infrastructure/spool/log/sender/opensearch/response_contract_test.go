package opensearch

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	pkgbackend "github.com/go-jappe/heterolog/pkg/opensearch"
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
			if _, err := writer.Write([]byte(`{"items":[{"index":{"_index":"logs","_id":"first","status":201,"result":"created"}},{"index":{"_index":"logs","_id":"second","status":201,"result":"created"}}]}`)); err != nil {
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
			client, err := pkgbackend.New(pkgbackend.Config{Endpoint: server.URL, AuthConfig: pkgbackend.AuthConfig{Type: pkgbackend.AuthTypeNone}})
			if err != nil {
				t.Fatal(err)
			}
			sender, err := NewOpensearchSpoolSender(fixedClock{now: now}, client, testMaxBulkRequestBytes())
			if err != nil {
				t.Fatal(err)
			}
			records := []spool.SpoolRecord{validSpoolRecord(t, "request-first", "logs", "first"), validSpoolRecord(t, "request-second", "logs", "second")}
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

func TestSpoolSenderClassifiesIngestDropAlongsideStoredAndRejectedDocuments(t *testing.T) {
	for _, op := range []pkgbackend.WriteOp{pkgbackend.WriteOpIndex, pkgbackend.WriteOpCreate} {
		for _, responseKind := range []string{"confirmed drop", "different ID", "missing index", "contradictory status", "error with noop"} {
			t.Run(string(op)+"/"+responseKind, func(t *testing.T) {
				noop := map[string]any{"_index": "logs-000042", "_id": "filtered", "result": "noop", "status": 200}
				switch responseKind {
				case "different ID":
					noop["_id"] = "another-record"
				case "missing index":
					delete(noop, "_index")
				case "contradictory status":
					noop["status"] = 201
				case "error with noop":
					noop["error"] = map[string]any{"type": "ingest_processor_exception", "reason": "processor failed"}
				}
				response := map[string]any{
					"errors": true,
					"items": []any{
						map[string]any{string(op): noop},
						map[string]any{string(op): map[string]any{"_index": "logs-000042", "_id": "stored", "result": "created", "status": 201}},
						map[string]any{string(op): map[string]any{"_index": "logs-000042", "_id": "retry", "status": 429, "error": map[string]any{"type": "es_rejected_execution_exception", "reason": "queue full"}}},
					},
				}
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					decoder := json.NewDecoder(r.Body)
					for _, id := range []string{"filtered", "stored", "retry"} {
						var metadata map[string]struct {
							ID    string `json:"_id"`
							Index string `json:"_index"`
						}
						var document map[string]any
						if err := decoder.Decode(&metadata); err != nil {
							t.Errorf("decode request metadata: %v", err)
							w.WriteHeader(http.StatusBadRequest)
							return
						}
						if err := decoder.Decode(&document); err != nil {
							t.Errorf("decode request document: %v", err)
							w.WriteHeader(http.StatusBadRequest)
							return
						}
						if len(metadata) != 1 || metadata[string(op)].ID != id || metadata[string(op)].Index != "logs-write" || document["message"] != id {
							t.Errorf("request = %+v / %+v, want %s for %q", metadata, document, op, id)
						}
					}
					var extra any
					if err := decoder.Decode(&extra); err != io.EOF {
						t.Errorf("unexpected additional request data: value=%+v error=%v", extra, err)
					}
					w.Header().Set("Content-Type", "application/json")
					if err := json.NewEncoder(w).Encode(response); err != nil {
						t.Errorf("write response: %v", err)
					}
				}))
				t.Cleanup(server.Close)
				client, err := pkgbackend.New(pkgbackend.Config{Endpoint: server.URL, AuthConfig: pkgbackend.AuthConfig{Type: pkgbackend.AuthTypeNone}})
				if err != nil {
					t.Fatal(err)
				}
				now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
				sender, err := NewOpensearchSpoolSender(fixedClock{now: now}, client, testMaxBulkRequestBytes())
				if err != nil {
					t.Fatal(err)
				}
				var records []spool.SpoolRecord
				for _, id := range []string{"filtered", "stored", "retry"} {
					records = append(records, spoolRecordForDocument(t, id, pkgbackend.DocumentWriteRequest{
						Index: "logs-write", ID: id, Op: op, Doc: map[string]any{"message": id},
					}))
				}
				report, err := sender.Send(context.Background(), records)
				if (err != nil) != (responseKind != "confirmed drop") {
					t.Fatalf("Send error = %v, response kind = %s", err, responseKind)
				}
				if err := report.Validate(len(records)); err != nil {
					t.Fatal(err)
				}
				if calls.Load() != 1 {
					t.Fatalf("HTTP attempts = %d, want one bulk send", calls.Load())
				}
				for _, outcome := range report {
					want := spool.RecordOutcomeStatusSuccess
					if outcome.InputIndex == 2 {
						want = spool.RecordOutcomeStatusRetryable
					}
					if responseKind != "confirmed drop" {
						want = spool.RecordOutcomeStatusInvalid
					}
					if outcome.Status != want || !outcome.AttemptedAt.Equal(now) {
						t.Fatalf("outcome = %+v, want %s at %s", outcome, want, now)
					}
					if (outcome.Err != nil) != (want != spool.RecordOutcomeStatusSuccess) {
						t.Fatalf("outcome error disagrees with %s: %+v", want, outcome)
					}
				}
			})
		}
	}
}
