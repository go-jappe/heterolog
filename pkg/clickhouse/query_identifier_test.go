package clickhouse

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestClientInsertPreservesQuotedIdentifiersOnWire(t *testing.T) {
	// Expectations are SQL literals, independent of the production quoting
	// helper. Backslash+n must name those two characters, not a newline.
	for _, tc := range []struct{ name, database, table, wantTarget string }{
		{"plain", "logs_db", "app_logs", "`logs_db`.`app_logs`"},
		{"table backslash", "", `logs\n2026`, "`logs\\\\n2026`"},
		{"database backslash", `logs\tprod`, "logs", "`logs\\\\tprod`.`logs`"},
		{"trailing backslash", "", `logs\`, "`logs\\\\`"},
		{"backtick", "", "logs`archive", "`logs``archive`"},
		{"backslash and backtick", "db\\`name", "logs\\`archive\\", "`db\\\\``name`.`logs\\\\``archive\\\\`"},
		{"qualified table", "", `db\prod.logs\n2026`, "`db\\\\prod`.`logs\\\\n2026`"},
		{"explicit database keeps table dot", "logs", "table.part", "`logs`.`table.part`"},
		{"spaces and Unicode", "記録 DB", "保存 ログ", "`記録 DB`.`保存 ログ`"},
		{"existing trimming", " logs_db ", " app_logs ", "`logs_db`.`app_logs`"},
		{"existing qualified trimming", "", " logs_db . app_logs ", "`logs_db`.`app_logs`"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			client, err := New(Config{
				Endpoint: "https://clickhouse.example.test", Database: tc.database, Table: tc.table,
				HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					calls++
					if got, want := r.URL.Query().Get("query"), "INSERT INTO "+tc.wantTarget+" FORMAT JSONEachRow"; got != want {
						t.Errorf("wire query=%q, want %q", got, want)
					}
					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
				})},
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.InsertLog(context.Background(), LogRecord{Message: "keep in the selected table"}); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatalf("HTTP calls=%d, want 1", calls)
			}
		})
	}
}
