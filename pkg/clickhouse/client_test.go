package clickhouse

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewUsesDefaultTimeoutOnlyWhenTimeoutIsZero(t *testing.T) {
	client, err := New(Config{Endpoint: "http://localhost:8123", Table: "logs"})
	require.NoError(t, err)
	assert.Equal(t, defaultTimeout, client.httpClient.Timeout)

	_, err = New(Config{Endpoint: "http://localhost:8123", Table: "logs", Timeout: -time.Second})
	require.ErrorIs(t, err, ErrNegativeTimeout)
}

func TestClientInsertLogsPostsJSONEachRow(t *testing.T) {
	var gotQuery string
	var gotWaitEndOfQuery string
	var gotRecords []LogRecord
	var gotHeader http.Header
	var gotUsername string
	var gotPassword string

	httpClient := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			require.Equal(t, http.MethodPost, r.Method)

			gotHeader = r.Header.Clone()
			gotQuery = r.URL.Query().Get("query")
			gotWaitEndOfQuery = r.URL.Query().Get(SettingWaitEndOfQuery)
			gotUsername, gotPassword, _ = r.BasicAuth()

			scanner := bufio.NewScanner(r.Body)
			for scanner.Scan() {
				var record LogRecord
				require.NoError(t, json.Unmarshal(scanner.Bytes(), &record))
				gotRecords = append(gotRecords, record)
			}
			require.NoError(t, scanner.Err())

			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     http.Header{},
				Body:       io.NopCloser(strings.NewReader("")),
			}, nil
		}),
	}

	client, err := New(Config{
		Endpoint: "http://clickhouse.local:8123",
		Database: "logs_db",
		Table:    "app_logs",
		Username: "default",
		Password: "secret",
		Headers: map[string]string{
			"X-Test": "ok",
		},
		HTTPClient: httpClient,
	})
	require.NoError(t, err)

	ts := time.Date(2026, 6, 25, 12, 34, 56, 0, time.UTC)
	res, err := client.InsertLogs(context.Background(), []LogRecord{
		{
			Timestamp: ts,
			Level:     "INFO",
			Message:   "hello",
			RequestID: "request-1",
			Fields: map[string]any{
				"item": "book",
			},
		},
	})

	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, res.StatusCode)
	assert.Equal(t, "INSERT INTO `logs_db`.`app_logs` FORMAT JSONEachRow", gotQuery)
	assert.Equal(t, "1", gotWaitEndOfQuery)
	assert.Equal(t, "application/json", gotHeader.Get("Content-Type"))
	assert.Equal(t, "logs_db", gotHeader.Get("X-ClickHouse-Database"))
	assert.Equal(t, "ok", gotHeader.Get("X-Test"))
	assert.Equal(t, "default", gotUsername)
	assert.Equal(t, "secret", gotPassword)
	require.Len(t, gotRecords, 1)
	assert.Equal(t, ts, gotRecords[0].Timestamp)
	assert.Equal(t, "INFO", gotRecords[0].Level)
	assert.Equal(t, "hello", gotRecords[0].Message)
	assert.Equal(t, "request-1", gotRecords[0].RequestID)
	assert.Equal(t, "book", gotRecords[0].Fields["item"])
}

func TestClientInsertLogsCompressesRequest(t *testing.T) {
	var gotEncoding string
	var gotBody string

	httpClient := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			gotEncoding = r.Header.Get("Content-Encoding")
			reader, err := gzip.NewReader(r.Body)
			require.NoError(t, err)
			defer reader.Close()

			body, err := io.ReadAll(reader)
			require.NoError(t, err)
			gotBody = string(body)

			return &http.Response{
				StatusCode: http.StatusNoContent,
				Status:     "204 No Content",
				Header:     http.Header{},
				Body:       io.NopCloser(strings.NewReader("")),
			}, nil
		}),
	}

	client, err := New(Config{
		Endpoint:        "http://clickhouse.local:8123",
		Table:           "app_logs",
		CompressRequest: true,
		HTTPClient:      httpClient,
	})
	require.NoError(t, err)

	_, err = client.InsertLog(context.Background(), LogRecord{
		Level:   "ERROR",
		Message: "compressed",
	})

	require.NoError(t, err)
	assert.Equal(t, "gzip", gotEncoding)
	assert.Contains(t, gotBody, `"level":"ERROR"`)
	assert.Contains(t, gotBody, `"message":"compressed"`)
	assert.True(t, strings.HasSuffix(gotBody, "\n"))
}

func TestClientInsertJSONEachRowSendsTheRowsAsGiven(t *testing.T) {
	var gotQuery, gotEncoding, gotBody string
	requests := 0
	httpClient := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			requests++
			gotQuery = r.URL.Query().Get("query")
			gotEncoding = r.Header.Get("Content-Encoding")
			reader, err := gzip.NewReader(r.Body)
			require.NoError(t, err)
			defer reader.Close()
			body, err := io.ReadAll(reader)
			require.NoError(t, err)
			gotBody = string(body)
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     http.Header{},
				Body:       io.NopCloser(strings.NewReader("")),
			}, nil
		}),
	}
	client, err := New(Config{
		Endpoint:        "http://clickhouse.local:8123",
		Database:        "otel",
		Table:           "otel_traces",
		CompressRequest: true,
		HTTPClient:      httpClient,
	})
	require.NoError(t, err)

	rows := "{\"TraceId\":\"a\"}\n{\"TraceId\":\"b\"}\n"
	_, err = client.InsertJSONEachRow(context.Background(), []byte(rows), InsertOptions{})
	require.NoError(t, err)
	assert.Equal(t, "INSERT INTO `otel`.`otel_traces` FORMAT JSONEachRow", gotQuery)
	assert.Equal(t, "gzip", gotEncoding)
	assert.Equal(t, rows, gotBody)

	// 空の本文は送りません。
	_, err = client.InsertJSONEachRow(context.Background(), nil, InsertOptions{})
	require.NoError(t, err)
	assert.Equal(t, 1, requests)
}

func TestClientInsertLogsReturnsRecordIndexWhenJSONEncodingFailsBeforeRequest(t *testing.T) {
	requestCount := 0
	httpClient := &http.Client{
		Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			requestCount++
			return nil, errors.New("request must not be sent")
		}),
	}
	client, err := New(Config{
		Endpoint:   "http://clickhouse.local:8123",
		Table:      "app_logs",
		HTTPClient: httpClient,
	})
	require.NoError(t, err)

	_, err = client.InsertLogs(context.Background(), []LogRecord{
		{Message: "valid"},
		{Message: "invalid", Fields: map[string]any{"unsupported": make(chan struct{})}},
		{Message: "not reached"},
	})

	var marshalErr *LogRecordMarshalError
	require.ErrorAs(t, err, &marshalErr)
	assert.Equal(t, 1, marshalErr.Index)
	assert.Error(t, marshalErr.Err)
	assert.Equal(t, 0, requestCount)
}

func TestClientInsertLogsWithOptionsSetsDeduplicationToken(t *testing.T) {
	for _, tc := range []struct{ name, token string }{
		{"ordinary", "insert-token-1"},
		{"surrounding whitespace is identity", " insert-token-1 "},
		{"whitespace only is specified", " \t\n "},
		{"URL and Unicode characters", "日本 +%&=?#\\\"\x00"},
		{"empty is unspecified", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			client, err := New(Config{
				Endpoint: "http://clickhouse.local:8123", Table: "app_logs",
				HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					calls++
					values := r.URL.Query()
					if tc.token == "" {
						assert.False(t, values.Has(SettingInsertDeduplicationToken), "empty token must be omitted")
						assert.False(t, values.Has(SettingInsertDeduplicate), "empty options must not override server settings")
					} else {
						assert.Equal(t, []string{tc.token}, values[SettingInsertDeduplicationToken], "distinct caller tokens must retain their identity on the wire")
						assert.Equal(t, []string{"1"}, values[SettingInsertDeduplicate])
					}
					return &http.Response{
						StatusCode: http.StatusOK, Header: make(http.Header),
						Body: io.NopCloser(strings.NewReader("")),
					}, nil
				})},
			})
			require.NoError(t, err)
			_, err = client.InsertLogsWithOptions(context.Background(), []LogRecord{{Level: "INFO", Message: "dedup"}}, InsertOptions{InsertDeduplicationToken: tc.token})
			require.NoError(t, err)
			assert.Equal(t, 1, calls)
		})
	}
}

func TestClientInsertLogsReturnsHTTPStatusError(t *testing.T) {
	httpClient := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Status:     "503 Service Unavailable",
				Header: http.Header{
					"Retry-After": []string{"2"},
				},
				Body: io.NopCloser(strings.NewReader("maintenance")),
			}, nil
		}),
	}

	client, err := New(Config{
		Endpoint:   "http://clickhouse.local:8123",
		Table:      "app_logs",
		HTTPClient: httpClient,
	})
	require.NoError(t, err)

	_, err = client.InsertLog(context.Background(), LogRecord{
		Level:   "WARN",
		Message: "will fail",
	})

	var statusErr *HTTPStatusError
	require.True(t, errors.As(err, &statusErr))
	assert.Equal(t, http.StatusServiceUnavailable, statusErr.StatusCode())
	assert.Equal(t, 2*time.Second, statusErr.RetryAfter())
	assert.Contains(t, string(statusErr.Body), "maintenance")
}

func TestClientInsertLogsRejectsClickHouseExceptionInSuccessfulHTTPResponse(t *testing.T) {
	const body = "Code: 27. DB::Exception: Cannot parse UUID. (CANNOT_PARSE_UUID) (version 26.5.3.52)"
	httpClient := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     http.Header{},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		}),
	}

	client, err := New(Config{
		Endpoint:   "http://clickhouse.local:8123",
		Table:      "app_logs",
		HTTPClient: httpClient,
	})
	require.NoError(t, err)

	_, err = client.InsertLog(context.Background(), LogRecord{Message: "invalid uuid"})

	var responseErr *HTTPStatusError
	require.ErrorAs(t, err, &responseErr)
	assert.Equal(t, http.StatusOK, responseErr.StatusCode())
	assert.Equal(t, 27, responseErr.ClickHouseExceptionCode())
	assert.Equal(t, ErrorTypeCannotParseUUID, responseErr.ClickHouseErrorType())
}

// HTTPStatusErrorの表示形式は、operatorがlogでHTTP statusとClickHouse exceptionを読む
// error文字列なので、型ごとに一度だけ文字列で固定します。bodyは表示しません。
func TestHTTPStatusErrorFormat(t *testing.T) {
	err := &HTTPStatusError{
		Code:   http.StatusOK,
		Status: "200 OK",
		Body:   []byte("Code: 27. DB::Exception: Cannot parse UUID. (CANNOT_PARSE_UUID) (version 26.5.3.52)"),
	}
	assert.Equal(
		t,
		"clickhouse response error: http_code=200 status=200 OK exception_code=27 error_type=CANNOT_PARSE_UUID",
		err.Error(),
	)
}

func TestClientInsertLogsRejectsSuccessfulHTTPResponseWithExceptionHeader(t *testing.T) {
	httpClient := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			header := make(http.Header)
			header.Set(clickHouseExceptionCodeHeader, "241")
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     header,
				Body:       io.NopCloser(strings.NewReader("")),
			}, nil
		}),
	}

	client, err := New(Config{
		Endpoint:   "http://clickhouse.local:8123",
		Table:      "app_logs",
		HTTPClient: httpClient,
	})
	require.NoError(t, err)

	_, err = client.InsertLog(context.Background(), LogRecord{Message: "late failure"})

	var responseErr *HTTPStatusError
	require.ErrorAs(t, err, &responseErr)
	assert.Equal(t, 241, responseErr.ClickHouseExceptionCode())
	assert.Empty(t, responseErr.ClickHouseErrorType())
}

func TestParseClickHouseExceptionUsesFinalErrorTypeToken(t *testing.T) {
	code, errorType := parseClickHouseException([]byte(
		"Code: 27. value contains (UNKNOWN_TABLE), parse failed. (CANNOT_PARSE_UUID) (version 26.5)",
	))

	assert.Equal(t, 27, code)
	assert.Equal(t, ErrorTypeCannotParseUUID, errorType)
}

// time.Durationで表せないRetry-Afterは、HTTP-dateと同じく最大値へ飽和させ、呼び出し側の
// 待機上限を適用させます。0にすると、長い待機を求めたserverへ即時再送してしまいます。
func TestParseRetryAfterSaturatesDurationOverflow(t *testing.T) {
	const maxDelay = time.Duration(1<<63 - 1)
	for _, tt := range []struct {
		value string
		want  time.Duration
	}{
		{value: "9223372036", want: 9223372036 * time.Second},
		{value: "9223372037", want: maxDelay},
		{value: "99999999999999999999", want: maxDelay},
		{value: "Fri, 31 Dec 9999 23:59:59 GMT", want: maxDelay},
		{value: "12a", want: 0},
		{value: "-1", want: 0},
	} {
		resp := &http.Response{Header: http.Header{"Retry-After": []string{tt.value}}}
		if got := parseRetryAfter(resp); got != tt.want {
			t.Errorf("parseRetryAfter(%q) = %s, want %s", tt.value, got, tt.want)
		}
	}
}

func TestNewValidatesEndpointAndTable(t *testing.T) {
	_, err := New(Config{Endpoint: "localhost:8123", Table: "logs"})
	require.ErrorIs(t, err, ErrInvalidEndpoint)

	_, err = New(Config{Endpoint: "http://localhost:8123"})
	require.ErrorIs(t, err, ErrTableRequired)

	_, err = New(Config{Endpoint: "http://localhost:8123?query=select", Table: "logs"})
	require.ErrorIs(t, err, ErrInvalidEndpoint)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
