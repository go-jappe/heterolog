package logger

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

// selfNotificationBackendModeは、fake backendが全てのrequestに返す応答です。条件は試験の間
// 変わりません。
type selfNotificationBackendMode string

const (
	// selfNotificationBackendErrorは、時計が進んでからerrorのstatusを返します。
	selfNotificationBackendError selfNotificationBackendMode = "backend_error"
	// selfNotificationBackendTimeoutは応答せず、clientのtimeoutでrequestを終わらせます。
	selfNotificationBackendTimeout selfNotificationBackendMode = "backend_timeout"
	// selfNotificationBackendSlowは、時計が進んでから成功を返します（送信遅延が閾値を超えます）。
	selfNotificationBackendSlow selfNotificationBackendMode = "slow_success"
)

// selfNotificationClientTimeoutは、backend_timeoutでclientがrequestを打ち切るまでの時間です。
// 送信先の停止を表す条件で、試験の待機には使いません。
const selfNotificationClientTimeout = 50 * time.Millisecond

// selfNotificationBackendは、fake backendが受け取ったrequestの本文を記録します。
type selfNotificationBackend struct {
	signal selfNotificationSignal
	mu     sync.Mutex
	bodies []string
	// textは、request本文から内部ログとapplicationのログを数える文字列を取り出します。
	text func(body string) string
	// internalMarkerは、textの中で内部ログ一件ごとに一度だけ現れる文字列です。
	internalMarker string
}

func newSelfNotificationBackend(internalMarker string, text func(string) string) *selfNotificationBackend {
	if text == nil {
		text = func(body string) string { return body }
	}
	return &selfNotificationBackend{text: text, internalMarker: internalMarker}
}

func (b *selfNotificationBackend) record(body string) {
	b.mu.Lock()
	b.bodies = append(b.bodies, body)
	b.mu.Unlock()
	b.signal.notify()
}

func (b *selfNotificationBackend) texts() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	texts := make([]string, 0, len(b.bodies))
	for _, body := range b.bodies {
		texts = append(texts, b.text(body))
	}
	return texts
}

// internalEntriesは、受け取った全requestの内部ログの件数です。
func (b *selfNotificationBackend) internalEntries() int {
	total := 0
	for _, text := range b.texts() {
		total += strings.Count(text, b.internalMarker)
	}
	return total
}

// internalRequestsは、内部ログを含むrequest（通知先のfallbackの送信）の数です。
func (b *selfNotificationBackend) internalRequests() int {
	count := 0
	for _, text := range b.texts() {
		if strings.Contains(text, b.internalMarker) {
			count++
		}
	}
	return count
}

// applicationRequestsは、内部ログを含まずapplicationのログを含むrequest（通知元の送信）の数です。
// Delivery Slackの内部ログは元の記録のmessageを含むため、内部ログを含むrequestは除きます。
func (b *selfNotificationBackend) applicationRequests() int {
	count := 0
	for _, text := range b.texts() {
		if strings.Contains(text, selfNotificationApplicationMessage) && !strings.Contains(text, b.internalMarker) {
			count++
		}
	}
	return count
}

// waitForRequestsは、requestがwant件に達するまで待ちます。
func (b *selfNotificationBackend) waitForRequests(t *testing.T, want int) {
	t.Helper()
	count := func() int {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.bodies)
	}
	if !b.signal.waitUntil(func() bool { return count() >= want }) {
		t.Fatalf("backend received %d requests, want %d:\n%s", count(), want, strings.Join(b.texts(), "\n"))
	}
}

func (b *selfNotificationBackend) String() string {
	return strings.Join(b.texts(), "\n")
}

// waitForSelfNotificationMeasurableLatencyは、monotonic clockが進むまで戻りません。送信遅延の
// 閾値は1nsなので、時計の分解能（Windowsの約15.6ms）でも計測値が閾値を超えます。
func waitForSelfNotificationMeasurableLatency() {
	start := time.Now()
	for !time.Now().After(start) {
		time.Sleep(time.Millisecond)
	}
}

// selfNotificationTransportは、Loki・CloudWatch Logs・ClickHouse・SlackのHTTP clientへ渡す
// fake backendです。
type selfNotificationTransport struct {
	backend     *selfNotificationBackend
	mode        selfNotificationBackendMode
	errorStatus int
	errorBody   string
	successBody string
}

func (tr *selfNotificationTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	var body []byte
	if request.Body != nil {
		var err error
		body, err = io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
	}
	tr.backend.record(string(body))
	if tr.mode == selfNotificationBackendTimeout {
		<-request.Context().Done()
		return nil, request.Context().Err()
	}
	waitForSelfNotificationMeasurableLatency()
	status, responseBody := http.StatusOK, tr.successBody
	if tr.mode == selfNotificationBackendError {
		status, responseBody = tr.errorStatus, tr.errorBody
	}
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(responseBody)),
		Request:    request,
	}, nil
}

func (tr *selfNotificationTransport) client() *http.Client {
	client := &http.Client{Transport: tr}
	if tr.mode == selfNotificationBackendTimeout {
		client.Timeout = selfNotificationClientTimeout
	}
	return client
}

// selfNotificationNetworkCaseは、network出力が自分自身を通知先に選んだ一つの構成です。
type selfNotificationNetworkCase struct {
	name     string
	contract selfNotificationContract
	mode     selfNotificationBackendMode
	// buildはfake backendとloggerの設定を作ります。
	build func(t *testing.T, mode selfNotificationBackendMode) selfNotificationNetworkSetup
	// internalPerEventは、元の記録一件あたりの内部ログの件数です。
	internalPerEvent int
	// wantErrorsは、同期API・Delivery APIの各記録がerrorを返すかです。
	wantErrors bool
	// awaitEachSendなら、各記録の後に通知元と通知先の送信を待ってから次を記録します。
	// 送信をtimerやworkerで行うSlack・Delivery ClickHouseで、記録ごとの送信を確定させます。
	awaitEachSend bool
}

// selfNotificationNetworkSetupは、構築に使う設定と観測先です。
type selfNotificationNetworkSetup struct {
	backend     *selfNotificationBackend
	cfg         pkglogger.LoggerConfig
	deliveryCfg pkglogger.DeliveryLoggerConfig
	// fallbackFailureは、通知先のfallbackが送信の失敗を標準loggerへ残す行の一部です。
	fallbackFailure string
	// fallbackRequestsFailは、通知先のfallbackの送信もbackendの条件で失敗するかです。
	fallbackRequestsFail bool
}

func selfNotificationDeliveryNoneFailureStore() pkgdurable.DeliveryFailureStoreConfig {
	return pkgdurable.DeliveryFailureStoreConfig{
		DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
	}
}

func selfNotificationNoneFailureStore() pkgdurable.FailureStoreConfig {
	return pkgdurable.FailureStoreConfig{
		DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
	}
}

func buildSelfNotificationLoki(t *testing.T, mode selfNotificationBackendMode) selfNotificationNetworkSetup {
	t.Helper()
	backend := newSelfNotificationBackend(selfNotificationInternalMarker, nil)
	transport := &selfNotificationTransport{
		backend:     backend,
		mode:        mode,
		errorStatus: http.StatusInternalServerError,
		errorBody:   `{"error":"self-notification backend failure"}`,
	}
	self := selfNotificationHandlers(pkglogger.LogOutputLoki)
	// NoBulk modeで記録ごとに一回pushします。push遅延の閾値は1nsです。
	lokiCfg := fallbackCloseLokiConfig(self)
	deliveryLokiCfg := fallbackCloseDeliveryLokiConfig(self)
	client := &pkgloki.Config{Endpoint: "http://loki.test", HTTPClient: transport.client()}
	outputs := pkglogger.LogOutputDestinations{pkglogger.LogOutputLoki}
	return selfNotificationNetworkSetup{
		backend: backend,
		cfg:     pkglogger.LoggerConfig{LogOutputDestinations: outputs, Loki: &lokiCfg, LokiClient: client},
		deliveryCfg: pkglogger.DeliveryLoggerConfig{
			LogOutputDestinations: outputs,
			Loki:                  &deliveryLokiCfg,
			LokiClient:            client,
			InternalLogBatchConfig: pkglogger.DeliveryInternalLogBatchConfig{
				Loki: pkglogger.DeliveryInternalLogLokiBatchConfig{
					FlushInterval:           time.Hour,
					MaxEntries:              1,
					BufferFlushBytes:        fallbackLokiBufferFlushBytes,
					FallbackQueueMaxEntries: fallbackLokiQueueMaxEntries,
				},
			},
		},
		fallbackFailure:      "failed to flush Loki fallback logger",
		fallbackRequestsFail: mode != selfNotificationBackendSlow,
	}
}

func buildSelfNotificationCloudWatchLogs(t *testing.T, mode selfNotificationBackendMode) selfNotificationNetworkSetup {
	t.Helper()
	backend := newSelfNotificationBackend(selfNotificationInternalMarker, nil)
	transport := &selfNotificationTransport{
		backend:     backend,
		mode:        mode,
		errorStatus: http.StatusInternalServerError,
		errorBody:   `{"__type":"ServiceUnavailableException","message":"self-notification backend failure"}`,
		successBody: `{}`,
	}
	self := selfNotificationHandlers(pkglogger.LogOutputCloudWatchLogs)
	runtime := pkglogger.RuntimeConfig{AppName: "self-notification-cloudwatchlogs", Target: "test", LogLevel: pkglogger.LogLevelDebug}
	body := pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano}
	cfg := &pkglogger.CloudWatchLogsConfig{
		CloudWatchLogsLoggerBaseConfig: pkglogger.CloudWatchLogsLoggerBaseConfig{
			RuntimeConfig:          runtime,
			LogBodyConfig:          body,
			FlushInterval:          time.Hour,
			BufferFlushMaxEvents:   1,
			BufferFlushBytes:       1 << 20,
			FallbackQueueMaxEvents: 10,
			NoBulkFlushModeEnabled: true,
			HandlerConfig:          self,
		},
		CloudWatchLogsFlushManagerConfig: pkglogger.CloudWatchLogsFlushManagerConfig{
			FailureStoreConfig:         selfNotificationNoneFailureStore(),
			Resource:                   activationResource(),
			LogGroupName:               "/self/notification",
			LogStreamName:              "stream",
			PostErrorThresholdDuration: time.Nanosecond,
			PutLatencyEnabled:          true,
		},
	}
	deliveryCfg := &pkglogger.DeliveryCloudWatchLogsConfig{
		DeliveryCloudWatchLogsLoggerBaseConfig: pkglogger.DeliveryCloudWatchLogsLoggerBaseConfig{
			RuntimeConfig: pkglogger.DeliveryRuntimeConfig{AppName: runtime.AppName, Target: runtime.Target},
			LogBodyConfig: body,
			HandlerConfig: self,
		},
		DeliveryCloudWatchLogsFlushManagerConfig: pkglogger.DeliveryCloudWatchLogsFlushManagerConfig{
			FailureStoreConfig:         selfNotificationDeliveryNoneFailureStore(),
			Resource:                   activationResource(),
			LogGroupName:               "/self/notification",
			LogStreamName:              "stream",
			PostErrorThresholdDuration: time.Nanosecond,
			PutLatencyEnabled:          true,
		},
	}
	client := &pkgcloudwatchlogs.Config{
		Region:          "ap-northeast-1",
		Endpoint:        "http://cloudwatchlogs.test",
		LogGroupName:    "/self/notification",
		AccessKeyID:     "self-notification-access-key",
		SecretAccessKey: "self-notification-secret-key",
		HTTPClient:      transport.client(),
	}
	outputs := pkglogger.LogOutputDestinations{pkglogger.LogOutputCloudWatchLogs}
	return selfNotificationNetworkSetup{
		backend: backend,
		cfg:     pkglogger.LoggerConfig{LogOutputDestinations: outputs, CloudWatchLogs: cfg, CloudWatchLogsClient: client},
		deliveryCfg: pkglogger.DeliveryLoggerConfig{
			LogOutputDestinations: outputs,
			CloudWatchLogs:        deliveryCfg,
			CloudWatchLogsClient:  client,
			InternalLogBatchConfig: pkglogger.DeliveryInternalLogBatchConfig{
				CloudWatchLogs: pkglogger.DeliveryInternalLogCloudWatchLogsBatchConfig{
					FlushInterval:          time.Hour,
					BufferFlushMaxEvents:   1,
					BufferFlushBytes:       1 << 20,
					FallbackQueueMaxEvents: 10,
				},
			},
		},
		fallbackFailure:      "failed to flush CloudWatch Logs fallback logger",
		fallbackRequestsFail: mode != selfNotificationBackendSlow,
	}
}

func buildSelfNotificationClickHouse(t *testing.T, mode selfNotificationBackendMode) selfNotificationNetworkSetup {
	t.Helper()
	backend := newSelfNotificationBackend(`"line":"`+selfNotificationInternalMarker, nil)
	transport := &selfNotificationTransport{
		backend:     backend,
		mode:        mode,
		errorStatus: http.StatusInternalServerError,
		errorBody:   `Code: 999. DB::Exception: self-notification backend failure`,
	}
	self := selfNotificationHandlers(pkglogger.LogOutputClickHouse)
	// 件数の閾値1で、記録ごとにその場でINSERTします。INSERT遅延の閾値は1nsです。
	cfg := fallbackIsolationClickHouseConfig()
	cfg.ClickHouseLoggerBaseConfig.MaxEntries = 1
	cfg.ClickHouseLoggerBaseConfig.FallbackQueueMaxEntries = 10
	cfg.ClickHouseLoggerBaseConfig.HandlerConfig = self
	cfg.ClickHouseFlushManagerConfig.PostErrorThresholdDuration = time.Nanosecond
	cfg.ClickHouseFlushManagerConfig.InsertLatencyEnabled = true
	deliveryCfg := fallbackIsolationDeliveryClickHouseConfig()
	deliveryCfg.DeliveryClickHouseLoggerBaseConfig.MaxEntries = 1
	deliveryCfg.DeliveryClickHouseLoggerBaseConfig.FallbackQueueMaxEntries = 10
	deliveryCfg.DeliveryClickHouseLoggerBaseConfig.HandlerConfig = self
	deliveryCfg.DeliveryClickHouseFlushManagerConfig.PostErrorThresholdDuration = time.Nanosecond
	deliveryCfg.DeliveryClickHouseFlushManagerConfig.InsertLatencyEnabled = true
	client := &pkgclickhouse.Config{Endpoint: "http://clickhouse.test", Table: "logs", HTTPClient: transport.client()}
	outputs := pkglogger.LogOutputDestinations{pkglogger.LogOutputClickHouse}
	return selfNotificationNetworkSetup{
		backend: backend,
		cfg:     pkglogger.LoggerConfig{LogOutputDestinations: outputs, ClickHouse: &cfg, ClickHouseClient: client},
		deliveryCfg: pkglogger.DeliveryLoggerConfig{
			LogOutputDestinations: outputs,
			ClickHouse:            &deliveryCfg,
			ClickHouseClient:      client,
		},
		fallbackFailure:      "ClickHouse fallback logger dropped",
		fallbackRequestsFail: mode != selfNotificationBackendSlow,
	}
}

// buildSelfNotificationOpenSearchは、localのfake OpenSearch serverを使います。OpenSearchの
// clientはHTTP clientを差し替えられないためです。documentの登録は時計が進んでから成功し
// （index遅延が閾値1nsを超えます）、通知先のfallbackが使うbulk送信はerrorを返します。
func buildSelfNotificationOpenSearch(t *testing.T, mode selfNotificationBackendMode) selfNotificationNetworkSetup {
	t.Helper()
	require.Equal(t, selfNotificationBackendSlow, mode, "the fake OpenSearch server only implements slow successful indexing")
	backend := newSelfNotificationBackend(`"internal":true`, nil)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		backend.record(string(body))
		waitForSelfNotificationMeasurableLatency()
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/_create/") {
			parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(pkgopensearch.IndexResponse{
				Index: parts[0], ID: parts[len(parts)-1], Version: 1, Result: "created", Status: http.StatusCreated,
			})
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)
	self := selfNotificationHandlers(pkglogger.LogOutputOpenSearch)
	runtime := pkglogger.RuntimeConfig{AppName: "self-notification-opensearch", Target: "test", LogLevel: pkglogger.LogLevelDebug}
	body := pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano}
	cfg := testOpenSearchFailureSpoolConfig(selfNotificationNoneFailureStore())
	cfg.OpenSearchLoggerBaseConfig.NoBulkFlushModeEnabled = true
	// MaxBulkDocumentは通知先のfallbackのqueueの上限も兼ね、満杯なら最古の内部ログを捨てます。
	// 全ての内部ログを数えるため、記録の件数より大きくします（fallbackはCloseで送信します）。
	cfg.OpenSearchLoggerBaseConfig.MaxBulkDocument = 10
	cfg.OpenSearchLoggerBaseConfig.RuntimeConfig = runtime
	cfg.OpenSearchLoggerBaseConfig.LogBodyConfig = body
	cfg.OpenSearchLoggerBaseConfig.HandlerConfig = self
	cfg.OpenSearchFlushManagerConfig.Index = "self-notification"
	cfg.OpenSearchFlushManagerConfig.Resource = activationResource()
	cfg.OpenSearchFlushManagerConfig.PostErrorThresholdDuration = time.Nanosecond
	cfg.OpenSearchFlushManagerConfig.IndexLatencyEnabled = true
	deliveryCfg := &pkglogger.DeliveryOpenSearchConfig{
		DeliveryOpenSearchLoggerBaseConfig: pkglogger.DeliveryOpenSearchLoggerBaseConfig{
			RuntimeConfig: pkglogger.DeliveryRuntimeConfig{AppName: runtime.AppName, Target: runtime.Target},
			LogBodyConfig: body,
			HandlerConfig: self,
		},
		DeliveryOpenSearchFlushManagerConfig: pkglogger.DeliveryOpenSearchFlushManagerConfig{
			Index:                      "self-notification",
			FailureStoreConfig:         selfNotificationDeliveryNoneFailureStore(),
			Resource:                   activationResource(),
			PostErrorThresholdDuration: time.Nanosecond,
			IndexLatencyEnabled:        true,
		},
	}
	client := testOpenSearchClient(server.URL)
	outputs := pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
	return selfNotificationNetworkSetup{
		backend: backend,
		cfg:     pkglogger.LoggerConfig{LogOutputDestinations: outputs, OpenSearch: cfg, OpenSearchClient: client},
		deliveryCfg: pkglogger.DeliveryLoggerConfig{
			LogOutputDestinations: outputs,
			OpenSearch:            deliveryCfg,
			OpenSearchClient:      client,
			InternalLogBatchConfig: pkglogger.DeliveryInternalLogBatchConfig{
				OpenSearch: pkglogger.DeliveryInternalLogOpenSearchBatchConfig{
					FlushInterval:       time.Hour,
					MaxBulkDocument:     10,
					BufferFlushBytes:    testOpenSearchBufferFlushBytes,
					MaxBulkRequestBytes: testOpenSearchMaxBulkRequestBytes,
				},
			},
		},
		fallbackFailure:      "failed to flush OpenSearch fallback logger",
		fallbackRequestsFail: true,
	}
}

// selfNotificationSlackTextは、Slack APIへのform本文から投稿したtextを取り出します。
func selfNotificationSlackText(body string) string {
	form, err := url.ParseQuery(body)
	if err != nil {
		return body
	}
	return form.Get("text")
}

// buildSelfNotificationSlackは、内部ログのERRORだけをSlack自身へ通知します。休止時間の閾値を
// 1nsにして、記録ごとに即時送信させます。送信遅延の閾値は1nsです。通知先のfallbackは、失敗した
// 送信ごとに標準loggerへ一行残します。
func buildSelfNotificationSlack(t *testing.T, mode selfNotificationBackendMode) selfNotificationNetworkSetup {
	t.Helper()
	backend := newSelfNotificationBackend(selfNotificationInternalMarker, selfNotificationSlackText)
	transport := &selfNotificationTransport{
		backend:     backend,
		mode:        mode,
		errorStatus: http.StatusInternalServerError,
		errorBody:   `{"ok":false,"error":"self-notification backend failure"}`,
		successBody: `{"ok":true,"channel":"dummy-channel","ts":"1"}`,
	}
	errorsOnly := pkgloggerhandler.HandlerConfig{OnErrorConfig: []string{pkglogger.LogOutputSlack.String()}}
	cfg := fallbackIsolationSlackConfig(errorsOnly)
	cfg.HTTPClient = transport.client()
	cfg.MaxRestThreshold = time.Nanosecond
	cfg.SlackPostWarnThresholdDuration = time.Nanosecond
	cfg.SlackPostErrorThresholdDuration = time.Nanosecond
	deliveryCfg := fallbackIsolationDeliverySlackConfig(errorsOnly)
	deliveryCfg.HTTPClient = transport.client()
	deliveryCfg.SlackPostWarnThresholdDuration = time.Nanosecond
	deliveryCfg.SlackPostErrorThresholdDuration = time.Nanosecond
	outputs := pkglogger.LogOutputDestinations{pkglogger.LogOutputSlack}
	return selfNotificationNetworkSetup{
		backend: backend,
		cfg:     pkglogger.LoggerConfig{LogOutputDestinations: outputs, Slack: &cfg},
		deliveryCfg: pkglogger.DeliveryLoggerConfig{
			LogOutputDestinations: outputs,
			Slack:                 &deliveryCfg,
			InternalLogBatchConfig: pkglogger.DeliveryInternalLogBatchConfig{
				Slack: pkglogger.DeliveryInternalLogSlackBatchConfig{
					Intervals:              []time.Duration{time.Hour},
					MaxLogsPerNotification: 10,
					MaxRestThreshold:       time.Nanosecond,
				},
			},
		},
		fallbackFailure:      "failed to flush Slack fallback logger",
		fallbackRequestsFail: mode != selfNotificationBackendSlow,
	}
}

// TestSelfSelectedNetworkNotificationsUseSeparateFallbackInstancesは、Slack・OpenSearch・Loki・
// CloudWatch Logs・ClickHouseの内部ログの通知先に同じ出力先を選んだ本番の配線で、送信先の
// 失敗・timeout・遅延が続いても、元の記録ごとの内部ログが決まった件数だけ同じ送信先へ送られ、
// 通知が繰り返されないことを確認します。通知先は通知元とは別のfallback instanceで、自身の
// 送信の失敗を通知せず、失敗した送信ごとに標準loggerへ一行残します。
func TestSelfSelectedNetworkNotificationsUseSeparateFallbackInstances(t *testing.T) {
	cases := []selfNotificationNetworkCase{
		{name: "loki", build: buildSelfNotificationLoki, internalPerEvent: 1},
		{name: "cloudwatchlogs", build: buildSelfNotificationCloudWatchLogs, internalPerEvent: 2},
		{name: "clickhouse", build: buildSelfNotificationClickHouse, internalPerEvent: 1},
	}
	var tests []selfNotificationNetworkCase
	for _, base := range cases {
		for _, contract := range selfNotificationContracts {
			test := base
			test.contract, test.mode, test.wantErrors = contract, selfNotificationBackendError, true
			if base.name == "clickhouse" && contract == selfNotificationDelivery {
				// Delivery ClickHouseはqueueへの受理を返し、送信できなかった受理済みのログを
				// Closeのerrorとして返します。Closeを完了させるため、送信は遅延して成功させます。
				// workerが複数の記録を一回のINSERTへまとめないよう、記録ごとに送信を待ちます。
				test.mode, test.wantErrors, test.awaitEachSend = selfNotificationBackendSlow, false, true
			}
			tests = append(tests, test)
		}
		test := base
		test.contract, test.mode, test.wantErrors = selfNotificationSync, selfNotificationBackendTimeout, true
		tests = append(tests, test)
	}
	for _, contract := range selfNotificationContracts {
		tests = append(tests,
			selfNotificationNetworkCase{
				name: "opensearch", contract: contract, mode: selfNotificationBackendSlow,
				build: buildSelfNotificationOpenSearch, internalPerEvent: 1,
			},
			selfNotificationNetworkCase{
				name: "slack", contract: contract, mode: selfNotificationBackendSlow,
				build: buildSelfNotificationSlack, internalPerEvent: 1, awaitEachSend: true,
			},
		)
	}
	tests = append(tests, selfNotificationNetworkCase{
		name: "slack", contract: selfNotificationDelivery, mode: selfNotificationBackendError,
		build: buildSelfNotificationSlack, internalPerEvent: 1, wantErrors: true, awaitEachSend: true,
	})
	for _, test := range tests {
		t.Run(test.name+"/"+string(test.contract)+"/"+string(test.mode), func(t *testing.T) {
			runSelfNotificationScenario(t, func(t *testing.T, env *selfNotificationEnv) {
				setup := test.build(t, test.mode)
				logger := newSelfNotificationLogger(t, env, test.contract, setup.cfg, setup.deliveryCfg)
				results := make([]error, 0, selfNotificationEvents)
				for index := range selfNotificationEvents {
					results = append(results, completeSelfNotificationStep(t, "a write", func() error {
						return logger.write(index)
					}))
					if test.awaitEachSend {
						// 通知元の送信と、その遅延の内部ログを送る通知先の送信を待ちます。
						setup.backend.waitForRequests(t, 2*(index+1))
					}
				}
				logger.closeLogger(t)

				requireSelfNotificationResults(t, logger, results, test.wantErrors, nil)
				backend := setup.backend
				// 件数の確認は全て行い、どの性質が崩れたかを報告します。
				assert.Equal(t, selfNotificationEvents, backend.applicationRequests(), "application requests:\n%s", backend)
				assert.Equal(t, test.internalPerEvent*selfNotificationEvents, backend.internalEntries(), "internal log entries:\n%s", backend)
				if setup.fallbackFailure != "" {
					// 通知先の送信もbackendの条件で失敗する場合は、requestごとに一行だけ標準loggerへ
					// 残ります。通知先自身の失敗は通知されないため、内部ログはこれ以上増えません。
					want := 0
					if setup.fallbackRequestsFail {
						want = backend.internalRequests()
					}
					assert.Equal(t, want, env.standardLog.count(setup.fallbackFailure), "fallback failures on the standard logger:\n%s", env.standardLog)
				}
			})
		})
	}
}
