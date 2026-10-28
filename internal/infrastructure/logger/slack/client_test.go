package slack

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	slackgo "github.com/slack-go/slack"
	"github.com/stretchr/testify/require"
)

func TestNewSlackHTTPClientUsesConfiguredTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const timeout = 30 * time.Millisecond
		client := newSlackHTTPClient(timeout, nil)
		if got := client.Timeout; got != timeout {
			t.Fatalf("HTTP client timeout = %s, want %s", got, timeout)
		}

		client.Transport = waitingForRequestContextRoundTripper{}
		start := time.Now()
		_, err := client.Get("http://slack-timeout.test")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("HTTP request error = %v, want %v", err, context.DeadlineExceeded)
		}
		// bubble内の時計は全goroutineが待機したときだけ進むため、経過時間は期限と一致します。
		if elapsed := time.Since(start); elapsed != timeout {
			t.Fatalf("HTTP request elapsed = %s, want configured timeout %s", elapsed, timeout)
		}
	})
}

type waitingForRequestContextRoundTripper struct{}

func (waitingForRequestContextRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	<-r.Context().Done()
	return nil, r.Context().Err()
}

// 設定のHTTPClientがnilの場合は、net/httpの既定transport・cookie設定を使い、redirectは
// 追従しません。
func TestNewSlackHTTPClientWithoutConfiguredClientUsesNetHTTPDefaults(t *testing.T) {
	t.Parallel()
	const timeout = 30 * time.Second
	client := newSlackHTTPClient(timeout, nil)
	require.Nil(t, client.Transport, "nil Transport selects http.DefaultTransport")
	require.Nil(t, client.Jar)
	require.NotNil(t, client.CheckRedirect)
	require.ErrorIs(t, client.CheckRedirect(nil, nil), http.ErrUseLastResponse)
	require.Equal(t, timeout, client.Timeout)
}

type redirectRoundTripFunc func(*http.Request) (*http.Response, error)

func (f redirectRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// redirectは、設定した送信先が本来の送信先ではないことを表します。301/302/303を追従すると
// 本文なしのGETへ変わり、307/308を追従するとmessageを別URLへ再送します。どのredirectも
// 追従せず、元の要求一回のresponseをSDKへ返し、呼び出し元のredirect方針も使いません。
func TestNewSlackHTTPClientReturnsEveryRedirectWithoutFollowing(t *testing.T) {
	t.Parallel()
	for _, status := range []int{
		http.StatusMovedPermanently,
		http.StatusFound,
		http.StatusSeeOther,
		http.StatusTemporaryRedirect,
		http.StatusPermanentRedirect,
	} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			t.Parallel()
			var requests []string
			policyCalls := 0
			base := &http.Client{
				Transport: redirectRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					requests = append(requests, r.Method+" "+r.URL.Path)
					if r.URL.Path != "/redirected" {
						return &http.Response{StatusCode: status, Header: http.Header{"Location": []string{"/redirected"}},
							Body: io.NopCloser(strings.NewReader("redirected by gateway")), Request: r}, nil
					}
					// 転送先は、どの要求にもSlackの成功に見える応答を返します。
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
						Body: io.NopCloser(strings.NewReader(`{"ok":true,"channel":"C1","ts":"1.2"}`)), Request: r}, nil
				}),
				CheckRedirect: func(*http.Request, []*http.Request) error {
					policyCalls++
					return nil
				},
			}
			client := newSlackHTTPClient(30*time.Second, base)
			response, err := client.Post("https://slack.com/api/chat.postMessage", "application/x-www-form-urlencoded", strings.NewReader("text=preserve+this+log"))
			require.NoError(t, err)
			defer response.Body.Close()
			require.Equal(t, status, response.StatusCode)
			require.Equal(t, []string{"POST /api/chat.postMessage"}, requests)
			require.Zero(t, policyCalls)
		})
	}
}

// 設定したHTTPClientはcopyして使い、Transport・Jarを引き継ぎます。Timeoutは設定の値へ
// 置き換え、redirectは追従しないようにして、呼び出し元のclientは変更しません。
func TestNewSlackHTTPClientCopiesConfiguredClientWithoutMutatingIt(t *testing.T) {
	t.Parallel()
	const timeout = 30 * time.Second
	transport := &slackAPITransport{}
	jar, err := cookiejar.New(nil)
	require.NoError(t, err)
	errConfiguredRedirectPolicy := errors.New("configured redirect policy")
	base := &http.Client{
		Transport: transport,
		Jar:       jar,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errConfiguredRedirectPolicy
		},
		Timeout: time.Hour,
	}

	client := newSlackHTTPClient(timeout, base)

	require.NotSame(t, base, client)
	require.Same(t, transport, client.Transport)
	require.Same(t, jar, client.Jar)
	require.NotNil(t, client.CheckRedirect)
	require.ErrorIs(t, client.CheckRedirect(nil, nil), http.ErrUseLastResponse)
	require.Equal(t, timeout, client.Timeout)

	require.Equal(t, time.Hour, base.Timeout, "the caller's client Timeout must not change")
	require.Same(t, transport, base.Transport)
	require.Same(t, jar, base.Jar)
	require.ErrorIs(t, base.CheckRedirect(nil, nil), errConfiguredRedirectPolicy)
}

// 設定のTimeoutは、元のclientのTimeoutが未設定・短い・長いいずれの場合も優先します。
func TestNewSlackHTTPClientConfiguredTimeoutTakesPrecedence(t *testing.T) {
	t.Parallel()
	const timeout = 30 * time.Second
	for _, baseTimeout := range []time.Duration{0, time.Millisecond, time.Hour} {
		t.Run(baseTimeout.String(), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				base := &http.Client{
					Transport: waitingForRequestContextRoundTripper{},
					Timeout:   baseTimeout,
				}
				client := newSlackHTTPClient(timeout, base)

				start := time.Now()
				_, err := client.Get("http://slack-timeout.test")
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Equal(t, timeout, time.Since(start))
				require.Equal(t, baseTimeout, base.Timeout)
			})
		})
	}
}

// 設定したHTTPClientのtransportは、通常・Delivery・internal-log fallbackのSlack API送信に
// 使われます。clientはloggerごとの設定なので、別々のclientを持つloggerを並行して使えます。
func TestSlackLoggersSendThroughConfiguredHTTPClient(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tracer, meter := observer.NopTracer{}, observer.NopMeter{}
	for _, test := range []struct {
		name string
		send func(t *testing.T, httpClient *http.Client, message string)
	}{
		{
			name: "normal",
			send: func(t *testing.T, httpClient *http.Client, message string) {
				cfg := normalSlackLifecycleConfig([]time.Duration{time.Hour})
				cfg.HTTPClient = httpClient
				logger, err := NewSlackLogger(ctx, NewConfigAdapter(cfg), tracer, meter)
				require.NoError(t, err)
				require.NoError(t, logger.testInfo(ctx, message))
				require.NoError(t, logger.Close())
			},
		},
		{
			name: "internal-log fallback",
			send: func(t *testing.T, httpClient *http.Client, message string) {
				cfg := fallbackSlackTestConfig([]time.Duration{time.Hour})
				cfg.HTTPClient = httpClient
				named := NewNamedInfoFallbackHandler(NewConfigAdapter(cfg))
				named.Handler(ctx, fallbackSlackTestEntry(pkglogger.LogLevelInfo, message), tracer, meter)
				require.NoError(t, named.Closer.Close())
			},
		},
		{
			name: "delivery",
			send: func(t *testing.T, httpClient *http.Client, message string) {
				cfg := deliverySlackLevelTestConfig(pkglogger.LogLevelDebug)
				cfg.MaxMessageLength = 10_000
				cfg.HTTPClient = httpClient
				logger, err := NewDeliverySlackLogger(ctx, NewDeliveryConfigAdapter(cfg), tracer, meter)
				require.NoError(t, err)
				report, err := logger.testInfo(ctx, message)
				require.NoError(t, err)
				require.Equal(t, pkglogger.DeliverySucceeded, requireSingleSlackDeliveryTarget(t, report).Status)
			},
		},
		{
			name: "delivery internal-log fallback",
			send: func(t *testing.T, httpClient *http.Client, message string) {
				cfg := deliveryFallbackSlackTestConfig()
				cfg.HTTPClient = httpClient
				named := NewNamedInfoFallbackHandler(NewDeliveryFallbackConfigAdapter(
					cfg,
					pkglogger.DeliveryInternalLogSlackBatchConfig{
						Intervals:              []time.Duration{time.Hour},
						MaxLogsPerNotification: 10,
						MaxRestThreshold:       time.Hour,
					},
				))
				named.Handler(ctx, fallbackSlackTestEntry(pkglogger.LogLevelInfo, message), tracer, meter)
				require.NoError(t, named.Closer.Close())
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			transport := &slackAPITransport{}
			httpClient := &http.Client{Transport: transport, Timeout: time.Hour}
			message := "configured HTTP client: " + test.name

			test.send(t, httpClient, message)

			requests := transport.snapshot()
			require.Len(t, requests, 1)
			require.Equal(t, slackgo.APIURL+"chat.postMessage", requests[0].url)
			require.Equal(t, "dummy-token", requests[0].token)
			require.Contains(t, requests[0].text, message)
			// loggerは呼び出し元のclientを変更しません。
			require.Same(t, transport, httpClient.Transport)
			require.Equal(t, time.Hour, httpClient.Timeout)
		})
	}
}

// slackTimeoutPathは、設定のHTTPClientでSlack APIを呼び出す一つの構築経路です。startは
// httpClientとtimeoutを設定してconstructorでloggerを作り、一件の送信を始め、その送信の失敗を
// 観測したかを返す関数を返します。失敗のerrorを受け取れない経路（内部ログ用のfallbackは失敗を
// 標準loggerへ残すだけです）はerrにnilを返します。
type slackTimeoutPath struct {
	name  string
	start func(t *testing.T, httpClient *http.Client, timeout time.Duration) (failed func() (err error, ok bool))
}

// slackFallbackFailureLineは、内部ログ用のfallbackが失敗した送信ごとに標準loggerへ残す行の一部です。
const slackFallbackFailureLine = "failed to flush Slack fallback logger"

func slackTimeoutPaths() []slackTimeoutPath {
	tracer, meter := observer.NopTracer{}, observer.NopMeter{}
	fallbackFailed := func(standardLog *slackStandardLog) func() (error, bool) {
		return func() (error, bool) { return nil, standardLog.count(slackFallbackFailureLine) > 0 }
	}
	return []slackTimeoutPath{
		{
			name: "normal",
			start: func(t *testing.T, httpClient *http.Client, timeout time.Duration) func() (error, bool) {
				cfg := normalSlackLifecycleConfig([]time.Duration{time.Hour})
				cfg.Timeout = timeout
				cfg.HTTPClient = httpClient
				logger, err := NewSlackLogger(context.Background(), NewConfigAdapter(cfg), tracer, meter)
				require.NoError(t, err)
				// 失敗した送信のentryはbufferへ戻り、Closeが再送します。
				t.Cleanup(func() { _ = logger.Close() })
				failures := make(chan error, 1)
				logger.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
					select {
					case failures <- err:
					default:
					}
				}
				require.NoError(t, logger.testInfo(context.Background(), "bounded by the configured timeout"))
				return func() (error, bool) {
					select {
					case err := <-failures:
						return err, true
					default:
						return nil, false
					}
				}
			},
		},
		{
			name: "internal-log fallback",
			start: func(t *testing.T, httpClient *http.Client, timeout time.Duration) func() (error, bool) {
				standardLog := captureSlackStandardLog(t)
				cfg := fallbackSlackTestConfig([]time.Duration{time.Hour})
				cfg.Timeout = timeout
				cfg.HTTPClient = httpClient
				named := NewNamedInfoFallbackHandler(NewConfigAdapter(cfg))
				t.Cleanup(func() { _ = named.Closer.Close() })
				named.Handler(context.Background(), fallbackSlackTestEntry(pkglogger.LogLevelInfo, "bounded by the configured timeout"), tracer, meter)
				return fallbackFailed(standardLog)
			},
		},
		{
			name: "delivery",
			start: func(t *testing.T, httpClient *http.Client, timeout time.Duration) func() (error, bool) {
				cfg := deliverySlackLevelTestConfig(pkglogger.LogLevelDebug)
				cfg.Timeout = timeout
				cfg.HTTPClient = httpClient
				logger, err := NewDeliverySlackLogger(context.Background(), NewDeliveryConfigAdapter(cfg), tracer, meter)
				require.NoError(t, err)
				result := make(chan error, 1)
				go func() {
					report, err := logger.testInfo(context.Background(), "bounded by the configured timeout")
					// timeoutした送信は、Slackが受け取ったか確認できません。
					if status := requireSingleSlackDeliveryTarget(t, report).Status; err != nil && status != pkglogger.DeliveryUnconfirmed {
						t.Errorf("timed-out delivery status = %v, want %v", status, pkglogger.DeliveryUnconfirmed)
					}
					result <- err
				}()
				return func() (error, bool) {
					select {
					case err := <-result:
						return err, true
					default:
						return nil, false
					}
				}
			},
		},
		{
			name: "delivery internal-log fallback",
			start: func(t *testing.T, httpClient *http.Client, timeout time.Duration) func() (error, bool) {
				standardLog := captureSlackStandardLog(t)
				cfg := deliveryFallbackSlackTestConfig()
				cfg.Timeout = timeout
				cfg.HTTPClient = httpClient
				named := NewNamedInfoFallbackHandler(NewDeliveryFallbackConfigAdapter(
					cfg,
					pkglogger.DeliveryInternalLogSlackBatchConfig{
						Intervals:              []time.Duration{time.Hour},
						MaxLogsPerNotification: 10,
						MaxRestThreshold:       time.Hour,
					},
				))
				t.Cleanup(func() { _ = named.Closer.Close() })
				named.Handler(context.Background(), fallbackSlackTestEntry(pkglogger.LogLevelInfo, "bounded by the configured timeout"), tracer, meter)
				return fallbackFailed(standardLog)
			},
		},
	}
}

// waitingSlackAPITransportは、requestのcontextが終わるまで応答せず、そのerrorを返します。
// releaseは後片付けで待機中と以後のrequestを解放し、Closeの再送が待ち続けないようにします。
type waitingSlackAPITransport struct {
	released    chan struct{}
	releaseOnce sync.Once
}

// errSlackTestRequestReleasedは、試験の後片付けで解放したrequestのerrorです。
var errSlackTestRequestReleased = errors.New("slack test request released")

func newWaitingSlackAPITransport() *waitingSlackAPITransport {
	return &waitingSlackAPITransport{released: make(chan struct{})}
}

func (rt *waitingSlackAPITransport) RoundTrip(r *http.Request) (*http.Response, error) {
	select {
	case <-r.Context().Done():
		return nil, r.Context().Err()
	case <-rt.released:
		return nil, errSlackTestRequestReleased
	}
}

func (rt *waitingSlackAPITransport) release() {
	rt.releaseOnce.Do(func() { close(rt.released) })
}

// 設定のTimeoutは、HTTPClientのTimeoutが未設定・短い・長いいずれの場合も優先して、通常・
// 内部ログ用・Delivery・Delivery内部ログ用のSlack API呼び出しを打ち切ります。各経路の
// constructorが設定のHTTPClientをcopyし、呼び出し元のclientは変更しません。
// HTTPClientがnilの場合は、TestSlackLoggersBoundDefaultHTTPClientByConfiguredTimeoutで確認します。
func TestSlackLoggersConfiguredTimeoutTakesPrecedenceOverHTTPClientTimeout(t *testing.T) {
	const timeout = 30 * time.Second
	for _, path := range slackTimeoutPaths() {
		for _, baseTimeout := range []time.Duration{0, time.Millisecond, time.Hour} {
			t.Run(path.name+"/client timeout "+baseTimeout.String(), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					transport := newWaitingSlackAPITransport()
					httpClient := &http.Client{Transport: transport, Timeout: baseTimeout}
					failed := path.start(t, httpClient, timeout)
					// cleanupは登録と逆順に実行するため、失敗時も先に待機中のrequestを解放してから
					// loggerを閉じます。
					t.Cleanup(transport.release)

					// bubble内の時計は全goroutineが待機したときだけ進むため、送信は設定のTimeoutの
					// 経過で、その直前ではなく打ち切られることを確認できます。
					time.Sleep(timeout - time.Nanosecond)
					synctest.Wait()
					if err, ok := failed(); ok {
						t.Fatalf("Slack API call ended before the configured timeout %s: %v", timeout, err)
					}
					time.Sleep(time.Nanosecond)
					synctest.Wait()
					err, ok := failed()
					if !ok {
						t.Fatalf("Slack API call was not ended by the configured timeout %s", timeout)
					}
					if err != nil && !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("Slack API call error = %v, want %v", err, context.DeadlineExceeded)
					}
					require.Same(t, transport, httpClient.Transport, "the caller's client must not change")
					require.Equal(t, baseTimeout, httpClient.Timeout, "the caller's client must not change")
				})
			})
		}
	}
}

// slackDefaultHTTPClientHelperEnvは、TestSlackLoggersBoundDefaultHTTPClientByConfiguredTimeoutが
// 試験binaryを子processとして実行し、TestSlackLoggersDefaultHTTPClientHelperProcessを
// 動かすときに設定するenvironment variableです。
const slackDefaultHTTPClientHelperEnv = "HETEROLOG_SLACK_DEFAULT_HTTP_CLIENT_HELPER"

// HTTPClientがnilの場合はnet/httpの既定transportを使い、設定のTimeoutで打ち切ります。既定の
// transportはprocess全体で共有されるため変更せず、子processのproxy設定（HTTPS_PROXY）を
// 応答しないlocalのproxyへ向けて、四つの経路の送信がTimeoutで終わることを確認します。
// 経路ごとに専用のproxyと子processを使い、その経路の送信が既定transport（proxy設定）を
// 通ったことを経路ごとに確認します。実際のSlackへは接続しません。
func TestSlackLoggersBoundDefaultHTTPClientByConfiguredTimeout(t *testing.T) {
	if os.Getenv(slackDefaultHTTPClientHelperEnv) != "" {
		t.Skip("running as the helper process")
	}
	for _, path := range slackTimeoutPaths() {
		t.Run(path.name, func(t *testing.T) {
			proxy := startHangingConnectProxy(t)
			// t.Runは名前の空白を"_"へ置き換えるため、子processではその名前で一つの経路だけを選びます。
			subtest := strings.ReplaceAll(path.name, " ", "_")
			cmd := exec.Command(
				os.Args[0],
				"-test.run=^TestSlackLoggersDefaultHTTPClientHelperProcess$/^"+regexp.QuoteMeta(subtest)+"$",
				"-test.count=1",
				"-test.v",
				"-test.timeout=60s",
			)
			cmd.Env = append(
				environWithoutProxySettings(),
				slackDefaultHTTPClientHelperEnv+"=1",
				"HTTPS_PROXY=http://"+proxy.addr(),
				"HTTP_PROXY=http://"+proxy.addr(),
			)
			output := runSlackHelperProcess(t, cmd)
			// 子processがskipされず、この経路の試験を実行して合格したことを確認します。
			if !strings.Contains(output, "--- PASS: TestSlackLoggersDefaultHTTPClientHelperProcess/"+subtest+" ") {
				t.Fatalf("helper process did not run and pass the %s path:\n%s", path.name, output)
			}
			targets := proxy.connectTargets()
			if len(targets) == 0 {
				t.Fatalf("the %s path sent no request through the default transport's proxy:\n%s", path.name, output)
			}
			for _, target := range targets {
				if !strings.HasPrefix(target, "CONNECT slack.com:443 ") {
					t.Fatalf("%s path proxy request = %q, want CONNECT to the Slack API", path.name, target)
				}
			}
		})
	}
}

// TestSlackLoggersDefaultHTTPClientHelperProcessは、TestSlackLoggersBoundDefaultHTTPClientByConfiguredTimeout
// の子processとしてだけ実行します。HTTPClientがnilの各経路の送信が、応答しないproxyで設定の
// Timeoutの経過後に失敗することを、hang検出の上限付きで確認します。
func TestSlackLoggersDefaultHTTPClientHelperProcess(t *testing.T) {
	if os.Getenv(slackDefaultHTTPClientHelperEnv) != "1" {
		t.Skip("helper process for TestSlackLoggersBoundDefaultHTTPClientByConfiguredTimeout")
	}
	requireSlackAPIRequestsUseLoopbackProxy(t)
	// 負荷が高くても送信がproxyへ届いてから打ち切られるよう、Timeoutには余裕を持たせます。
	const timeout = time.Second
	for _, path := range slackTimeoutPaths() {
		t.Run(path.name, func(t *testing.T) {
			started := time.Now()
			failed := path.start(t, nil, timeout)
			deadline := time.Now().Add(slackHangTimeout)
			for {
				err, ok := failed()
				if ok {
					if elapsed := time.Since(started); elapsed < timeout {
						t.Fatalf("Slack API call failed after %s, before the configured timeout %s: %v", elapsed, timeout, err)
					}
					var netErr net.Error
					if err != nil && !errors.Is(err, context.DeadlineExceeded) && !(errors.As(err, &netErr) && netErr.Timeout()) {
						t.Fatalf("Slack API call error = %v, want a timeout", err)
					}
					return
				}
				if time.Now().After(deadline) {
					// 打ち切られない送信はloggerのCloseも待ち続けるため、後片付けを待たずに子processを
					// 終了し、親の試験が出力とともに失敗を報告します。
					fmt.Fprintf(os.Stderr, "%s: Slack API call with the default HTTP client was not ended by the configured timeout %s\n", path.name, timeout)
					os.Exit(1)
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}

// requireSlackAPIRequestsUseLoopbackProxyは、既定transportのproxy設定（http.ProxyFromEnvironment）で
// Slack APIへのrequestがloopbackのproxyへ送られることを確認し、そうでなければ試験を中止します。
// helperが通常の環境で誤って実行されても、実際のSlackへ接続しないためです。
func requireSlackAPIRequestsUseLoopbackProxy(t *testing.T) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, slackgo.APIURL+"chat.postMessage", nil)
	if err != nil {
		t.Fatalf("build the Slack API request for the proxy check: %v", err)
	}
	proxyURL, err := http.ProxyFromEnvironment(request)
	if err != nil {
		t.Fatalf("refusing to run: the proxy for Slack API requests cannot be resolved: %v", err)
	}
	if proxyURL == nil {
		t.Fatal("refusing to run: Slack API requests would not go through a proxy")
	}
	if ip := net.ParseIP(proxyURL.Hostname()); ip == nil || !ip.IsLoopback() {
		t.Fatalf("refusing to run: the proxy for Slack API requests %q is not a loopback address", proxyURL.Host)
	}
}

// hangingConnectProxyは、proxyとして受け付けたrequestの最初の行を記録し、応答せずに接続を
// 保持します。clientのTimeoutで接続が閉じられるまで待ちます。
type hangingConnectProxy struct {
	listener net.Listener
	mu       sync.Mutex
	targets  []string
	conns    []net.Conn
}

func startHangingConnectProxy(t *testing.T) *hangingConnectProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	proxy := &hangingConnectProxy{listener: listener}
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go proxy.serve(conn)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-accepted
		proxy.mu.Lock()
		defer proxy.mu.Unlock()
		for _, conn := range proxy.conns {
			_ = conn.Close()
		}
	})
	return proxy
}

func (p *hangingConnectProxy) addr() string { return p.listener.Addr().String() }

func (p *hangingConnectProxy) serve(conn net.Conn) {
	p.mu.Lock()
	p.conns = append(p.conns, conn)
	p.mu.Unlock()
	reader := bufio.NewReader(conn)
	if line, err := reader.ReadString('\n'); err == nil {
		p.mu.Lock()
		p.targets = append(p.targets, strings.TrimSpace(line))
		p.mu.Unlock()
	}
	// 応答せず、clientが接続を閉じるまで読み続けます。
	_, _ = io.Copy(io.Discard, reader)
	_ = conn.Close()
}

func (p *hangingConnectProxy) connectTargets() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.targets...)
}

// environWithoutProxySettingsは、子processへ渡すenvironmentから既存のproxy設定を除きます。
// NO_PROXYなどが残ると、Slack APIへの送信が試験のproxyを経由しなくなるためです。
func environWithoutProxySettings() []string {
	var environ []string
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		switch strings.ToUpper(key) {
		case "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "ALL_PROXY", "REQUEST_METHOD", slackDefaultHTTPClientHelperEnv:
			continue
		}
		environ = append(environ, entry)
	}
	return environ
}

// runSlackHelperProcessは子processを実行して出力を返します。子processは自身の各送信を
// hang検出の上限付きで待ち、-test.timeoutでも止まります。この上限はそれらが働かない場合の
// 停止の検出だけに使います。
func runSlackHelperProcess(t *testing.T, cmd *exec.Cmd) string {
	t.Helper()
	var output strings.Builder
	cmd.Stdout = &output
	cmd.Stderr = &output
	require.NoError(t, cmd.Start())
	finished := make(chan error, 1)
	go func() { finished <- cmd.Wait() }()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatalf("helper process error = %v:\n%s", err, output.String())
		}
	case <-time.After(90 * time.Second):
		_ = cmd.Process.Kill()
		<-finished
		t.Fatalf("helper process did not finish:\n%s", output.String())
	}
	return output.String()
}

// HTTPClientを指定しても、応答本文全体を検証してから配送成功とします。
// slack-goはtsのない成功応答も受理するため、Unconfirmedは検証が適用されたことを示します。
func TestDeliverySlackLoggerValidatesResponseFromConfiguredHTTPClient(t *testing.T) {
	t.Parallel()
	transport := &slackAPITransport{responseBody: `{"ok":true,"channel":"dummy-info-channel"}`}
	cfg := deliverySlackLevelTestConfig(pkglogger.LogLevelDebug)
	cfg.HTTPClient = &http.Client{Transport: transport}
	logger, err := NewDeliverySlackLogger(
		context.Background(),
		NewDeliveryConfigAdapter(cfg),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)

	report, err := logger.testInfo(context.Background(), "response without timestamp")
	require.Error(t, err)
	result := requireSingleSlackDeliveryTarget(t, report)
	require.Equal(t, pkglogger.DeliveryUnconfirmed, result.Status)
	require.Same(t, err, result.Err)
	require.Len(t, transport.snapshot(), 1)
}

// slackAPITransportは、設定のHTTPClientへ渡すtransportとしてSlack API requestを記録します。
// responseBodyが空の場合は、送信先channelを含む成功応答を返します。
type slackAPITransport struct {
	responseBody string

	mu       sync.Mutex
	requests []slackAPIRequest
}

type slackAPIRequest struct {
	url   string
	token string
	text  string
}

func (rt *slackAPITransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		var readErr error
		body, readErr = io.ReadAll(req.Body)
		if err := errors.Join(readErr, req.Body.Close()); err != nil {
			return nil, err
		}
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, err
	}

	rt.mu.Lock()
	rt.requests = append(rt.requests, slackAPIRequest{
		url:   req.URL.String(),
		token: form.Get("token"),
		text:  form.Get("text"),
	})
	rt.mu.Unlock()

	response := rt.responseBody
	if response == "" {
		response = fmt.Sprintf(`{"ok":true,"channel":%q,"ts":"1"}`, form.Get("channel"))
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     http.StatusText(http.StatusOK),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(response)),
		Request:    req,
	}, nil
}

func (rt *slackAPITransport) snapshot() []slackAPIRequest {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return append([]slackAPIRequest(nil), rt.requests...)
}
