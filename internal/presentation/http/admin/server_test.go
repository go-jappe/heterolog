package admin

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgadmin "github.com/go-jappe/heterolog/pkg/admin"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestServerConfigListenAddress(t *testing.T) {
	const token = tokenForTest
	for _, tt := range []struct {
		name string
		cfg  pkgadmin.ServerConfig
		want string
		err  error
		// anyErrは、sentinelを持たないerrorを期待することを表します。
		anyErr bool
	}{
		{name: "empty host listens on loopback", cfg: pkgadmin.ServerConfig{Addr: ":9100", Token: token}, want: "127.0.0.1:9100"},
		{name: "IPv6 loopback", cfg: pkgadmin.ServerConfig{Addr: "[::1]:9100", Token: token}, want: "[::1]:9100"},
		{name: "localhost", cfg: pkgadmin.ServerConfig{Addr: "localhost:9100", Token: token}, want: "localhost:9100"},
		{name: "all interfaces with TLS", cfg: pkgadmin.ServerConfig{Addr: "0.0.0.0:9100", Token: token, TLSCertFile: "cert.pem", TLSKeyFile: "key.pem"}, want: "0.0.0.0:9100"},
		{name: "all interfaces without TLS", cfg: pkgadmin.ServerConfig{Addr: "0.0.0.0:9100", Token: token}, err: ErrServerInsecure},
		{name: "only a key file", cfg: pkgadmin.ServerConfig{Addr: "127.0.0.1:9100", Token: token, TLSKeyFile: "key.pem"}, err: ErrServerTLSIncomplete},
		{name: "missing port", cfg: pkgadmin.ServerConfig{Addr: "127.0.0.1", Token: token}, anyErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := listenAddress(tt.cfg)
			if tt.anyErr {
				if err == nil {
					t.Errorf("listenAddress() = %q, want an error", got)
				}
				return
			}
			if !errors.Is(err, tt.err) || got != tt.want {
				t.Errorf("listenAddress() = %q, %v; want %q, %v", got, err, tt.want, tt.err)
			}
		})
	}
}

// hostを省略すると127.0.0.1だけで待ち受け、HTTP clientから管理用APIを使えます。
func TestStartServerServesTheAdminAPIOnLoopback(t *testing.T) {
	controller := newControllerForTest(t)
	server, err := StartServer(pkgadmin.ServerConfig{Addr: ":0", Token: tokenForTest},
		pkgadmin.Members{Loggers: map[string]pkglogger.SettingsController{"app": controller}}, &auditLoggerForTest{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	if !strings.HasPrefix(server.Addr(), "127.0.0.1:") {
		t.Fatalf("Addr() = %q, want a 127.0.0.1 address", server.Addr())
	}
	request, err := http.NewRequest(http.MethodPatch, "http://"+server.Addr()+"/settings",
		strings.NewReader(`{"loggers":{"app":{"outputs":{"stdout":{"level":"WARN"}}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+tokenForTest)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("PATCH = %d %s, want 200", response.StatusCode, body)
	}
	if got := stdoutSettings(controller).Level; got != pkglogger.LogLevelWarn {
		t.Errorf("stdout level = %v, want WARN", got)
	}
	if err := server.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

func TestStartServerRejectsUnsafeConfigurations(t *testing.T) {
	loggers := map[string]pkglogger.SettingsController{"app": newControllerForTest(t)}
	for _, tt := range []struct {
		name string
		cfg  pkgadmin.ServerConfig
		want error
	}{
		{"no address", pkgadmin.ServerConfig{Token: tokenForTest}, ErrServerAddrRequired},
		{"all interfaces without TLS", pkgadmin.ServerConfig{Addr: "0.0.0.0:0", Token: tokenForTest}, ErrServerInsecure},
		{"host name without TLS", pkgadmin.ServerConfig{Addr: "admin.example.test:0", Token: tokenForTest}, ErrServerInsecure},
		{"only a certificate", pkgadmin.ServerConfig{Addr: "127.0.0.1:0", Token: tokenForTest, TLSCertFile: "cert.pem"}, ErrServerTLSIncomplete},
		{"short token", pkgadmin.ServerConfig{Addr: "127.0.0.1:0", Token: "short"}, ErrTokenTooShort},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server, err := StartServer(tt.cfg, pkgadmin.Members{Loggers: loggers}, &auditLoggerForTest{})
			if server != nil {
				_ = server.Close()
			}
			if !errors.Is(err, tt.want) || strings.Contains(err.Error(), tokenForTest) {
				t.Errorf("StartServer() error = %v, want %v without the token", err, tt.want)
			}
		})
	}
}

// writeSelfSignedCertificateForTestは、127.0.0.1向けの自己署名証明書と鍵をPEMで書き、
// そのpathと、証明書を信頼するpoolを返します。
func writeSelfSignedCertificateForTest(t *testing.T) (certFile, keyFile string, pool *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "heterolog admin test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	certFile = filepath.Join(directory, "cert.pem")
	keyFile = filepath.Join(directory, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool = x509.NewCertPool()
	pool.AddCert(certificate)
	return certFile, keyFile, pool
}

// 証明書と鍵を指定すると、HTTPSで待ち受けます。
func TestStartServerServesHTTPSWithTheGivenCertificate(t *testing.T) {
	certFile, keyFile, pool := writeSelfSignedCertificateForTest(t)
	server, err := StartServer(pkgadmin.ServerConfig{Addr: "127.0.0.1:0", Token: tokenForTest, TLSCertFile: certFile, TLSKeyFile: keyFile},
		pkgadmin.Members{Loggers: map[string]pkglogger.SettingsController{"app": newControllerForTest(t)}}, &auditLoggerForTest{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })

	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}
	request, err := http.NewRequest(http.MethodGet, "https://"+server.Addr()+"/settings", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+tokenForTest)
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("HTTPS GET: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Errorf("HTTPS GET = %d, want 200", response.StatusCode)
	}
	plain, err := http.NewRequest(http.MethodGet, "http://"+server.Addr()+"/settings", nil)
	if err != nil {
		t.Fatal(err)
	}
	plain.Header.Set("Authorization", "Bearer "+tokenForTest)
	if response, err := http.DefaultClient.Do(plain); err == nil {
		_ = response.Body.Close()
		if response.StatusCode == http.StatusOK {
			t.Error("plain HTTP GET to the HTTPS server succeeded")
		}
	}
}

// pausedAuditLoggerは、最初の監査logを記録する前だけ待機します。
type pausedAuditLogger struct {
	*auditLoggerForTest
	paused  atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (l *pausedAuditLogger) Info(ctx context.Context, message string, fields ...pkglogger.Field) (pkglogger.DeliveryReport, error) {
	if l.paused.CompareAndSwap(false, true) {
		close(l.entered)
		<-l.release
	}
	return l.auditLoggerForTest.Info(ctx, message, fields...)
}

// 監査logの送信と、直列に処理する先のPATCHを待つ時間が、応答の書き込み期限を
// 使い切らないことを、実際のHTTP接続で確かめます。期限は試験のために短くします。
func TestStartServerRespondsAfterSlowAuditAndQueuedPatch(t *testing.T) {
	const responseWriteTimeout = 500 * time.Millisecond
	controller := newControllerForTest(t)
	audit := &pausedAuditLogger{
		auditLoggerForTest: &auditLoggerForTest{},
		entered:            make(chan struct{}),
		release:            make(chan struct{}),
	}
	release := sync.OnceFunc(func() { close(audit.release) })
	defer release()
	server, err := startServer(pkgadmin.ServerConfig{Addr: "127.0.0.1:0", Token: tokenForTest},
		pkgadmin.Members{Loggers: map[string]pkglogger.SettingsController{"app": controller}}, audit, responseWriteTimeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	transport := &http.Transport{}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}
	type result struct {
		status    int
		body      []byte
		requestID string
		err       error
	}
	patch := func(body string, wrote chan<- struct{}) <-chan result {
		request, err := http.NewRequest(http.MethodPatch, "http://"+server.Addr()+"/settings", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+tokenForTest)
		if wrote != nil {
			request = request.WithContext(httptrace.WithClientTrace(request.Context(), &httptrace.ClientTrace{
				WroteRequest: func(info httptrace.WroteRequestInfo) {
					if info.Err == nil {
						close(wrote)
					}
				},
			}))
		}
		finished := make(chan result, 1)
		go func() {
			response, err := client.Do(request)
			if err != nil {
				finished <- result{err: err}
				return
			}
			body, err := io.ReadAll(response.Body)
			_ = response.Body.Close()
			finished <- result{status: response.StatusCode, body: body, requestID: response.Header.Get("X-Request-Id"), err: err}
		}()
		return finished
	}
	first := patch(`{"loggers":{"app":{"outputs":{"stdout":{"sampling":{"info_rate":10}}}}}}`, nil)
	boundedwait.Receive(t, audit.entered, "first PATCH audit record")
	secondWritten := make(chan struct{})
	second := patch(`{"loggers":{"app":{"outputs":{"stdout":{"sampling":{"warn_rate":20}}}}}}`, secondWritten)
	boundedwait.Receive(t, secondWritten, "queued PATCH request write")
	// 応答の書き込み期限を確実に超えてから監査を再開します。期限を要求の始まりから
	// 数えていれば、どちらの応答も書けません。
	time.Sleep(2 * responseWriteTimeout)
	release()
	for index, finished := range []<-chan result{first, second} {
		response := boundedwait.Receive(t, finished, "PATCH response after audit")
		if response.err != nil {
			t.Errorf("PATCH %d: %v", index+1, response.err)
			continue
		}
		if response.status != http.StatusOK || response.requestID == "" {
			t.Errorf("PATCH %d = %d %s, request ID %q; want 200 and a request ID", index+1, response.status, response.body, response.requestID)
		}
		var document settingsDocument
		if err := json.Unmarshal(response.body, &document); err != nil {
			t.Errorf("PATCH %d body: %v", index+1, err)
		}
		if got := document.Loggers["app"].Outputs[pkglogger.LogOutputStdout].Sampling; got.InfoRate != 10 || (index == 1 && got.WarnRate != 20) {
			t.Errorf("PATCH %d response sampling = %+v, want the applied change", index+1, got)
		}
		if len(document.Warnings) != 0 {
			t.Errorf("PATCH %d warnings = %q, want none after successful audit delivery", index+1, document.Warnings)
		}
	}
	if got := stdoutSettings(controller).Sampling; got.SamplingInfoRate != 10 || got.SamplingWarnRate != 20 {
		t.Errorf("sampling = %+v, want both info_rate 10 and warn_rate 20 applied", got)
	}
	if got := audit.snapshot(); len(got) != 6 {
		t.Errorf("audit records = %q, want all three records of both PATCH requests", got)
	}
}

type deadlineRecordingWriter struct {
	*httptest.ResponseRecorder
	deadlines []time.Time
}

func (w *deadlineRecordingWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadlines = append(w.deadlines, deadline)
	return nil
}

// 処理中は期限を設けず、応答を書き始めた時点で一度だけ期限を設けます。
func TestResponseWriteTimeoutStartsWithTheResponse(t *testing.T) {
	for _, explicitHeader := range []bool{false, true} {
		t.Run(fmt.Sprint(explicitHeader), func(t *testing.T) {
			writer := &deadlineRecordingWriter{ResponseRecorder: httptest.NewRecorder()}
			var started time.Time
			handler := responseWriteTimeoutHandler{timeout: time.Second, handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if len(writer.deadlines) != 0 {
					t.Fatal("write deadline was set before the response started")
				}
				started = time.Now()
				if explicitHeader {
					w.WriteHeader(http.StatusOK)
				}
				_, _ = io.WriteString(w, "first")
				_, _ = io.WriteString(w, "second")
			})}
			handler.ServeHTTP(writer, httptest.NewRequest(http.MethodGet, "/settings", nil))
			if len(writer.deadlines) != 1 {
				t.Fatalf("write deadlines = %v, want one deadline for the whole response", writer.deadlines)
			}
			if deadline := writer.deadlines[0]; deadline.Before(started.Add(time.Second)) || deadline.After(time.Now().Add(time.Second)) {
				t.Errorf("write deadline = %s, want one second from the first write", deadline)
			}
			if got := writer.Body.String(); got != "firstsecond" {
				t.Errorf("response body = %q, want firstsecond", got)
			}
		})
	}
}
