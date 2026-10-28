package admin

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	pkgadmin "github.com/go-jappe/heterolog/pkg/admin"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

var (
	// ErrServerAddrRequiredは、ServerConfigにAddrがないことを表します。
	ErrServerAddrRequired = errors.New("admin server address is required")
	// ErrServerInsecureは、TLSなしでloopback以外のaddressで待ち受けようとしたことを
	// 表します。tokenをnetwork上に平文で流さないため拒否します。
	ErrServerInsecure = errors.New("admin server listening on a non-loopback address requires TLS")
	// ErrServerTLSIncompleteは、TLSCertFileとTLSKeyFileの片方だけを指定したことを表します。
	ErrServerTLSIncomplete = errors.New("admin server TLS requires both a certificate file and a key file")
)

// ValidateServerConfigは、StartServerと同じ規則で設定を確かめます。errorにtokenは含めません。
func ValidateServerConfig(cfg pkgadmin.ServerConfig) error {
	_, err := listenAddress(cfg)
	return err
}

// listenAddressは、設定を確かめ、待ち受けるaddressを返します。
func listenAddress(cfg pkgadmin.ServerConfig) (string, error) {
	if strings.TrimSpace(cfg.Addr) == "" {
		return "", ErrServerAddrRequired
	}
	host, port, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		return "", fmt.Errorf("invalid admin server address %q: %w", cfg.Addr, err)
	}
	if host == "" {
		host = "127.0.0.1"
	}
	useTLS := cfg.TLSCertFile != "" || cfg.TLSKeyFile != ""
	if useTLS && (cfg.TLSCertFile == "" || cfg.TLSKeyFile == "") {
		return "", ErrServerTLSIncomplete
	}
	if !useTLS && !isLoopbackHost(host) {
		return "", fmt.Errorf("%w: %q", ErrServerInsecure, cfg.Addr)
	}
	if len(cfg.Token) < pkgadmin.MinTokenLength {
		return "", ErrTokenTooShort
	}
	return net.JoinHostPort(host, port), nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Serverは、NewHandlerの管理用APIを、ShutdownかCloseまで提供します。
type Server struct {
	server   *http.Server
	listener net.Listener
	done     chan struct{}
	serveErr error
}

// defaultResponseWriteTimeoutは、応答を書き始めてから書き終えるまでの期限です。
const defaultResponseWriteTimeout = 10 * time.Second

// StartServerは、管理用API専用のHTTP serverを起動し、待ち受けを始めてから返ります。
// loggersは、APIで使う名前と、設定を読み取り・変更するloggerの対応で、auditは監査logの
// 記録先です。cfgがhostを明示してTLSを有効にしない限り、loopback addressで待ち受けます。
// 止めるにはShutdownかCloseを呼びます。auditは閉じないため、serverを止めた後で閉じます。
func StartServer(cfg pkgadmin.ServerConfig, loggers map[string]pkglogger.SettingsController, audit pkglogger.DeliveryLogger) (*Server, error) {
	return startServer(cfg, loggers, audit, defaultResponseWriteTimeout)
}

// startServerは、応答の書き込み期限をresponseWriteTimeoutにして、StartServerの処理を
// 行います。試験は短い期限を渡します。
func startServer(cfg pkgadmin.ServerConfig, loggers map[string]pkglogger.SettingsController, audit pkglogger.DeliveryLogger, responseWriteTimeout time.Duration) (*Server, error) {
	address, err := listenAddress(cfg)
	if err != nil {
		return nil, err
	}
	handler, err := NewHandler(pkgadmin.HandlerConfig{Token: cfg.Token, Loggers: loggers}, audit)
	if err != nil {
		return nil, err
	}
	var tlsConfig *tls.Config
	if cfg.TLSCertFile != "" {
		certificate, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load admin server certificate: %w", err)
		}
		tlsConfig = &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen for the admin server: %w", err)
	}
	if tlsConfig != nil {
		listener = tls.NewListener(listener, tlsConfig)
	}
	s := &Server{
		server: &http.Server{
			Handler:           responseWriteTimeoutHandler{handler: handler, timeout: responseWriteTimeout},
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			// 監査logの送信と、ほかのPATCHの終了待ちには固定の応答期限を設けません。
			// 応答を書き始めてからの期限はresponseWriteTimeoutHandlerが設定します。
			IdleTimeout:    time.Minute,
			MaxHeaderBytes: 16 << 10,
		},
		listener: listener,
		done:     make(chan struct{}),
	}
	go func() {
		defer close(s.done)
		if err := s.server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
			s.serveErr = err
		}
	}()
	return s, nil
}

// responseWriteTimeoutHandlerは、要求の処理時間を含めず、応答の書き込みだけに期限を
// 設けます。監査loggerは一件ごとに自身のtimeoutを持ち、要求ごとの記録数や、直列に
// 処理するPATCHの待ち時間も異なるため、要求全体に固定のWriteTimeoutは使いません。
type responseWriteTimeoutHandler struct {
	handler http.Handler
	timeout time.Duration
}

func (h responseWriteTimeoutHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.handler.ServeHTTP(&responseWriteTimeoutWriter{ResponseWriter: w, timeout: h.timeout}, r)
}

type responseWriteTimeoutWriter struct {
	http.ResponseWriter
	timeout time.Duration
	started bool
}

func (w *responseWriteTimeoutWriter) beginWrite() {
	if w.started {
		return
	}
	w.started = true
	_ = http.NewResponseController(w.ResponseWriter).SetWriteDeadline(time.Now().Add(w.timeout))
}

func (w *responseWriteTimeoutWriter) WriteHeader(status int) {
	w.beginWrite()
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseWriteTimeoutWriter) Write(body []byte) (int, error) {
	w.beginWrite()
	return w.ResponseWriter.Write(body)
}

// Unwrapは、ResponseControllerが元のResponseWriterの機能へ到達できるようにします。
func (w *responseWriteTimeoutWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Addrは、待ち受けているaddressを返します。ServerConfig.Addrでport 0を指定した場合は、
// 選ばれたportを含みます。
func (s *Server) Addr() string {
	return s.listener.Addr().String()
}

// Shutdownは、新しい接続の受け付けを止め、処理中の要求が終わるかctxが終わるまで待ちます。
// ctxが先に終わった場合はctxのerrorを返し、そうでなければserverを止めたerrorがあれば返します。
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.server.Shutdown(ctx)
	if err == nil {
		<-s.done
		err = s.serveErr
	}
	return err
}

// Closeは、serverをすぐに止め、処理中の接続を閉じます。
func (s *Server) Close() error {
	err := s.server.Close()
	<-s.done
	return errors.Join(err, s.serveErr)
}
