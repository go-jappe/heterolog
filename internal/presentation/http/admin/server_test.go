package admin

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	server, err := StartServer(pkgadmin.ServerConfig{Addr: ":0", Token: tokenForTest, OnChange: func(pkgadmin.Change) {}},
		map[string]pkglogger.SettingsController{"app": controller})
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
			server, err := StartServer(tt.cfg, loggers)
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
		map[string]pkglogger.SettingsController{"app": newControllerForTest(t)})
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
