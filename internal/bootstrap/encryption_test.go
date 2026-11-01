package bootstrap_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/cipherfile"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
)

// 暗号化を有効にした3種のfile loggerは、公開鍵だけでlog fileを書き、生成した秘密鍵で
// 復号すると書いた記録が戻ります。
func TestFileLoggersEncryptLogFiles(t *testing.T) {
	identityPath := filepath.Join(t.TempDir(), "identity")
	recipient, keyID, err := bootstrap.GenerateEncryptionIdentity(identityPath)
	if err != nil {
		t.Fatal(err)
	}
	if keyID == "" || !strings.HasPrefix(recipient, "hlog-pub1-") {
		t.Fatalf("GenerateEncryptionIdentity returned %q, %q", recipient, keyID)
	}
	for _, kind := range []string{"sync", "async", "delivery"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			cfg := directoryWhitespaceConfig(root, "encrypted", "logs")
			cfg.File.FsstoreConfig.Encryption = pkgfsstore.EncryptionConfig{Enabled: true, Recipient: recipient}
			const message = "secret record that must not be readable on disk"
			testutil.CaptureStdout(t, func() {
				write, closeLogger := fileOutputWriter(t, kind, cfg)
				t.Cleanup(func() { _ = closeLogger() })
				if err := write(message); err != nil {
					t.Fatal(err)
				}
				if err := closeLogger(); err != nil {
					t.Fatal(err)
				}
			})
			paths, err := filepath.Glob(filepath.Join(root, "encrypted", "logs", "*.log"))
			if err != nil || len(paths) != 1 {
				t.Fatalf("log files = %v, %v", paths, err)
			}
			contents, err := os.ReadFile(paths[0])
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(contents, []byte(message)) || !bytes.HasPrefix(contents, []byte(cipherfile.Magic)) {
				t.Fatalf("the log file is readable: %q", contents)
			}
			var plain bytes.Buffer
			report, err := bootstrap.DecryptFile(identityPath, "", paths[0], &plain)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(plain.String(), message) || !report.Complete() || report.Records != 1 {
				t.Fatalf("decrypted %q with report %+v", plain.String(), report)
			}
		})
	}
}

// 公開鍵のない暗号化の設定と、読めない公開鍵は、loggerの構築のerrorです。
func TestFileLoggerRejectsEncryptionWithoutRecipient(t *testing.T) {
	for _, recipient := range []string{"", "not-a-key"} {
		cfg := directoryWhitespaceConfig(t.TempDir(), "encrypted", "logs")
		cfg.File.FsstoreConfig.Encryption = pkgfsstore.EncryptionConfig{Enabled: true, Recipient: recipient}
		logger, err := bootstrap.New(context.Background(), cfg)
		if err == nil {
			_ = logger.Close()
			t.Fatalf("recipient %q: New succeeded", recipient)
		}
		if recipient == "" && !strings.Contains(err.Error(), cipherfile.ErrRecipientRequired.Error()) {
			t.Fatalf("error %v does not name the missing recipient", err)
		}
		if recipient != "" && !strings.Contains(err.Error(), cipherfile.ErrInvalidRecipient.Error()) {
			t.Fatalf("error %v does not name the invalid recipient", err)
		}
	}
}

// 暗号化と圧縮を両方有効にすると、圧縮は無視され、構築時に警告が出ます。
func TestFileLoggerWarnsWhenEncryptionSuppressesCompression(t *testing.T) {
	identityPath := filepath.Join(t.TempDir(), "identity")
	recipient, _, err := bootstrap.GenerateEncryptionIdentity(identityPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg := directoryWhitespaceConfig(t.TempDir(), "encrypted", "logs")
	cfg.File.FsstoreConfig.Encryption = pkgfsstore.EncryptionConfig{Enabled: true, Recipient: recipient}
	cfg.File.FsstoreConfig.Lifecycle.EnableFileCompress = true
	cfg.File.FsstoreConfig.Lifecycle.CompressFormat = pkgfsstore.CompressGzip
	// 内部logの警告の通知先をstdout loggerにして、構築時の警告を受け取ります。stdout loggerは
	// Warn以上を標準errorに書きます。
	cfg.File.HandlerConfig = pkgloggerhandler.HandlerConfig{OnWarnConfig: []string{pkglogger.LogOutputStdout.String()}}
	cfg.Stdout = bootstrap.DefaultConfig().Stdout
	output := captureStderr(t, func() {
		logger, err := bootstrap.New(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := logger.Close(); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(output, cipherfile.CompressionSuppressedWarning) {
		t.Fatalf("no compression warning in the internal log output: %q", output)
	}
}

// captureStderrは、fnを実行する間だけos.Stderrを一時fileへ差し替え、書かれた内容を返します。
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "stderr")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	original := os.Stderr
	os.Stderr = file
	defer func() { os.Stderr = original }()
	fn()
	os.Stderr = original
	output, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	return string(output)
}
