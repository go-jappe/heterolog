package bootstrap_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/ulikunitz/xz"
)

func TestXZRecoveryPreservesSourceUntilPeerIsComplete(t *testing.T) {
	for _, truncated := range []bool{false, true} {
		name := "complete peer"
		if truncated {
			name = "header without blocks or footer"
		}
		t.Run(name, func(t *testing.T) {
			root, target, logDir := t.TempDir(), "xz-recovery", "logs"
			dir := filepath.Join(root, target, logDir)
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			raw := filepath.Join(dir, target+"_"+time.Now().UTC().AddDate(0, 0, -1).Format("20060102")+"_1.log")
			payload := []byte("valuable log from the previous process\n")
			if err := os.WriteFile(raw, payload, 0o600); err != nil {
				t.Fatal(err)
			}
			var compressed bytes.Buffer
			w, err := xz.NewWriter(&compressed)
			if err != nil {
				t.Fatal(err)
			}
			_, writeErr := w.Write(payload)
			if err := errors.Join(writeErr, w.Close()); err != nil {
				t.Fatal(err)
			}
			peer := compressed.Bytes()
			if truncated {
				peer = peer[:xz.HeaderLen]
			}
			if err := os.WriteFile(raw+".xz", peer, 0o600); err != nil {
				t.Fatal(err)
			}

			cfg := xzRecoveryConfig(root, target, logDir)
			runRecovery := func() {
				t.Helper()
				logger, err := bootstrap.New(context.Background(), cfg)
				if err != nil {
					t.Fatal(err)
				}
				logErr := logger.Info(context.Background(), "trigger recovery scan")
				if err := errors.Join(logErr, logger.Close()); err != nil {
					t.Fatal(err)
				}
			}
			// 元のfileを退役させてよいのは、検証済みの完全な対のfileができてからだけです。
			// 修復は複数回の実行にわたることがありますが、どの実行でもlogを失ってはいけません。
			requireSourceUntilVerifiedPeer := func() {
				t.Helper()
				got, err := os.ReadFile(raw)
				if err == nil {
					if !bytes.Equal(got, payload) {
						t.Fatalf("retained source changed: data=%q", got)
					}
					return
				}
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatal(err)
				}
				requireXZRecoveryPayload(t, raw+".xz", payload)
			}
			runRecovery()
			requireSourceUntilVerifiedPeer()
			if truncated {
				// 新しいruntimeは、残っている元のfileを通常のcompressorで復旧し、完全な対のfileを
				// 公開した後にだけ退役させる必要があります。
				runRecovery()
				requireSourceUntilVerifiedPeer()
			}
			if _, err := os.Stat(raw); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("source was not retired after verified compression: %v", err)
			}
			archive := requireXZRecoveryPayload(t, raw+".xz", payload)
			if !truncated && !bytes.Equal(archive, peer) {
				t.Fatal("complete peer was overwritten")
			}
		})
	}
}

// requireXZRecoveryPayloadは、pathがpayloadの完全なXZ archiveであることを確かめ、
// 圧縮したbyte列を返します。
func requireXZRecoveryPayload(t *testing.T, path string, payload []byte) []byte {
	t.Helper()
	archive, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("source was retired without a compressed peer: %v", err)
	}
	reader, err := xz.NewReader(bytes.NewReader(archive))
	if err != nil {
		t.Fatalf("source was retired with an invalid compressed peer: %v", err)
	}
	decoded, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(decoded, payload) {
		t.Fatalf("recovered archive: data=%q err=%v", decoded, err)
	}
	return archive
}

func xzRecoveryConfig(root, target, dir string) pkglogger.LoggerConfig {
	cfg := loggerruntime.DefaultConfig()
	cfg.Stdout = nil
	cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
	cfg.File = &pkglogger.FileLoggerConfig{
		FsstoreConfig: pkgfsstore.Config{
			Layout: pkgfsstore.FsstoreConfig{
				Target: target, LogPath: root, LogDir: dir,
				FileNameFormat: pkgfsstore.FileNameFormat, FileDateFormat: pkgfsstore.FileDateFormat,
			},
			Lifecycle: pkgfsstore.FileLifecycleConfig{
				RotationType: pkgfsstore.FileRotationDaily, UsageCacheTTL: time.Hour,
				MaxFileBackups: 10, MaxFileAge: 365, MaxFileSize: 1 << 20, MaxDirSize: 100 << 20,
				EnableFileCompress: true, CompressFormat: pkgfsstore.CompressXz,
			},
			Duration: pkgfsstore.DurationConfig{
				CompressWarnThresholdDuration: time.Hour, CompressErrorThresholdDuration: time.Hour,
				WriteFileWarnThresholdDuration: time.Hour, WriteFileErrorThresholdDuration: time.Hour,
			},
			Retry: pkgfsstore.RetryConfig{MaxWriteRetries: 0, FileOperationRetryWaitTime: time.Millisecond},
		},
		RuntimeConfig: pkglogger.RuntimeConfig{AppName: "xz-recovery", Target: target, LogLevel: pkglogger.LogLevelInfo},
		LogBodyConfig: pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano},
	}
	return cfg
}
