package heterolog_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/ulikunitz/xz"
)

func TestPublicXZRecoveryPreservesSourceUntilPeerIsComplete(t *testing.T) {
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
				logger, err := heterolog.New(context.Background(), cfg)
				if err != nil {
					t.Fatal(err)
				}
				logErr := logger.Info(context.Background(), "trigger recovery scan")
				if err := errors.Join(logErr, logger.Close()); err != nil {
					t.Fatal(err)
				}
			}
			// The source may be retired only once a verified complete peer exists.
			// The repair may take more than one run, but no run may lose the log.
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
				// A new runtime must recover a retained source using the normal
				// compressor, then retire it only after publishing a complete peer.
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

// requireXZRecoveryPayload checks that path is a complete XZ archive of payload
// and returns its compressed bytes.
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

func xzRecoveryConfig(root, target, dir string) heterolog.Config {
	cfg := heterolog.DefaultConfig()
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
			Retry: pkgfsstore.RetryConfig{MaxWriteAttempts: 1, FileOperationRetryWaitTime: time.Millisecond},
		},
		RuntimeConfig: pkglogger.RuntimeConfig{AppName: "xz-recovery", Target: target, LogLevel: heterolog.LevelInfo},
		LogBodyConfig: pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano},
	}
	return cfg
}
