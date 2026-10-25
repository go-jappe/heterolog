package logrecovery

import (
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRecoverRetryModeReusesManualProcessingAfterCorruptPeerRemoval(t *testing.T) {
	cfg := loadRecoveryTestConfig(t, map[string]string{"HETEROLOG_MAX_LOG_FILE_AGE": "1", "HETEROLOG_MAX_LOG_FILE_BACKUPS": "1"})
	directory := filepath.Join(cfg.FsstoreConfig.LogPath, "service", "sync")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	source := filepath.Join(directory, "service_20000101_1.log")
	payload := []byte("retry existing log\n")
	require.NoError(t, os.WriteFile(source, payload, 0o600))
	require.NoError(t, os.WriteFile(source+".gz", []byte("corrupt old peer"), 0o600))
	var retries atomic.Int64
	report, err := Recover(context.Background(), cfg, Options{
		Retry: true, RetryInterval: time.Millisecond,
		OnProgress: func(progress Progress) {
			if progress.Phase == ProgressRetryWaiting {
				retries.Add(1)
			}
		},
	})
	require.NoError(t, err)
	// 破損した圧縮ファイルを除いた後、同じ処理を再試行して保持した元ログを圧縮する経路の
	// 試験です。再試行の回数は処理の進め方によるため固定せず、一度以上の再試行を前提とします。
	require.GreaterOrEqual(t, retries.Load(), int64(1), "corrupt peer removal must leave the source for a retry")
	require.Empty(t, report.Errors)
	require.NoFileExists(t, source)
	file, err := os.Open(source + ".gz")
	require.NoError(t, err)
	defer file.Close()
	reader, err := gzip.NewReader(file)
	require.NoError(t, err, "corrupt peer must be replaced by a complete archive")
	recovered, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, payload, recovered)
	require.Len(t, report.Directories, 1)
	require.Contains(t, report.Directories[0].CompressedFiles, source+".gz")
	require.Empty(t, report.Directories[0].FailedFiles)
}
