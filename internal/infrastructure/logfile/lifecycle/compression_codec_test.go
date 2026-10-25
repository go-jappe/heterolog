package lifecycle

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"

	"github.com/stretchr/testify/require"
)

var errRunnerTestCompressedFileCorrupt = errors.New("runner test compressed file is corrupt")

const runnerTestCompressionMagic = "runner-test-codec\x00"

// runnerTestCompressionCodecは独自formatの圧縮と完全性検証が、同じinstanceから
// default/error/debugおよびfailure archiveの各FileSeriesManagerへ届くことを確認するcodecです。
type runnerTestCompressionCodec struct {
	mu            sync.Mutex
	compressCalls int
	verifyCalls   int
	invalidOutput bool
}

func (*runnerTestCompressionCodec) Format() pkgfsstore.CompressFormat {
	return pkgfsstore.CompressFormat("br")
}

func (c *runnerTestCompressionCodec) Compress(in, out *os.File) error {
	c.mu.Lock()
	c.compressCalls++
	c.mu.Unlock()

	if c.invalidOutput {
		_, err := io.WriteString(out, "invalid runner codec output")
		return err
	}
	if _, err := io.WriteString(out, runnerTestCompressionMagic); err != nil {
		return err
	}
	_, err := io.Copy(out, in)
	return err
}

func (c *runnerTestCompressionCodec) VerifyCompressedIntegrity(in *os.File) error {
	c.mu.Lock()
	c.verifyCalls++
	c.mu.Unlock()

	content, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	if !bytes.HasPrefix(content, []byte(runnerTestCompressionMagic)) {
		return errors.Join(pkgfsstore.ErrCorruptCompressedFile, errRunnerTestCompressedFileCorrupt)
	}
	return nil
}

func (c *runnerTestCompressionCodec) calls() (compress int, verify int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.compressCalls, c.verifyCalls
}

func TestNewLogFileLifecyclesPropagatesCustomCompressionCodecToEveryFileSeries(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "custom-codec-log-runner"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	codec := &runnerTestCompressionCodec{}
	cfg := pkgfsstore.LogFileLifecycleRunnerConfig{
		FsstoreConfig: pkgfsstore.Config{
			Layout: pkgfsstore.FsstoreConfig{
				Target:                     target,
				LogPath:                    root,
				LogDir:                     "logs",
				ErrorLogDir:                "logs",
				IsSeparateErrorLogsEnabled: true,
				SeparateErrorLogsPrefix:    "error_",
				IsSeparateDebugLogsEnabled: true,
				SeparateDebugLogsPrefix:    "debug_",
				FileNameFormat:             pkgfsstore.FileNameFormat,
				FileDateFormat:             pkgfsstore.FileDateFormat,
			},
			Lifecycle: pkgfsstore.FileLifecycleConfig{
				UsageCacheTTL:      time.Hour,
				EnableRotate:       true,
				RotationType:       pkgfsstore.FileRotationDaily,
				MaxFileBackups:     10,
				MaxFileAge:         36500,
				MaxFileSize:        1 << 20,
				MaxDirSize:         20 << 20,
				EnableFileCompress: true,
				// custom codecが設定値より優先されることも同時に確認します。
				CompressFormat: pkgfsstore.CompressGzip,
			},
			CompressionCodec: codec,
		},
		RunnerConfig: pkgfsstore.AppendLogRunnerConfig{Interval: time.Minute},
	}

	date := currentLogDate()
	inactivePaths := []string{
		filepath.Join(logDir, target+"_"+date+"_1.log"),
		filepath.Join(logDir, "error_"+target+"_"+date+"_1.log"),
		filepath.Join(logDir, "debug_"+target+"_"+date+"_1.log"),
	}
	latestPaths := []string{
		filepath.Join(logDir, target+"_"+date+"_2.log"),
		filepath.Join(logDir, "error_"+target+"_"+date+"_2.log"),
		filepath.Join(logDir, "debug_"+target+"_"+date+"_2.log"),
	}
	for _, path := range append(append([]string(nil), inactivePaths...), latestPaths...) {
		require.NoError(t, os.WriteFile(path, []byte(filepath.Base(path)), 0o600))
	}

	lifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
	require.NoError(t, err)
	require.Len(t, lifecycles, 1)
	require.Len(t, lifecycles[0].fileSeriesManagers, 3)
	for _, manager := range lifecycles[0].fileSeriesManagers {
		require.Equal(t, pkgfsstore.CompressFormat("br"), manager.store.GetCompressFormat())
	}

	report, err := lifecycles[0].RunOnce(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Compressed, 3)
	for _, path := range inactivePaths {
		require.NoFileExists(t, path)
		require.FileExists(t, path+".br")
		require.NoFileExists(t, path+pkgfsstore.ExtensionCompressGzip)
	}
	for _, path := range latestPaths {
		require.FileExists(t, path)
	}
	compressCalls, verifyCalls := codec.calls()
	require.Equal(t, 3, compressCalls)
	require.Equal(t, 3, verifyCalls)
}

func TestNewDeliveryFileLifecyclesUsesUserDefinedCompressionCodec(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "custom-codec-delivery-runner"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	codec := &runnerTestCompressionCodec{}
	cfg := pkgfsstore.DeliveryFileLifecycleRunnerConfig{
		FsstoreConfig: pkgfsstore.DeliveryFileLifecycleStoreConfig{
			Layout: pkgfsstore.FsstoreConfig{
				Target:         target,
				LogPath:        root,
				LogDir:         "logs",
				FileNameFormat: pkgfsstore.FileNameFormat,
				FileDateFormat: pkgfsstore.FileDateFormat,
			},
			Lifecycle: pkgfsstore.DeliveryFileLifecycleConfig{
				UsageCacheTTL:      time.Hour,
				EnableRotate:       true,
				RotationType:       pkgfsstore.FileRotationDaily,
				MaxFileBackups:     10,
				MaxFileAge:         7,
				MaxFileSize:        1 << 20,
				MaxDirSize:         100 << 20,
				EnableFileCompress: true,
				// userが追加したcodecのFormatを、組み込み形式の設定値より優先する。
				CompressFormat: pkgfsstore.CompressGzip,
			},
			CompressionCodec: codec,
		},
		RunnerConfig: pkgfsstore.AppendLogRunnerConfig{Interval: time.Minute},
	}

	date := currentLogDate()
	inactivePath := filepath.Join(logDir, target+"_"+date+"_1.log")
	latestPath := filepath.Join(logDir, target+"_"+date+"_2.log")
	require.NoError(t, os.WriteFile(inactivePath, []byte("delivery payload"), 0o600))
	require.NoError(t, os.WriteFile(latestPath, []byte("active delivery payload"), 0o600))

	lifecycles, err := NewDeliveryFileLifecycles(DeliveryFileOptions{Config: cfg})
	require.NoError(t, err)
	require.Len(t, lifecycles, 1)
	require.Len(t, lifecycles[0].fileSeriesManagers, 1)
	require.Equal(
		t,
		pkgfsstore.CompressFormat("br"),
		lifecycles[0].fileSeriesManagers[0].store.GetCompressFormat(),
	)

	report, err := lifecycles[0].RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, []CompressionReport{{
		SourcePath:     inactivePath,
		CompressedPath: inactivePath + ".br",
	}}, report.Compressed)
	require.NoFileExists(t, inactivePath)
	require.FileExists(t, inactivePath+".br")
	require.NoFileExists(t, inactivePath+pkgfsstore.ExtensionCompressGzip)
	require.FileExists(t, latestPath)

	compressCalls, verifyCalls := codec.calls()
	require.Equal(t, 1, compressCalls)
	require.Equal(t, 1, verifyCalls)
}

func TestFileSeriesManagerRetriesWhenCustomCodecGeneratesInvalidOutput(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "invalid-custom-codec-output"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	date := currentLogDate()
	sourcePath := filepath.Join(logDir, target+"_"+date+"_1.log")
	activePath := filepath.Join(logDir, target+"_"+date+"_2.log")
	require.NoError(t, os.WriteFile(sourcePath, []byte("source must survive"), 0o600))
	require.NoError(t, os.WriteFile(activePath, []byte("active"), 0o600))

	codec := &runnerTestCompressionCodec{invalidOutput: true}
	config := appendlog.NewConfigAdapter(pkgfsstore.Config{
		Layout: pkgfsstore.FsstoreConfig{
			Target:         target,
			LogPath:        root,
			LogDir:         "logs",
			FileNameFormat: pkgfsstore.FileNameFormat,
			FileDateFormat: pkgfsstore.FileDateFormat,
		},
		Lifecycle: pkgfsstore.FileLifecycleConfig{
			EnableRotate:       true,
			RotationType:       pkgfsstore.FileRotationDaily,
			MaxFileBackups:     10,
			MaxFileSize:        1024,
			EnableFileCompress: true,
			CompressFormat:     pkgfsstore.CompressGzip,
		},
		CompressionCodec: codec,
	})
	recorder := &recordingUploader{}
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   config,
		Uploader: recorder,
	})

	for range 2 {
		report, err := manager.RunOnce(context.Background())
		require.ErrorIs(t, err, infrafsstore.ErrInvalidCompressedFile)
		require.ErrorIs(t, err, errRunnerTestCompressedFileCorrupt)
		require.Empty(t, report.Compressed)
		require.Empty(t, report.ObjectStorageUploads)
		require.Empty(t, recorder.pathsSnapshot())
		require.FileExists(t, sourcePath)
		require.NoFileExists(t, sourcePath+".br")
		require.NoFileExists(t, sourcePath+".br.tmp")
	}

	compressCalls, verifyCalls := codec.calls()
	require.Equal(t, 2, compressCalls)
	require.Equal(t, 2, verifyCalls)
	require.FileExists(t, activePath)
}

func TestNewFailureArchiveLifecyclesPropagatesCustomCompressionCodecToBothKinds(t *testing.T) {
	codec := &runnerTestCompressionCodec{}
	durableCfg := newFailureArchiveLifecycleDurableConfig(t, objectstorage.UploadTargetConfig{})
	durableCfg.CompressionCodec = codec
	// custom codecが設定値より優先されることも同時に確認します。
	durableCfg.CompressFormat = pkgfsstore.CompressZstd
	cfg := newFailureArchiveTestConfigAdapter(durableCfg)

	lifecycles, err := NewFailureArchiveLifecycles(FailureArchiveOptions{Config: cfg})
	require.NoError(t, err)
	require.Len(t, lifecycles, 2)

	for index, terminalDir := range []string{cfg.DirCaseRetryable(), cfg.DirCasePermanent()} {
		manager := lifecycles[index].fileSeriesManagers[0]
		require.Equal(t, pkgfsstore.CompressFormat("br"), manager.store.GetCompressFormat())

		dir, pathErr := failurearchive.GetFileArchiveDirectoryPath(cfg, terminalDir)
		require.NoError(t, pathErr)
		inactivePath := filepath.Join(dir, cfg.Target()+"_20240101_1.jsonl")
		latestPath := filepath.Join(dir, cfg.Target()+"_20240101_2.jsonl")
		require.NoError(t, os.WriteFile(inactivePath, []byte("{}\n"), 0o600))
		require.NoError(t, os.WriteFile(latestPath, []byte("{}\n"), 0o600))

		report, runErr := lifecycles[index].RunOnce(context.Background())
		require.NoError(t, runErr)
		require.Equal(t, []CompressionReport{{
			SourcePath:     inactivePath,
			CompressedPath: inactivePath + ".br",
		}}, report.Compressed)
		require.NoFileExists(t, inactivePath)
		require.FileExists(t, inactivePath+".br")
		require.NoFileExists(t, inactivePath+pkgfsstore.ExtensionCompressZstd)
		require.FileExists(t, latestPath)
	}

	compressCalls, verifyCalls := codec.calls()
	require.Equal(t, 2, compressCalls)
	require.Equal(t, 2, verifyCalls)
}

func TestNewFailureArchiveLifecyclesUsesDeliveryPublicConfigCustomCompressionCodec(t *testing.T) {
	codec := &runnerTestCompressionCodec{}
	baseCfg := newFailureArchiveLifecycleDurableConfig(t, objectstorage.UploadTargetConfig{})
	baseCfg.CompressionCodec = codec
	// delivery public configでもcustom codecが組み込み形式の設定値より優先される。
	baseCfg.CompressFormat = pkgfsstore.CompressXz
	deliveryCfg := pkgdurable.DeliveryFileArchiveConfig(baseCfg)
	deliveryAdapter := loggerdurable.NewDeliveryFailureStoreConfigAdapter(
		pkgdurable.DeliveryFailureStoreConfig{FileArchiveConfig: deliveryCfg},
	)
	cfg := deliveryAdapter.DeliveryDurableStoreConfig().FileArchiveConfig()

	lifecycles, err := NewFailureArchiveLifecycles(FailureArchiveOptions{Config: cfg})
	require.NoError(t, err)
	require.Len(t, lifecycles, 2)

	for index, terminalDir := range []string{cfg.DirCaseRetryable(), cfg.DirCasePermanent()} {
		manager := lifecycles[index].fileSeriesManagers[0]
		require.Equal(t, pkgfsstore.CompressFormat("br"), manager.store.GetCompressFormat())

		dir, pathErr := failurearchive.GetFileArchiveDirectoryPath(cfg, terminalDir)
		require.NoError(t, pathErr)
		inactivePath := filepath.Join(dir, cfg.Target()+"_20240101_1.jsonl")
		latestPath := filepath.Join(dir, cfg.Target()+"_20240101_2.jsonl")
		require.NoError(t, os.WriteFile(inactivePath, []byte("{}\n"), 0o600))
		require.NoError(t, os.WriteFile(latestPath, []byte("{}\n"), 0o600))

		report, runErr := lifecycles[index].RunOnce(context.Background())
		require.NoError(t, runErr)
		require.Equal(t, []CompressionReport{{
			SourcePath:     inactivePath,
			CompressedPath: inactivePath + ".br",
		}}, report.Compressed)
		require.NoFileExists(t, inactivePath)
		require.FileExists(t, inactivePath+".br")
		require.NoFileExists(t, inactivePath+pkgfsstore.ExtensionCompressXz)
		require.FileExists(t, latestPath)
	}

	compressCalls, verifyCalls := codec.calls()
	require.Equal(t, 2, compressCalls)
	require.Equal(t, 2, verifyCalls)
}

func TestFileSeriesManagerRecoveryUsesCustomCodecToClassifyCompressedPeer(t *testing.T) {
	tests := []struct {
		name        string
		peerContent []byte
		wantError   error
		wantArchive bool
	}{
		{
			name:        "valid peer",
			peerContent: []byte(runnerTestCompressionMagic + "valid payload"),
			wantArchive: true,
		},
		{
			name:        "corrupt peer",
			peerContent: []byte("not the custom format"),
			wantError:   errRunnerTestCompressedFileCorrupt,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := resolvedTempDirForTest(t)
			const target = "custom-codec-recovery"
			logDir := filepath.Join(root, target, "logs")
			require.NoError(t, os.MkdirAll(logDir, 0o750))

			date := currentLogDate()
			sourcePath := filepath.Join(logDir, target+"_"+date+"_1.log")
			peerPath := sourcePath + ".br"
			latestPath := filepath.Join(logDir, target+"_"+date+"_2.log")
			require.NoError(t, os.WriteFile(sourcePath, []byte("valid payload"), 0o600))
			require.NoError(t, os.WriteFile(peerPath, test.peerContent, 0o600))
			require.NoError(t, os.WriteFile(latestPath, []byte("active"), 0o600))

			codec := &runnerTestCompressionCodec{}
			config := appendlog.NewConfigAdapter(pkgfsstore.Config{
				Layout: pkgfsstore.FsstoreConfig{
					Target:         target,
					LogPath:        root,
					LogDir:         "logs",
					FileNameFormat: pkgfsstore.FileNameFormat,
					FileDateFormat: pkgfsstore.FileDateFormat,
				},
				Lifecycle: pkgfsstore.FileLifecycleConfig{
					EnableRotate:       true,
					RotationType:       pkgfsstore.FileRotationDaily,
					MaxFileBackups:     10,
					MaxFileSize:        1024,
					EnableFileCompress: true,
					CompressFormat:     pkgfsstore.CompressGzip,
				},
				CompressionCodec: codec,
			})
			recorder := &recordingUploader{}
			manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
				Config:   config,
				FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
				Uploader: recorder,
			})

			report, err := manager.RunOnce(context.Background())
			if test.wantError != nil {
				require.ErrorIs(t, err, infrafsstore.ErrInvalidCompressedFile)
				require.ErrorIs(t, err, test.wantError)
				require.Empty(t, report.ObjectStorageUploads)
				require.Empty(t, recorder.pathsSnapshot())
				require.Empty(t, report.Purge.ByGeneration.RemovalResults)
				require.Empty(t, report.Purge.ByDirSize.RemovalResults)
				require.FileExists(t, sourcePath)
				require.NoFileExists(t, peerPath)

				// 破損peerを除去した周期では停止し、次周期にsourceから再生成します。
				report, err = manager.RunOnce(context.Background())
				require.NoError(t, err)
				require.Len(t, report.Compressed, 1)
				require.Len(t, report.ObjectStorageUploads, 1)
				require.Equal(t, []string{peerPath}, recorder.pathsSnapshot())
				require.NoFileExists(t, sourcePath)
				require.NoFileExists(t, peerPath)
				require.Len(t, report.UploadedFileRemovals, 1)
				require.NoError(t, report.UploadedFileRemovals[0].Err)
			} else {
				require.NoError(t, err)
				require.Len(t, report.ObjectStorageUploads, 1)
				require.Equal(t, []string{peerPath}, recorder.pathsSnapshot())
				require.NoFileExists(t, sourcePath)
				require.NoFileExists(t, logpattern.CompressionSourceDeletePendingPath(sourcePath))
				require.NoFileExists(t, peerPath)
				require.Len(t, report.UploadedFileRemovals, 1)
				require.NoError(t, report.UploadedFileRemovals[0].Err)
			}

			compressCalls, verifyCalls := codec.calls()
			if test.wantError != nil {
				require.Equal(t, 1, compressCalls)
				require.Equal(t, 2, verifyCalls)
			} else {
				require.Zero(t, compressCalls)
				require.Equal(t, 1, verifyCalls)
			}
			require.FileExists(t, latestPath)
		})
	}
}

var _ pkgfsstore.CompressionCodec = (*runnerTestCompressionCodec)(nil)
