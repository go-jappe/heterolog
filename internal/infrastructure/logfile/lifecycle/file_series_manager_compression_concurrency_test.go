package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

func TestFileSeriesManagerCompressionRetentionDoesNotBlockWriterTransitions(t *testing.T) {
	for _, phase := range []string{"compress", "verify peer", "retire source"} {
		t.Run(phase, func(t *testing.T) {
			for _, outcome := range []string{"success", "error", "panic"} {
				t.Run(outcome, func(t *testing.T) {
					entered := make(chan struct{})
					release := make(chan struct{})
					var enteredOnce, releaseOnce sync.Once
					releaseOperation := func() { releaseOnce.Do(func() { close(release) }) }
					operationErr := errors.New("blocked file series operation failed")
					const operationPanic = "blocked file series operation panicked"
					gate := func() error {
						enteredOnce.Do(func() { close(entered) })
						<-release
						switch outcome {
						case "error":
							return operationErr
						case "panic":
							panic(operationPanic)
						}
						return nil
					}
					codec := &fileSeriesConcurrencyCodec{runnerTestCompressionCodec: &runnerTestCompressionCodec{}}
					switch phase {
					case "compress":
						codec.beforeCompress = gate
					case "verify peer":
						codec.beforeVerify = gate
					}
					manager, sourcePath := newCompressionConcurrencyManager(t, codec, phase != "compress")
					if phase == "retire source" {
						manager.removeSourceAfterCompression = func(source, peer string) error {
							if err := gate(); err != nil {
								return err
							}
							return appendlog.RemoveSourceAfterCompression(source, peer)
						}
					}
					guard := manager.directoryGuard
					writer := appendlog.NewDirectoryGuardToken()
					nextWriterPath := filepath.Join(filepath.Dir(sourcePath), "unrelated-writer-next.log")
					require.NoError(t, guard.TransitionActive(writer, func() (string, error) {
						return filepath.Join(filepath.Dir(sourcePath), "unrelated-writer-current.log"), nil
					}))
					t.Cleanup(func() { guard.RemoveToken(writer) })
					done := runCompressionConcurrencyManager(manager)
					// 失敗時もcodecを先に解放し、実file操作の終了後にTempDirを片付けます。
					t.Cleanup(func() {
						releaseOperation()
						select {
						case <-done:
						case <-time.After(boundedwait.HangTimeout):
							t.Error("file series operation did not finish during cleanup")
						}
					})
					select {
					case <-entered:
					case <-time.After(boundedwait.HangTimeout):
						t.Fatal("file series operation did not reach its gate")
					}

					// 圧縮対象とは無関係なwriter切替は、codec・検証・retirementを待ちません。
					transitioned := make(chan error, 1)
					go func() {
						transitioned <- guard.TransitionActive(writer, func() (string, error) {
							return nextWriterPath, nil
						})
						close(transitioned)
					}()
					t.Cleanup(func() {
						releaseOperation()
						select {
						case <-transitioned:
						case <-time.After(boundedwait.HangTimeout):
							t.Error("writer transition did not finish during cleanup")
						}
					})
					select {
					case err := <-transitioned:
						require.NoError(t, err)
					case <-time.After(boundedwait.HangTimeout):
						t.Fatal("unrelated writer transition waited for compression state lock")
					}

					// states lockを解放していても、rawと生成予定peerは他ownerが確保できません。
					peerPath := sourcePath + ".br"
					for _, path := range []string{sourcePath, peerPath} {
						require.False(t, claimCompressionConcurrencyPath(guard, path), "operation did not retain %s", path)
					}
					releaseOperation()
					var result compressionConcurrencyRunResult
					select {
					case result = <-done:
					case <-time.After(boundedwait.HangTimeout):
						t.Fatal("file series operation did not finish after release")
					}
					switch outcome {
					case "success":
						require.NoError(t, result.err)
						require.Nil(t, result.panicValue)
						require.NoFileExists(t, sourcePath)
						require.FileExists(t, peerPath)
					case "error":
						require.ErrorIs(t, result.err, operationErr)
						require.Nil(t, result.panicValue)
						require.FileExists(t, sourcePath)
					case "panic":
						require.Equal(t, operationPanic, result.panicValue)
						require.FileExists(t, sourcePath)
					}
					if outcome == "panic" || (outcome == "error" && phase != "verify peer") {
						for _, path := range []string{sourcePath, peerPath} {
							require.False(t, claimCompressionConcurrencyPath(guard, path), "accepted recovery lost protection for %s", path)
						}
						require.ErrorIs(t, manager.ForceClose(), pkgshutdown.ErrForcedClose)
						require.ErrorIs(t, boundedwait.Close(t, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager)), pkgshutdown.ErrForcedClose)
					}
					for _, path := range []string{sourcePath, peerPath} {
						require.True(t, claimCompressionConcurrencyPath(guard, path), "completed operation leaked retention for %s", path)
					}
				})
			}
		})
	}
}

func TestFileSeriesManagerOverlappingRunOnceDoesNotCompressSameSourceTwice(t *testing.T) {
	// 二つ目のRunOnceが待機していることは、実時間の待機ではなくbubble内の全goroutineが
	// 停止した時点で確認します。
	synctest.Test(t, testFileSeriesManagerOverlappingRunOnceDoesNotCompressSameSourceTwice)
}

func testFileSeriesManagerOverlappingRunOnceDoesNotCompressSameSourceTwice(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	releaseCompression := func() { releaseOnce.Do(func() { close(release) }) }
	codec := &fileSeriesConcurrencyCodec{
		runnerTestCompressionCodec: &runnerTestCompressionCodec{},
		beforeCompress: func() error {
			enteredOnce.Do(func() { close(entered) })
			<-release
			return nil
		},
	}
	manager, sourcePath := newCompressionConcurrencyManager(t, codec, false)
	first := runCompressionConcurrencyManager(manager)
	var second <-chan compressionConcurrencyRunResult
	t.Cleanup(func() {
		releaseCompression()
		for _, done := range []<-chan compressionConcurrencyRunResult{first, second} {
			if done == nil {
				continue
			}
			select {
			case <-done:
			case <-time.After(boundedwait.HangTimeout):
				t.Error("overlapping RunOnce did not finish during cleanup")
			}
		}
	})
	select {
	case <-entered:
	case <-time.After(boundedwait.HangTimeout):
		t.Fatal("first RunOnce did not start compression")
	}
	second = runCompressionConcurrencyManager(manager)
	// states lockを解放しても、同じdirectoryの二つ目のRunOnceはlifecycle lockで待機します。
	synctest.Wait()
	select {
	case result := <-second:
		t.Fatalf("overlapping RunOnce overtook unfinished compression: %+v", result)
	default:
	}
	releaseCompression()
	compressedFiles := 0
	for _, done := range []<-chan compressionConcurrencyRunResult{first, second} {
		select {
		case result := <-done:
			require.NoError(t, result.err)
			require.Nil(t, result.panicValue)
			compressedFiles += len(result.report.Compressed)
		case <-time.After(boundedwait.HangTimeout):
			t.Fatal("overlapping RunOnce did not finish after release")
		}
	}
	require.Equal(t, 1, compressedFiles)
	compressCalls, verifyCalls := codec.calls()
	require.Equal(t, 1, compressCalls)
	require.Equal(t, 1, verifyCalls)
	require.NoFileExists(t, sourcePath)
	require.FileExists(t, sourcePath+".br")
}

func TestFileSeriesManagerDefersRawRecoveryWhenOnlyCompressedPeerIsRetained(t *testing.T) {
	for _, purge := range []string{"age", "generation", "directory size"} {
		t.Run(purge, func(t *testing.T) {
			codec := &fileSeriesConcurrencyCodec{runnerTestCompressionCodec: &runnerTestCompressionCodec{}}
			manager, sourcePath := newCompressionConcurrencyManager(t, codec, true)
			peerPath := sourcePath + ".br"
			limits := &appendlog.FilePurgeConfigAdapter{}
			switch purge {
			case "age":
				limits.MaxFileAgeConfig = 1
				// age purgeはfile名の日付で判定するため、保持期間を過ぎた期間の世代に置き換えます。
				expiredSource := filepath.Join(filepath.Dir(sourcePath), "compression-concurrency_"+logDateDaysBefore(10)+"_1.log")
				require.NoError(t, os.Rename(sourcePath, expiredSource))
				require.NoError(t, os.Rename(peerPath, expiredSource+".br"))
				sourcePath, peerPath = expiredSource, expiredSource+".br"
			case "generation":
				limits.MaxFileBackupsConfig = 1
				// 保護対象peerより新しい圧縮世代を置き、世代数上限の対象にします。
				prefix := strings.TrimSuffix(sourcePath, "_1.log")
				require.NoError(t, os.Rename(
					prefix+"_2.log",
					prefix+"_3.log",
				))
				require.NoError(t, os.WriteFile(
					prefix+"_2.log.br",
					[]byte(runnerTestCompressionMagic+"newer compressed generation"), 0o600,
				))
			case "directory size":
				limits.MaxFileDirSizeConfig = 1
			}
			manager.Config = appendlog.WithPurgeConfig(manager.Config, limits)
			retireCalls := 0
			manager.removeSourceAfterCompression = func(source, peer string) error {
				retireCalls++
				return appendlog.RemoveSourceAfterCompression(source, peer)
			}
			owner := appendlog.NewDirectoryGuardToken()
			manager.directoryGuard.ProtectPaths(owner, peerPath)
			t.Cleanup(func() { manager.directoryGuard.UnprotectPaths(owner, peerPath) })

			// uploader無しでもpeer-only競合をpendingへ引き継ぎ、purge側のdecodeや
			// source削除を止めます。raw自身は別ownerに保護されていない条件です。
			report, err := manager.RunOnce(context.Background())
			require.NoError(t, err)
			compressCalls, verifyCalls := codec.calls()
			require.Zero(t, compressCalls)
			require.Zero(t, verifyCalls)
			require.Zero(t, retireCalls)
			require.Empty(t, report.Compressed)
			require.Empty(t, report.ObjectStorageUploads)
			require.Empty(t, report.UploadedFileRemovals)
			// 削除の対象になり得たpurgeが実行され、保護したsourceとpeerを残したことを確認します。
			switch purge {
			case "age":
				require.True(t, report.Purge.ByAge.Ran)
				require.Empty(t, report.Purge.ByAge.RemovalResults)
			case "generation":
				require.Empty(t, report.Purge.ByGeneration.RemovalResults)
			case "directory size":
				require.True(t, report.Purge.ByDirSize.Ran)
				require.Empty(t, report.Purge.ByDirSize.RemovalResults)
			}
			require.FileExists(t, sourcePath)
			require.FileExists(t, peerPath)
			require.True(t, claimCompressionConcurrencyPath(manager.directoryGuard, sourcePath))
			require.False(t, claimCompressionConcurrencyPath(manager.directoryGuard, peerPath), "other owner's retention was removed")

			manager.directoryGuard.UnprotectPaths(owner, peerPath)
			require.True(t, claimCompressionConcurrencyPath(manager.directoryGuard, peerPath))
			_, err = manager.RunOnce(context.Background())
			require.NoError(t, err)
			compressCalls, verifyCalls = codec.calls()
			require.Zero(t, compressCalls, "valid peer must be recovered without recompression")
			require.Equal(t, 1, verifyCalls)
			require.Equal(t, 1, retireCalls)
			require.NoFileExists(t, sourcePath)
		})
	}
}

type fileSeriesConcurrencyCodec struct {
	*runnerTestCompressionCodec
	beforeCompress func() error
	beforeVerify   func() error
}

func (c *fileSeriesConcurrencyCodec) Compress(in, out *os.File) error {
	if c.beforeCompress != nil {
		if err := c.beforeCompress(); err != nil {
			return err
		}
	}
	return c.runnerTestCompressionCodec.Compress(in, out)
}

func (c *fileSeriesConcurrencyCodec) VerifyCompressedIntegrity(in *os.File) error {
	if c.beforeVerify != nil {
		if err := c.beforeVerify(); err != nil {
			return err
		}
	}
	return c.runnerTestCompressionCodec.VerifyCompressedIntegrity(in)
}

func newCompressionConcurrencyManager(
	t *testing.T,
	codec *fileSeriesConcurrencyCodec,
	existingPeer bool,
) (*FileSeriesManager, string) {
	t.Helper()
	root := resolvedTempDirForTest(t)
	const target = "compression-concurrency"
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o700))
	date := currentLogDate()
	sourcePath := filepath.Join(directory, target+"_"+date+"_1.log")
	activePath := filepath.Join(directory, target+"_"+date+"_2.log")
	require.NoError(t, os.WriteFile(sourcePath, []byte("inactive payload"), 0o600))
	require.NoError(t, os.WriteFile(activePath, []byte("active payload"), 0o600))
	if existingPeer {
		require.NoError(t, os.WriteFile(sourcePath+".br", []byte(runnerTestCompressionMagic+"inactive payload"), 0o600))
	}
	config := appendlog.NewConfigAdapter(pkgfsstore.Config{
		Layout: pkgfsstore.FsstoreConfig{
			Target: target, LogPath: root, LogDir: "logs",
			FileNameFormat: pkgfsstore.FileNameFormat, FileDateFormat: pkgfsstore.FileDateFormat,
		},
		Lifecycle: pkgfsstore.FileLifecycleConfig{
			EnableRotate: true, RotationType: pkgfsstore.FileRotationDaily,
			MaxFileBackups: 10, MaxFileSize: 1024,
			EnableFileCompress: true, CompressFormat: pkgfsstore.CompressGzip,
		},
		CompressionCodec: codec,
	})
	return NewFileSeriesManager(FileSeriesManagerOptions{
		Config: config,
		FileName: logpattern.FileNamePattern{
			Dir: directory, Target: target, Extension: "log",
		},
	}), sourcePath
}

type compressionConcurrencyRunResult struct {
	report     RunReport
	err        error
	panicValue any
}

func runCompressionConcurrencyManager(manager *FileSeriesManager) <-chan compressionConcurrencyRunResult {
	done := make(chan compressionConcurrencyRunResult, 1)
	go func() {
		var result compressionConcurrencyRunResult
		defer func() {
			result.panicValue = recover()
			done <- result
			close(done)
		}()
		result.report, result.err = manager.RunOnce(context.Background())
	}()
	return done
}

// claimCompressionConcurrencyPathは別ownerとして競合判定し、成功したprobeも直ちに解除します。
func claimCompressionConcurrencyPath(guard *appendlog.DirectoryGuard, path string) bool {
	token := appendlog.NewDirectoryGuardToken()
	claimed := guard.ProtectPathsIfNoOtherOwnerConflict(token, nil, []string{path})
	if claimed {
		guard.UnprotectPaths(token, path)
	}
	return claimed
}
