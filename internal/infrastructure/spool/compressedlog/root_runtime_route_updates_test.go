package compressedlog

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestManagedRouteUpdatesReachReusedRunnerAtFileSelection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := managedUploaderTestConfig(t.TempDir())
		cfg.RunnerInterval = time.Second
		runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
		type replayResult struct {
			report RunReport
			err    error
		}
		results := make(chan replayResult, 1)
		firstUpload := &replaytest.Uploader{}
		first, err := runtime.RegisterRoute(ManagedRouteConfig{
			FileName: replaytest.FileNamePattern("app_"),
			RunObserver: func(_ context.Context, replay *RunReport, _ *objectstoragespool.TerminalPurgeReport, err error) {
				if replay != nil {
					results <- replayResult{report: *replay, err: err}
				}
			},
		}, firstUpload)
		if err != nil {
			t.Fatal(err)
		}
		defer first.Close()
		receiveReplay := func() replayResult {
			t.Helper()
			synctest.Wait()
			select {
			case result := <-results:
				return result
			default:
				t.Fatal("coordinator did not finish its replay cycle")
				return replayResult{}
			}
		}
		// RootRuntimeが構築した同じcompressedlogworker.WorkerとRunnerで、空の周期を一度実行してから
		// 以下の送信先登録の変更を行います。
		synctest.Wait()
		time.Sleep(cfg.RunnerInterval)
		if result := receiveReplay(); result.err != nil || len(result.report.CompressedLogFiles) != 0 {
			t.Fatalf("initial empty replay = %+v, %v", result.report, result.err)
		}
		replayAfterRouteChange := func(change func()) replayResult {
			t.Helper()
			func() {
				runtime.saveOrClaimMu.Lock()
				defer runtime.saveOrClaimMu.Unlock()
				time.Sleep(cfg.RunnerInterval)
				synctest.Wait()
				// ファイル確保のlock待ちに入った後の登録変更も、
				// 実際に対象ファイルの送信先を選ぶ時点で反映されます。
				change()
			}()
			return receiveReplay()
		}
		publish := func(name string) {
			t.Helper()
			if _, err := first.SaveRetryableFailure(context.Background(), replaytest.WriteSource(t, name)); err != nil {
				t.Fatal(err)
			}
		}

		publish("error_app_20260922_1.log.gz")
		lateUpload := &replaytest.Uploader{DestinationID: "s3"}
		var late *ManagedRoute
		defer func() { _ = late.Close() }()
		result := replayAfterRouteChange(func() {
			late, err = runtime.RegisterRoute(ManagedRouteConfig{
				FileName: replaytest.FileNamePattern("error_app_"),
			}, lateUpload)
			if err != nil {
				t.Fatal(err)
			}
		})
		if result.err != nil || len(result.report.CompressedLogFiles) != 1 ||
			result.report.CompressedLogFiles[0].State != pkgspool.StateDone || lateUpload.CallCount() != 1 {
			t.Fatalf("late route replay = %+v, %v; upload calls = %d", result.report, result.err, lateUpload.CallCount())
		}

		publish("app_20260922_2.log.gz")
		overlapUpload := &replaytest.Uploader{DestinationID: "other-s3"}
		var overlap *ManagedRoute
		defer func() { _ = overlap.Close() }()
		result = replayAfterRouteChange(func() {
			overlap, err = runtime.RegisterRoute(ManagedRouteConfig{
				FileName: replaytest.FileNamePattern("app_"),
			}, overlapUpload)
			if err != nil {
				t.Fatal(err)
			}
		})
		// 同じ名前に合っても、別宛先のrouteは送信先候補に入りません。
		if result.err != nil || len(result.report.CompressedLogFiles) != 1 ||
			result.report.CompressedLogFiles[0].State != pkgspool.StateDone ||
			firstUpload.CallCount() != 1 || overlapUpload.CallCount() != 0 {
			t.Fatalf("late overlap replay = %+v, %v; upload calls = %d/%d", result.report, result.err,
				firstUpload.CallCount(), overlapUpload.CallCount())
		}

		publish("app_20260922_3.log.gz")
		result = replayAfterRouteChange(func() {
			// 終了要求は受理済み周期の完了を待つため、周期を解放してからCloseを待ちます。
			overlap.RequestClose()
		})
		if err := overlap.Close(); err != nil {
			t.Fatal(err)
		}
		if result.err != nil || len(result.report.CompressedLogFiles) != 1 ||
			result.report.CompressedLogFiles[0].State != pkgspool.StateDone ||
			firstUpload.CallCount() != 2 || overlapUpload.CallCount() != 0 {
			t.Fatalf("removed overlap replay = %+v, %v; upload calls = %d/%d", result.report, result.err,
				firstUpload.CallCount(), overlapUpload.CallCount())
		}
	})
}
