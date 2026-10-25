package worker

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/retryafter"
	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgs3 "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// retryAfterRestartFixtureは、同じspool rootを使う再送workerを作り直し、process再起動後の
// workerを再現します。時刻は明示的なclockで進めます。
type retryAfterRestartFixture struct {
	t    *testing.T
	root objectstoragespool.Root
	now  time.Time
}

func newRetryAfterRestartFixture(t *testing.T) *retryAfterRestartFixture {
	t.Helper()
	return &retryAfterRestartFixture{
		t:    t,
		root: replaytest.NewStore(t).Root(),
		now:  time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC),
	}
}

func (f *retryAfterRestartFixture) stores() *objectstoragespool.DestinationStores {
	f.t.Helper()
	stores, err := objectstoragespool.NewDestinationStores(f.root)
	if err != nil {
		f.t.Fatal(err)
	}
	return stores
}

func (f *retryAfterRestartFixture) destination(id objectstorage.DestinationID) *objectstoragespool.Store {
	f.t.Helper()
	store, err := f.stores().ForDestination(id)
	if err != nil {
		f.t.Fatal(err)
	}
	return store
}

// startは、新しいDestinationStoresとregistryを持つworkerを作ります。
func (f *retryAfterRestartFixture) start(maxRetryAfter time.Duration, uploaders ...*replaytest.Uploader) *Worker {
	f.t.Helper()
	routes := make(RouteRegistry, 0, len(uploaders))
	for _, upload := range uploaders {
		routes = append(routes, RouteRegistration{FileName: replaytest.FileNamePattern("app_"), Uploader: upload})
	}
	worker, err := NewDestinationWorker(f.stores(), routes, 5*time.Minute, WithMaxRetryAfter(maxRetryAfter))
	if err != nil {
		f.t.Fatal(err)
	}
	worker.retryAfter.clock = func() time.Time { return f.now }
	return worker
}

func (f *retryAfterRestartFixture) markerPath(id objectstorage.DestinationID) string {
	return filepath.Join(f.root.Path(), string(id), retryafter.DirectoryName, string(id)+".json")
}

func throttledUploader(id objectstorage.DestinationID, retryAfter time.Duration) *replaytest.Uploader {
	return &replaytest.Uploader{DestinationID: id, Err: &pkgs3.HTTPStatusError{
		Code:               http.StatusTooManyRequests,
		RetryAfterDuration: retryAfter,
	}}
}

func requireNoRetryAfterMarkerReports(t *testing.T, report RunReport) {
	t.Helper()
	if len(report.RetryAfterMarkers) != 0 {
		t.Fatalf("RetryAfterMarkers = %+v, want none", report.RetryAfterMarkers)
	}
}

// 再起動後のworkerは、宛先別spoolに保存した期限をその宛先へ最初に送信する前に読み込み、
// 期限までその宛先のファイルを取得しない。他の宛先の再送は続ける。
func TestDestinationWorkerRestoresPersistedRetryAfterAfterRestart(t *testing.T) {
	fixture := newRetryAfterRestartFixture(t)
	throttledFile := replaytest.SaveFile(t, fixture.destination("s3-a"), "app_20260906_1.log.gz")
	first := fixture.start(0, throttledUploader("s3-a", time.Hour), &replaytest.Uploader{DestinationID: "s3-b"})
	failed, err := first.RunOnce(context.Background())
	notBefore := fixture.now.Add(time.Hour)
	if err == nil || failed.File.DestinationID != "s3-a" || !failed.File.DeferredUntil.Equal(notBefore) {
		t.Fatalf("throttled RunOnce() report/error = %+v/%v, want s3-a deferred until %s", failed, err, notBefore)
	}
	requireNoRetryAfterMarkerReports(t, failed)
	if _, err := os.Stat(fixture.markerPath("s3-a")); err != nil {
		t.Fatalf("Retry-After marker was not saved: %v", err)
	}

	fixture.now = fixture.now.Add(time.Minute)
	otherFile := replaytest.SaveFile(t, fixture.destination("s3-b"), "app_20260906_2.log.gz")
	resumedUpload := &replaytest.Uploader{DestinationID: "s3-a"}
	otherUpload := &replaytest.Uploader{DestinationID: "s3-b"}
	restarted := fixture.start(0, resumedUpload, otherUpload)
	other, err := restarted.RunOnce(context.Background())
	if err != nil || other.File.DestinationID != "s3-b" || other.File.FileName != otherFile.FileName || !other.File.UploadSucceeded {
		t.Fatalf("RunOnce() after restart report/error = %+v/%v, want only s3-b uploaded", other, err)
	}
	requireNoRetryAfterMarkerReports(t, other)
	idle, err := restarted.RunOnce(context.Background())
	if err != nil || idle.File.Claimed {
		t.Fatalf("RunOnce() before the persisted deadline report/error = %+v/%v, want no claim", idle, err)
	}
	if got := resumedUpload.CallCount(); got != 0 {
		t.Fatalf("s3-a upload calls before the persisted deadline = %d, want 0", got)
	}
	if got := replaytest.OnlyFile(t, fixture.destination("s3-a"), pkgspool.StateActive); got.FileName != throttledFile.FileName {
		t.Fatalf("waiting file = %+v, want %q in active", got, throttledFile.FileName)
	}

	fixture.now = notBefore
	resumed, err := restarted.RunOnce(context.Background())
	if err != nil || resumed.File.DestinationID != "s3-a" || !resumed.File.UploadSucceeded || resumedUpload.CallCount() != 1 {
		t.Fatalf("RunOnce() at the persisted deadline report/error = %+v/%v, want s3-a uploaded", resumed, err)
	}
	requireNoRetryAfterMarkerReports(t, resumed)
	if _, err := os.Lstat(fixture.markerPath("s3-a")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired Retry-After marker still exists: %v", err)
	}
}

// 宛先別spoolを単一のstoreとして再送するworkerも、保存した期限を送信前に読み込む。
func TestWorkerRestoresPersistedRetryAfterForDestinationStore(t *testing.T) {
	fixture := newRetryAfterRestartFixture(t)
	replaytest.SaveFile(t, fixture.destination("s3-a"), "app_20260906_1.log.gz")
	newWorker := func(upload *replaytest.Uploader) *Worker {
		worker, err := NewWorker(fixture.destination("s3-a"), RouteRegistry{{
			FileName: replaytest.FileNamePattern("app_"), Uploader: upload,
		}}, 5*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		worker.retryAfter.clock = func() time.Time { return fixture.now }
		return worker
	}
	if _, err := newWorker(throttledUploader("s3-a", time.Hour)).RunOnce(context.Background()); err == nil {
		t.Fatal("throttled RunOnce() error = nil")
	}

	fixture.now = fixture.now.Add(time.Minute)
	upload := &replaytest.Uploader{DestinationID: "s3-a"}
	deferred, err := newWorker(upload).RunOnce(context.Background())
	if err != nil || deferred.File.DeferredReason != DeferredReasonRetryAfter || deferred.File.UploadAttempted ||
		deferred.File.State != pkgspool.StateActive || upload.CallCount() != 0 {
		t.Fatalf("RunOnce() after restart report/error = %+v/%v, want the file kept until the persisted deadline", deferred, err)
	}
}

// 上限を超える保存済みの期限は、読み込んだ時点から上限までに短縮して保存し直し、一度だけ通知する。
// 二回目の再起動では短縮後の期限をそのまま使い、待機を延ばさない。
func TestDestinationWorkerPersistsClampedRetryAfterAcrossRestart(t *testing.T) {
	fixture := newRetryAfterRestartFixture(t)
	replaytest.SaveFile(t, fixture.destination("s3-a"), "app_20260906_1.log.gz")
	// 以前の設定では長い上限で期限を保存していた状態から始めます。
	if _, err := fixture.start(24*time.Hour, throttledUploader("s3-a", 10*time.Hour)).RunOnce(context.Background()); err == nil {
		t.Fatal("throttled RunOnce() error = nil")
	}

	fixture.now = fixture.now.Add(time.Minute)
	clampedAt := fixture.now
	wantNotBefore := clampedAt.Add(time.Hour)
	upload := &replaytest.Uploader{DestinationID: "s3-a"}
	clamped, err := fixture.start(time.Hour, upload).RunOnce(context.Background())
	if err != nil || clamped.File.Claimed {
		t.Fatalf("RunOnce() with a persisted deadline beyond the maximum report/error = %+v/%v, want no claim", clamped, err)
	}
	if len(clamped.RetryAfterMarkers) != 1 {
		t.Fatalf("RetryAfterMarkers = %+v, want one stored clamp", clamped.RetryAfterMarkers)
	}
	marker := clamped.RetryAfterMarkers[0]
	if marker.DestinationID != "s3-a" || marker.Err != nil || marker.Clamp == nil || !marker.Clamp.Stored ||
		marker.Clamp.MaxRetryAfter != time.Hour || !marker.Clamp.NotBefore.Equal(wantNotBefore) ||
		marker.Clamp.Requested != 10*time.Hour-time.Minute {
		t.Fatalf("stored clamp report = %+v (clamp %+v), want 9h59m capped at 1h until %s", marker, marker.Clamp, wantNotBefore)
	}

	fixture.now = clampedAt.Add(30 * time.Minute)
	restarted := fixture.start(time.Hour, upload)
	waiting, err := restarted.RunOnce(context.Background())
	if err != nil || waiting.File.Claimed {
		t.Fatalf("RunOnce() after the second restart report/error = %+v/%v, want no claim", waiting, err)
	}
	requireNoRetryAfterMarkerReports(t, waiting)

	fixture.now = wantNotBefore.Add(-time.Nanosecond)
	if early, err := restarted.RunOnce(context.Background()); err != nil || early.File.Claimed {
		t.Fatalf("RunOnce() before the clamped deadline report/error = %+v/%v, want no claim", early, err)
	}
	fixture.now = wantNotBefore
	resumed, err := restarted.RunOnce(context.Background())
	if err != nil || !resumed.File.UploadSucceeded || upload.CallCount() != 1 {
		t.Fatalf("RunOnce() at the clamped deadline report/error = %+v/%v, want uploaded once", resumed, err)
	}
}

// failingRetryAfterMarkerStoreは、保存の失敗を再現します。読み込みは期限なしを返します。
type failingRetryAfterMarkerStore struct {
	saveErr error
	saves   int
}

func (s *failingRetryAfterMarkerStore) Load(time.Time, time.Duration) (retryafter.State, *pkgspool.RetryAfterClamp, error) {
	return retryafter.State{}, nil, nil
}

func (s *failingRetryAfterMarkerStore) Save(retryafter.State) error {
	s.saves++
	return s.saveErr
}

func (s *failingRetryAfterMarkerStore) Remove() error { return nil }

// 期限を保存できなくても、process内では期限までその宛先へ送信せず、失敗を一度だけ通知する。
func TestDestinationWorkerHonorsRetryAfterWhenMarkerSaveFails(t *testing.T) {
	fixture := newRetryAfterRestartFixture(t)
	replaytest.SaveFile(t, fixture.destination("s3-a"), "app_20260906_1.log.gz")
	upload := throttledUploader("s3-a", time.Hour)
	worker := fixture.start(0, upload)
	injected := errors.New("injected marker save failure")
	markerStore := &failingRetryAfterMarkerStore{saveErr: injected}
	worker.retryAfter.newMarkerStore = func(objectstoragespool.Root) (retryAfterMarkerStore, error) { return markerStore, nil }

	failed, err := worker.RunOnce(context.Background())
	if err == nil || !failed.File.DeferredUntil.Equal(fixture.now.Add(time.Hour)) {
		t.Fatalf("throttled RunOnce() report/error = %+v/%v", failed, err)
	}
	if errors.Is(err, injected) {
		t.Fatalf("RunOnce() error = %v, want the marker failure only as a report", err)
	}
	if len(failed.RetryAfterMarkers) != 1 || failed.RetryAfterMarkers[0].DestinationID != "s3-a" ||
		!errors.Is(failed.RetryAfterMarkers[0].Err, injected) {
		t.Fatalf("RetryAfterMarkers = %+v, want the injected save failure", failed.RetryAfterMarkers)
	}
	upload.SetError(nil)

	fixture.now = fixture.now.Add(time.Hour - time.Nanosecond)
	deferred, err := worker.RunOnce(context.Background())
	if err != nil || deferred.File.Claimed || upload.CallCount() != 1 {
		t.Fatalf("RunOnce() before the deadline report/error = %+v/%v, want the deadline honored in process", deferred, err)
	}
	// 期限が残る間は保存を再試行しますが、続く失敗は同じ一連の失敗として通知し直しません。
	requireNoRetryAfterMarkerReports(t, deferred)
	if markerStore.saves != 2 {
		t.Fatalf("marker saves = %d, want the failed save retried once", markerStore.saves)
	}
}

// flakyRetryAfterMarkerStoreは、指定回数だけ保存に失敗し、その後は実際のmarkerへ保存します。
type flakyRetryAfterMarkerStore struct {
	retryAfterMarkerStore
	failures int
	err      error
	saves    int
}

func (s *flakyRetryAfterMarkerStore) Save(state retryafter.State) error {
	s.saves++
	if s.saves <= s.failures {
		return s.err
	}
	return s.retryAfterMarkerStore.Save(state)
}

// 保存に失敗した期限は、期限が残る間の後の周期で保存し直すため、再起動後も引き継ぐ。
// 失敗は一連の失敗につき一度だけ通知する。
func TestDestinationWorkerRetriesFailedMarkerSaveAcrossCycles(t *testing.T) {
	fixture := newRetryAfterRestartFixture(t)
	replaytest.SaveFile(t, fixture.destination("s3-a"), "app_20260906_1.log.gz")
	worker := fixture.start(0, throttledUploader("s3-a", time.Hour))
	injected := errors.New("injected transient marker save failure")
	var flaky *flakyRetryAfterMarkerStore
	worker.retryAfter.newMarkerStore = func(root objectstoragespool.Root) (retryAfterMarkerStore, error) {
		inner, err := newRetryAfterMarkerStore(root)
		flaky = &flakyRetryAfterMarkerStore{retryAfterMarkerStore: inner, failures: 2, err: injected}
		return flaky, err
	}
	notBefore := fixture.now.Add(time.Hour)
	failed, err := worker.RunOnce(context.Background())
	if err == nil || !failed.File.DeferredUntil.Equal(notBefore) || len(failed.RetryAfterMarkers) != 1 ||
		!errors.Is(failed.RetryAfterMarkers[0].Err, injected) {
		t.Fatalf("throttled RunOnce() report/error = %+v/%v, want one save failure report", failed, err)
	}
	if _, err := os.Lstat(fixture.markerPath("s3-a")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("marker exists after the failed save: %v", err)
	}
	for cycle, wantSaves := range []int{2, 3} {
		fixture.now = fixture.now.Add(time.Minute)
		deferred, err := worker.RunOnce(context.Background())
		if err != nil || deferred.File.Claimed {
			t.Fatalf("deferred cycle %d report/error = %+v/%v", cycle, deferred, err)
		}
		requireNoRetryAfterMarkerReports(t, deferred)
		if flaky.saves != wantSaves {
			t.Fatalf("saves after deferred cycle %d = %d, want %d", cycle, flaky.saves, wantSaves)
		}
	}
	if _, err := os.Stat(fixture.markerPath("s3-a")); err != nil {
		t.Fatalf("marker was not saved by a later cycle: %v", err)
	}

	upload := &replaytest.Uploader{DestinationID: "s3-a"}
	restarted, err := fixture.start(0, upload).RunOnce(context.Background())
	if err != nil || restarted.File.Claimed || upload.CallCount() != 0 {
		t.Fatalf("RunOnce() after restart report/error = %+v/%v, want the retried deadline honored", restarted, err)
	}
	fixture.now = notBefore
	if resumed, err := fixture.start(0, upload).RunOnce(context.Background()); err != nil || !resumed.File.UploadSucceeded {
		t.Fatalf("RunOnce() at the deadline report/error = %+v/%v, want uploaded", resumed, err)
	}
}

// 壊れたmarkerで再送を止めない。markerがないものとして送信し、読み込みの失敗を通知する。
func TestDestinationWorkerIgnoresCorruptRetryAfterMarker(t *testing.T) {
	fixture := newRetryAfterRestartFixture(t)
	replaytest.SaveFile(t, fixture.destination("s3-a"), "app_20260906_1.log.gz")
	path := fixture.markerPath("s3-a")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"version":1,"destination_id":"s3-a","not_before":"broken"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	upload := &replaytest.Uploader{DestinationID: "s3-a"}
	report, err := fixture.start(0, upload).RunOnce(context.Background())
	if err != nil || !report.File.UploadSucceeded || upload.CallCount() != 1 {
		t.Fatalf("RunOnce() with a corrupt marker report/error = %+v/%v, want uploaded", report, err)
	}
	var markerErr *retryafter.MarkerError
	if len(report.RetryAfterMarkers) != 1 || !errors.Is(report.RetryAfterMarkers[0].Err, retryafter.ErrInvalidMarker) ||
		!errors.As(report.RetryAfterMarkers[0].Err, &markerErr) || markerErr.Operation != retryafter.MarkerOperationLoad {
		t.Fatalf("RetryAfterMarkers = %+v, want one load failure for the corrupt marker", report.RetryAfterMarkers)
	}
}

// 壊れたmarkerは、その宛先が次に返したRetry-Afterの期限で置き換える。置き換えた期限は再起動後も守り、
// 期限まではその宛先のファイルを取得しない。
func TestDestinationWorkerReplacesCorruptRetryAfterMarkerWithNextRetryAfter(t *testing.T) {
	fixture := newRetryAfterRestartFixture(t)
	replaytest.SaveFile(t, fixture.destination("s3-a"), "app_20260906_1.log.gz")
	path := fixture.markerPath("s3-a")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	failed, err := fixture.start(0, throttledUploader("s3-a", time.Hour)).RunOnce(context.Background())
	notBefore := fixture.now.Add(time.Hour)
	if err == nil || !failed.File.UploadAttempted || !failed.File.DeferredUntil.Equal(notBefore) {
		t.Fatalf("throttled RunOnce() with a corrupt marker report/error = %+v/%v, want deferred until %s", failed, err, notBefore)
	}
	if len(failed.RetryAfterMarkers) != 1 || !errors.Is(failed.RetryAfterMarkers[0].Err, retryafter.ErrInvalidMarker) {
		t.Fatalf("RetryAfterMarkers = %+v, want only the corrupt marker load failure", failed.RetryAfterMarkers)
	}
	markerStore, err := retryafter.NewObjectStorageStore(fixture.destination("s3-a").Root().Path(), "s3-a")
	if err != nil {
		t.Fatal(err)
	}
	if saved, _, err := markerStore.Load(fixture.now, time.Hour); err != nil || !saved.NotBefore.Equal(notBefore) {
		t.Fatalf("marker after the next Retry-After = %+v/%v, want the corrupt marker replaced by %s", saved, err, notBefore)
	}

	fixture.now = fixture.now.Add(time.Minute)
	resumedUpload := &replaytest.Uploader{DestinationID: "s3-a"}
	restarted := fixture.start(0, resumedUpload)
	waiting, err := restarted.RunOnce(context.Background())
	if err != nil || waiting.File.Claimed || resumedUpload.CallCount() != 0 {
		t.Fatalf("RunOnce() after restart report/error = %+v/%v, want the replaced deadline honored", waiting, err)
	}
	requireNoRetryAfterMarkerReports(t, waiting)

	fixture.now = notBefore
	resumed, err := restarted.RunOnce(context.Background())
	if err != nil || !resumed.File.UploadSucceeded || resumedUpload.CallCount() != 1 {
		t.Fatalf("RunOnce() at the replaced deadline report/error = %+v/%v, want uploaded", resumed, err)
	}
}

// clampFailingMarkerStoreは、読み込み時に上限より先の保存済み期限を短縮したものの、短縮した期限を
// 保存できなかった状態を返します。以後の保存は成功として記録します。
type clampFailingMarkerStore struct {
	stored       time.Time
	clampSaveErr error
	saves        []retryafter.State
}

func (s *clampFailingMarkerStore) Load(now time.Time, maxWait time.Duration) (retryafter.State, *pkgspool.RetryAfterClamp, error) {
	clamped := now.Add(maxWait).Round(0)
	return retryafter.State{NotBefore: clamped}, &pkgspool.RetryAfterClamp{
		Requested:     s.stored.Sub(now),
		MaxRetryAfter: maxWait,
		NotBefore:     clamped,
		Stored:        true,
	}, s.clampSaveErr
}

func (s *clampFailingMarkerStore) Save(state retryafter.State) error {
	s.saves = append(s.saves, state)
	return nil
}

func (s *clampFailingMarkerStore) Remove() error { return nil }

// 読み込み時に短縮した期限を保存できなかった場合も、process内では短縮後の期限まで宛先を待たせ、
// 保存の再試行の間隔の後に保存し直す。短縮と保存の失敗は一つの結果として一度だけ通知する。
func TestDestinationWorkerRetriesSavingDeadlineClampedAtLoad(t *testing.T) {
	fixture := newRetryAfterRestartFixture(t)
	replaytest.SaveFile(t, fixture.destination("s3-a"), "app_20260906_1.log.gz")
	upload := &replaytest.Uploader{DestinationID: "s3-a"}
	worker := fixture.start(time.Hour, upload)
	injected := errors.New("injected clamped marker save failure")
	markerStore := &clampFailingMarkerStore{stored: fixture.now.Add(10 * time.Hour), clampSaveErr: injected}
	worker.retryAfter.newMarkerStore = func(objectstoragespool.Root) (retryAfterMarkerStore, error) { return markerStore, nil }
	wantNotBefore := fixture.now.Add(time.Hour)

	clamped, err := worker.RunOnce(context.Background())
	if err != nil || clamped.File.Claimed || upload.CallCount() != 0 {
		t.Fatalf("RunOnce() with a clamped deadline report/error = %+v/%v, want the destination kept waiting", clamped, err)
	}
	if len(clamped.RetryAfterMarkers) != 1 {
		t.Fatalf("RetryAfterMarkers = %+v, want one clamp result", clamped.RetryAfterMarkers)
	}
	marker := clamped.RetryAfterMarkers[0]
	if marker.DestinationID != "s3-a" || marker.Clamp == nil || !marker.Clamp.Stored ||
		!marker.Clamp.NotBefore.Equal(wantNotBefore) || !errors.Is(marker.Err, injected) {
		t.Fatalf("clamp result = %+v (clamp %+v), want the stored clamp and its save failure", marker, marker.Clamp)
	}
	if len(markerStore.saves) != 0 {
		t.Fatalf("marker saves = %+v, want the retry to wait for the retry interval", markerStore.saves)
	}

	// fixture.startは周期を渡さないため、保存の再試行は既定の1秒後です。
	fixture.now = fixture.now.Add(defaultRetryAfterSaveRetryInterval)
	retried, err := worker.RunOnce(context.Background())
	if err != nil || retried.File.Claimed || upload.CallCount() != 0 {
		t.Fatalf("RunOnce() after the retry interval report/error = %+v/%v, want the destination still waiting", retried, err)
	}
	requireNoRetryAfterMarkerReports(t, retried)
	if len(markerStore.saves) != 1 || !markerStore.saves[0].NotBefore.Equal(wantNotBefore) {
		t.Fatalf("marker saves = %+v, want the clamped deadline %s saved again", markerStore.saves, wantNotBefore)
	}
}

// 期限を過ぎたmarkerは読み込み時に削除し、送信を制限しない。
func TestDestinationWorkerRemovesExpiredRetryAfterMarker(t *testing.T) {
	fixture := newRetryAfterRestartFixture(t)
	store := fixture.destination("s3-a")
	replaytest.SaveFile(t, store, "app_20260906_1.log.gz")
	markerStore, err := retryafter.NewObjectStorageStore(store.Root().Path(), "s3-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := markerStore.Save(retryafter.State{NotBefore: fixture.now.Add(-time.Second)}); err != nil {
		t.Fatal(err)
	}
	upload := &replaytest.Uploader{DestinationID: "s3-a"}
	report, err := fixture.start(0, upload).RunOnce(context.Background())
	if err != nil || !report.File.UploadSucceeded {
		t.Fatalf("RunOnce() with an expired marker report/error = %+v/%v, want uploaded", report, err)
	}
	requireNoRetryAfterMarkerReports(t, report)
	if _, err := os.Lstat(fixture.markerPath("s3-a")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired Retry-After marker still exists: %v", err)
	}
}

// attemptRecordingMarkerStoreは、保存の試行時刻を記録し、保存を失敗させます。
type attemptRecordingMarkerStore struct {
	failingRetryAfterMarkerStore
	attempts []time.Time
}

func (s *attemptRecordingMarkerStore) Save(state retryafter.State) error {
	s.attempts = append(s.attempts, time.Now())
	return s.failingRetryAfterMarkerStore.Save(state)
}

// 保存に失敗した宛先の期限は、runnerの周期から始めて失敗のたびに倍にした間隔（上限1分）で
// 保存し直し、期限が来たら再試行をやめる。警告は一連の失敗につき一度だけ残す。
func TestDestinationWorkerBacksOffFailedMarkerSaves(t *testing.T) {
	start := time.Now()
	synctest.Test(t, func(t *testing.T) {
		// 実際のfilesystemが付ける更新時刻と、bubble内の時刻の前後関係を揃えます。
		time.Sleep(time.Until(start))
		const interval = time.Second
		stores, err := objectstoragespool.NewDestinationStores(replaytest.NewStore(t).Root())
		if err != nil {
			t.Fatal(err)
		}
		store, err := stores.ForDestination("s3-a")
		if err != nil {
			t.Fatal(err)
		}
		replaytest.SaveFile(t, store, "app_20260906_1.log.gz")
		upload := throttledUploader("s3-a", 3*time.Minute)
		worker, err := NewDestinationWorker(stores, RouteRegistry{{FileName: replaytest.FileNamePattern("app_"), Uploader: upload}},
			5*time.Minute, WithRetryAfterSaveRetryInterval(interval))
		if err != nil {
			t.Fatal(err)
		}
		markerStore := &attemptRecordingMarkerStore{failingRetryAfterMarkerStore: failingRetryAfterMarkerStore{
			saveErr: errors.New("injected persistent marker save failure"),
		}}
		worker.retryAfter.newMarkerStore = func(objectstoragespool.Root) (retryAfterMarkerStore, error) { return markerStore, nil }

		failed, err := worker.RunOnce(context.Background())
		notBefore := time.Now().Add(3 * time.Minute)
		if err == nil || !failed.File.DeferredUntil.Equal(notBefore) || len(failed.RetryAfterMarkers) != 1 {
			t.Fatalf("throttled RunOnce() report/error = %+v/%v, want one save failure report", failed, err)
		}
		upload.SetError(nil)
		reports := 0
		for range 5 * 60 {
			time.Sleep(interval)
			report, err := worker.RunOnce(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			reports += len(report.RetryAfterMarkers)
		}
		if reports != 0 {
			t.Fatalf("marker reports after the first failure = %d, want the streak reported once", reports)
		}
		want := []time.Duration{interval, 2 * interval, 4 * interval, 8 * interval, 16 * interval, 32 * interval, time.Minute}
		var gaps []time.Duration
		for index := 1; index < len(markerStore.attempts); index++ {
			gaps = append(gaps, markerStore.attempts[index].Sub(markerStore.attempts[index-1]))
		}
		if !slices.Equal(gaps, want) {
			t.Fatalf("save retry gaps = %v, want %v (doubling from the runner interval, capped at 1m)", gaps, want)
		}
		for _, attempt := range markerStore.attempts {
			if !attempt.Before(notBefore) {
				t.Fatalf("save retried at %s, at or after the deadline %s", attempt, notBefore)
			}
		}
		if got := upload.CallCount(); got != 2 {
			t.Fatalf("upload calls = %d, want the waiting file sent once after the deadline", got)
		}
	})
}
