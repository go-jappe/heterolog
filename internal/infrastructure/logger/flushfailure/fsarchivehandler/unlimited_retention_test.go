package fsarchivehandler

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

var unlimitedRetentionFailureCategories = []failurearchive.FailureCategory{
	failurearchive.FailureCategoryRetryable,
	failurearchive.FailureCategoryPermanent,
}

// unlimitedRetentionArchiveConfigは、保持日数・保持世代数・ディレクトリ最大サイズをすべて0
// （上限なし）にした設定です。
func unlimitedRetentionArchiveConfig(cfg *testArchiveConfig) {
	cfg.maxFileAge = 0
	cfg.maxFileBackups = 0
	cfg.maxDirSize = 0
}

// countArchiveDirectoryScansは、failure categoryごとのディレクトリ全体の走査回数を数えます。
// 走査自体は本番と同じglobで行います。
func countArchiveDirectoryScans(handler *FsArchiveHandler) func(failurearchive.FailureCategory) int {
	retryableDirectory := handler.Fsstore.FileNameRetryable.Dir
	permanentDirectory := handler.Fsstore.FileNamePermanent.Dir
	var mu sync.Mutex
	scans := map[failurearchive.FailureCategory]int{}
	handler.globArchiveFiles = func(pattern logpattern.FileNamePattern, mode logpattern.GlobMode) ([]string, error) {
		if mode != logpattern.GlobAllFiles {
			return logpattern.GlobFileNames(pattern, mode)
		}
		mu.Lock()
		switch pattern.Dir {
		case retryableDirectory:
			scans[failurearchive.FailureCategoryRetryable]++
		case permanentDirectory:
			scans[failurearchive.FailureCategoryPermanent]++
		}
		mu.Unlock()
		return logpattern.GlobLifecycleFileNames(pattern, mode)
	}
	return func(category failurearchive.FailureCategory) int {
		mu.Lock()
		defer mu.Unlock()
		return scans[category]
	}
}

// writeArchivesPurgedByMinimumRetentionは、最小値の保持上限（1日・2世代・10 MiB）なら
// どの条件でも削除対象を含むrotation済みarchiveを作ります。3世代・合計12 MiBで、
// 2世代はnowの前日より古い日付です。
func writeArchivesPurgedByMinimumRetention(t *testing.T, directory string, now time.Time) []string {
	t.Helper()
	const fileSize int64 = 4 * 1024 * 1024
	paths := make([]string, 0, 3)
	for _, date := range []time.Time{now.AddDate(0, 0, -36), now.AddDate(0, 0, -35), now.AddDate(0, 0, -1)} {
		path := filepath.Join(directory, "test-app_"+date.Format("20060102")+"_1.jsonl")
		writeArchiveSizedFile(t, path, fileSize, date)
		paths = append(paths, path)
	}
	return paths
}

// 保持上限をすべて0にすると、期限切れ・超過世代・容量超過のarchiveも削除しません。
// 保持日数0では日付が変わっても期限確認のための走査を行わず、書き込みごとの全体走査も
// 行いません。一方、UsageCacheTTLごとの走査は継続し、圧縮の再開や送信待ちの保護を止めません。
func TestInlineArchiveUnlimitedRetentionKeepsArchivesAndScansOnUsageCacheTTL(t *testing.T) {
	now := time.Date(2042, time.May, 6, 23, 30, 0, 0, time.UTC)
	var (
		notificationsMu sync.Mutex
		warnings        []*logging.LogEntry
		errorsNotified  []error
	)
	pipeline := newTestArchiveHandler(t, now, unlimitedRetentionArchiveConfig, InternalLogHandlers{
		OnError: func(_ context.Context, err error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
			notificationsMu.Lock()
			errorsNotified = append(errorsNotified, err)
			notificationsMu.Unlock()
		},
		OnWarn: func(_ context.Context, entry *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
			notificationsMu.Lock()
			warnings = append(warnings, entry)
			notificationsMu.Unlock()
		},
	})
	handler := pipeline.handler
	clock := &archivePurgeClock{now: now}
	handler.Clock = clock
	scans := countArchiveDirectoryScans(handler)

	var archives []string
	for _, category := range unlimitedRetentionFailureCategories {
		activePath, err := pipeline.ActiveArchivePath(category)
		require.NoError(t, err)
		archives = append(archives, activePath)
		archives = append(archives, writeArchivesPurgedByMinimumRetention(
			t,
			failureArchiveDirectoryForFailureCategory(handler, category),
			now,
		)...)
	}
	// 走査の有無を前回の確認からの増減で確認し、一回の観測内のglob回数には依存しません。
	previousScans := map[failurearchive.FailureCategory]int{}
	requireArchiveScans := func(scanned bool, phase string) {
		t.Helper()
		for _, category := range unlimitedRetentionFailureCategories {
			got := scans(category)
			if scanned {
				require.Greater(t, got, previousScans[category], "%s: %s must observe the whole directory", phase, category)
			} else {
				require.Equal(t, previousScans[category], got, "%s: %s must not observe the whole directory", phase, category)
			}
			previousScans[category] = got
		}
	}
	write := func(at time.Time, id string) {
		t.Helper()
		clock.Set(at)
		require.NoError(t, pipeline.HandleFailure(context.Background(), retryableArchiveResult(at, id)))
	}

	// 構築後最初の書き込みでは、保持上限がなくても使用量の初回観測として走査します。
	write(now, "initial-observation")
	requireArchiveScans(true, "initial observation")

	// 同じ時刻の書き込みは走査しません。保持日数0では期限確認による全体走査も行いません。
	write(now, "same-time")
	requireArchiveScans(false, "within usage cache TTL")

	// TTL内に日付が変わっても、期限による削除がないため走査しません。
	nextDay := time.Date(2042, time.May, 7, 0, 10, 0, 0, time.UTC)
	require.Less(t, nextDay.Sub(now), testUsageCacheTTL)
	write(nextDay, "next-day")
	requireArchiveScans(false, "next day within usage cache TTL")

	// 容量上限がなくてもUsageCacheTTLごとの走査は継続します。
	write(now.Add(testUsageCacheTTL), "usage-cache-expiry")
	requireArchiveScans(true, "usage cache expiry")

	for _, path := range archives {
		require.FileExists(t, path)
	}
	notificationsMu.Lock()
	defer notificationsMu.Unlock()
	require.Empty(t, errorsNotified)
	require.Empty(t, warnings, "unlimited directory size must not warn about exceeding a limit")
}

// 日付の変更によるrotation後のcleanupも、保持上限0の条件では削除しません。rotation後の旧active
// fileは削除候補になりますが、保持日数・保持世代数・ディレクトリ上限がすべて0なら既存のarchiveと
// ともに残します。rotationで使用量の概算を無効にするため、UsageCacheTTL内でも全体を観測します。
func TestInlineArchiveRotationWithUnlimitedRetentionKeepsRotatedArchives(t *testing.T) {
	now := time.Date(2042, time.May, 6, 23, 30, 0, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		unlimitedRetentionArchiveConfig(cfg)
		cfg.enableRotate = true
	})
	handler := pipeline.handler
	clock := &archivePurgeClock{now: now}
	handler.Clock = clock
	scans := countArchiveDirectoryScans(handler)

	var archives []string
	rotatedPaths := map[failurearchive.FailureCategory]string{}
	for _, category := range unlimitedRetentionFailureCategories {
		activePath, err := pipeline.ActiveArchivePath(category)
		require.NoError(t, err)
		rotatedPaths[category] = activePath
		archives = append(archives, activePath)
		archives = append(archives, writeArchivesPurgedByMinimumRetention(
			t,
			failureArchiveDirectoryForFailureCategory(handler, category),
			now,
		)...)
	}
	require.NoError(t, pipeline.HandleFailure(context.Background(), retryableArchiveResult(now, "before-rotation")))
	initialScans := map[failurearchive.FailureCategory]int{}
	for _, category := range unlimitedRetentionFailureCategories {
		initialScans[category] = scans(category)
		require.Positive(t, initialScans[category], "%s initial full scans", category)
	}

	nextDay := time.Date(2042, time.May, 7, 0, 10, 0, 0, time.UTC)
	require.Less(t, nextDay.Sub(now), testUsageCacheTTL)
	clock.Set(nextDay)
	require.NoError(t, pipeline.HandleFailure(context.Background(), loggerfailure.SendResult{
		Meta: loggerfailure.Meta{AttemptedAt: nextDay},
		RetryableRecords: []loggerfailure.FailedRecord{
			archiveFailedRecord("after-rotation-retryable", nextDay, 429, "retryable"),
		},
		DeadRecords: []loggerfailure.FailedRecord{
			archiveFailedRecord("after-rotation-permanent", nextDay, 400, "dead"),
		},
	}))
	for _, category := range unlimitedRetentionFailureCategories {
		activePath, err := pipeline.ActiveArchivePath(category)
		require.NoError(t, err)
		require.NotEqual(t, rotatedPaths[category], activePath, "%s archive must rotate on the next day", category)
		require.Len(t, readArchiveRecords(t, activePath), 1)
		require.Greater(t, scans(category), initialScans[category], "%s full scans after rotation", category)
	}
	for _, path := range archives {
		require.FileExists(t, path)
	}
}

// 保持上限のうち0にした条件だけを無効にします。同じarchiveでも最小値の上限なら削除されることを
// 対照として確認し、0では同じ走査で削除しないことを各failure categoryで確認します。
func TestInlineArchiveZeroRetentionLimitDisablesOnlyThatPurge(t *testing.T) {
	now := time.Date(2042, time.May, 6, 12, 0, 0, 0, time.UTC)
	const largeFileSize int64 = 4 * 1024 * 1024
	type archiveFixture struct {
		name string
		size int64
	}
	tests := []struct {
		name string
		// baseは対象外の条件で削除しない上限を設定します。
		base          func(*testArchiveConfig)
		zero          func(*testArchiveConfig)
		minimum       func(*testArchiveConfig)
		files         []archiveFixture
		purgedByLimit []string
	}{
		{
			name: "age",
			base: func(cfg *testArchiveConfig) {
				cfg.maxFileBackups = 10
				cfg.maxDirSize = 100 * 1024 * 1024
			},
			zero:    func(cfg *testArchiveConfig) { cfg.maxFileAge = 0 },
			minimum: func(cfg *testArchiveConfig) { cfg.maxFileAge = infrafsstore.MinAllowedMaxFileAge },
			files: []archiveFixture{
				{name: "test-app_20420401_1.jsonl", size: 1},
				{name: "test-app_20420505_1.jsonl", size: 1},
			},
			purgedByLimit: []string{"test-app_20420401_1.jsonl"},
		},
		{
			name: "generation",
			base: func(cfg *testArchiveConfig) {
				cfg.maxFileAge = 30
				cfg.maxDirSize = 100 * 1024 * 1024
			},
			zero:    func(cfg *testArchiveConfig) { cfg.maxFileBackups = 0 },
			minimum: func(cfg *testArchiveConfig) { cfg.maxFileBackups = infrafsstore.MinAllowedMaxFileBackups },
			files: []archiveFixture{
				{name: "test-app_20420506_2.jsonl", size: 1},
				{name: "test-app_20420506_3.jsonl", size: 1},
				{name: "test-app_20420506_4.jsonl", size: 1},
			},
			purgedByLimit: []string{"test-app_20420506_2.jsonl"},
		},
		{
			name: "directory size",
			base: func(cfg *testArchiveConfig) {
				cfg.maxFileAge = 30
				cfg.maxFileBackups = 10
			},
			zero:    func(cfg *testArchiveConfig) { cfg.maxDirSize = 0 },
			minimum: func(cfg *testArchiveConfig) { cfg.maxDirSize = infrafsstore.MinAllowedMaxDirSize },
			files: []archiveFixture{
				{name: "test-app_20420504_1.jsonl", size: largeFileSize},
				{name: "test-app_20420505_1.jsonl", size: largeFileSize},
				{name: "test-app_20420506_2.jsonl", size: largeFileSize},
			},
			purgedByLimit: []string{"test-app_20420504_1.jsonl"},
		},
	}
	for _, test := range tests {
		for _, category := range unlimitedRetentionFailureCategories {
			for _, limit := range []struct {
				name        string
				apply       func(*testArchiveConfig)
				wantRemoved []string
			}{
				{name: "minimum", apply: test.minimum, wantRemoved: test.purgedByLimit},
				{name: "zero", apply: test.zero},
			} {
				t.Run(test.name+"/"+string(category)+"/"+limit.name, func(t *testing.T) {
					var (
						notificationsMu sync.Mutex
						warnings        []*logging.LogEntry
					)
					pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
						test.base(cfg)
						limit.apply(cfg)
					}, InternalLogHandlers{
						OnWarn: func(_ context.Context, entry *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
							notificationsMu.Lock()
							warnings = append(warnings, entry)
							notificationsMu.Unlock()
						},
					})
					handler := pipeline.handler
					scans := countArchiveDirectoryScans(handler)
					directory := failureArchiveDirectoryForFailureCategory(handler, category)
					for _, file := range test.files {
						writeArchiveSizedFile(t, filepath.Join(directory, file.name), file.size, now.Add(-time.Hour))
					}

					require.NoError(t, handler.triggerArchiveCleanup(context.Background(), category))
					require.Equal(t, 1, scans(category), "cleanup must inspect the directory")
					removed := make(map[string]struct{}, len(limit.wantRemoved))
					for _, name := range limit.wantRemoved {
						removed[name] = struct{}{}
					}
					for _, file := range test.files {
						path := filepath.Join(directory, file.name)
						if _, ok := removed[file.name]; ok {
							require.NoFileExists(t, path)
						} else {
							require.FileExists(t, path)
						}
					}
					activePath, err := pipeline.ActiveArchivePath(category)
					require.NoError(t, err)
					require.FileExists(t, activePath)
					if limit.name == "zero" {
						notificationsMu.Lock()
						defer notificationsMu.Unlock()
						require.Empty(t, warnings)
					}
				})
			}
		}
	}
}

// 保持上限をすべて0（削除しない）にしても、空き容量が閾値を下回る間は新しい失敗を
// archiveへ保存せず、過負荷policy（drop）で処理します。容量確保のために既存のarchiveを
// 削除することもありません。空き容量が閾値まで戻れば、次の失敗から通常どおり保存します。
func TestArchiveDiskSpaceThresholdProtectsDiskWithUnlimitedRetention(t *testing.T) {
	now := time.Date(2042, time.May, 6, 12, 0, 0, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		unlimitedRetentionArchiveConfig(cfg)
		cfg.overload.overloadPolicy = pkgdurable.OverloadPolicyDrop
	})
	handler := pipeline.handler
	minimum := handler.Config.OverloadPolicyConfig().MinAvailableDiskSpace()
	var free atomic.Uint64
	free.Store(minimum - 1)
	handler.availableDiskSpace = func(string) (uint64, error) {
		return free.Load(), nil
	}

	var archives []string
	for _, category := range unlimitedRetentionFailureCategories {
		archives = append(archives, writeArchivesPurgedByMinimumRetention(
			t,
			failureArchiveDirectoryForFailureCategory(handler, category),
			now,
		)...)
	}
	result := loggerfailure.SendResult{
		Meta: loggerfailure.Meta{Backend: "opensearch", AttemptedAt: now},
		RetryableRecords: []loggerfailure.FailedRecord{
			archiveFailedRecord("retryable-record", now, 429, "too_many_requests"),
		},
		DeadRecords: []loggerfailure.FailedRecord{
			archiveFailedRecord("permanent-record", now, 400, "bad_request"),
		},
	}

	report, err := pipeline.HandleFailureWithReport(context.Background(), result)
	require.NoError(t, err)
	require.Empty(t, report.Durables, "overloaded archive must not report a durable save")
	require.Len(t, report.Evacuations, 1)
	require.Equal(t, pkglogger.EvacuationDropped, report.Evacuations[0].Kind)
	require.Equal(t, pkglogger.EvacuationSucceeded, report.Evacuations[0].Status)
	require.Empty(t, readArchiveRecords(t, handler.Fsstore.ActiveFilePathRetryable))
	require.Empty(t, readArchiveRecords(t, handler.Fsstore.ActiveFilePathPermanent))
	for _, path := range archives {
		require.FileExists(t, path)
	}

	// 閾値ちょうどの空き容量に戻れば、同じ設定のまま保存を再開します。
	free.Store(minimum)
	report, err = pipeline.HandleFailureWithReport(context.Background(), result)
	require.NoError(t, err)
	require.Empty(t, report.Evacuations)
	require.NotEmpty(t, report.Durables)
	for _, durableResult := range report.Durables {
		require.Equal(t, pkglogger.DurableSucceeded, durableResult.Status)
	}
	require.Len(t, readArchiveRecords(t, handler.Fsstore.ActiveFilePathRetryable), 1)
	require.Len(t, readArchiveRecords(t, handler.Fsstore.ActiveFilePathPermanent), 1)
	for _, path := range archives {
		require.FileExists(t, path)
	}
}

// 保持上限0の判定はpurge本体でも行います。保持期限の確認日として呼ばれても、保持日数0なら
// 期限によるpurgeを実行せず、世代数・容量の上限0でも削除しません。削除後の使用量には
// 全fileの容量を残します。
func TestPurgeObservedArchiveFilesSkipsZeroRetentionLimits(t *testing.T) {
	now := time.Date(2042, time.May, 6, 12, 0, 0, 0, time.UTC)
	cfg := newValidArchiveHandlerConfig(t)
	unlimitedRetentionArchiveConfig(&cfg)
	fileName := logpattern.FileNamePattern{
		Dir:       t.TempDir(),
		Target:    "test-app",
		Extension: "jsonl",
	}
	// 直接構築のhandlerは共有Guardを使います。
	handler := &FsArchiveHandler{
		Config: cfg,
		Clock:  archiveFixedClock{now: now},
		Tracer: observer.NopTracer{},
		Meter:  observer.NopMeter{},
	}
	paths := writeArchivesPurgedByMinimumRetention(t, fileName.Dir, now)
	files := infrafsstore.GetFilesInfo(paths)
	require.Len(t, files, len(paths))
	var totalBytes uint64
	for _, file := range files {
		totalBytes += file.Size
	}

	result := handler.purgeObservedArchiveFiles(
		context.Background(),
		failurearchive.FailureCategoryRetryable,
		time.Time{},
		now,
		true,
		newArchiveDirectoryInspection(infrafsstore.FileInspection{RegularFiles: files, Paths: archiveInfoPathsForTest(files)}, fileName),
		nil,
	)
	require.NoError(t, handler.reportArchivePurge(context.Background(), result, now))
	require.False(t, result.ageExecuted)
	require.False(t, result.generationExecuted)
	require.False(t, result.dirSizeExecuted)
	require.Equal(t, totalBytes, result.observedDirectoryBytes)
	for _, path := range paths {
		require.FileExists(t, path)
	}
}
