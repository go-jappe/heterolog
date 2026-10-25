package lifecycle

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

var (
	// ErrPurgeByAgeは、保持期間によるpurgeで一部のfileを削除できなかったことを表します。
	// 各fileの削除errorもerrors.Is/Asで判定でき、対象pathはRunReportの削除結果にあります。
	ErrPurgeByAge = errors.New("purge appendlog files by age")
	// ErrPurgeByGenerationは、世代数によるpurgeで一部のfileを削除できなかったことを表します。
	ErrPurgeByGeneration = errors.New("purge appendlog files by generation")
	// ErrPurgeByDirectorySizeは、directory容量によるpurgeで一部のfileを削除できなかったことを表します。
	ErrPurgeByDirectorySize = errors.New("purge appendlog files by dir size")
)

// purgeFileSeriesByAgeAndGenerationは、このFileSeriesManagerが担当するfile系列を対象に、
// age、generationの順でpurgeします。directory-size purgeはDirectoryLifecycleが担当します。
// pendingPaths、未圧縮file、未upload fileは、同じ実行中に削除しません。
// Guardから受け取る保護一覧に、pendingPathsと系列の処理状態から求めた削除禁止パスを追加します。
// 呼び出し元は、このdirectoryのlifecycle lockを保持してください。
func (m *FileSeriesManager) purgeFileSeriesByAgeAndGeneration(
	ctx context.Context,
	pendingPaths map[string]struct{},
) (fileSeriesPurgeResult, error) {
	result := fileSeriesPurgeResult{directoryPurgeSafe: true}
	files, err := m.listLifecycleTargetFiles()
	if err != nil {
		result.directoryPurgeSafe = false
		return result, err
	}
	fileInfo := fsstore.GetFilesInfo(files)
	purgeAt := m.Clock.Now()
	purgeConfig := m.Config.Purge()
	maxAgeDays := purgeConfig.MaxFileAge()
	maxBackups := purgeConfig.MaxFileBackups()
	var agePurgeExcludedPaths map[string]struct{}

	if maxAgeDays > 0 {
		var (
			ran            bool
			remaining      []fsstore.FileInfo
			removalResults []fsstore.FileRemovalResult
		)
		m.directoryGuard.RunPurgeWithProtection(fileInfo, func(protected map[string]struct{}) map[string]struct{} {
			for path := range pendingPaths {
				protected[pathidentity.AbsoluteOrCleanPath(path)] = struct{}{}
			}
			agePurgeExcludedPaths = m.fileSeriesPurgeExclusions(fileInfo, protected)
			return agePurgeExcludedPaths
		}, func(purge *appendlog.Purge) {
			if !fsstore.ShouldPurgeByAgeToday(purgeAt, m.lastAgePurgeAt) {
				remaining = append([]fsstore.FileInfo(nil), fileInfo...)
				return
			}
			_, span := m.store.Tracer.StartSpan(ctx, appendlog.SpanFilePurgeByAge)
			defer span.End()
			purgeResult := purge.PurgeByAge(
				fileInfo,
				maxAgeDays,
				purgeAt,
				m.lastAgePurgeAt,
			)
			m.forgetRemovedCompressedPeers(purgeResult.RemovalResults...)
			ran = purgeResult.Ran
			remaining = purgeResult.Remaining
			removalResults = purgeResult.RemovalResults
			if purgeResult.Ran {
				// 一部fileの削除に失敗しても、日次age走査を同じ日に繰り返しません。
				m.lastAgePurgeAt = purgeAt
			}
		})
		result.report.ByAge = PurgeStepReport{Ran: ran, RemovalResults: removalResults}
		fileInfo = remaining
		if purgeErr := fsstore.JoinFileRemovalFailures(removalResults); purgeErr != nil {
			result.directoryPurgeSafe = false
			return result, fmt.Errorf("%w: %w", ErrPurgeByAge, purgeErr)
		}
	}

	if maxBackups > 0 {
		fileName, fileNameErr := m.fileNamePattern()
		if fileNameErr != nil {
			result.directoryPurgeSafe = false
			return result, fileNameErr
		}
		var (
			ran            bool
			remaining      []fsstore.FileInfo
			removalResults []fsstore.FileRemovalResult
		)
		m.directoryGuard.RunPurgeWithProtection(fileInfo, func(protected map[string]struct{}) map[string]struct{} {
			// ageのRemainingは保護fileも保持します。age中にrotationされても、
			// 開始時に除外した旧active等を同じpassのgenerationで削除しません。
			for path := range agePurgeExcludedPaths {
				protected[path] = struct{}{}
			}
			for path := range pendingPaths {
				protected[pathidentity.AbsoluteOrCleanPath(path)] = struct{}{}
			}
			return m.fileSeriesPurgeExclusions(fileInfo, protected)
		}, func(purge *appendlog.Purge) {
			candidates := purge.DeletionCandidates(fileInfo)
			// 保持数は未圧縮・圧縮形式を問わず、日付・番号で決まる世代単位で数えます。
			if logpattern.CountGenerations(
				candidates,
				fileName.Extension,
			) <= uint64(maxBackups) {
				remaining = append([]fsstore.FileInfo(nil), fileInfo...)
				return
			}
			_, span := m.store.Tracer.StartSpan(ctx, appendlog.SpanFilePurgeByGen)
			defer span.End()
			purgeResult := purge.PurgeGenerationSeries(
				fileInfo,
				fileName.Extension,
				maxBackups,
			)
			m.forgetRemovedCompressedPeers(purgeResult.RemovalResults...)
			ran = purgeResult.Ran
			remaining = purgeResult.Remaining
			removalResults = purgeResult.RemovalResults
		})
		result.report.ByGeneration = PurgeStepReport{Ran: ran, RemovalResults: removalResults}
		fileInfo = remaining
		if purgeErr := fsstore.JoinFileRemovalFailures(removalResults); purgeErr != nil {
			result.directoryPurgeSafe = false
			return result, fmt.Errorf("%w: %w", ErrPurgeByGeneration, purgeErr)
		}
	}

	return result, nil
}

// fileSeriesPurgeExclusionsは、active/retained・今回の失敗pathを含む除外一覧に、
// まだ圧縮またはuploadを終えていないpathを追加します。peerの完全性検証を含むため、
// states lockの外で呼び出します。返り値は引数のmapから独立しています。
func (m *FileSeriesManager) fileSeriesPurgeExclusions(
	files []fsstore.FileInfo,
	protectedPaths map[string]struct{},
) map[string]struct{} {
	exclusions := make(map[string]struct{}, len(protectedPaths))
	for path := range protectedPaths {
		exclusions[pathidentity.AbsoluteOrCleanPath(path)] = struct{}{}
	}
	for _, file := range files {
		path := pathidentity.AbsoluteOrCleanPath(file.Path)
		if pathidentity.Contains(exclusions, path) {
			continue
		}
		if m.shouldProtectFromLifecyclePurge(path) {
			exclusions[path] = struct{}{}
		}
	}
	return exclusions
}
