package compressedlogarchive

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	fsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgobjectstorage "github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

var archiveFailureCategories = [...]uploader.FailureCategory{
	uploader.FailureCategoryRetryable,
	uploader.FailureCategoryPermanent,
}

// PurgeConfigは失敗分類ごとのarchive保持設定を構築時の値として保持します。
type PurgeConfig struct {
	Retryable pkgobjectstorage.CompressedLogFileUploadFailureArchivePurgeConfig
	Permanent pkgobjectstorage.CompressedLogFileUploadFailureArchivePurgeConfig
}

// PurgeReportは、archiveの削除・残存件数や容量などのpurge結果を失敗分類別に保持します。
// ByFailureCategoryの区分の分類語は　retryable or permanent　です。
type PurgeReport struct {
	ByFailureCategory map[uploader.FailureCategory]FailureCategoryPurgeReport
	// PreservedFileCountはarchive root直下の.preservedにある管理者確認待ちfile数です。
	// failure category間で共有するため、一回のRunで一度だけ数えます。
	PreservedFileCount int
}

// FailureCategoryPurgeReportは一つの分類で確認・削除・保持したentry数とbyte数です。
// RetainedUnsafeEntryCountは、想定した名前のdirect-child regular fileではないため
// 自動削除しなかったentry数です。Errorsに検査失敗の理由を保持します。
// root直下の保全fileはこの分類の件数・容量には含めません。
type FailureCategoryPurgeReport struct {
	Enabled bool

	ScannedEntryCount        int
	ScannedBytes             uint64
	DeletedFileCount         int
	DeletedBytes             uint64
	RemainingEntryCount      int
	RemainingBytes           uint64
	RetainedUnsafeEntryCount int
	CapacityLimitExceeded    bool

	Errors []error
}

// Purgerはretryableとpermanentのarchiveを保持期限・容量で削除整理し、
// 保存途中で残った一時ファイルも削除します。圧縮・upload・再送は行いません。
type Purger struct {
	store  *Store
	config PurgeConfigProvider
	// clockが返す時刻とタイムゾーンを保持期限の判定に使います。
	clock clock.Clock
}

// NewPurgerは有効な分類について保持期限と最大容量を検証します。
// timeSourceの時刻とタイムゾーンを変換せず使います。nilならUTC Clockです。
func NewPurger(store *Store, config PurgeConfigProvider, timeSource clock.Clock) (*Purger, error) {
	if store == nil {
		return nil, errors.New("compressed log file upload failure archive purge store is nil")
	}
	if err := ValidatePurgeConfig(config); err != nil {
		return nil, err
	}
	if timeSource == nil {
		timeSource = clock.NewUTCClock()
	}
	return &Purger{store: store, config: config, clock: timeSource}, nil
}

// ValidatePurgeConfigは、Purgerを作成せずに保持設定を検証します。
// 同じ保存先のPurgerを共有する場合も、受け取った設定の不正はここで検出します。
// 無効な分類の保持期限・最大容量は検証しません。
func ValidatePurgeConfig(config PurgeConfigProvider) error {
	if config == nil {
		return ErrNilPurgeConfig
	}
	var errs []error
	for _, failureCategory := range archiveFailureCategories {
		failureCategoryConfig, err := purgeConfigForFailureCategory(config, failureCategory)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if failureCategoryConfig == nil {
			errs = append(errs, &FailureCategoryPurgeConfigError{
				FailureCategory: failureCategory,
				Err:             ErrNilFailureCategoryPurgeConfig,
			})
			continue
		}
		if !failureCategoryConfig.Enabled() {
			continue
		}
		if failureCategoryConfig.MaxFileAgeDays() <= 0 {
			errs = append(errs, &FailureCategoryPurgeConfigError{
				FailureCategory: failureCategory,
				Err:             ErrInvalidPurgeMaxFileAgeDays,
				MaxFileAgeDays:  failureCategoryConfig.MaxFileAgeDays(),
			})
		}
		if failureCategoryConfig.MaxDirSize() == 0 {
			errs = append(errs, &FailureCategoryPurgeConfigError{
				FailureCategory: failureCategory,
				Err:             ErrInvalidPurgeMaxDirSize,
			})
		}
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("invalid compressed log file upload failure archive purge config: %w", err)
	}
	return nil
}

// Runは各分類について、保持期限を過ぎたファイルを削除し、容量超過ならさらに削除します。
// どちらもファイル名の日付・数値indexが古い順です。検査できないファイルは残して
// 他のファイルの処理を続け、エラーをreportと戻り値へ返します。
// 保持設定が無効でも、保存先に残った専用名の一時fileは掃除します。
// .preservedは削除・容量計算の対象にせず、保持設定が無効でも残存件数だけをreportへ返します。
func (p *Purger) Run(ctx context.Context) (PurgeReport, error) {
	report := PurgeReport{
		ByFailureCategory: make(
			map[uploader.FailureCategory]FailureCategoryPurgeReport,
			len(archiveFailureCategories),
		),
	}
	if p == nil || p.store == nil {
		return report, errors.New("compressed log file upload failure archive purge is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for _, failureCategory := range archiveFailureCategories {
		failureCategoryConfig, err := purgeConfigForFailureCategory(p.config, failureCategory)
		if err != nil {
			return report, err
		}
		enabled := failureCategoryConfig.Enabled()
		report.ByFailureCategory[failureCategory] = FailureCategoryPurgeReport{Enabled: enabled}
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}
	// Saveがfileを公開してcallerへdurable handoff成功を返すまでの間に、capacity
	// purgeが同じfileを削除しないよう一回の整理処理全体を排他にします。
	if err := p.store.operationMu.LockContext(ctx); err != nil {
		return report, err
	}
	defer p.store.operationMu.Unlock()
	var errs []error
	preservedFileCount, preservedErr := fsstore.CountPreservedFiles(ctx, p.store.Root().Path())
	report.PreservedFileCount = preservedFileCount
	if preservedErr != nil {
		errs = append(errs, fmt.Errorf("inspect preserved compressed log file archives: %w", preservedErr))
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		if !errors.Is(preservedErr, ctxErr) {
			errs = append(errs, ctxErr)
		}
		return report, errors.Join(errs...)
	}
	if err := p.store.EnsureDirectories(ctx); err != nil {
		return report, errors.Join(append(errs, err)...)
	}
	now := p.clock.Now()
	for _, failureCategory := range archiveFailureCategories {
		failureCategoryConfig, err := purgeConfigForFailureCategory(p.config, failureCategory)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		failureCategoryReport, err := p.purgeByFailureCategory(
			ctx,
			failureCategory,
			failureCategoryConfig,
			now,
		)
		report.ByFailureCategory[failureCategory] = failureCategoryReport
		if err != nil {
			errs = append(errs, &FailureCategoryPurgeError{FailureCategory: failureCategory, Err: err})
		}
		if ctx.Err() != nil {
			break
		}
	}
	return report, errors.Join(errs...)
}

// removeAbandonedTemporaryFilesは専用名の一時fileを掃除し、それ以外の一覧を返します。
// callerはStoreのoperationMuを保持し、保存中のfileがないことを保証します。
// 削除失敗や非通常fileはエラーで返し、通常のarchiveのpurge対象には含めません。
func removeAbandonedTemporaryFiles(ctx context.Context, directory string, entries []os.DirEntry) ([]os.DirEntry, error) {
	files := make([]os.DirEntry, 0, len(entries))
	removed := false
	var errs []error
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		name := entry.Name()
		if !isArchiveTemporaryFileName(name) {
			files = append(files, entry)
			continue
		}
		path := filepath.Join(directory, name)
		if _, err := fsstore.InspectRegularFile(path); err != nil {
			errs = append(errs, fmt.Errorf("inspect archive temporary file %q: %w", path, err))
			continue
		}
		if err := os.Remove(path); err != nil {
			errs = append(errs, fmt.Errorf("remove archive temporary file %q: %w", path, err))
			continue
		}
		removed = true
	}
	if removed {
		if err := fsstore.SyncDirectory(directory); err != nil {
			errs = append(errs, fmt.Errorf("sync archive directory after temporary file cleanup %q: %w", directory, err))
		}
	}
	return files, errors.Join(errs...)
}

type archiveFileCandidate struct {
	archivedFile ArchivedFile
	observedInfo os.FileInfo
	removable    bool
	removeTried  bool
}

func (p *Purger) purgeByFailureCategory(
	ctx context.Context,
	failureCategory uploader.FailureCategory,
	config FailureCategoryPurgeConfigProvider,
	now time.Time,
) (FailureCategoryPurgeReport, error) {
	report := FailureCategoryPurgeReport{Enabled: config.Enabled()}
	if err := ctx.Err(); err != nil {
		report.Errors = append(report.Errors, err)
		return report, err
	}

	failureCategoryDir, err := p.store.Root().FailureCategoryDir(failureCategory)
	if err != nil {
		report.Errors = append(report.Errors, err)
		return report, err
	}
	entries, err := fsstore.ReadDirectoryEntries(failureCategoryDir)
	if err != nil {
		report.Errors = append(report.Errors, err)
		return report, err
	}
	// 保持設定の有効・無効によらず未完成fileを掃除します。同じ一覧を再利用し、
	// 一時fileは通常のarchiveの件数・容量・削除候補には含めません。
	entries, err = removeAbandonedTemporaryFiles(ctx, failureCategoryDir, entries)
	if err != nil {
		report.Errors = append(report.Errors, err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		if !errors.Is(err, ctxErr) {
			report.Errors = append(report.Errors, ctxErr)
		}
		return report, errors.Join(report.Errors...)
	}
	if !config.Enabled() {
		return report, errors.Join(report.Errors...)
	}
	report.RemainingEntryCount = len(entries)
	candidates := make([]archiveFileCandidate, 0, len(entries))
	// 名前ごとにfilesystemへ問い合わせないよう、分類directoryの規則を一度だけ判定します。
	nameCase := pathidentity.DirectoryNameCase(failureCategoryDir)
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			report.Errors = append(report.Errors, err)
			break
		}
		report.ScannedEntryCount++
		candidate, candidateErrs := inspectArchiveEntryForNameCase(failureCategory, failureCategoryDir, entry.Name(), nameCase)
		for _, candidateErr := range candidateErrs {
			report.Errors = append(report.Errors, &ArchiveEntryError{Name: entry.Name(), Err: candidateErr})
		}
		if candidate.observedInfo != nil {
			sizeBytes := uint64(candidate.observedInfo.Size())
			if sizeBytes > math.MaxUint64-report.ScannedBytes {
				report.Errors = append(report.Errors, &ArchiveEntryError{Name: entry.Name(), Err: ErrArchiveByteCountOverflow})
				candidate.removable = false
			} else {
				report.ScannedBytes += sizeBytes
				report.RemainingBytes += sizeBytes
			}
		}
		if !candidate.removable {
			report.RetainedUnsafeEntryCount++
			continue
		}
		candidates = append(candidates, candidate)
	}
	if err := ctx.Err(); err != nil {
		return report, errors.Join(report.Errors...)
	}

	// 名前を解析できないファイルは容量に含めますが、削除候補には入れません。
	if err := fsstore.SortFilesByNameDateAndIndexAsc(candidates, func(candidate archiveFileCandidate) string {
		return candidate.archivedFile.Path
	}); err != nil {
		report.Errors = append(report.Errors, err)
		report.CapacityLimitExceeded = report.RemainingBytes > config.MaxDirSize()
		return report, errors.Join(report.Errors...)
	}

	cutoff := now.AddDate(0, 0, -config.MaxFileAgeDays())
	for index := range candidates {
		candidate := &candidates[index]
		order, err := fsstore.CompareFileNameDate(candidate.archivedFile.Path, cutoff)
		if err != nil {
			report.Errors = append(report.Errors, err)
			report.CapacityLimitExceeded = report.RemainingBytes > config.MaxDirSize()
			return report, errors.Join(report.Errors...)
		}
		// 日付の昇順なので、期限当日以降に達したら、残りも保持期限内です。
		if order >= 0 {
			break
		}
		if err := p.removeArchiveCandidate(ctx, candidate, &report); err != nil {
			report.Errors = append(report.Errors, err)
		}
	}

	for index := range candidates {
		if report.RemainingBytes <= config.MaxDirSize() {
			break
		}
		candidate := &candidates[index]
		if !candidate.removable || candidate.removeTried {
			continue
		}
		if err := p.removeArchiveCandidate(ctx, candidate, &report); err != nil {
			report.Errors = append(report.Errors, err)
		}
	}
	report.CapacityLimitExceeded = report.RemainingBytes > config.MaxDirSize()

	return report, errors.Join(report.Errors...)
}

// inspectArchiveEntryは、archiveディレクトリ内の1件を検査します。
// 日付・index付きの圧縮ログ名で、リンクではない通常ファイルなら削除候補とします。
// 検査に失敗した項目も、削除不可の情報とエラーを返します。通常ファイルの情報を
// 非負のサイズで取得できた場合は、名前が不正でも容量計算用にobservedInfoを保持します。
// 保持期限・容量による削除の要否はここでは判定せず、圧縮データの中身も検証しません。
// 名前の大小文字はfailureCategoryDirの規則（pathidentity.DirectoryNameCase）で比較します。
func inspectArchiveEntry(
	failureCategory uploader.FailureCategory,
	failureCategoryDir string,
	directoryEntryName string,
) (archiveFileCandidate, []error) {
	return inspectArchiveEntryForNameCase(
		failureCategory,
		failureCategoryDir,
		directoryEntryName,
		pathidentity.DirectoryNameCase(failureCategoryDir),
	)
}

// inspectArchiveEntryForNameCaseは、判定済みの規則nameCaseでinspectArchiveEntryと同じ検査を行います。
func inspectArchiveEntryForNameCase(
	failureCategory uploader.FailureCategory,
	failureCategoryDir string,
	directoryEntryName string,
	nameCase pathidentity.NameCase,
) (archiveFileCandidate, []error) {
	path := filepath.Join(failureCategoryDir, directoryEntryName)
	candidate := archiveFileCandidate{
		archivedFile: ArchivedFile{FailureCategory: failureCategory, Path: path},
		removable:    true,
	}
	var errs []error

	if err := validateArchivedFileNameForNameCase(directoryEntryName, nameCase); err != nil {
		candidate.removable = false
		errs = append(errs, err)
	} else if err := fsstore.ValidateFileNameDateAndIndex(path); err != nil {
		candidate.removable = false
		errs = append(errs, err)
	}
	info, err := fsstore.InspectRegularFile(path)
	if err != nil {
		candidate.removable = false
		errs = append(errs, fmt.Errorf("inspect archived file: %w", err))
		return candidate, errs
	}
	if info.Size() < 0 {
		candidate.removable = false
		errs = append(errs, errors.New("archived file has a negative size"))
		return candidate, errs
	}
	candidate.observedInfo = info
	return candidate, errs
}

func (p *Purger) removeArchiveCandidate(
	ctx context.Context,
	candidate *archiveFileCandidate,
	report *FailureCategoryPurgeReport,
) error {
	candidate.removeTried = true
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.store.removeObservedFile(candidate.archivedFile, candidate.observedInfo); err != nil {
		if errors.Is(err, ErrInvalidCompressedLogPath) && candidate.removable {
			candidate.removable = false
			report.RetainedUnsafeEntryCount++
		}
		return fmt.Errorf("remove compressed log file archive %q: %w", candidate.archivedFile.Path, err)
	}
	report.DeletedFileCount++
	sizeBytes := uint64(candidate.observedInfo.Size())
	report.DeletedBytes += sizeBytes
	report.RemainingEntryCount--
	report.RemainingBytes -= sizeBytes
	return nil
}

// removeObservedFileはscan後にpathが差し替えられていないことを再検証してから、
// direct-child regular file一件だけを削除します。
func (s *Store) removeObservedFile(archived ArchivedFile, observed os.FileInfo) error {
	if observed == nil {
		return fmt.Errorf("%w: observed file info is nil", ErrInvalidCompressedLogPath)
	}
	failureCategoryDir, err := s.root.FailureCategoryDir(archived.FailureCategory)
	if err != nil {
		return err
	}
	if err := ensureDirectChildRegularFile(failureCategoryDir, archived.Path); err != nil {
		return err
	}
	current, err := os.Lstat(archived.Path)
	if err != nil {
		return err
	}
	if !os.SameFile(observed, current) ||
		observed.Size() != current.Size() ||
		!observed.ModTime().Equal(current.ModTime()) {
		return fmt.Errorf("%w: archived file changed after purge scan", ErrInvalidCompressedLogPath)
	}
	if err := os.Remove(archived.Path); err != nil {
		return err
	}
	if err := fsstore.SyncDirectory(filepath.Dir(archived.Path)); err != nil {
		return fmt.Errorf("sync compressed log file archive removal: %w", err)
	}
	return nil
}

func purgeConfigForFailureCategory(
	config PurgeConfigProvider,
	failureCategory uploader.FailureCategory,
) (FailureCategoryPurgeConfigProvider, error) {
	switch failureCategory {
	case uploader.FailureCategoryRetryable:
		return config.RetryablePurge(), nil
	case uploader.FailureCategoryPermanent:
		return config.PermanentPurge(), nil
	default:
		return nil, fmt.Errorf(
			"%w: %q", ErrInvalidFailureCategory, failureCategory,
		)
	}
}
