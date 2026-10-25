package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// listLifecycleTargetFilesは対象targetのcanonicalなrotation fileを列挙します。
// 最新fileはまだ書き込み対象の可能性があるため除外し、残りはfile名の日付・番号順に
// 並べます。同じ世代の未圧縮fileと圧縮済みfileが共存する場合は、
// source残骸の判定を先に行えるよう未圧縮fileを先に返します。
// 最新世代はリンク等も含むファイル名で確定し、実際の処理対象だけを通常ファイルに限定します。
// 停止中専用の手動復旧では、最新fileも処理対象に含めます。
func (m *FileSeriesManager) listLifecycleTargetFiles() ([]string, error) {
	scan, err := m.scanLifecycleTargetFiles()
	if err != nil {
		return nil, err
	}
	// purgeは観測不完全な一覧では実行しません。圧縮・upload側は個別エラーを
	// 世代ごとに扱い、正常な後続ファイルを処理します。
	var errs []error
	for _, sourceGroup := range scan.sourceGroups.Sources {
		if sourceGroup.Err != nil {
			errs = append(errs, sourceGroup.Err)
		}
	}
	return scan.files, errors.Join(errs...)
}

type lifecycleFileScan struct {
	// filesは日付・世代番号順の処理一覧です。通常は最新の書き込み候補を除き、
	// 停止中の手動復旧では含めます。検査失敗した名前も
	// 残します。操作前に対応するsourceGroups.SourceのErrを確認します。
	files []string
	// sourceGroupsは走査した名前・通常ファイル・検査エラーを元ログごとにまとめます。
	// Files.Pathsには最新・非通常・検査失敗の名前も残し、完了位置が未完了世代を
	// 飛び越えないようにします。既存圧縮ファイルはRegularFilesから選びます。
	// 最新名の検査エラーと非通常の元ログは、従来どおり処理対象外です。
	sourceGroups fsstore.CompressionSourceGroups
}

// scanLifecycleTargetFilesは名前でlatestを確定し、通常ファイルと個別の検査エラーを
// 分けて返します。directory自体を走査できない場合だけ全体のerrorを返します。
func (m *FileSeriesManager) scanLifecycleTargetFiles() (lifecycleFileScan, error) {
	var scan lifecycleFileScan
	var observed fsstore.FileInspection
	fileName, err := m.fileNamePattern()
	if err != nil {
		return scan, err
	}

	matches, err := logpattern.GlobFileNames(
		fileName,
		logpattern.GlobByTarget,
	)
	if err != nil {
		return scan, fmt.Errorf("glob appendlog files: %w", err)
	}
	m.prunePathCaches(matches)

	// snapshotを先に取り、その後でlatestを解決する。writerがAからBへrotateする場合、
	// snapshot後のrotationならBがlatestとなってAはclose済み、latest解決後のrotationなら
	// snapshot内のAを保護してBはsnapshot外となるため、active fileを削除候補にしない。
	var latestFileNames map[string]struct{}
	if !m.manualRecovery {
		latestFileNames, err = m.latestFileNames(fileName)
		if err != nil {
			return scan, err
		}
	}

	type lifecycleFile struct {
		path       string
		generation fileGeneration
		compressed bool
	}
	ordered := make([]lifecycleFile, 0, len(matches))
	for _, path := range matches {
		cleaned := filepath.Clean(path)
		generation, ok := m.parseFileGeneration(cleaned)
		if !ok {
			// target prefixだけが一致するmemoやtemporary fileは、このFileSeriesManagerが
			// 生成したrotation fileではないためlifecycle対象に含めません。
			continue
		}
		// 圧縮・uploadだけでなく、圧縮済みファイルがある場合の元ログの退避・削除でも
		// リンクを処理しません。この除外で、上の最新世代の判定結果は変更しません。
		info, err := os.Lstat(cleaned)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		observed.Paths = append(observed.Paths, cleaned)
		var inspectionErr error
		if err != nil {
			inspectionErr = fmt.Errorf("inspect appendlog lifecycle file %q: %w", cleaned, err)
		} else if !info.Mode().IsRegular() {
			if m.hasUncompressedExtension(cleaned) {
				// 元ログ名のリンク・directoryは従来どおり処理対象外です。
				// 名前は残し、対応checkpointを不在と誤認して回収しません。
				continue
			}
			inspectionErr = fmt.Errorf("%w: path=%s mode=%s", appendlog.ErrCompressedPeerNotRegular, cleaned, info.Mode())
		} else {
			observed.RegularFiles = append(observed.RegularFiles, fsstore.FileInfo{
				Path: cleaned, Size: uint64(info.Size()), ModTime: info.ModTime(),
			})
		}
		// 最新名は処理対象にはせず、checkpointの生存世代には含めます。
		if pathidentity.Contains(latestFileNames, cleaned) {
			continue
		}
		if inspectionErr != nil {
			if observed.PathErrors == nil {
				observed.PathErrors = make(map[string]error)
			}
			observed.PathErrors[cleaned] = inspectionErr
		}
		ordered = append(ordered, lifecycleFile{
			path:       cleaned,
			generation: generation,
			compressed: !m.hasUncompressedExtension(cleaned),
		})
	}
	sort.SliceStable(ordered, func(left, right int) bool {
		comparison := ordered[left].generation.compare(ordered[right].generation)
		if comparison != 0 {
			return comparison < 0
		}
		if ordered[left].compressed != ordered[right].compressed {
			return !ordered[left].compressed
		}
		return ordered[left].path < ordered[right].path
	})

	scan.files = make([]string, len(ordered))
	for index := range ordered {
		scan.files[index] = ordered[index].path
	}
	// 元ログごとの分類はinlineと同じ部品へ渡します。上のLstat結果だけを使い、
	// ファイルの再検査や追加のディレクトリ走査は行いません。
	scan.sourceGroups = fsstore.GroupFilesByCompressionSource(observed, fileName.Extension)
	return scan, nil
}

// parseFileGenerationはfile名から並べ替えと成功記録のkeyに使う日付・番号を取得します。
func (m *FileSeriesManager) parseFileGeneration(path string) (fileGeneration, bool) {
	fileName, err := m.fileNamePattern()
	if err != nil {
		return fileGeneration{}, false
	}
	generation, canonical := logpattern.ParseCanonicalGenerationFilePath(path, fileName)
	if !canonical {
		return fileGeneration{}, false
	}
	return fileGeneration{
		Period: generation.Period,
		Index:  generation.Index,
	}, true
}

// prepareObjectStorageUploadRunは今回の宛先と完了位置を進める際に確認する既存世代を固定します。
func (m *FileSeriesManager) prepareObjectStorageUploadRun(
	ctx context.Context,
	paths []string,
	observedPaths []string,
) (*objectStorageUploadRunState, error) {
	if m.objectStorageUpload == nil {
		return nil, nil
	}
	fileName, err := m.fileNamePattern()
	if err != nil {
		return nil, err
	}
	state, err := m.objectStorageUpload.prepareRun(ctx)
	if err != nil {
		m.scheduleUploadPreparationRecovery(paths, err)
		return nil, err
	}
	if m.objectStorageUpload.checkpointStore != nil {
		// factoryを介さない利用でも、圧縮前に宛先の初期完了位置を登録します。
		var activePeriod string
		activePeriod, err = currentRotationPeriod(m.Config, m.Clock.Now())
		if err == nil {
			err = m.objectStorageUpload.registerRunDestinations(ctx, fileName, activePeriod, state)
		}
		if err != nil && !checkpointstore.IsPendingSaveOnly(err) {
			m.scheduleUploadPreparationRecovery(paths, err)
			return nil, err
		}
	}
	// 保存だけの失敗なら初期cursorはStoreのmemoryに受理済みです。
	// 宛先別のstateと先行世代を保ったまま続行します。
	m.setObservedUploadKeys(state, observedPaths)
	return state, err
}

func (m *FileSeriesManager) setObservedUploadKeys(state *objectStorageUploadRunState, observedPaths []string) {
	if state == nil {
		return
	}
	livePaths := append([]string(nil), observedPaths...)
	if m.directoryGuard != nil {
		m.directoryGuard.WithProtectedPaths(func(protected map[string]struct{}) {
			for path := range protected {
				livePaths = append(livePaths, path)
			}
		})
	}
	seen := make(map[string]struct{}, len(livePaths))
	for _, path := range livePaths {
		if pathidentity.ComparisonKey(filepath.Dir(path)) != pathidentity.ComparisonKey(m.fileName.Dir) {
			continue
		}
		generation, valid := m.parseFileGeneration(path)
		if !valid {
			continue
		}
		// raw・圧縮file・復旧用の保護名は同じ世代として完了位置を確認します。
		key := fileCheckpointKey(generation, !m.hasUncompressedExtension(path))
		if _, duplicate := seen[key.Generation]; !duplicate {
			seen[key.Generation] = struct{}{}
			state.observedKeys = append(state.observedKeys, key)
		}
	}
}

// hasUncompressedExtensionは、pathが圧縮前のbase extensionを持つかを返します。
// 大小文字は保存先directoryの規則（fileNameのEffectiveNameCase）で比較し、
// 世代の解析と同じ規則で未圧縮fileと圧縮fileを区別します。
func (m *FileSeriesManager) hasUncompressedExtension(path string) bool {
	fileName, err := m.fileNamePattern()
	if err != nil {
		return false
	}
	extension := strings.TrimPrefix(fileName.Extension, ".")
	_, generationExtension, ok := fsstore.SplitBaseAndOptionalCompressionExtensionsForNameCase(
		filepath.Base(path),
		extension,
		fileName.EffectiveNameCase(),
	)
	return ok && generationExtension == "."+extension
}

// prunePathCachesは、existingPathsに含まれないパスの送信済み記録と、
// 圧縮ファイルの検証済み記録をメモリから削除します。
// existingPathsには、ディレクトリから列挙した対象ログ系列の候補パス一覧を渡します。
// この関数自身は、ファイルの存在確認や種類の検査を行いません。
func (m *FileSeriesManager) prunePathCaches(existingPaths []string) {
	existing := make(map[string]struct{}, len(existingPaths))
	for _, path := range existingPaths {
		existing[pathidentity.ComparisonKey(path)] = struct{}{}
	}

	m.objectStorageUpload.pruneUploadedPaths(existing)

	m.compressedPeerCacheMu.Lock()
	defer m.compressedPeerCacheMu.Unlock()
	for path := range m.verifiedCompressedPeers {
		if _, exists := existing[path]; !exists {
			delete(m.verifiedCompressedPeers, path)
		}
	}
}

// fileNamePatternは構築時に保存先を解決した固定patternを検証して返します。
// Configからの補完やディレクトリ作成は行いません。必須値の欠落や保存先の
// 解決失敗は、列挙・圧縮・upload・削除に進む前にエラーにします。
func (m *FileSeriesManager) fileNamePattern() (logpattern.FileNamePattern, error) {
	fileName := m.fileName
	if strings.TrimSpace(fileName.Dir) == "" {
		return fileName, fmt.Errorf("%w: file directory must not be empty", ErrInvalidFileName)
	}
	if strings.TrimSpace(fileName.Target) == "" {
		return fileName, fmt.Errorf("%w: file target must not be empty", ErrInvalidFileName)
	}
	if strings.TrimSpace(strings.TrimPrefix(fileName.Extension, ".")) == "" {
		return fileName, fmt.Errorf("%w: file extension must not be empty", ErrInvalidFileName)
	}
	if m.fileNameResolutionErr != nil {
		return fileName, fmt.Errorf("%w: resolve file directory: %w", ErrInvalidFileName, m.fileNameResolutionErr)
	}
	layout := m.Config.Layout()
	if err := appendlog.ValidateLogFileNameConfigProvider(layout); err != nil {
		return fileName, fmt.Errorf("%w: %w", ErrInvalidFileName, err)
	}
	// 明示patternはlayoutのtargetやprefixを上書きできるため、実際の名前も
	// 検証します。Dirなどのpathにはfile名の文字制約を適用しません。
	if err := errors.Join(
		pkgfsstore.ValidateLogFileNameValue("file target", fileName.Target),
		pkgfsstore.ValidateLogFileNameValue("file prefix", fileName.Prefix),
		pkgfsstore.ValidateLogFileNameValue("file extension", fileName.Extension),
	); err != nil {
		return fileName, fmt.Errorf("%w: %w", ErrInvalidFileName, err)
	}
	return fileName, nil
}

// latestFileNames は lifecycle 処理から除外する最新 file 名を保守的に推定する。
// 現在期間の最大 index と、列挙した未圧縮ログ名の最新期間の最大 index の両方を除外する。
// これにより、日次・週次などのローテーション境界直後でも、まだ書き込み対象の可能性がある file を触らない。
func (m *FileSeriesManager) latestFileNames(fileName logpattern.FileNamePattern) (map[string]struct{}, error) {
	latest := make(map[string]struct{}, 2)

	current, err := m.currentPeriodLatestFileName(fileName)
	if err != nil {
		return nil, err
	}
	latest[current] = struct{}{}

	if existing, ok, err := m.latestExistingFileName(fileName); err != nil {
		return nil, err
	} else if ok {
		latest[existing] = struct{}{}
	}

	return latest, nil
}

// currentPeriodLatestFileName は「現在のローテーション期間の日付 + 最大 index」の .log を最新 file 名とみなす。
func (m *FileSeriesManager) currentPeriodLatestFileName(fileName logpattern.FileNamePattern) (string, error) {
	cfg := appendlog.NewNameLogPatternConfigAdapter(m.Config)
	dateString, err := currentRotationPeriod(m.Config, m.Clock.Now())
	if err != nil {
		return "", err
	}
	fileName.Date = dateString
	if err := logpattern.ResolveMaxIndex(&fileName); err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidLatestLog, err)
	}
	return filepath.Clean(logpattern.BuildFileFullPath(fileName.Date, fileName, cfg)), nil
}

// latestExistingFileNameは、列挙した未圧縮ログ名の日付・世代番号を正本として最新を選びます。
// リンク等も名前が一致すれば比較に含めます。purge対象を通常ファイルに限定する判定とは別です。
func (m *FileSeriesManager) latestExistingFileName(fileName logpattern.FileNamePattern) (string, bool, error) {
	matches, err := logpattern.GlobFileNames(fileName, logpattern.GlobByTarget)
	if err != nil {
		return "", false, fmt.Errorf("glob appendlog latest candidates: %w", err)
	}

	var bestPath string
	var bestDate string
	var bestIndex uint64
	for _, path := range matches {
		// 通常scanと同じ大小文字・canonical判定で、旧HETEROLOG_TARGET表記の最新rawも保護します。
		generation, canonical := logpattern.ParseCanonicalGenerationFilePath(path, fileName)
		if !canonical || generation.Kind != logpattern.GenerationFileKindUncompressed {
			continue
		}
		if bestPath == "" || generation.Period > bestDate || generation.Period == bestDate && generation.Index > bestIndex {
			bestPath = filepath.Clean(path)
			bestDate = generation.Period
			bestIndex = generation.Index
		}
	}

	if bestPath == "" {
		return "", false, nil
	}
	return bestPath, true, nil
}
