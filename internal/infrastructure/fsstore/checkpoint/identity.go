package checkpoint

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// ArchiveCheckpointSourceIDは、一つのappendlogログ系列を表す永続的なsource IDを
// 生成します。inlineとrunnerが同じseries identityを共有できるよう、
// lifecycle modeに依存しないpackageで規則を一元化します。
// fileName.Dirには初期化時にResolveDirectoryで解決した実体パスを渡します。
// この関数自体はリンクを調べず、起動時に確定した保存先からIDを生成します。
//
// fingerprintは正規化済みdirectory、prefix、target、extensionをNUL byteで連結した
// SHA-256です。Windowsではdirectoryのextended表記を統一します。file名を構成する各要素の
// 大小文字は、fileName.Dirの比較規則（fileNameのEffectiveNameCase）へ合わせます。
// 大小文字を区別しないdirectoryでは、設定の大小文字だけを変えても同じIDになります。
// 大小文字を区別するdirectoryでは各要素をそのまま使い、従来と同じIDを維持します。
// Darwinの大小文字を区別しないvolumeで大文字を含む構成要素は、OS名で判定していた
// 以前の規則とはIDが異なります。実際のfile名やobject keyは変更しません。
// 生成規則の変更でsource IDが変わると、変更前のcheckpointは読み込まれなくなります。
func ArchiveCheckpointSourceID(
	sourcePrefix string,
	class string,
	fileName logpattern.FileNamePattern,
) string {
	// ファイル名の構成要素だけを保存先directoryの比較規則へ揃え、sourcePrefix/classは変更しません。
	nameCase := fileName.EffectiveNameCase()
	identity := strings.Join([]string{
		pathidentity.ComparisonKey(fileName.Dir),
		nameCase.Key(fileName.Prefix),
		nameCase.Key(fileName.Target),
		nameCase.Key(strings.TrimPrefix(fileName.Extension, ".")),
	}, "\x00")
	digest := sha256.Sum256([]byte(identity))
	return sourcePrefix + "_" + class + "-" + hex.EncodeToString(digest[:])
}

// ArchiveCheckpointGenerationKeyは、圧縮suffixやlifecycle modeに依存しない
// logical generationの永続keyを返します。periodとindexはwriterのcanonicalな
// file名から解決した値を渡す必要があります。
func ArchiveCheckpointGenerationKey(period string, index uint64) string {
	return period + "-" + strconv.FormatUint(index, 10)
}

// ErrNonCanonicalGenerationPathは、checkpointの世代を決められない名前のlog file pathです。
var ErrNonCanonicalGenerationPath = errors.New("appendlog archive checkpoint compressed log file path is not a canonical generation")

// NonCanonicalGenerationPathErrorは、世代を決められなかったlog file pathを示します。
// errors.IsではErrNonCanonicalGenerationPathと一致します。
type NonCanonicalGenerationPathError struct {
	Path string
}

func (e *NonCanonicalGenerationPathError) Error() string {
	return fmt.Sprintf("%v: %q", ErrNonCanonicalGenerationPath, e.Path)
}

func (e *NonCanonicalGenerationPathError) Is(target error) bool {
	return target == ErrNonCanonicalGenerationPath
}

// ErrInvalidActivePeriodは、登録境界の判定に渡した現在のrotation期間がYYYYMMDDでないことを表します。
var ErrInvalidActivePeriod = errors.New("invalid active rotation period for destination registration")

// ErrRegistrationFileScanは、宛先の登録範囲を決めるためのlog fileの走査に失敗したことを表します。
// 原因のfilesystem errorもerrors.Is/Asで判定できます。
var ErrRegistrationFileScan = errors.New("scan log files for destination registration")

// ScanExistingFileKeysはディレクトリを走査し、登録時に存在する命名規則に合った
// 未圧縮・圧縮ファイルのcheckpointキー一覧を返します。
// directoryの名前だけを読み、現在uploadできないsymlink・directoryも含めます。
// 古いsymlink等が通常fileに置き換わっても新たな対象にはしません。
// 更新時刻とfile内容は宛先の対象範囲の判定に使いません。
func ScanExistingFileKeys(fileName logpattern.FileNamePattern) ([]CheckpointKey, error) {
	entries, err := infrafsstore.ReadDirectoryEntries(fileName.Dir)
	if errors.Is(err, os.ErrNotExist) {
		return []CheckpointKey{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrRegistrationFileScan, err)
	}
	keys := make([]CheckpointKey, 0, len(entries))
	seen := make(map[CheckpointKey]struct{}, len(entries))
	// 名前ごとにfilesystemへ問い合わせないよう、directoryの比較規則を一度だけ判定します。
	fileName.NameCase = fileName.EffectiveNameCase()
	for _, entry := range entries {
		generation, canonical := logpattern.ParseCanonicalGenerationFilePath(entry.Name(), fileName)
		if !canonical {
			continue
		}
		key := CheckpointKey{
			Generation: ArchiveCheckpointGenerationKey(generation.Period, generation.Index),
			Compressed: generation.Kind != logpattern.GenerationFileKindUncompressed,
		}
		if _, duplicate := seen[key]; !duplicate {
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
	}
	slices.SortFunc(keys, compareCheckpointKeys)
	return keys, nil
}

// ScanRegistrationFileKeysは、新しい宛先の登録境界を決めるため、ScanExistingFileKeysと
// 同じ走査で登録時点の世代を返します。activePeriodには、writerが現在書き込むrotation期間の
// 開始日（YYYYMMDD）を渡します。まだ追記され得る元ログ、つまりactivePeriod以降の期間の
// 元ログだけを未圧縮のkeyとして残します。それより前の期間の元ログはwriterが戻って追記
// しないため、圧縮ファイルと同じく登録前に完結した世代のkeyとして返し、新しい宛先の
// 対象外にします。返したkeyはRegisterDestinationsへ渡す登録境界の判定だけに使います。
func ScanRegistrationFileKeys(fileName logpattern.FileNamePattern, activePeriod string) ([]CheckpointKey, error) {
	if err := validateCheckpointGeneration(ArchiveCheckpointGenerationKey(activePeriod, 0)); err != nil {
		return nil, fmt.Errorf("%w: %q", ErrInvalidActivePeriod, activePeriod)
	}
	keys, err := ScanExistingFileKeys(fileName)
	if err != nil {
		return nil, err
	}
	result := make([]CheckpointKey, 0, len(keys))
	seen := make(map[CheckpointKey]struct{}, len(keys))
	for _, key := range keys {
		// YYYYMMDDの期間は辞書順と時系列順が一致します。
		if period, _, ok := splitGeneration(key.Generation); ok && !key.Compressed && period < activePeriod {
			key.Compressed = true
		}
		if _, duplicate := seen[key]; !duplicate {
			seen[key] = struct{}{}
			result = append(result, key)
		}
	}
	slices.SortFunc(result, compareCheckpointKeys)
	return result, nil
}

// ArchiveCheckpointGenerationKeysは、指定系列の元ログ・圧縮ログの名前を世代キーへ変換します。
// 同じ世代は一件にまとめ、別系列やtmpなどの名前は除外します。ファイル操作は行いません。
// 観測済みの名前と処理中の保護パスを渡し、存在する世代の成功記録を残すために使います。
func ArchiveCheckpointGenerationKeys(paths []string, fileName logpattern.FileNamePattern) []string {
	keys := make([]string, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	directoryKey := pathidentity.ComparisonKey(fileName.Dir)
	// directoryが一致した名前だけを解析するため、比較規則はfileName.Dirから一度だけ判定します。
	fileName.NameCase = fileName.EffectiveNameCase()
	for _, path := range paths {
		if pathidentity.ComparisonKey(filepath.Dir(path)) != directoryKey {
			continue
		}
		generation, ok := logpattern.ParseCanonicalGenerationFilePath(path, fileName)
		if !ok {
			continue
		}
		key := ArchiveCheckpointGenerationKey(generation.Period, generation.Index)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return keys
}

// ArchiveCheckpointGenerationKeyForPathは、未圧縮ログ・圧縮ログのパスが命名規則に
// 合うことを確認し、期間と世代番号からキーを作ります。例: app_20260915_3.log.gz → "20260915-3"。
// キーには圧縮済みかどうかの区別を含めず、inlineとrunnerで共通に使います。
func ArchiveCheckpointGenerationKeyForPath(
	path string,
	fileName logpattern.FileNamePattern,
) (string, error) {
	generation, canonical := logpattern.ParseCanonicalGenerationFilePath(path, fileName)
	if !canonical {
		return "", &NonCanonicalGenerationPathError{Path: path}
	}
	return ArchiveCheckpointGenerationKey(generation.Period, generation.Index), nil
}
