package logpattern

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

const (
	DefaultFileIndex = 1
)

type ConfigProvider interface {
	RotationType() pkgfsstore.FileRotationType
	RotationDayOfWeek() time.Weekday
	FileDateFormat() string
	FileNameFormat() string
	MaxFileSize() uint64
}

// ファイルパスを取得
func ResolveFilePath(f *FileNamePattern, c ConfigProvider) (string, error) {
	return ResolveFilePathAt(f, c, time.Now())
}

// ResolveFilePathAtは、atが属するrotation期間のfile pathを解決します。
// 日付、最大index、書き込み可能pathの選択を同じatから確定し、FileNamePatternの
// DateとIndexも返却pathと一致する状態へ更新します。
// 同じ期間を続ける場合は、fの現在のIndexより前のindexへ戻しません。active file以前の
// 世代や、起動時にAdvancePastRecordedGenerationで記録済みとして飛ばした世代を、
// directoryに残るファイルが減っていても再利用しないためです。
func ResolveFilePathAt(f *FileNamePattern, c ConfigProvider, at time.Time) (string, error) {
	dateString, err := ResolveDateStringForFileNameAt(c, at)
	if err != nil {
		return "", err
	}
	resolved := *f
	minimumIndex := 0
	if resolved.Date == "" {
		resolved.Date = dateString
		resolved.Index = DefaultFileIndex
	} else {
		comparison, err := compareFilePeriodDates(dateString, resolved.Date)
		if err != nil {
			return "", fmt.Errorf("compare active and requested file periods: %w", err)
		}
		if comparison > 0 {
			resolved.Date = dateString
			resolved.Index = DefaultFileIndex
		} else {
			// system clockの後退などで指定時刻がactive期間より古くても、file系列を
			// 過去へ戻さず、現在のactive期間でindexだけを解決します。
			dateString = resolved.Date
			minimumIndex = resolved.Index
		}
	}

	// (FileLoggerType)prefix (FileLoggerのcfg(コマンドラインarg))target (arg)date (ログディレクトリの該当ファイル名のfilexが現時点で最大のもの)fileindex のファイル名
	// 念の為、現時点の最大indexを取得してindexフィールドに設定
	if err := ResolveMaxIndex(&resolved); err != nil {
		return "", fmt.Errorf("failed to retrieve file max index when rotation: %w", err)
	}
	if resolved.Index < minimumIndex {
		resolved.Index = minimumIndex
	}

	// fileIndexフィールドとディレクトリ名からフルパス（ファイル名含む）を取得
	logFilePath := BuildFileFullPath(dateString, resolved, c)

	logFilePathIndexed, err := selectWritableFilePathForDate(logFilePath, &resolved, c, dateString)
	if err != nil {
		return logFilePathIndexed, fmt.Errorf("failed to select writable log file: %w", err)
	}
	*f = resolved
	return logFilePathIndexed, nil
}

func BuildFileFullPath(date string, fileName FileNamePattern, cfg ConfigProvider) string {
	return filepath.Join(fileName.Dir, createFileName(date, fileName, cfg))
}
func createFileName(
	dateStr string,
	fileName FileNamePattern,
	cfg ConfigProvider,
) string {
	return fmt.Sprintf(cfg.FileNameFormat(), fileName.Prefix, fileName.Target, dateStr, fileName.Index)
}

// ファイルのindex番号が書き込み可能な状態であればフルパスで返却
// 候補がシンボリックリンク、またはファイル最大値に達していれば次のindex番号を使用する
func SelectWritableFilePath(
	filePath string,
	fileName *FileNamePattern,
	cfg ConfigProvider,
) (string, error) {
	dateStr, err := ResolveDateStringForFileName(cfg)
	if err != nil {
		return "", err
	}
	return selectWritableFilePathForDate(filePath, fileName, cfg, dateStr)
}

// selectWritableFilePathForDateは、呼び出し元が一度だけ確定したrotation期間内で
// 書き込み可能なindexを選びます。処理途中で現在時刻を読み直しません。
// 圧縮済みの同じ世代は、保存先ディレクトリの規則で大小文字だけが異なる名前も含めて確認します。
func selectWritableFilePathForDate(
	filePath string,
	fileName *FileNamePattern,
	cfg ConfigProvider,
	dateStr string,
) (string, error) {
	nameCase := fileName.EffectiveNameCase()
	for {
		fileInfo, err := os.Lstat(filePath)
		rawExists := err == nil
		if err != nil && !os.IsNotExist(err) {
			return "", fmt.Errorf("failed to get file information: %w", err)
		}
		// ファイル名の世代番号は使用済みとして扱い、リンク経由ではファイルを開きません。
		// Lstatでリンク切れ・循環リンクも判定し、番号を1つ進めます。
		if rawExists && fileInfo.Mode()&os.ModeSymlink != 0 {
			fileName.Index++
			filePath = BuildFileFullPath(dateStr, *fileName, cfg)
			continue
		}
		peerNameExists, err := generationPeerNameExists(
			filePath,
			fileName.Extension,
			nameCase,
		)
		if err != nil {
			return "", err
		}
		retiredSourceExists, err := compressionSourceDeletePendingPathExists(filePath)
		if err != nil {
			return "", err
		}
		// 同じlogical generationの圧縮fileが残っているpathへraw fileを再作成すると、
		// 次の圧縮renameが未uploadまたは保持中の成果物を上書きします。rawの有無やsizeに
		// かかわらず、そのindexは使用済みとして次へ進みます。圧縮完了後の
		// source削除待ちが残るindexも、新しいrawと衝突しないよう同様に予約します。
		// 大小文字を区別しないディレクトリでは、大小文字だけが異なる名前も同じ世代です。
		if peerNameExists || retiredSourceExists {
			fileName.Index++
			filePath = BuildFileFullPath(dateStr, *fileName, cfg)
			continue
		}
		if !rawExists {
			return filePath, nil
		}

		// 存在するファイルのサイズが既定値より小さい、かつ次の番号のファイルがない場合は、そのファイルを使用
		// rotateBySizeはsize >= MaxFileSizeでrotationするため、ここも厳密に未満の
		// fileだけを再利用する。境界値で同じpathを再openするとrotationが成立しない。
		if uint64(fileInfo.Size()) < cfg.MaxFileSize() {
			logFileNameNextIndex := createFileNameUseNextFileIndex(dateStr, *fileName, cfg)
			logFilePathNext := filepath.Join(fileName.Dir, logFileNameNextIndex)
			nextExists, err := generationPathExists(logFilePathNext, fileName.Extension, nameCase)
			if err != nil {
				return "", err
			}
			if !nextExists {
				return filePath, nil
			}
		}

		// ファイル名の末尾の数字が変わる
		fileName.Index++
		filePath = BuildFileFullPath(dateStr, *fileName, cfg)
	}
}

func generationPathExists(rawPath string, baseExtension string, nameCase pathidentity.NameCase) (bool, error) {
	// 次世代がリンク切れでも、そのファイル名の世代を未使用とは扱いません。
	if _, err := os.Lstat(rawPath); err == nil {
		return true, nil
	} else if !os.IsNotExist(err) {
		return false, fmt.Errorf("stat log generation path %s: %w", rawPath, err)
	}
	retiredSourceExists, err := compressionSourceDeletePendingPathExists(rawPath)
	if err != nil || retiredSourceExists {
		return retiredSourceExists, err
	}
	return generationPeerNameExists(rawPath, baseExtension, nameCase)
}

func compressionSourceDeletePendingPathExists(rawPath string) (bool, error) {
	pendingPath := CompressionSourceDeletePendingPath(rawPath)
	// dangling symlinkも使用済み世代として予約するため、pending path自体の存在を確認します。
	exists, err := fsstore.PathExists(pendingPath)
	if err != nil {
		return false, fmt.Errorf("stat compression source delete-pending path %s: %w", pendingPath, err)
	}
	return exists, nil
}

// generationPeerNameExistsは、rawPathと同じディレクトリにある同じ世代の圧縮fileと
// 圧縮完了後のsource削除待ちを、そのディレクトリの規則nameCaseで名前から照合します。
// 削除待ちの完全一致はcompressionSourceDeletePendingPathExistsでも確認します。
func generationPeerNameExists(
	rawPath string,
	baseExtension string,
	nameCase pathidentity.NameCase,
) (bool, error) {
	rawName := nameCase.Key(filepath.Base(rawPath))
	candidates, err := findDirectoryFileNames(filepath.Dir(rawPath), func(name string) bool {
		if source, pending := compressionSourcePathFromDeletePendingPath(name, nameCase); pending {
			return nameCase.Key(source) == rawName
		}
		separator := strings.LastIndexByte(name, '.')
		return separator >= 0 && nameCase.Key(name[:separator]) == rawName &&
			isCompressedGenerationFile(name, baseExtension, nameCase)
	})
	if err != nil {
		return false, fmt.Errorf("list compressed and delete-pending peers for %s: %w", rawPath, err)
	}
	return len(candidates) > 0, nil
}

func createFileNameUseNextFileIndex(
	dateStr string,
	fileName FileNamePattern,
	cfg ConfigProvider,
) string {
	return fmt.Sprintf(cfg.FileNameFormat(), fileName.Prefix, fileName.Target, dateStr, fileName.Index+1)
}
