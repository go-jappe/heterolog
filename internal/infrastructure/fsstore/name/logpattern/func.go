package logpattern

import (
	"fmt"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkgtime "github.com/go-jappe/heterolog/pkg/time"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func ResolveMaxIndex(p *FileNamePattern) error {
	idx, err := FindMaxIndex(*p)
	if err != nil {
		return fmt.Errorf("failed to retrieve file index: %w", err)
	}
	p.Index = idx
	return nil
}

// AdvancePastRecordedGenerationは、ResolveMaxIndexで現在期間の最大indexを解決したpを、
// checkpointなどに記録済みの世代(period, index)より後から書き込むよう進めます。
// 記録済みの世代番号を新しい内容へ再利用すると、送信済みと判定されて送信されないまま
// 削除され、object storageの同じobjectも上書きするためです。
// periodがp.Dateより新しい場合は、system clockが記録済みの期間より前へ戻ったものとして
// その期間を続け、期間内に残るファイルの最大indexも解決し直します。periodが空、または
// p.Dateより古い場合は変更しません。後続のResolveFilePathAtはこのIndexより前へ戻りません。
// indexがintで次の番号を表せない場合はエラーを返し、pを変更しません。
func AdvancePastRecordedGeneration(p *FileNamePattern, period string, index uint64) error {
	if period == "" {
		return nil
	}
	comparison, err := compareFilePeriodDates(period, p.Date)
	if err != nil {
		return fmt.Errorf("compare recorded and current file periods: %w", err)
	}
	if comparison < 0 {
		return nil
	}
	if index >= uint64(math.MaxInt) {
		return fmt.Errorf("recorded file generation %s-%d leaves no next index", period, index)
	}
	advanced := *p
	if comparison > 0 {
		advanced.Date = period
		if err := ResolveMaxIndex(&advanced); err != nil {
			return err
		}
	}
	if next := int(index) + 1; advanced.Index < next {
		advanced.Index = next
	}
	*p = advanced
	return nil
}

// 日付の次につくファイル番号の最大値を取得する
// 大小文字はl.Dirの規則（lのEffectiveNameCase）で照合し、同じファイルを指す名前の世代も含めます。
func FindMaxIndex(l FileNamePattern) (int, error) {
	nameCase := l.EffectiveNameCase()
	prefix := nameCase.Key(l.Prefix + l.Target + "_" + l.Date + "_")
	files, err := findDirectoryFileNames(l.Dir, func(name string) bool {
		return strings.HasPrefix(nameCase.Key(name), prefix)
	})
	if err != nil {
		return 0, fmt.Errorf("failed to list log files: %w", err)
	}
	maxIndex := DefaultFileIndex

	for _, filePath := range files {
		fileName := filepath.Base(filePath)
		if isCompressionSourceDeletePendingPath(fileName, nameCase) {
			fileName, _ = compressionSourcePathFromDeletePendingPath(fileName, nameCase)
			// pendingはraw sourceだけを予約し、圧縮fileへ付けた類似suffixは含めません。
			if _, matched := fsstore.TrimFileNameSuffixForNameCase(fileName, "."+l.Extension, nameCase); !matched {
				continue
			}
		}
		stem, _, ok := fsstore.SplitBaseAndOptionalCompressionExtensionsForNameCase(
			fileName,
			l.Extension,
			nameCase,
		)
		if !ok {
			continue
		}
		indexText, matched := strings.CutPrefix(nameCase.Key(stem), prefix)
		if matched {
			index, err := strconv.Atoi(indexText)
			// writerが生成するcanonicalな10進表記だけをindex候補にします。
			// `01`を1として扱うと、存在しないcanonical pathをactiveとして保護し、
			// 実際のactive fileをlifecycle処理へ渡す可能性があります。
			if err == nil && indexText == strconv.Itoa(index) && index > maxIndex {
				maxIndex = index
			}
		}
	}

	return maxIndex, nil
}

// ResolveDateStringForFileNameはweekly rotationの曜日を検証し、ログfile名に使う
// 現在期間の日付文字列を返します。未知のrotationはdailyとして扱います。
func ResolveDateStringForFileName(cfg ConfigProvider) (string, error) {
	return ResolveDateStringForFileNameAt(cfg, time.Now())
}

// ResolveDateStringForFileNameAtは、呼び出し元が一度だけ確定したfile operation時刻を
// 基準に、file名の日付部分を解決します。rotationでは判定と新しいfile名の生成へ同じ時刻を
// 渡し、処理中の日付変更による不一致を防ぎます。
func ResolveDateStringForFileNameAt(cfg ConfigProvider, at time.Time) (string, error) {
	if err := pkgfsstore.ValidateFileRotationWeekday(cfg.RotationType(), cfg.RotationDayOfWeek()); err != nil {
		return "", fmt.Errorf("resolve file-name rotation date: %w", err)
	}

	switch cfg.RotationType() {
	case pkgfsstore.FileRotationDaily:
		return getFormatedDateForFileName(cfg, at, 0), nil
	case pkgfsstore.FileRotationWeekly:
		return getFormatedDateWeeklyRotation(cfg, at), nil
	case pkgfsstore.FileRotationMonthly:
		return getFormattedDateForFileNameMonth(at), nil
	default:
		return getFormatedDateForFileName(cfg, at, 0), nil
	}
}

// Weeklyログローテーションファイル命名のための日付の文字列を返却
func getFormatedDateWeeklyRotation(cfg ConfigProvider, now time.Time) string {
	targetWeekday := pkgtime.GetLastWeekday(now, cfg.RotationDayOfWeek())
	return targetWeekday.Format(cfg.FileDateFormat())
}

// compareFilePeriodDatesはYYYYMMDDで表したrotation期間の開始日を比較します。
// leftが前なら-1、同じなら0、後なら1を返します。
func compareFilePeriodDates(left, right string) (int, error) {
	leftDate, err := time.Parse(pkgfsstore.FileDateFormat, left)
	if err != nil {
		return 0, fmt.Errorf("parse left file period date %q: %w", left, err)
	}
	rightDate, err := time.Parse(pkgfsstore.FileDateFormat, right)
	if err != nil {
		return 0, fmt.Errorf("parse right file period date %q: %w", right, err)
	}
	return leftDate.Compare(rightDate), nil
}

// ログファイルの日付をフォーマット ローテート月別に使用
func getFormattedDateForFileNameMonth(now time.Time) string {
	// 当月の1日を取得
	firstOfMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	return firstOfMonth.Format("20060102") // "YYYYMMDD" 形式
}

// 指定日からファイル名のフォーマット形式の文字列数値を取得
// 当日 0 昨日 -1 明日 1
func getFormatedDateForFileName(cfg ConfigProvider, now time.Time, daysOffset int) string {
	return now.AddDate(0, 0, daysOffset).Format(cfg.FileDateFormat())
}
