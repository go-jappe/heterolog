package logpattern

import (
	"context"
	"fmt"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"os"
	"time"
)

// checkAndRotate は日付の変更、またはファイルサイズの超過を検知した場合にローテーション（新規作成）します
func CheckAndRotate(
	ctx context.Context,
	file *os.File,
	p ConfigProvider,
	fileName *FileNamePattern,
	rotate func(context.Context) error,
) error {
	return CheckAndRotateAt(ctx, file, p, fileName, time.Now(), rotate)
}

// CheckAndRotateAtは、日付rotation判定と後続のfile openが共有する基準時刻atで
// rotation要否を判定します。file sizeによるrotation判定は時刻に依存しません。
func CheckAndRotateAt(
	ctx context.Context,
	file *os.File,
	p ConfigProvider,
	fileName *FileNamePattern,
	at time.Time,
	rotate func(context.Context) error,
) error {
	// 日付変更によるローテーションの実行（ローテーション頻度の条件を満たした場合のみ）
	rotatedByDate, err := rotateByDateAt(ctx, file, p, fileName, rotate, at)
	if err != nil {
		return err
	}
	// 日付rotationが成功した場合、引数fileはすでにclose済みです。新しいactive fileの
	// size判定は次回呼び出しで行い、同じ呼び出し内でold fileをStatしません。
	if rotatedByDate {
		return nil
	}

	// 1ファイルあたりのファイルサイズ超過でローテーション
	return rotateBySize(ctx, file, p, fileName, rotate)
}

// 1ファイルあたりのファイルサイズ超過でローテーション
func rotateBySize(
	ctx context.Context,
	file *os.File,
	p ConfigProvider,
	fileName *FileNamePattern,
	rotate func(context.Context) error,
) error {
	if file == nil {
		return nil
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if uint64(info.Size()) >= p.MaxFileSize() {
		previousIndex := fileName.Index
		fileName.Index++
		if err := rotate(ctx); err != nil {
			fileName.Index = previousIndex
			return err
		}
	}
	return nil
}

// rotateByDateは、現在時刻が新しいrotation期間に入っていれば新しいファイルを作成します。
func rotateByDate(
	ctx context.Context,
	file *os.File,
	p ConfigProvider,
	fileName *FileNamePattern,
	rotate func(context.Context) error,
) (bool, error) {
	return rotateByDateAt(ctx, file, p, fileName, rotate, time.Now())
}

// rotateByDateAtは、呼び出し元が確定したrotation実行時刻の期間とactive fileの期間を
// 比較し、実行時刻側が新しい場合だけrotationします。system clockが後退してもfile系列を
// 過去の期間へ戻しません。
func rotateByDateAt(
	ctx context.Context,
	file *os.File,
	p ConfigProvider,
	fileName *FileNamePattern,
	rotate func(context.Context) error,
	now time.Time,
) (bool, error) {
	requestedPeriod, err := ResolveDateStringForFileNameAt(p, now)
	if err != nil {
		return false, err
	}
	activeFileDate, err := fsstore.ExtractDateFromFileName(file.Name())
	if err != nil {
		return false, err
	}
	activePeriod := activeFileDate.Format(pkgfsstore.FileDateFormat)
	comparison, err := compareFilePeriodDates(requestedPeriod, activePeriod)
	if err != nil {
		return false, fmt.Errorf("compare active and requested file periods: %w", err)
	}
	if comparison <= 0 {
		return false, nil
	}

	previous := *fileName
	fileName.Index = DefaultFileIndex // 連番リセット
	if err := rotate(ctx); err != nil {
		*fileName = previous
		return false, err
	}

	return true, nil
}
