package compressedlog

import (
	"errors"
	"fmt"
	"time"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// OnWarnとRunnerReporterへ渡す警告・実行errorの種類です。
var (
	// ErrPreservedFilesRequireReviewは、spoolの.preservedに管理者の確認を待つfileが残っていることを表します。
	// 実際に渡す警告はPreservedFilesWarningです。
	ErrPreservedFilesRequireReview = errors.New("preserved compressed log file spool files require administrator review")
	// ErrRetryAfterClampedは、upload先が上限を超えるRetry-Afterを指定したため、上限まで短縮して
	// 待機することを表します。実際に渡す警告はRetryAfterClampWarningです。
	ErrRetryAfterClamped = errors.New("compressed log file upload destination requested Retry-After beyond the maximum")
	// ErrRetryAfterMarkerは、宛先の待機期限を保存したmarkerの読み込み・保存・削除に失敗したことを
	// 表します。実際に渡す警告はRetryAfterMarkerWarningです。
	ErrRetryAfterMarker = errors.New("compressed log file upload destination Retry-After marker failed")
	// ErrReplayPanicは、再送処理がpanicで中断したことを表します。
	ErrReplayPanic = errors.New("object storage compressed log file replay panic")
	// ErrTerminalPurgePanicは、terminal purgeがpanicで中断したことを表します。
	ErrTerminalPurgePanic = errors.New("object storage compressed log file terminal purge panic")
)

// PreservedFilesWarningは、全宛先の.preservedに残る保全fileの件数を表します。
// errors.Is(err, ErrPreservedFilesRequireReview)で判定できます。
type PreservedFilesWarning struct {
	// Countは、保全fileの件数です。
	Count int
	// Rootは、spool rootのpathです。
	Root string
}

func (w *PreservedFilesWarning) Error() string {
	return fmt.Sprintf("%d preserved compressed log file spool file(s) under %q require administrator review", w.Count, w.Root)
}

func (w *PreservedFilesWarning) Unwrap() error { return ErrPreservedFilesRequireReview }

// RetryAfterClampWarningは、upload先が指定したRetry-Afterを上限まで短縮したことを表します。
// errors.Is(err, ErrRetryAfterClamped)で判定できます。
type RetryAfterClampWarning struct {
	// DestinationIDは、Retry-Afterを指定したupload先です。
	DestinationID objectstorage.DestinationID
	// Requestedは、upload先が指定した待機時間です。
	Requested time.Duration
	// MaxRetryAfterは、適用した待機時間の上限です。
	MaxRetryAfter time.Duration
	// NotBeforeは、短縮後に送信を再開する時刻です。
	NotBefore time.Time
	// Storedは、再起動前に保存した期限を、読み込んだ時点から上限までに短縮したことを表します。
	// Requestedは読み込んだ時点から保存済みの期限までの時間です。
	Stored bool
}

func (w *RetryAfterClampWarning) Error() string {
	if w.Stored {
		return fmt.Sprintf(
			"compressed log file upload destination %q has a persisted Retry-After deadline %s ahead; replay waits the maximum %s until %s",
			w.DestinationID,
			w.Requested,
			w.MaxRetryAfter,
			w.NotBefore.Format(time.RFC3339Nano),
		)
	}
	return fmt.Sprintf(
		"compressed log file upload destination %q requested Retry-After %s; replay waits the maximum %s until %s",
		w.DestinationID,
		w.Requested,
		w.MaxRetryAfter,
		w.NotBefore.Format(time.RFC3339Nano),
	)
}

func (w *RetryAfterClampWarning) Unwrap() error { return ErrRetryAfterClamped }

// RetryAfterMarkerWarningは、宛先の待機期限を保存したmarkerの読み込み・保存・削除に失敗したことを
// 表します。errors.Is(err, ErrRetryAfterMarker)で判定でき、Errの原因もerrors.Is・errors.Asで辿れます。
// 読み込めないmarkerは期限がないものとして再送を続け、次のRetry-Afterで置き換えます。
// 保存に失敗した期限はprocess内で守り、期限が残る間は保存を再試行します。保存の失敗は
// 一連の失敗につき一度だけ通知します。保存できないまま再起動した場合は引き継がれない場合があります。
type RetryAfterMarkerWarning struct {
	DestinationID objectstorage.DestinationID
	Err           error
}

func (w *RetryAfterMarkerWarning) Error() string {
	return fmt.Sprintf("compressed log file upload destination %q Retry-After marker: %v", w.DestinationID, w.Err)
}

func (w *RetryAfterMarkerWarning) Unwrap() []error { return []error{ErrRetryAfterMarker, w.Err} }

// PanicErrorは、再送またはterminal purgeのpanicを回復したerrorです。
// Kindは、ErrReplayPanicまたはErrTerminalPurgePanicで、errors.Isで判定できます。
type PanicError struct {
	Kind error
	// Valueは、回復したpanicの値です。
	Value any
}

func (e *PanicError) Error() string { return fmt.Sprintf("%v: %v", e.Kind, e.Value) }

func (e *PanicError) Unwrap() error { return e.Kind }
