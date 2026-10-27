package worker

import (
	"context"

	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

type WorkFileStore interface {
	// ListProcessingFileCandidatesAfterは、processing配下でafterRelativePathより後にある
	// 通常ファイルを、時刻別ディレクトリ・ファイル名の順に最大limit件返します。
	// 空文字なら先頭から探します。境界のファイルは削除済みでも構いません。
	// 先頭への折り返しは呼び出し元が行います。
	ListProcessingFileCandidatesAfter(ctx context.Context, afterRelativePath string, limit int) ([]pkgspool.FileCandidate, error)

	// ClaimOldestActiveFile は、active 配下の最古 file を1件だけ processing へ移し、
	// 移動後の processing path を返します。
	//
	// 対象がなければ ok=false を返します。
	ClaimOldestActiveFile(ctx context.Context) (processingPath string, ok bool, err error)

	ReadProcessingRecords(ctx context.Context, processingPath string) ([]pkgspool.SpoolRecord, error)

	MoveProcessingToDone(ctx context.Context, processingPath string) error
	MoveProcessingToDead(ctx context.Context, processingPath string) error
	// MoveProcessingToQuarantineはfile全体を隔離し、reasonを移動先file名へ保存します。
	MoveProcessingToQuarantine(
		ctx context.Context,
		processingPath string,
		reason pkgspool.QuarantineReason,
	) error

	ProcessingFileExists(ctx context.Context, processingPath string) (bool, error)

	// RemoveProcessingFileはprocessing fileを削除します。
	// 対象がすでに存在しない場合も、削除済みとして成功を返す必要があります。
	RemoveProcessingFile(ctx context.Context, processingPath string) error

	StoreProcessingRecordsToDone(
		ctx context.Context,
		processingPath string,
		records []pkgspool.SpoolRecord,
	) error

	StoreProcessingRecordsToDead(
		ctx context.Context,
		processingPath string,
		records []pkgspool.SpoolRecord,
	) error

	StoreProcessingRecordsToQuarantine(
		ctx context.Context,
		processingPath string,
		records []pkgspool.SpoolRecord,
	) error
}

// ActiveSpoolWriter は、retry などで再処理が必要な record を active spool に保存する書き込み口です。
// dead / quarantine / doneへの遷移はWorkFileStoreが担当します。
type ActiveSpoolWriter interface {
	// StoreActive は、再処理が必要な records を active spool に保存します。
	StoreActive(ctx context.Context, records []pkgspool.SpoolRecord) error
}

// ProcessingTerminalReconcilerは、再起動後に保存済みのterminal decisionを回復します。
// filesystem以外のstoreでは実装は任意です。errorなしで空のstateを返した場合は証拠が
// 見つからなかったこと、errorなしで空でないstateを返した場合はcleanupが完了したことを
// 意味します。errorはどれも送信を止め、曖昧なdataや一部だけ分割されたdataを保持しなければなりません。
type ProcessingTerminalReconciler interface {
	ReconcileProcessingTerminal(ctx context.Context, processingPath string, records []pkgspool.SpoolRecord) (pkgspool.State, error)
}
