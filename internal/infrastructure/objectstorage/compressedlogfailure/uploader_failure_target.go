package compressedlogfailure

import (
	"context"
	"fmt"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// failureTargetは一つの宛先に設定された退避先です。保存と送信前の再利用確認は
// 同じ一覧を使い、どちらか片方だけに退避先を追加することを防ぎます。
type failureTarget interface {
	kind() failurestore.DurableType
	root() string
	name() string
	lock(context.Context) (func(), error)
	save(context.Context, string, uploader.FailureCategory) (failureTargetFile, error)
	reuse(context.Context, string) (failureTargetFile, bool, error)
	// preserveは保存結果の矛盾に備え、元データをroot直下の.preservedへコピー・同期します。
	// 元データと通常の退避ファイルは移動せず、保全コピーはpurge・再送の対象外です。
	preserve(context.Context, string) (string, error)
	savedFileDetails(failureTargetFile) (SavedTargetDetails, error)
	close() error
	requestForceClose()
}

type failureTargetFile struct {
	saved     checkpoint.SavedFailureTarget
	path      string
	conflicts []infrafsstore.ConflictRename
}

func validateFailureTargetFile(target failureTarget, file failureTargetFile) error {
	if file.saved == nil {
		return fmt.Errorf("%s returned no failure target", target.name())
	}
	if file.saved.Kind() != target.kind() || file.saved.Root() != target.root() {
		return fmt.Errorf("%s returned a different failure target: type %q, root %q", target.name(), file.saved.Kind(), file.saved.Root())
	}
	return nil
}

// retryAfterRecordingTargetは、直接uploadが一時的な失敗とともに返したRetry-Afterを、
// 宛先の待機期限として記録できる退避先です。
type retryAfterRecordingTarget interface {
	recordUploadRetryAfter(ctx context.Context, uploadErr error)
}

type spoolFailureTarget struct{ route *compressedlog.ManagedRoute }

// recordUploadRetryAfterは、宛先別spoolを再送するworkerと共有する宛先の待機期限へ記録し、
// markerへ保存します。Retry-Afterを含まないerrorでは何もしません。
func (t spoolFailureTarget) recordUploadRetryAfter(ctx context.Context, uploadErr error) {
	t.route.RecordUploadRetryAfter(ctx, uploadErr)
}

func (t spoolFailureTarget) kind() failurestore.DurableType { return failurestore.DurableTypeSpool }
func (t spoolFailureTarget) root() string                   { return pathidentity.ComparisonKey(t.route.Root().Path()) }
func (t spoolFailureTarget) name() string                   { return "compressed log file spool" }
func (t spoolFailureTarget) close() error {
	err := t.route.Close()
	// 強制終了の戻り値だけで、まだ通知中のrouteを解放済みと扱いません。
	<-t.route.ShutdownDone()
	return err
}
func (t spoolFailureTarget) requestForceClose() { _ = t.route.RequestForceClose() }
func (t spoolFailureTarget) lock(ctx context.Context) (func(), error) {
	return t.route.BlockPurge(ctx)
}

func (t spoolFailureTarget) file(file objectstoragespool.CompressedLogFile) failureTargetFile {
	return failureTargetFile{
		saved:     checkpoint.SavedSpoolTarget{Target: t.root(), State: file.State},
		path:      file.Path,
		conflicts: file.ConflictRenames,
	}
}

func (t spoolFailureTarget) save(ctx context.Context, sourcePath string, category uploader.FailureCategory) (failureTargetFile, error) {
	var file objectstoragespool.CompressedLogFile
	var err error
	if category == uploader.FailureCategoryPermanent {
		file, err = t.route.SavePermanentFailureWithPurgeLockHeld(ctx, sourcePath)
	} else {
		file, err = t.route.SaveRetryableFailureWithPurgeLockHeld(ctx, sourcePath)
	}
	return t.file(file), err
}

func (t spoolFailureTarget) reuse(ctx context.Context, sourcePath string) (failureTargetFile, bool, error) {
	file, found, err := t.route.ConfirmAndSyncSavedFileWhilePurgeBlocked(ctx, sourcePath)
	return t.file(file), found, err
}

func (t spoolFailureTarget) preserve(ctx context.Context, sourcePath string) (string, error) {
	return t.route.PreserveFile(ctx, sourcePath)
}

func (t spoolFailureTarget) savedFileDetails(file failureTargetFile) (SavedTargetDetails, error) {
	saved, ok := file.saved.(checkpoint.SavedSpoolTarget)
	if !ok {
		return nil, &SavedTargetTypeError{Kind: t.kind(), Name: t.name(), Saved: file.saved, Want: "SavedSpoolTarget"}
	}
	return SavedSpoolDetails{Target: saved.Root(), FilePath: file.path, State: saved.State}, nil
}

type archiveFailureTarget struct{ route archiveRoute }

func (t archiveFailureTarget) kind() failurestore.DurableType {
	return failurestore.DurableTypeFileArchive
}
func (t archiveFailureTarget) root() string {
	return pathidentity.ComparisonKey(t.route.store.Root().Path())
}
func (t archiveFailureTarget) name() string       { return "compressed log file upload failure archive" }
func (t archiveFailureTarget) close() error       { return t.route.owner.close() }
func (t archiveFailureTarget) requestForceClose() { t.route.owner.requestForceClose() }

func (t archiveFailureTarget) file(file compressedlogarchive.ArchivedFile) failureTargetFile {
	return failureTargetFile{
		saved:     checkpoint.SavedArchiveTarget{Target: t.root(), FailureCategory: file.FailureCategory},
		path:      file.Path,
		conflicts: file.ConflictRenames,
	}
}

func (t archiveFailureTarget) lock(ctx context.Context) (func(), error) {
	if t.route.owner == nil || t.route.owner.coordinator == nil {
		return nil, nil
	}
	return t.route.owner.coordinator.lockPurgeDuringSave(ctx)
}

func (t archiveFailureTarget) save(ctx context.Context, sourcePath string, category uploader.FailureCategory) (failureTargetFile, error) {
	file, err := t.route.store.Save(ctx, compressedlogarchive.SaveInput{
		SourcePath: sourcePath, FailureCategory: category,
	})
	return t.file(file), err
}

func (t archiveFailureTarget) reuse(ctx context.Context, sourcePath string) (failureTargetFile, bool, error) {
	file, found, err := t.route.store.ReuseExistingFile(ctx, sourcePath)
	return t.file(file), found, err
}

func (t archiveFailureTarget) preserve(ctx context.Context, sourcePath string) (string, error) {
	return t.route.store.PreserveFile(ctx, sourcePath)
}

func (t archiveFailureTarget) savedFileDetails(file failureTargetFile) (SavedTargetDetails, error) {
	saved, ok := file.saved.(checkpoint.SavedArchiveTarget)
	if !ok {
		return nil, &SavedTargetTypeError{Kind: t.kind(), Name: t.name(), Saved: file.saved, Want: "SavedArchiveTarget"}
	}
	return SavedArchiveDetails{Target: saved.Root(), FilePath: file.path, FailureCategory: saved.FailureCategory}, nil
}
