package processing

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

// ErrEmptyCompressedPathは、圧縮処理が成功を返したのに成果物のパスがない場合です。
var ErrEmptyCompressedPath = errors.New("compression completed without a compressed file path")

// ErrObservedCompressedFileDisappearedは、走査時に観測した既存の圧縮ファイルが検証の時点で
// なくなっていたことを表します。元ログは変更せず、次の走査で状態を判定し直します。
var ErrObservedCompressedFileDisappeared = errors.New("observed compressed file disappeared")

// CompressionSourcePreservedErrorは、完全でも内容の異なる圧縮ファイルと共存した元ログを
// 削除せず.preservedへ退避したことを報告するerrorです。PreservedPathが退避先で、
// Errは内容の不一致を表す検証error（fsstore.ErrCompressedContentMismatchを含む）です。
type CompressionSourcePreservedError struct {
	SourcePath     string
	CompressedPath string
	PreservedPath  string
	Err            error
}

func (e *CompressionSourcePreservedError) Error() string {
	return fmt.Sprintf("%v: source %q preserved at %q", e.Err, e.SourcePath, e.PreservedPath)
}

func (e *CompressionSourcePreservedError) Unwrap() error { return e.Err }

// CompressionRequestは、保護済みの元ログ一件と、観測から決めた圧縮先です。
// CompressedPathは既存ファイルまたは新規作成予定のパスです。空なら新規圧縮へ進みます。
// ExistingFileObservedがtrueなら、観測した圧縮ファイルの消失時は次の走査まで保留します。
type CompressionRequest struct {
	SourcePath           string
	CompressedPath       string
	ExistingFileObserved bool
	// RemoveCorruptPeerは、破損を確認した圧縮ファイルを削除する実行経路で指定します。
	// falseなら検証エラーを返して両ファイルを残し、通常走査などの後続処理へ委ねます。
	RemoveCorruptPeer bool
	// PreserveMismatchedSourceは、完全でも内容が元ログと一致しない圧縮ファイルを確認した
	// 実行経路で、元ログを.preservedへ退避する場合に指定します。退避後は圧縮ファイルだけが
	// 残り、次の走査からその世代の圧縮済みファイルとして扱われます。
	// falseなら検証エラーを返して両ファイルを残し、通常走査などの後続処理へ委ねます。
	PreserveMismatchedSource bool
}

// CompressionOperationsは圧縮・検証に使う保存実装です。VerifyExistingは、圧縮先が
// 存在しなければfalseを返し、存在すれば完全性と元ログとの内容一致を確認してからtrueを返します。
// 完全でも内容が一致しなければfsstore.ErrCompressedContentMismatchを返します。
// Compressは圧縮ファイルの作成と元ログの削除待ちへの移行・削除まで行います。
// PreserveSourceは内容の一致しない圧縮ファイルと共存する元ログを退避し、保全先を返します。
// RemoveSource・RemoveInvalid・PreserveSourceを省略すると、共通のfilesystem操作を使います。
// 呼び出し元は検証済みcacheや計測など、各実行経路で必要な処理をここへ接続します。
type CompressionOperations struct {
	VerifyExisting func(sourcePath, compressedPath string) (bool, error)
	Compress       func(context.Context, string) (string, error)
	RemoveSource   func(sourcePath, compressedPath string) error
	RemoveInvalid  func(sourcePath, compressedPath string) error
	PreserveSource func(sourcePath string) (string, error)
}

// CompressionFailureStageは、通知や再開先を決めるための失敗段階です。
// 既存ファイルの検査・新規圧縮・既存ファイル再利用後の元ログ削除を区別します。
type CompressionFailureStage uint8

const (
	CompressionFailureNone CompressionFailureStage = iota
	CompressionFailureExistingFile
	CompressionFailureNewFile
	CompressionFailureSourceRemoval
)

// CompressionResultは、圧縮ファイルの準備状況と、対象ファイルの状態が変化した可能性を表します。
type CompressionResult struct {
	// CompressedPathは、新規作成した、または再利用する圧縮ファイルのパスです。
	// 空文字は、この結果では対象パスを返せないことを表します。
	// パスが入っていても送信可能とは限らないため、ReadyForUploadで判断してください。
	CompressedPath string
	// ReadyForUploadは、圧縮ファイルの作成・検証と、元ログの削除待ち名への変更・同期が
	// 完了し、後続の送信へ進めることを表します。削除待ちファイルの削除や、その削除結果の
	// 同期だけが失敗した場合もtrueとなり、エラーが返っていても送信を継続できます。
	ReadyForUpload bool
	// ExistingFileVerifiedは、既存の圧縮ファイルの存在確認・内容検証に成功したことを表します。
	// 元ログの削除前にtrueとなり、削除に失敗してもその値を保持します。
	ExistingFileVerified bool
	// FilesMayHaveChangedは、対象ファイルの作成・名前変更・削除によって、状態が変わった可能性を表します。
	// 実際の変更完了は保証せず、元ログの削除失敗時もtrueになり得ます。
	// 呼び出し元は、保持しているファイル情報の再確認や、ディレクトリ使用量の概算の無効化を判断します。
	FilesMayHaveChanged bool
	// FailureStageは、既存ファイルの検査、新規圧縮、再利用後の元ログ削除のどこで失敗したかを表します。
	// 失敗がなければCompressionFailureNoneです。具体的なエラーは、併せて返されるerrorで確認します。
	FailureStage CompressionFailureStage
	// PreservedSourcePathは、内容の一致しない圧縮ファイルと共存した元ログを退避した保全先です。
	// 空文字なら退避していません。退避後のdirectory同期だけが失敗した場合も設定します。
	// 退避した場合もerrorを返し、FailureStageはCompressionFailureExistingFileです。
	PreservedSourcePath string
}

// ProcessCompressionは一件の元ログについて、既存圧縮ファイルの検証・再利用、または
// 新規圧縮を行います。既存圧縮ファイルは完全性に加えて元ログとの内容一致も確認し、
// 一致した場合だけ元ログを削除します。RemoveCorruptPeerを指定した経路では、破損を
// 確認した既存ファイルを元ログを残して削除し、同じ呼び出しでは再圧縮しません。
// PreserveMismatchedSourceを指定した経路では、内容の一致しない既存ファイルを残して
// 元ログを.preservedへ退避し、退避先を含むerrorで報告します。元ログは削除しません。
// 未知のcodecやI/O障害では、元ログと圧縮ファイルを両方残します。
// 呼び出し元は対象パスの保護とdirectoryのlifecycle lockを取得してから呼び、結果に
// 応じた再試行登録・報告・送信を行ってください。この関数は走査や通知を行いません。
func ProcessCompression(ctx context.Context, request CompressionRequest, operations CompressionOperations) (CompressionResult, error) {
	if request.CompressedPath != "" {
		exists, err := operations.VerifyExisting(request.SourcePath, request.CompressedPath)
		if err == nil && !exists && request.ExistingFileObserved {
			err = fmt.Errorf("%w: %s", ErrObservedCompressedFileDisappeared, request.CompressedPath)
		}
		if err != nil {
			result := CompressionResult{FailureStage: CompressionFailureExistingFile}
			switch {
			case request.RemoveCorruptPeer && errors.Is(err, pkgfsstore.ErrCorruptCompressedFile):
				removeInvalid := operations.RemoveInvalid
				if removeInvalid == nil {
					removeInvalid = appendlog.RemoveInvalidCompressedFile
				}
				removeErr := removeInvalid(request.SourcePath, request.CompressedPath)
				result.FilesMayHaveChanged = removeErr == nil
				err = errors.Join(err, removeErr)
			case request.PreserveMismatchedSource && errors.Is(err, fsstore.ErrCompressedContentMismatch):
				preserveSource := operations.PreserveSource
				if preserveSource == nil {
					preserveSource = fsstore.PreserveCompressionSourceWithMismatchedPeer
				}
				preservedPath, preservationErr := preserveSource(request.SourcePath)
				result.PreservedSourcePath = preservedPath
				result.FilesMayHaveChanged = preservedPath != ""
				// 退避できても内容の不一致は運用者へ通知します。退避先を含め、
				// 元ログの内容が元の場所と.preservedのどちらに残るかを明示します。
				if preservedPath != "" {
					err = &CompressionSourcePreservedError{
						SourcePath: request.SourcePath, CompressedPath: request.CompressedPath,
						PreservedPath: preservedPath, Err: err,
					}
				}
				err = errors.Join(err, preservationErr)
			}
			return result, err
		}
		if exists {
			removeSource := operations.RemoveSource
			if removeSource == nil {
				removeSource = appendlog.RemoveSourceAfterCompression
			}
			result := CompressionResult{
				CompressedPath: request.CompressedPath, ExistingFileVerified: true, FilesMayHaveChanged: true,
			}
			err := removeSource(request.SourcePath, request.CompressedPath)
			result.ReadyForUpload = err == nil || errors.Is(err, appendlog.ErrCompressionSourceCleanup)
			if err != nil {
				result.FailureStage = CompressionFailureSourceRemoval
			}
			return result, err
		}
	}

	compressedPath, err := operations.Compress(ctx, request.SourcePath)
	result := CompressionResult{
		CompressedPath: compressedPath, FilesMayHaveChanged: compressedPath != "",
	}
	if err == nil && compressedPath == "" {
		err = ErrEmptyCompressedPath
	}
	result.ReadyForUpload = compressedPath != "" && (err == nil || errors.Is(err, appendlog.ErrCompressionSourceCleanup))
	if err != nil {
		result.FailureStage = CompressionFailureNewFile
	}
	return result, err
}

// VerifyCompressionPeerは、既存圧縮ファイルが通常ファイルであることを再確認し、
// そのファイルの拡張子に合ったcodecで検証するverifyを呼びます。verifyには、組み込み形式なら
// 最後まで展開しながら元ログとの内容一致も確認する関数
// （fsstore.VerifyExistingCompressedFileMatchesSourceを使う関数など）を渡します。
// 完全性だけの検証では、別の内容を持つ完全なファイルを根拠に元ログを削除してしまいます。
// 見つからなければfalseを返し、元ログの変更は行いません。
func VerifyCompressionPeer(sourcePath, compressedPath string, verify func(sourcePath, compressedPath string) error) (bool, error) {
	info, err := appendlog.InspectCompressedFile(sourcePath, compressedPath)
	if err != nil || info == nil {
		return false, err
	}
	if err := verify(sourcePath, compressedPath); err != nil {
		return false, err
	}
	return true, nil
}
