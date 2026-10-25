package worker

import "fmt"

// 1 件のファイル移動結果。
type MoveResult struct {
	// 移動元パス。
	FromPath string

	// 移動先パス。
	ToPath string
}

// 1 件のファイル読み込み結果。
type ReadResult struct {
	// 読み込んだパス。
	Path string

	// 読み込んだ内容。
	Body []byte
}

// パスに紐づく処理失敗。
type PathError struct {
	// 失敗した対象パス。
	// パスに紐づかない失敗の場合は空になる。
	Path string

	// 元のエラー。
	Err error
}

// パスがあれば「path: error」の形式で返す。
func (e PathError) Error() string {
	if e.Path == "" {
		return e.Err.Error()
	}
	return fmt.Sprintf("%s: %v", e.Path, e.Err)
}

// 元のエラーを返し、errors.Is・errors.Asで辿れるようにする。
func (e PathError) Unwrap() error {
	return e.Err
}

// 複数パスの読み込み結果。
type ReadPathResult struct {
	// 読み込みに成功した結果。
	Results []ReadResult

	// 読み込みに失敗した結果。
	Failed []PathError
}

// 失敗が 1 件以上あるかを返す。
func (r ReadPathResult) HasError() bool {
	return len(r.Failed) > 0
}

// 複数パスの移動結果。
type MovePathResult struct {
	// 移動に成功した結果。
	Results []MoveResult

	// 移動に失敗した結果。
	Failed []PathError
}

// 失敗が 1 件以上あるかを返す。
func (r MovePathResult) HasError() bool {
	return len(r.Failed) > 0
}
