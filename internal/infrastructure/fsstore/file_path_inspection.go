package fsstore

import (
	"fmt"
	"os"
)

// FileInspectionは、指定されたパス一覧の種類・サイズ・更新日時と検査エラーをまとめた結果です。
// 同じlifecycle処理の候補選別で共有します。後続の判断に使うファイルの名前や情報が
// 変わった場合は、古い検査結果のまま処理を続けないでください。
// 実際にファイルを開く際の安全確認は、この検査とは別に行います。
type FileInspection struct {
	// Pathsは元の名前一覧です。リンクや情報取得に失敗した名前も残し、世代判定や
	// checkpointの存在確認で、それらを未作成・消失と誤認しないようにします。
	Paths []string
	// RegularFilesはLstatで通常ファイルと確認できたものだけを、入力順で保持します。
	RegularFiles []FileInfo
	// PathErrorsは非通常ファイルの拒否理由とLstatの失敗です。キーは入力のパスそのものです。
	// 非通常ファイルの拒否はErrNotRegularFileをwrapします。
	// 通知や処理中止の判断は呼び出し側が行います。
	PathErrors map[string]error
}

// InspectFilePathsは、各パスをLstatし、通常ファイルの情報とパスごとの検査エラーを返します。
// リンク先はたどりません。個別の検査失敗でもパスをPathsから除去せず、PathErrorsへ記録します。
func InspectFilePaths(paths []string) FileInspection {
	inspection := FileInspection{Paths: append([]string(nil), paths...)}
	for _, path := range paths {
		info, err := os.Lstat(path)
		if err == nil && info.Mode().IsRegular() {
			inspection.RegularFiles = append(inspection.RegularFiles, FileInfo{
				Path: path, Size: uint64(info.Size()), ModTime: info.ModTime(),
			})
			continue
		}
		if err == nil {
			err = newNotRegularFileError(path, info.Mode(), fmt.Sprintf("path %q is not a regular file", path))
		}
		if inspection.PathErrors == nil {
			inspection.PathErrors = make(map[string]error)
		}
		inspection.PathErrors[path] = err
	}
	return inspection
}
