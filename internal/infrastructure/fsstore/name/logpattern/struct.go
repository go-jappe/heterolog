package logpattern

import "github.com/go-jappe/heterolog/pkg/path/pathidentity"

// 命名規則はlogと同じパターン
type FileNamePattern struct {
	Dir       string // ファイルディレクトリ
	Prefix    string // ファイル名の接頭辞
	Target    string // 本アプリケーション名・サービス名
	Date      string // 日付形式
	Extension string // 拡張子
	Index     int    // ファイル名の接尾辞の連番
	// NameCaseはDir直下のファイル名を照合するときの大小文字の扱いです。
	// 未指定ならpathidentity.DirectoryNameCaseでDirのfilesystemから判定します。
	// 試験や判定済みの規則を再利用する場合だけ指定し、生成するファイル名は変更しません。
	NameCase pathidentity.NameCase
}

// EffectiveNameCaseは、このpatternのファイル名照合に使う大小文字の規則を返します。
// NameCaseが指定されていればその値を、未指定ならDirの判定結果を返します。
// 空のDirは、ファイル一覧の取得と同じく現在のディレクトリとして判定します。
func (p FileNamePattern) EffectiveNameCase() pathidentity.NameCase {
	return p.NameCase.ForDirectory(p.Dir)
}
