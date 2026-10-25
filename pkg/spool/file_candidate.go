package spool

// FileCandidate は、状態ディレクトリの列挙で見つかった通常 spool ファイルです。
type FileCandidate struct {
	// Path は、読み込み・移動・削除に渡すローカルファイルシステムの操作用パスです。
	Path string

	// RelativePath は、状態ディレクトリからの相対パスです。
	// filepath の OS 固有表記を使い、年/月/日/時/分の各ディレクトリ名と
	// ファイル名を順に辞書順で比較する走査の再開境界です。
	// 操作用 Path やファイルの存在・同一性を表すものではありません。
	RelativePath string
}
