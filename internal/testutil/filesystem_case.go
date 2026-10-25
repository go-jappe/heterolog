package testutil

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/pathcaserule"
)

// UseDirectoryCaseRuleForTestは、root配下のdirectoryの大小文字の規則を差し替えます。
// caseSensitiveがtrueなら大小文字を区別し、falseなら区別しない規則にします。
// pathidentityのComparisonKey・DirectoryNameCaseなどがfilesystemを問い合わせずにこの規則を使うため、
// 両方の規則を、実行するOSに依存せず試験できます。rootのsymlinkを解決した実体pathにも
// 同じ規則を適用します。rootの名前自体は親の規則に従います。
//
// 登録時に作成済みのrootは実体でも照合します。大小文字を区別しないfilesystemでは、本番の比較keyが
// rootの祖先の名前を小文字へ揃えるため、keyを再び問い合わせると登録時と異なる表記になります。
// 未作成のrootは表記だけで照合するため、keyを再び比較する試験では作成済みのdirectoryを指定してください。
//
// 規則はprocess全体に適用されます。t.Parallelを使う試験やその親からは呼び出さず、
// 規則の差し替えが必要な試験でも、別名のdirectoryを実際には作成しないでください。
// 大小文字を区別しないfilesystemは、作成済みの別名を同じ実体へ解決するためです。
// 試験の終了時に元の判定へ戻します。pathidentityの試験からも使えるよう、
// このpackageはpathidentityをimportしません。
func UseDirectoryCaseRuleForTest(t testing.TB, root string, caseSensitive bool) {
	t.Helper()
	absolute, err := filepath.Abs(root)
	if err != nil {
		t.Fatal(err)
	}
	rule := directoryCaseRuleRootForTest{spellings: []string{filepath.Clean(absolute)}}
	if resolved := resolveExistingAncestorForTest(rule.spellings[0]); resolved != rule.spellings[0] {
		rule.spellings = append(rule.spellings, resolved)
	}
	if info, err := os.Stat(rule.spellings[0]); err == nil && info.IsDir() {
		rule.info = info
	}
	restore, err := pathcaserule.Set(func(directory string) (bool, bool) {
		if rule.contains(directory) {
			return caseSensitive, true
		}
		return false, false
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restore)
}

// directoryCaseRuleRootForTestは、規則を差し替えるrootの表記と、登録時に存在したrootの実体です。
// spellingsは絶対化した登録時の表記と、既存祖先のsymlinkを解決した表記です。
// infoは登録時にrootが存在しなかった場合はnilです。
type directoryCaseRuleRootForTest struct {
	spellings []string
	info      os.FileInfo
}

// containsは、directoryがroot自身またはその配下かを返します。
// 登録した表記で照合できなければ、directoryとその祖先のうち存在するものをrootの実体と比べます。
// 大小文字を揃えた表記やsymlinkを通した表記も同じ実体なら照合し、大小文字を区別するfilesystemで
// 大小文字だけ異なる別のdirectoryは照合しません。未作成の子は、最も近い既存祖先から判定します。
// 登録した表記の祖先はrootの親から上のdirectoryなので、そこで比較を終えます。
// ComparisonKeyがvolume rootから各階層を問い合わせても、rootの祖先ではfilesystemを読み取りません。
func (r directoryCaseRuleRootForTest) contains(directory string) bool {
	for _, spelling := range r.spellings {
		if directoryWithinForTest(spelling, directory) {
			return true
		}
	}
	if r.info == nil {
		return false
	}
	for current := filepath.Clean(directory); !r.spelledAncestor(current); current = filepath.Dir(current) {
		if info, err := os.Stat(current); err == nil && os.SameFile(r.info, info) {
			return true
		}
		if filepath.Dir(current) == current {
			break
		}
	}
	return false
}

// spelledAncestorは、directoryが登録したいずれかの表記の祖先かを返します。
func (r directoryCaseRuleRootForTest) spelledAncestor(directory string) bool {
	for _, spelling := range r.spellings {
		if directoryWithinForTest(directory, spelling) {
			return true
		}
	}
	return false
}

// resolveExistingAncestorForTestは、最も近い既存祖先のsymlinkを解決し、未作成の部分を結合します。
// pathidentity.ResolveDirectoryと同じく、OSが用意する一時directoryのsymlinkも実体pathへ揃えます。
func resolveExistingAncestorForTest(path string) string {
	var missing []string
	for current := path; ; current = filepath.Dir(current) {
		if resolved, err := filepath.EvalSymlinks(current); err == nil {
			for index := len(missing) - 1; index >= 0; index-- {
				resolved = filepath.Join(resolved, missing[index])
			}
			return filepath.Clean(resolved)
		}
		if filepath.Dir(current) == current {
			return path
		}
		missing = append(missing, filepath.Base(current))
	}
}

// directoryWithinForTestは、directoryがroot自身またはその配下かを返します。
func directoryWithinForTest(root, directory string) bool {
	relative, err := filepath.Rel(root, directory)
	if err != nil {
		return false
	}
	return relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)))
}
