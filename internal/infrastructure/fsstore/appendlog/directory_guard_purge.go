package appendlog

import (
	"maps"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// RunPurgeWithProtectionは、Guardの保護一覧とprepareProtectionが返す追加の保護対象を
// Purgeに設定してから、そのPurgeを引数にして削除処理runPurgeを実行します。
//
// prepareProtectionは、開始時のGuardの保護一覧のコピーを受け取り、upload待ちなどの
// 追加の削除禁止パスを返します。返したパスはこの関数がPurgeへ追加するため、runPurgeでの
// 登録処理は不要です。受け取ったmapを変更しても、Guard由来の保護は解除されません。
// prepareProtectionは必ず指定し、追加の保護対象がない場合もnilを返す関数を渡してください。
//
// runPurgeには、実行したい削除処理を書きます。
// 容量上限による削除ならPurgeByDirectorySize、保持期限による削除ならPurgeByAge、
// 世代数による削除ならPurgeGenerationSeriesを、引数のPurgeから呼び出します。
// 保護中のファイルの除外は、Purgeの削除メソッド内で行います。
// runPurge側に除外処理を任せないことで、除外漏れによる誤削除を防ぎます。
//
// filesには保護中のファイルも含め、ディレクトリ容量にはそれらも計上します。
// prepareProtection、runPurgeの順に同期実行し、終了後に一時保護を解除します。
//
// 削除候補は実行中だけ「今回のpurgeが処理中」と登録します。終了時にはpanicの場合も、
// 今回の登録だけを解除します。
//
// states lockは一覧の取得・登録中だけ保持し、両コールバックの実行中は解放します。
// 呼び出し側は別途lifecycle lockを保持し、削除候補が書き込み先に選ばれないように
// してください。「処理中」の登録だけでは、writerによる再openは防げません。
func (g *DirectoryGuard) RunPurgeWithProtection(
	files []fsstore.FileInfo,
	prepareProtection func(protectedPaths map[string]struct{}) map[string]struct{},
	runPurge func(*Purge),
) {
	owner := NewDirectoryGuardToken()
	retainedPaths := make([]string, 0, len(files))
	purge := &Purge{
		files:    append([]fsstore.FileInfo(nil), files...),
		observed: make(map[string]fsstore.FileInfo, len(files)),
		removed:  make(map[string]struct{}),
	}
	defer func() {
		g.UnprotectPaths(owner, retainedPaths...)
	}()

	func() {
		g.statesMu.Lock()
		defer g.statesMu.Unlock()
		// 先に「削除禁止一覧」を取得します。今回の登録後に取得すると、
		// 自分の削除候補まで削除禁止になってしまうためです。
		purge.protectedPaths, purge.activePaths = g.protectedAndActivePathsLocked()
		// filesのうち削除禁止でないパスを、今回のpurgeが処理中として登録します。
		for _, file := range files {
			path := pathidentity.AbsoluteOrCleanPath(file.Path)
			if path == "" {
				continue
			}
			key := pathidentity.ComparisonKey(path)
			if _, exists := purge.observed[key]; exists {
				continue
			}
			purge.observed[key] = file
			if pathidentity.Contains(purge.protectedPaths, path) {
				continue
			}
			g.stateForTokenLocked(owner).retain(path)
			retainedPaths = append(retainedPaths, path)
		}
	}()

	// 元の保護一覧を渡すと、準備処理がmapを変更して保護を解除できてしまうためコピーします。
	// 返されたmapも保持せず、追加分だけを取り込み、削除開始前に保護一覧を確定します。
	for path := range prepareProtection(maps.Clone(purge.protectedPaths)) {
		if normalized := pathidentity.AbsoluteOrCleanPath(path); normalized != "" {
			purge.protectedPaths[normalized] = struct{}{}
		}
	}
	runPurge(purge)
}

// PurgeはRunPurgeWithProtection内で観測済みのfileだけを削除します。保護一覧や観測情報は私有し、
// 呼び出し側の変更で削除禁止を解除できません。
// コールバック内でのみ同期的に使用し、終了後の再利用や別goroutineへの受け渡しはしないでください。
// ゼロ値も削除できるfileを持ちません。コピーせず、渡されたポインタを使用してください。
// 保持期限はファイル名の日付、削除順は日付・数値indexを使用します。
type Purge struct {
	files          []fsstore.FileInfo
	observed       map[string]fsstore.FileInfo
	removed        map[string]struct{}
	protectedPaths map[string]struct{}
	activePaths    map[string]struct{}
}

// ProtectedPathsはGuard由来の削除禁止パスとprepareProtectionによる追加分のコピーを返します。
// 今回のpurge自身による「処理中」の登録は含めません。
// 返したmapを変更しても削除禁止一覧は変わりません。
func (p *Purge) ProtectedPaths() map[string]struct{} {
	return maps.Clone(p.protectedPaths)
}

// ActivePathsは開始時の書き込み先のコピーを返します。容量などの診断用です。
func (p *Purge) ActivePaths() map[string]struct{} {
	return maps.Clone(p.activePaths)
}

// DeletionCandidatesは、filesから保護中・開始時に未登録・このPurgeで削除済みの
// ファイルを除いた一覧を返します。重複したパスは一件にまとめ、開始時の観測情報を使います。
// 保持期限・世代数・容量の判定や、ファイル削除は行いません。
// 期限・世代数による削除の内部処理や件数判定・診断に使います。呼び出し側が削除前に呼ぶ必要はありません。
// 容量による削除では保護中のファイルも容量に数えるため、この関数では絞り込まず、
// PurgeByDirectorySizeから削除関数へ除外一覧を渡します。
func (p *Purge) DeletionCandidates(files []fsstore.FileInfo) []fsstore.FileInfo {
	candidates := make([]fsstore.FileInfo, 0, len(files))
	seen := make(map[string]struct{}, len(files))
	for _, file := range files {
		key := pathidentity.ComparisonKey(file.Path)
		observed, exists := p.observed[key]
		if !exists || pathidentity.Contains(p.protectedPaths, observed.Path) {
			continue
		}
		if _, removed := p.removed[key]; removed {
			continue
		}
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		candidates = append(candidates, observed)
	}
	return candidates
}

// PurgeByAgeは、nowの日付で未実行の場合だけ、ファイル名の日付で保持期限を判定して削除します。
// Guardの保護対象とprepareProtectionの追加分、開始時に未登録のファイルは内部で除外します。
// 呼び出し側で保護対象を除外する必要はありません。
// Remainingには、保護中・未登録・削除に失敗したファイルや、期限内のファイルも入力順で残します。
func (p *Purge) PurgeByAge(files []fsstore.FileInfo, maxAgeDays int, now, lastPurgeAt time.Time) fsstore.PurgeResult {
	result := fsstore.PurgeByFileNameDateIfNeeded(p.DeletionCandidates(files), maxAgeDays, now, lastPurgeAt)
	return p.finish(files, result)
}

// PurgeGenerationSeriesは、ファイル名の日付・番号で決まる世代がmaxBackupsを超えた場合に、
// 古い超過世代の未圧縮・圧縮ファイルをまとめて削除します。圧縮形式ごとには数えません。
// Guardの保護対象とprepareProtectionの追加分、開始時に未登録のファイルは、内部で削除候補から除外します。
// これらのファイルは保持件数にも数えず、同じ世代の他のファイルを削除しても残します。
// 呼び出し側での除外処理は不要です。Remainingには、保護中・未登録のファイルも入力順で残します。
func (p *Purge) PurgeGenerationSeries(files []fsstore.FileInfo, baseExtension string, maxBackups int) fsstore.PurgeResult {
	result := logpattern.PurgeGenerationsByFileNameIfNeeded(p.DeletionCandidates(files), baseExtension, maxBackups)
	return p.finish(files, result)
}

// PurgeByDirectorySizeは、合計容量がmaxBytesを超えていれば、ファイル名の日付・番号が古い順に削除します。
// 開始時にGuardが保護していたファイルと、prepareProtectionが追加したファイルは、
// このメソッドの内部で削除候補から除外します。呼び出し側での除外処理は不要です。
// 容量は開始時の全ファイルから、このPurgeで削除済みの分を除いて計算します。
// 保護中のファイルも容量に含めます。
func (p *Purge) PurgeByDirectorySize(maxBytes uint64) fsstore.PurgeResult {
	allFiles := p.remainingFiles(p.files)
	excluded := make(map[string]struct{}, len(p.protectedPaths))
	maps.Copy(excluded, p.protectedPaths)
	// 一つのpathを複数回渡されても、同じfileを繰り返し削除・容量計上しません。
	uniqueFiles := make([]fsstore.FileInfo, 0, len(allFiles))
	seen := make(map[string]struct{}, len(allFiles))
	for _, file := range allFiles {
		key := pathidentity.ComparisonKey(file.Path)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		uniqueFiles = append(uniqueFiles, file)
		if _, observed := p.observed[key]; !observed {
			excluded[file.Path] = struct{}{}
		}
	}
	result := fsstore.PurgeByDirectorySizeFromFileNameIfNeeded(uniqueFiles, maxBytes, excluded)
	return p.finish(p.files, result)
}

func (p *Purge) remainingFiles(files []fsstore.FileInfo) []fsstore.FileInfo {
	remaining := make([]fsstore.FileInfo, 0, len(files))
	for _, file := range files {
		key := pathidentity.ComparisonKey(file.Path)
		if _, removed := p.removed[key]; removed {
			continue
		}
		if observed, exists := p.observed[key]; exists {
			file = observed
		}
		remaining = append(remaining, file)
	}
	return remaining
}

func (p *Purge) finish(files []fsstore.FileInfo, result fsstore.PurgeResult) fsstore.PurgeResult {
	for _, removal := range result.RemovalResults {
		if removal.Err == nil {
			p.removed[pathidentity.ComparisonKey(removal.File.Path)] = struct{}{}
		}
	}
	result.Remaining = p.remainingFiles(files)
	return result
}
