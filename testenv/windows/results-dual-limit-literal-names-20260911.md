# Windowsの二つのメモリ上限と、ログ名のliteral照合（2026-09-11）

## 修正範囲

### Job全体・process単体の上限

Windowsで両上限がある場合、従来のJob全体の観測に加え、process単体の上限・現在の
private commit量を`MemoryPressure.ProcessLimit`へ保持します。使用率と残り容量を
それぞれの上限で判定し、どれかが閾値に達すれば、その観測を過負荷候補とします。
最大使用率だけを選んだり、別々の上限の使用量・残量を一つの組に混ぜたりしません。

- 片方の取得失敗によって、もう片方の有効な過負荷情報を消しません。
- 両方が危険でも、連続回数は一観測につき一回だけ加算します。
- 使用量が低下すれば判定を解除します。過去最大値は使いません。
- process上限しかない場合や、上限がなくhostを観測する場合の既存fieldは維持します。
- Linux/macOSの取得処理は変更していません。追加制約がない観測の判定は従来どおりです。
- 共通monitorと`pkg/memorypressure`のpolicyは同じ閾値判定を使用します。

今回対象は、現在所属するJobから取得できるJob/processの二つの上限です。
祖先Jobの列挙は追加していません。既存の単一値metricsは引き続きprimaryの観測を表し、
追加process値はsnapshotに保持します。設定値・`.env`の変更はありません。

### ファイル名の`[]`を検索記号にしない

directory、prefix、target、extension、既存圧縮peer名をglob式へ埋め込まず、
`ReadDir`で取得したbasenameを文字列として比較します。

- `app[1]`を`app1`と取り違えず、未閉じの`[`も通常の名前として扱います。
- Linuxでは有効な`*`、`?`、backslashを含む名前にも対応します。
- Windowsの大小文字照合と、返却するpathの原表記は維持します。
- 意図して許容していた日付・番号部分、任意の一段圧縮suffixは維持します。
- 圧縮済み／source削除待ちの世代を再使用せず、近隣名を別系列として除外します。
- inline purgeの取得済み一覧filterも、同じliteral照合に変更しました。追加走査はしません。

Linuxも不存在以外のdirectory列挙エラーを呼び出し元へ返します。従来の`filepath.Glob`は
これを無視する場合がありました。`[`を疑似的な検索失敗に使っていたrunnerの試験は、
NULを含むdirectory名のopenエラーへ変更し、後段purgeの禁止と内容保持を確認します。
NULは試験入力だけです。製品への試験用関数注入やOS固有のfixture追加は行いません。

## Linux・ビルド検証

Go 1.25.0、`-mod=readonly`で実施しました。

- memorypressure／memory monitor／logpatternの全体：`-race -count=10 -cpu=1,2,4`成功。
- inlineの系列filter：対象3試験を`-race -count=10`で成功。
- runnerの走査失敗時の安全動作：`-race -count=10 -cpu=1,2,4`成功。
- ログ出力・appendlog／runner・failure archive・設定を含む関連一式の最終`-race`成功。
- Windows amd64／386のmemorypressure・monitor、amd64のlogpattern・logger・runnerで`go vet`成功。
- 継続試験用のbuild scriptにmemorypressure／monitorを追加し、18 packageの媒体生成に成功。
- Windows向けの全package test compileに成功（`-exec /bin/true -run '^$' ./...`、native実行ではない）。
- `git diff --check`成功。

試験途中には既存の圧縮・lazy role競合試験で短い完了期限の超過も発生しました。
runner圧縮競合は単独10回、最終logger全体と関連一式は成功しています。
この既存試験の期限設定や、圧縮・lockの製品処理は今回変更していません。

## Windows 11 VMでの検証

offlineの既存Windows 11 Enterprise Evaluation 25H2（`10.0.26200.6584`）を使用しました。
amd64はnative、386はWOW64で実行し、Windowsではrace detectorを使用していません。
最上位test数を集計し、子process／subtestを重複計上しません。

| 試験 | PASS | FAIL | 最上位SKIP |
| --- | ---: | ---: | ---: |
| 修正前Windows provider＋両上限回帰 | 0 | 1（期待した失敗） | 0 |
| 修正後memory amd64全体＋native回帰10回 | 28 | 0 | 0 |
| 修正後memory 386全体＋native回帰10回 | 28 | 0 | 0 |
| logpattern全体＋literal名回帰10回 | 57 | 0 | 0 |
| monitor全体＋追加process制約回帰10回 | 39 | 0 | 0 |

この媒体の修正後は152件成功、各終了codeと全体判定は0です。
修正前providerは、Jobの上限が取得できても追加process上限がゼロ値のままで、
両上限の回帰が期待どおり失敗しました。比較には保存した修正前`windows.go`のGo overlayを
使用し、作業treeは巻き戻していません。共通型とnative試験は修正後と同じです。

native回帰は、専用の子processへJob/process両上限を設定し、48 MiBをcommitして解放します。
256 MiB／1 GiBの上限配置を反転させ、片方だけが閾値に達する状態で、使用率・残量の
検出と解放後の解除を確認します。単独上限の既存回帰も維持しています。

一般権限でのlogpattern全体には、既存のdangling symlink作成試験のsubtest skipが1件あります。
今回追加したliteral名・メモリ判定の回帰にはskipはありません。
既存symlink試験は管理者権限でも別実行し、親1件とmissing／通常file／dangling symlinkの
全3 subtestが成功、skipなし、終了code 0を確認しました。

追加媒体では、inline系列filterの3試験を10回（30 PASS）、runner圧縮中のwriter進行を
3回（3 PASS）確認しました。runnerの走査失敗試験は、最初の通常fileをDirとする入力では
Windowsの`ReadDir`が空一覧を返し、安全な観測として扱われたため10回失敗しました。
これは狙った走査エラーを作れていないfixtureの問題です。NULを含むDirへ試験入力だけを
変更した後、別媒体・別結果directoryで10回成功、FAIL／SKIP 0、終了code 0を確認しました。
失敗時の結果も上書きせず保持しています。

最終コードに対する成功確認は最初の152件＋追加43件＋管理者補完1件で196件です。
一般権限の既存symlink subtestは上記管理者実行で補完しています。

## 媒体・結果保存先

最初の媒体：`/tmp/windows-dual-limit.cJ1whZ/dual-limit-tests.iso`

SHA-256：`dc16c031a5430cf4700a445fa9fa0f80a6f1b2a3c49a077302af198424f83edc`

VM内結果：`C:\Users\vmtest\dual-limit-normal-20260911-cJ1whZ`

管理者補完結果：`C:\Users\vmtest\dual-limit-symlink-admin-20260911-cJ1whZ`

追加媒体：`/tmp/windows-dual-limit.cJ1whZ/followup-tests.iso`

SHA-256：`dd2a19ab984cf152975ba730334a7cb67cc6640b52a39c5ce5451c36ef0b801b`

追加結果：`C:\Users\vmtest\dual-limit-followup-20260911-cJ1whZ`

runner再試験媒体：`/tmp/windows-dual-limit.cJ1whZ/retry-tests.iso`

SHA-256：`00e175e023a6fd6cdd34d8b9a3dcbffb744b33afc89ee21c4c4ccd7e021dde78`

runner再試験結果：`C:\Users\vmtest\dual-limit-runner-retry-20260911-cJ1whZ`

集計画面：`/tmp/windows-dual-limit-control.mYVXiM/summary-complete.png`

媒体は明示したtest exeと実行・集計scriptだけです。作業tree、`.env`、資格情報は含めず、
host directory共有・外部networkも使用しません。生ログはVM内に保持し、hostへ回収せず、
件数・終了codeと今回の合成データによる失敗原因だけを画面で確認しています。

終了後はWindowsを正常shutdownし、launcher終了code 0とQMP／TPM socketの消失を
確認しました。仮想diskと各試験結果は保持しています。
