# OS共通のログファイル名colon検証（2026-09-12）

この記録はcolon修正時点の結果です。ここに残した全体試験の51件は、その後に原因を
修正・検証しました。最新の結果と権限条件は
[Windows全体試験の51件の失敗解消](results-test-portability-20260912.md)を参照してください。

## 修正範囲

ログファイル名を構成する設定値の`:`を、Linuxを含む全OSで拒否します。
Windowsでは`file:stream`が通常fileではなくNTFSの別streamとして扱われ、
書込成功でも通常のfile一覧から圧縮・upload対象を発見できなくなるためです。
事前のWindows VM診断では、実際のFileLoggerの生成・書込・Closeに成功しても
`GlobFileNames`の結果が0件になることを確認しました。

- 判定は`pkg/fsstore.ValidateLogFileNameValue`に一元化しました。
- Target、使用中のerror/debug prefix、file名format、file日付formatが対象です。
- runnerへ明示するfile patternのTarget／Prefix／Extensionも検証します。
- 通常・async・Delivery・internal logのmappingと、Logger／runner生成入口で検証します。
- file archiveのTarget等にも同じ制約を適用します。
- 不正設定はfile作成・directory使用権取得より前に拒否します。直接構築する
  FileSeriesManagerは既存APIどおり、lifecycle構成・実行時の検証からerrorを返します。
- 未使用backendや、分離が無効なerror/debug prefixは対象にしません。
- 保存先pathの`C:\logs`などのドライブ指定は対象外です。
- ログ本文の時刻表示formatと、literalな`[]`を含む名前は引き続き使用できます。
- 値の書換え、既存fileの移動・削除、`.env`、保存形式の変更はありません。

今回追加したのはcolonの禁止です。その他の文字を一律に禁止する変更はしていません。

## 回帰試験

共通validator、appendlog layout、file archive layout、Logger constructor、runner、
mapping、file archive pipelineへ恒久的な回帰試験を追加しました。

Logger constructorでは、各role・対応lifecycle modeでの拒否、file未作成、directoryの
使用権取得より前の検証を確認します。未使用prefixを残した設定では実際の書込も確認します。
mappingではsync／async／Delivery、通常出力／handler用／internal overrideと、
選択中・未選択のfailure archiveを区別します。

temp envから実Map入口までの試験には、internal Target=`app:stream`とLogDir=`..`を
組み合わせる再現条件も含めました。作業treeの`.env`は読み替え・変更していません。

Windows実行では既存の共通test helperが`os.Clearenv()`で`TEMP`等を消してから
`t.TempDir()`を呼び、一般ユーザーでは書き込めない`C:\Windows`への作成に失敗しました。
`loadConfigFromTempEnv`／`loadConfigErrorFromTempEnv`の2箇所は、一時directoryを
環境変数の消去前に確保する順序へ修正しました。製品の環境変数処理は変更していません。

既存のconstructor呼出しへ追加した検証だけをGo overlayで外すと、新回帰が失敗する
ことも確認しました。作業treeは巻き戻していません。

## Linux・ビルド検証

Go 1.25.0、`-mod=readonly`で実施しました。

- `pkg/fsstore`、`internal/config/...`、`internal/infrastructure/fsstore/...`、
  FileLogger、FsArchiveHandlerの関連全体で`-race`成功。
- 新しい共通・mapping・file archive回帰の10回反復に成功。
- FileLogger／runnerは新回帰を`-race -count=10 -cpu=1,2,4`でも確認しました。
- Windows amd64向けの関連`go vet`と、対象8 packageのtest buildに成功。
- 独立レビューと`git diff --check`で問題は見つかりませんでした。

## Windows 11 VM検証

Windows 11 Enterprise Evaluation 25H2、一般ユーザー、ローカルNTFSで実行しました。
外部network・host directory共有は使用せず、専用read-only ISOへ対象8 packageの
test exeと実行・集計scriptのみを格納しました。認証情報や`.env`は持ち込んでいません。
生ログはVM内に保持し、件数と必要なassertionだけを画面で確認しています。
終了後はWindowsから正常shutdownし、launcherの終了code 0とQMP／TPM socketの
消滅を確認しました。VMは停止しています。

### 追加回帰

追加19件の親testを各10回、合計190回実行し、すべて成功しました。
追加回帰の失敗・親testのskip・subtestのskipはいずれも0件、各実行の終了codeも0です。
configだけは上記のfixture修正後にtest exeを作り直して再実行しました。
その他7 packageの製品・試験codeとexeは初回成功時から変更していません。

| package | 追加の親test数 | 10回反復の成功数 |
| --- | ---: | ---: |
| pkg/fsstore | 1 | 10 |
| appendlog | 3 | 30 |
| failurearchive | 2 | 20 |
| FileLogger | 3 | 30 |
| appendlog runner | 5 | 50 |
| config/validation/mapped | 3 | 30 |
| config（fixture修正版） | 1 | 10 |
| FsArchiveHandler | 1 | 10 |

### 既存全体試験の未通過項目

今回の追加回帰とは別に、既存全体試験のFileLogger／FsArchiveHandlerでも失敗を
確認しました。全体試験がすべて成功したという結果ではありません。

親testの結果は次のとおりです。configはfixture修正版の結果です。

| package | 成功 | 失敗 | skip |
| --- | ---: | ---: | ---: |
| pkg/fsstore | 10 | 0 | 0 |
| appendlog | 81 | 0 | 1 |
| failurearchive | 25 | 0 | 0 |
| FileLogger | 250 | 9 | 5 |
| appendlog runner | 204 | 0 | 1 |
| config/validation/mapped | 49 | 0 | 0 |
| config | 265 | 33 | 0 |
| FsArchiveHandler | 161 | 9 | 0 |

FileLoggerの5件のskipは、別の出力に続く行中にtest結果が出ていたため初回集計では
見落としていました。行中の結果も再集計し、250成功＋9失敗＋5skip＝一覧264件と
一致しました。未完了のtestが5件あるという意味ではありません。
appendlogでは上記の親testのskip以外に、2件のsubtestのskipがあります。
合計は1045成功・51失敗・7親test skipで、nativeのtest一覧1103件と一致します。
親testのskipは、Unix向けsource identity 1件、FileLoggerの実`.env`／varlog／
Tempo／Collector等に依存する5件、runnerのsymlink試験1件です。
appendlogの2 subtestのskipも、source／pending symlinkに関するものです。

- `TestFileLoggerPendingRetryWaitsForRawSourceRetirement`：試験でappend用に開いた
  raw file handleを`Truncate`する箇所が`Access is denied`。colon検証より後の
  別の回復処理を準備するfixtureです。
- `TestInlineFailureArchivePendingSourceConflictReportsAndPreservesQuarantine`：
  errorの`%q`表記による`C:\\Users\\...`と、生pathの`C:\Users\...`を
  `Contains`で比較する既存assertionが一致しません。

これらの既存試験の変更、その他の失敗の包括的な修正は今回の範囲に含めていません。
この結果をWindows全体の不具合解消や、全既存試験の成功とは扱いません。
config全体にも33件の未通過試験が残っています。追加した実環境変数からのmapping
回帰は成功していますが、他の全失敗がfixtureだけの問題であるとは断定していません。

### 再現情報

- 初回ISO：`/tmp/windows-colon-validation.BmjuWe/colon-validation-tests.iso`
- SHA-256：`6a7a510fce8678f945b081f71aeda833562f5a958e965fbebfe3fd67857c2cce`
- 初回guest結果：`C:\Users\vmtest\colon-validation-normal-20260912-BmjuWe`
- 初回集計画面：`/tmp/windows-colon-validation-control.DgFijn/summary.png`
- FileLoggerのskipを含む再集計：`/tmp/windows-colon-validation-control.DgFijn/file-full-counts-corrected.png`
- FileLogger／FsArchiveHandlerの既存失敗名一覧：`/tmp/windows-colon-validation-control.DgFijn/existing-failure-names.png`
- 既存skip名一覧：`/tmp/windows-colon-validation-control.DgFijn/existing-skip-names.png`
- config fixture失敗の画面：`/tmp/windows-colon-validation-control.DgFijn/config-repeat-first-assertion.png`
- FileLoggerの上記assertion：`/tmp/windows-colon-validation-control.DgFijn/file-full-cause.png`
- FsArchiveHandlerの上記assertion：`/tmp/windows-colon-validation-control.DgFijn/fsarchivehandler-full-cause.png`
- config再実行ISO：`/tmp/windows-colon-config-fixture.tHL30n/config-fixture-tests.iso`
- 再実行ISOのSHA-256：`8167772c982a7169e2cd400fdbbd4d1524d53be43713661214a685db0cce361f`
- 修正版config.exeのSHA-256：`826022eae4830512ba91b344acf3285fdbc927de0720629dced7f7c8d9e759c0`
- 再実行guest結果：`C:\Users\vmtest\colon-config-retry-20260912-tHL30n`
- 再実行集計画面：`/tmp/windows-colon-validation-control.DgFijn/config-retry-summary-complete.png`
