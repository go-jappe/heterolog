# Windows全体試験の51件の失敗解消（2026-09-12）

## 対象

[colon検証時の全体試験](results-log-file-name-validation-20260912.md)で残した
config 33件、FileLogger 9件、FsArchiveHandler 9件をすべて対象にしました。
ここでの件数は親test数です。同じ親testの中で複数subtestが失敗していても1件です。

前回は1045成功・51失敗・7親test skipでした。「既存試験」とは前回のcolon修正で
新設したものではない試験を指し、修正前の製品との差分比較で無関係と確認済みという
意味ではありません。今回は全51件の最初の失敗をWindows上の実errorから分類しました。

## 原因と修正

| 原因 | 親test数 | 対応 |
| --- | ---: | --- |
| config試験のrepository root／設定fixtureがVM媒体にない | 26 | 公開exampleだけから安全なdummy設定とroot markerを生成 |
| 環境変数消去後のTempDir作成がC:\Windowsへ向かう | 4 | 消去前にtest用directoryを確保。同型の準備処理も修正 |
| configの正規化済みpathと元表記の比較 | 2 | 既存のpath正規化契約と同じ期待値で比較 |
| retry数値試験がOSのspool基準pathまで消していた | 1 | 数値検証とは独立したtest用保存先を明示 |
| append handleにTruncateしてrotationを準備 | 10 | 既存handleへの実追記でrotation閾値に到達させる |
| quoted／正規化済みpathと生pathの比較 | 3 | 同じ引用形式・正規化で比較。JSONは復号後の値を比較 |
| 一般ユーザーのsymlink作成権限不足 | 4 | 専用VMの既存の管理者権限でも別枠実行し、本物のsymlinkを使用 |
| Unix chmodによる削除拒否がWindowsでは再現されない | 1 | Windowsではfile／親directoryの削除権限を拒否するfixtureを使用 |

この51件への対応では製品codeを変更していません。
製品のappend権限・ローテーション・圧縮・再送契約を変える修正ではありません。
試験をskip化して失敗を隠す変更もしていません。

削除失敗の試験では、初回のupload成功後に実際のRemoveを拒否します。
圧縮データがそのまま残ることを確認し、subtestのcleanupで元の権限と試験用handleを
解放してから次周期を実行します。次周期での再送・削除成功も確認対象です。
Windowsではfile自身のDELETEと親directoryのFILE_DELETE_CHILDを両方拒否します。
子fileの保護DACLを先に設定し、次に親directoryを変更します。逆順では親の継承ACL変更に
よって子を開けなくなり、障害注入の準備自体が失敗しました。cleanupは逆順に行い、
親を復元してから子の元の継承状態へ戻します。
後者の定数は[Microsoftのfile access rights定義](https://learn.microsoft.com/en-us/windows/win32/fileio/file-access-rights-constants)
に従っています。

## 継続実行できる試験環境

- `configfixture`を常設し、公開`.env.example`のみから匿名化したfixtureを生成します。
  元の認証情報・宛先・コメント等は持ち込まず、未知の文字列は生成時に拒否します。
- 実運用`.env`入力、symlink経由の入力、既存出力directoryの上書きも拒否します。
- 既存のローカル実`.env`監査試験は維持します。Windows媒体での同試験は、実運用設定の
  監査ではなく、生成したfixtureの読込・mapping契約の確認です。
- `run-logger-tests.cmd`で対象8 packageの全体試験を実行できます。configだけは生成fixtureを
  作業directoryにし、一時fileと結果は別の書込可能directoryへ置きます。
- 集計は行中に出たGoのtest結果も数え、subtestを親testの件数へ重複計上しません。
  この集計自体にも合成ログを使った回帰試験を追加しました。

## Linux検証

Go 1.25.0、`-mod=readonly`で次を確認しました。

- `pkg/fsstore`、`internal/config/...`、`internal/infrastructure/fsstore/...`、
  FileLogger、FsArchiveHandler、fixture生成toolの全体`-race`成功。
- configの修正対象7件を10回、FileLogger対象9件を`-count=3 -cpu=1,2,4`で反復成功。
- FsArchiveHandlerの修正対象を反復し、削除拒否試験も10回成功。
- 実`.env`を使わないsynthetic rootからのconfig全体試験も成功。
  実行後のfixture rootには生成した4file以外の書込がないことを確認。
- 独立レビュー、`git diff --check`、Windows向けtest build成功。
- 修正対象のconfig、FileLogger、FsArchiveHandler、fixture toolのWindows向け`go vet`成功。

## Windows検証

Windows 11 Enterprise Evaluation 25H2、ローカルNTFSで実行しました。
外部networkとhost directory共有は使用していません。生ログはVM内に保持し、
hostでは集計と必要な試験assertionの画面だけを確認しています。

初回修正版の一般権限全体試験は1091成功・5失敗・7親test skipでした。
残る5件はsymlink作成権限4件と、今回追加した削除拒否fixtureのACL準備順序1件です。
管理者でsymlinkを実際に作成すると4件は成功しました。ACL準備順序も修正した
最終版では、管理者による対象8 packageの全体試験がすべて終了code 0になりました。

| package | 管理者: 成功 | 失敗 | 親test skip |
| --- | ---: | ---: | ---: |
| pkg/fsstore | 10 | 0 | 0 |
| appendlog | 81 | 0 | 1 |
| failurearchive | 25 | 0 | 0 |
| FileLogger | 259 | 0 | 5 |
| appendlog runner | 205 | 0 | 0 |
| config/validation/mapped | 49 | 0 | 0 |
| config | 298 | 0 | 0 |
| FsArchiveHandler | 170 | 0 | 0 |
| 合計 | 1097 | 0 | 6 |

管理者全体試験のsubtest skipは0件です。6件の親test skipは、Unix専用の
source identity 1件と、`ENV_PATH`で実設定を明示したときだけ実行するFileLogger 5件です。
後者は実`.env`からの読込・出力確認3件と、Tempo／OTel Collector接続試験各1件です。
前回失敗した51件をskipに変更したものではありません。

symlinkを作る6件の親test（元の失敗4件と、前回権限不足でskipしたrunner／appendlogの
親test各1件）も管理者で各10回実行し、60成功・失敗0・親／subtest skip 0でした。
一覧取得で6件が選ばれることと、反復後の結果が60件になることの両方を確認しました。

反復用PowerShell scriptの初回実行では、引用していない固定flagが`-test`として渡り、
Go testが開始前に終了code 2で停止しました。この実行は試験成功件数へ含めていません。
固定flagを文字列として渡すように修正し、同一test exeで新しい結果directoryへ再実行しました。
`.cmd`による全体試験にはこの問題はありません。

最終版の一般権限全体試験は1092成功・4失敗・7親test skipでした。
FileLoggerは257成功・2失敗・5skip、FsArchiveHandlerは168成功・2失敗・0skip、
runnerは204成功・0失敗・1skipです。その他5 packageは管理者の表と同じ結果でした。
一般権限ではsymlink作成権限を付与していないため、4件はfixtureを作成できません。
最終logでも4件すべてが`A required privilege is not held by the client`であり、
同じ一般ユーザーの実symlink作成probeもerrno 1314になることを確認しました。
これらは上記の管理者側で実際に検証・反復済みです。一般権限の全体試験自体が
失敗0になった、という結果ではありません。appendlogのsubtest skip 2件も同じ権限条件です。
削除拒否fixtureは一般権限でも成功し、試験用ACLの復元後の再送・削除も確認できました。

一般権限で元の47件を各10回実行し、470成功・失敗0・親／subtest skip 0を確認しました。
元の51件のうち4件は上記の管理者側で各10回成功したため、元の全51件について
実行可能な権限条件で合計510回の成功を確認できました。

| 一般権限の追加実行 | 成功 | 失敗 | 親／subtest skip |
| --- | ---: | ---: | ---: |
| configの旧失敗33件 × 10 | 330 | 0 | 0 |
| FileLoggerの旧失敗7件 × 10 | 70 | 0 | 0 |
| FsArchiveHandlerの旧失敗7件 × 10 | 70 | 0 | 0 |
| colon回帰19件 × 10 | 190 | 0 | 0 |
| configfixture tool 6件 | 6 | 0 | 0 |
| 合計 | 666 | 0 | 0 |

各packageの終了codeはすべて0です。集計scriptの合成ログ試験も`SUMMARY_SELF_TEST PASS`
となりました。この合成ログは上記666件のGo test成功数へ含めていません。

Developer Modeを有効にする永続的なHKLM変更は権限審査で拒否されたため、実施して
いません。変更前の一般ユーザーでsymlink作成がerrno 1314で拒否されることを確認しました。
永続設定やユーザーの権利割当は変更せず、既存の管理者コンソールでの実行も別枠で
検証します。製品が通常利用時に管理者権限を必要とする、という意味ではありません。

検証後はWindowsから正常shutdownしました。launcherの終了code 0、QMP／TPM socketと
PID fileの消失、QEMU／swtpm processが残っていないことを確認しています。
`vnc.sock`の古いfile nodeだけは残っていますが、接続先processはなく、今回は削除していません。
許可されたSlackの正常終了通知も送信し、APIの`ok=true`を確認しました。

### 証跡

- 原因調査画面：`/tmp/windows-existing-failures-control.9rXYZq/`
- 初回修正版媒体：`/tmp/windows-all-failures-fixed.FjkRaA/native-tests.iso`
- 初回媒体SHA-256：`a26bb565e2ffbd30fe0cd8bd08f30a1a43ac171bfcde18cf901580e822e5c770`
- ACL準備順序も修正した最終媒体：`/tmp/windows-fixed-acl-order.qsz0ae/fixed-tests.iso`
- 最終媒体SHA-256：`36920e927faea18bc3d3e945ff31c34c65a98f8d3a86f2076a07bc2b80559895`
- 反復script修正版媒体：`/tmp/windows-fixed-repeat-quoted.z9Y5KF/fixed-tests.iso`
- 反復媒体SHA-256：`0621cadbdbf8f08fa63155b0278b023751a208010a6151990e27d513e422f316`
- FileLogger exe：`8b3255b856e63809fb3490d374f45d0e7f45dae786e904a4569802fdf6416457`
- config exe：`a118dfbaf42dfea9a592081518ca2ed5618332c9b89658f4456f52c9a890230d`
- 最終FsArchiveHandler exe：`c07c7e9df88600bb23ea9baf081f6851a0a3d15f3802a9c5fc69f44870c500de`
- 最終管理者全体試験：`C:\Users\vmtest\fixed-admin-final-qsz0ae`
- 最終管理者集計画面：`/tmp/windows-existing-failures-control.9rXYZq/final-admin-summary.png`
- 管理者symlink反復：`C:\Users\vmtest\fixed-admin-symlinks-z9Y5KF`
- symlink反復集計画面：`/tmp/windows-existing-failures-control.9rXYZq/final-admin-symlinks-summary.png`
- 最終一般権限全体試験：`C:\Users\vmtest\fixed-normal-final-z9Y5KF`
- 最終一般権限集計画面：`/tmp/windows-existing-failures-control.9rXYZq/final-normal-summary.png`
- 一般権限の残4件とsymlink probe：`/tmp/windows-existing-failures-control.9rXYZq/final-normal-four-permission-failures.png`
- 一般権限反復：`C:\Users\vmtest\fixed-normal-repeat-z9Y5KF`
- 一般権限反復集計・集計自己試験画面：`/tmp/windows-existing-failures-control.9rXYZq/final-normal-repeat-summary.png`
- skip対象名・理由：同directoryの`final-normal-skip-reasons.png`、`final-file-skip-conditions.png`
- Slack完了通知のtimestamp：`1789152284.765169`
