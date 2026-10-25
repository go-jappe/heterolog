# 全packageのWindows native試験（2026-10-22〜24）

## 対象と実行方法

CI（`.github/workflows/test.yml`）のwindows-latest jobが実行する`go test ./...`と同じく、
test fileを持つ全packageをWindows上で実行しました。最初の媒体では121 package、並行作業で
`internal/infrastructure/fsstore/spool/retryafter`が加わった後の媒体では122 packageです。
test fileのない`cmd/log-recovery`はWindows向けにbuildし、`-h`が終了code 0、引数なしが
`--target is required`で終了code 2になることを確認しました。

- Go 1.27.3、`GOOS=windows GOARCH=amd64 CGO_ENABLED=0`、`-tags=timetzdata`、`-mod=readonly`で
  packageごとにtest exeを作成しました（[`build-all-package-test-media.sh`](build-all-package-test-media.sh)）。
- 媒体にはgit checkoutに含まれる範囲だけを`source.tar`として入れました。`.gitignore`の除外対象
  （`/.env`、`/.env.*`、各種tool directory、build・test出力）と`/.claude/`、`/.git`は含めず、
  作成のたびにISOとtarの全file一覧で`.env`や認証情報がないことを確認しています。
- 既存のWindows 11 Enterprise Evaluation 25H2（`10.0.26200.6584`）VMをofflineで起動しました
  （4 vCPU・8 GiB、host directory共有なし）。
- [`run-all-package-tests.ps1`](run-all-package-tests.ps1)がsource treeをNTFSへ展開し、各exeを
  `TEMP`下の`b001\<package名>.test.exe`へcopyして、package directoryを作業directoryに
  `-test.paniconexit0 -test.timeout=10m0s -test.v=true`で起動しました。packageは一つずつ実行しました。
  CIの`go test`はpackageを並列に実行する点が異なります。
- 結果は64 MiBの`TESTRESULTS`へ新しいfolderを作って書き出し、停止後に複製したimageからhostで読みました。

| 条件 | 権限 | `TEMP` / `TMP` |
| --- | --- | --- |
| A | 一般（Medium integrity `S-1-16-8192`） | `C:\heterolog-all-tests-20261022\temp-a` などの通常の長いpath |
| B | 一般 | `C:\HETERO~2`（`C:\heterolog-long-directory-name-temp`の8.3短縮名） |
| C | 管理者コンソール（High integrity `S-1-16-12288`） | Bと同じ`C:\HETERO~2` |

GitHubのWindows runnerは利用者名`runneradmin`が長いため、`TEMP`が
`C:\Users\RUNNER~1\AppData\Local\Temp`という短縮名になります。Bはこの条件を再現したものです。
`dir /x`で短縮名を確認し、管理者コンソールの`fsutil 8dot3name query C:`（読み取りのみ）で
「ボリュームの状態は0（8dot3名の作成は有効）、レジストリの状態は2（ボリューム単位・既定値）」を確認しました。
Cは管理者権限でsymlinkを実際に作れるため、一般権限ではskipになる試験も実行されます。

## 結果

件数は最上位testとsubtestを分けて数えています。subtestを親testへ重複計上していません。
最終は9回目（round 4と、errorの原因を保持する修正の後）です。7回目から9回目は、利用者が手動でログインしたVMで
実行しました。
5回目と6回目は自動ログインのpasswordが一致せず、試験を実行していません。

### 初回: 修正前のコード（2026-10-22 22:29作成の媒体、121 package）

| 条件 | package 成功 | package 失敗 | test 成功 | 失敗 | skip | subtest 成功 | 失敗 | skip |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A | 110 | 11 | 3999 | 22 | 57 | 7561 | 44 | 196 |
| B | 109 | 12 | 3998 | 23 | 57 | 7560 | 44 | 196 |
| C | 109 | 12 | 4040 | 23 | 15 | 7812 | 43 | 9 |

`internal/config`と`logfile/lifecycle`はpanicで途中終了したため、後続の試験は実行されていません。

### 途中: 1回目の修正後（2026-10-22 23:38作成の媒体、122 package）

| 条件 | package 成功 | package 失敗 | test 成功 | 失敗 | skip | subtest 成功 | 失敗 | skip |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A | 118 | 4 | 4351 | 6 | 71 | 9487 | 10 | 225 |
| B | 118 | 4 | 4351 | 6 | 71 | 9487 | 10 | 225 |
| C | 117 | 5 | 4512 | 8 | 17 | 9837 | 10 | 16 |

A・Bでは`logfile/lifecycle`がtimeoutで停止しました（下記の失敗表 No.16）。Aは10分で停止し、
同じ停止を待たないよう、この回のB・Cだけは`-test.timeout=2m0s`で実行しました。

### 3回目: 並行作業の反映後（2026-10-23 00:45作成の媒体、122 package）

| 条件 | package 成功 | package 失敗 | test 成功 | 失敗 | skip | subtest 成功 | 失敗 | skip |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A | 121 | 1 | 4464 | 1 | 72 | 9568 | 5 | 227 |
| B | 121 | 1 | 4464 | 1 | 72 | 9568 | 5 | 227 |
| C | 120 | 2 | 4518 | 2 | 17 | 9842 | 5 | 16 |

残った2件は並行作業（Retry-Afterの永続化）の担当範囲だったため、この回では報告だけとし、
並行作業の完了後に4回目で修正しました（No.18・No.19）。
3条件とも試験によるcheckout複製へのfile追加・削除はありませんでした。
この媒体と同じsnapshotのLinuxでの`go test ./...`は122 package成功、test 4503成功・21 skip、
subtest 9902成功・8 skipです。

この媒体では追加で次を10回ずつ実行し、すべて成功しました。

| 権限・`TEMP` | 対象 | 成功 | 失敗 | skip |
| --- | --- | ---: | ---: | ---: |
| 一般・`C:\HETERO~2` | config・fsstore・lifecycle・testutilの修正対象（12 test） | 110 | 0 | 10 |
| 管理者・`C:\HETERO~2` | config・lifecycle・testutilの修正対象（9 test） | 90 | 0 | 0 |

一般権限のskip 10件は、symlinkの作成権限がないため停止せずにskipするようにしたNo.16の試験です。

### 4回目: 残り2件の修正後（2026-10-23 01:45作成の媒体、122 package）

| 条件 | package 成功 | package 失敗 | test 成功 | 失敗 | skip | subtest 成功 | 失敗 | skip |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A | 121 | 1 | 4465 | 1 | 71 | 9572 | 1 | 227 |
| B | 122 | 0 | 4466 | 0 | 71 | 9573 | 0 | 227 |
| C | 122 | 0 | 4520 | 0 | 17 | 9847 | 0 | 16 |

No.18・No.19の試験は3条件とも成功しました。No.19の試験は一般権限でもskipせずに実行されるため、
A・Bのskipが1件減っています。Aの1件は、変更していない`logger/file`の試験で初めて出た
時間依存の失敗です（No.20）。3条件とも試験によるcheckout複製へのfile追加・削除はありませんでした。

追加で次を実行しました。

| 権限・`TEMP` | 対象 | 回数 | 成功 | 失敗 | skip |
| --- | --- | ---: | ---: | ---: | ---: |
| 一般・通常のTEMP（A） | No.18・No.19の試験（2 test、subtest 5件） | 10 | 20（subtest 50） | 0 | 0 |
| 一般・`C:\HETERO~2`（B） | 同上 | 10 | 20（subtest 50） | 0 | 0 |
| 管理者・`C:\HETERO~2`（C） | 同上 | 10 | 20（subtest 50） | 0 | 0 |
| 一般・`C:\HETERO~2`（B） | 修正前の`logger/file`の過負荷試験2件（No.20） | 50 | 76（subtest 1171） | 24（subtest 29） | 0 |

最後の行の失敗は、すべて`TestFileOverloadThresholdAcrossModesAndRoles`の同じ検証
（`overload_test.go:112`）で、29件とも余分な通知は書き込み遅延の通知（`log write delay`）でした。

### 5回目・6回目: 未実行（ログイン時のpassword不一致）

- 5回目: No.20の試験修正とNo.21の製品修正を入れた媒体（2026-10-23 02:54作成、
  `heterolog-windows-all-tests-20261023-r3.iso`、122 package）でVMを起動しましたが、ログイン時に
  passwordが一致しませんでした。手順の規則どおり再入力はせず、ログイン画面の電源メニューから
  guestをシャットダウンしました。なお、不一致の表示の後に送った操作キーと文字列がpassword欄へ入り、
  2回目の失敗として送信されています（passwordではありません）。
- 6回目: 利用者の承認を得て、ログインを1回だけ試しました。媒体は当時のtree（No.20・No.21の修正と、
  別作業によるOnError再入の修正を含む）から作成しました（2026-10-23 06:19作成、
  `heterolog-windows-all-tests-20261023-r4.iso`、123 package）。ロック画面が表示されてから2分以上待ち、
  ロック画面をmouseのclickで閉じました。password欄が空でフォーカスがあること（caretの表示）を
  画面で確認し、LEDの状態（CapsLock OFF・NumLock OFF。password入力の処理がNumLockをONにして
  確認します）を確認してから、passwordを1回入力し、Enterを1回押しました。結果は再び不一致でした。
  その後はkeyを一切送らず、mouseのclickだけでログイン画面の電源メニューからシャットダウンしました。
- 保存済みのpassword fileは2026-09-10から変更されておらず、4回目までは同じ手順でログインできていました。
  原因は確認していません。この間の変更（No.20・No.21と、別作業によるOnError再入の修正）は7回目で確認しました。

### 7回目: OnError再入の修正後（2026-10-23 08:30作成の媒体、123 package）

| 条件 | package 成功 | package 失敗 | test 成功 | 失敗 | skip | subtest 成功 | 失敗 | skip |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A | 123 | 0 | 4507 | 0 | 71 | 9698 | 0 | 228 |
| B | 123 | 0 | 4507 | 0 | 71 | 9698 | 0 | 228 |
| C | 123 | 0 | 4561 | 0 | 17 | 9973 | 0 | 16 |

3条件とも全packageが成功し、A・Bの件数は完全に一致しました。試験によるcheckout複製へのfile追加・削除は
ありませんでした。`log-recovery.exe`の起動確認（`-h`が0、引数なしが2）も3条件で成功しています。

- 媒体（`heterolog-windows-all-tests-20261023-r5.iso`）は、作業担当が
  [`build-all-package-test-media.sh`](build-all-package-test-media.sh)で当時のtreeから作成しました。
  媒体の`source.tar`が当時のtreeと一致し、`.env.example`以外の`.env`を含まないことをhostで確認しました。
- VMは利用者がofflineで起動し、手動でログインしました。試験の操作はログイン後のdesktopから行い、
  passwordの入力やログインの試行はしていません。表示は5分の無操作で消えるため、画面が真っ暗なときだけ
  Shiftで表示を戻しました（sessionはlockされていませんでした）。

追加で次を繰り返し、すべて成功しました。

| 権限・`TEMP` | 対象 | 回数 | 成功 | 失敗 | skip |
| --- | --- | ---: | ---: | ---: | ---: |
| 一般・通常のTEMP（A） | `TestFileOverloadThresholdAcrossModesAndRoles`（No.20） | 10 | 10（subtest 180） | 0 | 0 |
| 一般・`C:\HETERO~2`（B） | 同上（4回目の修正前の計測と同じ50回） | 50 | 50（subtest 900） | 0 | 0 |
| 管理者・`C:\HETERO~2`（C） | `TestPathExists`（No.21の`symlink-loop-parent`を含むsubtest 7件） | 10 | 10（subtest 70） | 0 | 0 |
| A・B・Cの各条件 | 各loggerの`internal_log_reentry_test.go`の26 test、`self_notification_*_test.go`の6 test（`TestConcurrentSelfSelectedFileNotificationsAreAllDelivered`を含む）、名前が一致した`TestFileOverloadFallbackDoesNotRedispatchErrors` | 10 | 各条件330（subtest 1100） | 0 | 0 |

### 8回目: round 3の修正後（2026-10-23 13:22作成の媒体、124 package）

round 3では試験の監査と修正（製品の変更を含む293 file）を行いました。主な変更は、待機の上限を
`internal/testutil/boundedwait`（実時間10秒・bubble時間10分）へ統一したこと、backendとFileLoggerのlockを
synctestで観測できる`WaitableMutex`へ置き換えたこと、日付の計算を試験対象と同じ時計・time zoneから
求めるようにしたこと、fsarchivehandlerの世代順序の競合（`archive_upload_order.go`）の修正です。

| 条件 | package 成功 | package 失敗 | test 成功 | 失敗 | skip | subtest 成功 | 失敗 | skip |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A | 124 | 0 | 4568 | 0 | 72 | 9857 | 0 | 228 |
| B | 124 | 0 | 4568 | 0 | 72 | 9857 | 0 | 228 |
| C | 124 | 0 | 4622 | 0 | 18 | 10134 | 0 | 14 |

3条件とも全packageが成功し、A・Bの件数は完全に一致しました。試験によるcheckout複製へのfile追加・削除は
ありませんでした。`log-recovery.exe`の起動確認も3条件で成功しています。

- 媒体（`heterolog-windows-all-tests-20261023-r6.iso`）は、作業担当が同じscriptで現在のtreeから作成しました。
  `source.tar`が現在のtreeと一致し、`.env.example`以外の`.env`を含まないことをhostで確認しました。
- VMはofflineで起動しました。ログインは利用者が手動で行い、待つ間は画面の取得だけで入力していません。
  ログイン後の操作と、表示が消えたときのShift・UACの同意（はい）の扱いは7回目と同じです。

追加で次を繰り返し、すべて成功しました。

| 権限・`TEMP` | 対象 | 回数 | 成功 | 失敗 | skip |
| --- | --- | ---: | ---: | ---: | ---: |
| A・B・Cの各条件 | round 3で変更したWindows専用の試験（checkpointの共有ACL handleと読み取れないcheckpointの復旧、ドライブ指定のcolon、target大小文字の再起動2件。計5 test） | 10 | 各条件50（subtest 40） | 0 | 0 |
| 一般・通常のTEMP（A） | fsarchivehandlerの世代順序の試験8件（`TestFailureArchiveDeferredGenerationRetriesWhenOlderCompletesDuringItsAttempt`、Closeの待機を含む） | 20 | 160 | 0 | 0 |
| 一般・通常のTEMP（A） | 待機の上限・lock・時刻の扱いを変えた12 package全体（logger/file、logfile/lifecycle、fsstore/checkpoint、fsarchivehandler、logger、logger/common、5つのbackend、testutil/boundedwait） | 3 | 5397（subtest 7062） | 0 | 96（subtest 180） |
| 管理者・`C:\HETERO~2`（C） | 同じ12 package全体（symlinkの試験も実行） | 3 | 5460（subtest 7242） | 0 | 33 |

Aのskipは、symlinkの作成権限が必要な試験と、下記「残るskip」の試験です。

### 9回目: round 4とerrorの原因を保持する修正の後（2026-10-23 20:19作成の媒体、126 package）

| 条件 | package 成功 | package 失敗 | test 成功 | 失敗 | skip | subtest 成功 | 失敗 | skip |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A | 126 | 0 | 4618 | 0 | 72 | 10007 | 0 | 228 |
| B | 126 | 0 | 4618 | 0 | 72 | 10007 | 0 | 228 |
| C | 126 | 0 | 4672 | 0 | 18 | 10284 | 0 | 14 |

3条件とも全packageが成功し、A・Bの件数は完全に一致しました。skipの一覧は3条件とも8回目と同じです。
試験によるcheckout複製へのfile追加・削除はなく、`log-recovery.exe`の起動確認（`-h`が0、引数なしが2）も
3条件で成功しました。

- 媒体（`heterolog-windows-all-tests-20261023-r7.iso`）は、作業担当が同じscriptで現在のtreeから作成しました。
  `source.tar`が現在のtreeと一致し、`.env.example`以外の`.env`を含まないことをhostで確認しました。
- 手順は8回目と同じです（offlineで起動し、利用者が手動でログインするまで画面の取得だけで待つ）。
  利用者からは途中で一度画面がlockされたと連絡がありましたが、こちらの入力はログイン後のdesktopを
  画面で確認してからだけ行い、lock画面へは何も送っていません。実行中は数分ごとに画面を取得し、
  desktopが見えているときだけShiftを押して無操作の時間を区切りました。
- 8回目の停止時に適用された更新により、管理者コンソールに表示されるbuildは`10.0.26200.6584`から
  `10.0.26200.9445`になっていました。

追加で次を繰り返し、すべて成功しました。

| 権限・`TEMP` | 対象 | 回数 | 成功 | 失敗 | skip |
| --- | --- | ---: | ---: | ---: | ---: |
| A・B・Cの各条件 | FileLoggerの作成失敗の原因を保持する試験（Windows専用の拒否fixtureを使う`TestFileLoggerCreationFileSystemErrorsKeepTheirCause`など4件）、synctestの待機上限を使うlogger setの試験（`TestLoggerSetColdRoleCallersWaitForItsConstruction`・`TestLoggerSetCloseIncludesUnpublishedColdRole`・`TestDeliveryLoggerSetCloseWaitsForUnpublishedRole`と関連2件）、通知先の設定検証（root 1件、logger 3件） | 10 | 各条件130（subtest 380） | 0 | 0 |
| A・B・Cの各条件 | `internal/testutil/boundedwait`、`internal/presentation/cli/logrecovery`（CLIの設定検証）、`pkg/internal/endpointurl`の全試験 | 10 | 各条件380（subtest 570） | 0 | 0 |
| 一般・通常のTEMP（A） | 変更の多い9 package全体（logger/file、logger、logger/stdout、logger/multi、logger/slack、logfile/lifecycle、fsstore/checkpoint、fsstore/spool/worker、application/logging） | 3 | 3705（subtest 6387） | 0 | 84（subtest 213） |
| 管理者・`C:\HETERO~2`（C） | 同じ9 package全体（symlinkの試験も実行） | 3 | 3759（subtest 6600） | 0 | 30 |

### 10回目（最終）: CIで時々落ちる試験を探す並行・反復試験（2026-10-24 07:41作成の媒体、127 package）

GitHub ActionsのWindows runner（2 vCPU）では、Windowsだけで時々落ちる試験が続けて見つかりました
（`b78d849`・`3473fca`で修正）。CIに近い負荷で残りを探すため、`3473fca`の媒体で全package試験を
複数同時に、CPU 0・1だけ（`start /affinity 3`）で実行しました。

| 実行 | 権限・`TEMP` | 同時実行 | `-Count` | package 成功 | package 失敗 | test 成功 | 失敗 | skip | subtest 成功 | 失敗 | skip |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `p1`・`p2`・`p3` | 一般・通常のTEMP | 3 | 3 | 各127 | 0 | 各13862 | 0 | 各219 | 各30069 | 0 | 各696 |
| `a1`・`a2`・`a3`・`a4` | 管理者・通常のTEMP | 4 | 2 | 各127 | 0 | 各9355 | 0 | 各36 | 各20608 | 0 | 各28 |

7実行とも全packageが成功し、timeoutで止めたpackage、`TEMP`の残り、checkout複製へのfile追加・削除は
ありませんでした。各実行のpackage所要時間の合計は約940〜970秒で、9回目の順次実行（約330〜360秒）の
約2.7倍でした。全packageを合わせて17回実行し、新しい失敗は見つかりませんでした。

- 媒体（`heterolog-windows-all-tests-20261024.iso`）は、作業担当が同じscriptで`3473fca`のtreeから作成し、
  `source.tar`などの一覧に`.env.example`以外の`.env`や認証情報がないことをhostで確認しました。
- commit IDは、2026-10-24にcommit messageだけを書き換えた後のものです。書き換え前の`ca6f8cd`・`6889042`と
  同じtreeです。
- 実行前にLinuxで`go test -count=2 ./...`が全package成功することを確かめ、同じprocess内で繰り返すことによる
  失敗と区別できるようにしました。
- 利用者がログインしたdesktopから操作しました。一般権限の3実行は8:20〜8:47、管理者の4実行は8:52〜9:16です。
  最初の起動では、入力中に`p1`の窓が前面に出たため、`p2`・`p3`の起動コマンドの大半が`p1`の窓に入りました。
  元の窓から起動し直したので結果には影響していません。`p1`の終了後にその入力が実行されましたが、
  結果directoryが既にあるためエラーで止まりました。

## 失敗の原因と対応

「初回」〜「4回目」は失敗を確認した回、「最終」は9回目での結果です。括弧内は、その試験を繰り返した回の結果です。

| No. | 試験 | 条件 | 原因 | 分類 | 対応 | 最終 |
| ---: | --- | --- | --- | --- | --- | --- |
| 1 | config `TestConfiguredS3DestinationsUploadIndependently` | 初回 A/B/C | `os.Clearenv`で`SystemRoot`が消え、その後の`httptest.NewServer`がWinsockの初期化に失敗してpanic（`WSAEPROVIDERFAILEDINIT`）。後続のconfig試験も止まった | 試験（環境の隔離方法） | `testutil.ClearEnvironment`を追加し、config試験の環境消去でOSの実行環境の変数（`SystemRoot`・`LOCALAPPDATA`・`APPDATA`・`USERPROFILE`・`HOME`・`TEMP`・`TMP`・`TMPDIR`）だけを残す | 成功 |
| 2 | config `TestLoadConfigMapsDebugLogDirectoriesForEveryLoggerMode`、`TestMapFileRolePurgePreservesContractsAndInternalOverrides`、`TestLogDirectoryEnvironmentDefaultsAndWhitespace` | 初回 A/B/C | 環境消去で`LOCALAPPDATA`等も消え、Windowsの既定`SYSTEM_LOG_DIR`が空になり保存先の解決が`ErrEmptyStorageDirectory`で失敗 | 試験（同上） | No.1と同じ | 成功 |
| 3 | appendlog `TestRotateWithRetryResumesOpeningWithoutClosingOldHandleAgain`、FileLogger `TestFileLoggersRecoverAfterRotationOpenFailure`、`TestFileLoggersCloseDuringPendingRotationReleasesProtection` | 初回 A/B/C | WindowsのGoの`(*os.File).Stat`は閉じたhandleを検査せず`GetFileType`へ渡すため、`os.ErrClosed`ではなく`ERROR_INVALID_HANDLE`を返す | 試験（確認方法のOS差） | `testutil.IsClosedFileError`で閉じたfileのerrorをOSごとに判定 | 成功 |
| 4 | FsArchiveHandler `TestHandleFailureDelegatesArchiveWriteError` | 初回 A/B/C | 製品の追記前の`Stat`が閉じたhandleで`ERROR_INVALID_HANDLE`を返し、原因は保持しているが`os.ErrClosed`ではない | 試験（同上） | No.3と同じ判定で原因の保持を確認 | 成功 |
| 5 | checkpoint `TestScanExistingFileKeysPropagatesDirectoryReadFailure` | 初回 A/B/C | `OpenDirectoryForRead`は通常fileも開け、そのReadDirが返す`ENOTDIR`はWindowsでは`ERROR_PATH_NOT_FOUND`と同じ値。呼出側が`os.ErrNotExist`（未作成のdirectory）として空の一覧に読み替え、設定誤りを報告しなかった | 製品 | Windowsの`OpenDirectoryForRead`が開いたhandleのdirectory属性を確認し、通常fileを`ERROR_DIRECTORY`で拒否。Linuxで同じ値の扱いをoverlayで再現し、同じ失敗になることも確認 | 成功 |
| 6 | lifecycle `TestArchiveCheckpointFactoryClosesNotificationsOnConstructionFailure` | 初回 A/B/C | No.5と同じ原因でStoreが返り、試験が通知workerを閉じないまま失敗してsynctestのdeadlock panicになり、後続のlifecycle試験も止まった | 製品＋試験 | No.5の修正に加え、想定外に返ったStoreをbubble内の`t.Cleanup`で閉じる | 成功 |
| 7 | failurearchive `TestWindowsFailureArchiveDirectoryCaseChangeFindsExistingFiles` | 初回 A/B/C | 保存先の解決は実体path（大小文字とBの8.3短縮名を正規化）を返す設計だが、試験が呼び出し元の表記を期待していた | 試験（期待値） | 入力側の一時directoryを`filepath.EvalSymlinks`で実体の表記にし、大小文字違いの設定が元と同じ実体directoryへ到達することを確認。設定値から求めるpathは呼び出し元の表記のまま | 成功 |
| 8 | logger `TestNewLoggerCloseCompletesRegisteredClickHouseAndSlackFallbackHandlers`ほか2件 | 初回 A/B/C | Lokiのpush遅延を1ns超で判定する試験。Windowsのmonotonic clockはtimer割込み単位（標準約15.6ms）で進むため、即時応答の偽transportでは計測値が0になる | 試験（時間計測の前提） | 偽transportがclockの進むまで応答を返さないようにした | 成功（追加で20回×4件成功） |
| 9 | FileLogger `TestManagedFileArchiveUploaderCloseWaitsForPreservationWarning` | 初回 A/B/C | 警告文はpathを`%q`で表示し、Windowsのbackslashがescapeされる | 試験 | 期待値も`strconv.Quote`で同じ形式にする | 成功 |
| 10 | FileLogger `TestFileLoggerAllowsWindowsDrivePathsInDirectoryConfig` | 初回 A/B/C | 検証が保存先を解決するようになり、`LOG_DIR`のドライブ指定を`SYSTEM_LOG_DIR`の下へ結合したpathはWindowsでは構文error（`ERROR_INVALID_NAME`） | 試験（期待値のOS差） | colon検証が拒否しないことを両OSで確認し、WindowsではOSのpath構文errorで拒否されることを確認 | 成功 |
| 11 | compressedlogfailure `TestManagedUploaderPreservesSourceBeforeAcceptingConflictingSavedResult`ほか2件 | 初回 A/B/C | 期待pathを比較用の識別子（Windowsでは小文字化）から求め、警告の実際のpathと文字列比較していた | 試験 | `os.SameFile`で同じfileかを確認 | 成功 |
| 12 | testutil `TestDenyWindowsAccessDirectoryPreservesChildren`、`TestDenyFileRemovalRestoresWindowsDACLs` | 初回・途中 A/B/C | fixtureの親を`CreateDirectory`へ記述子を渡して作ると`SE_DACL_AUTO_INHERITED`が付かず、子が旧形式のDACLになる。`GetNamedSecurityInfo`は旧形式の子の継承ACEを親の現在のDACLと照合して表示するため、親を拒否している間は子が変わったように見える | 試験（fixture） | 親を作成後に`SetNamedSecurityInfo`で保護・自動継承のDACLにし、その条件もfixtureで確認。helperは変更前の実装のまま（下記） | 成功（3回目に10回×2権限） |
| 13 | fsstore `TestPreserveRegularFileConcurrentCopiesPublishOneVerifiedFile` | 初回 Bのみ | 8並列の保全で、先に`MoveFileEx`した処理のhandle（DELETE access）が閉じる前に、他の処理が既存fileの照合を`FILE_SHARE_DELETE`なしで開いて共有違反 | 製品（Windowsの共有mode） | 内容照合と既存保全fileの同期のopenで`FILE_SHARE_DELETE`も共有し、決定的な回帰試験を追加 | 成功（途中の媒体で50回、3回目で10回） |
| 14 | config `TestStorageDirectoryPathIdentityForUncreatedChildren/case-sensitive` | 途中 A/B/C（初回は未実行） | 保存先の重なり判定が親子関係に`filepath.Rel`を使い、Windowsでは常に大小文字を区別せずに比べる。大小文字を区別するdirectoryの子を同じ保存先と誤判定 | 製品 | 比較用識別子（大小文字の規則を適用済み）の区切り単位の一致で判定 | 成功 |
| 15 | lifecycle `TestLifecycleFactoriesRejectColonInLogFileNamesBeforeCreatingFiles/target/*` | 途中 A/B/C | factoryが保存先を解決してからfile名を検証しており、Windowsではcolonを含むtargetがpathの構文errorになり、`ErrInvalidFileNameConfig`より先に失敗 | 製品（検証順序） | file名の検証を保存先の解決より前に行う | 成功 |
| 16 | lifecycle `TestLogFileProcessingRecoveryCompletesLaterFileWithoutCrossingUnsafePeer` | 途中 A/B（timeout） | 障害注入後にsymlink作成権限不足でskipし、cleanupが失敗時と異なり受理済み復旧の通常終了を待ち続けた | 試験（cleanup） | skipした試験もcleanupで中止を通知してから閉じる。Linuxでskipを模したoverlayで停止の再現と修正後の即時skipを確認 | A・Bはskip（停止なし）、Cは成功 |
| 17 | lifecycle `TestLifecycleWindowsTargetCaseRestartRetriesArchiveAndProtectsLatestRaw` | 途中 C（A/Bはtimeoutで未実行） | 失敗したuploadは次の通常周期ではなく受理済みの復旧（既定5秒後）が再開する設計だが、試験は2回目の周期での削除を期待 | 試験（期待値） | synctestのbubble内で復旧間隔を進め、再送・削除の完了と、次の周期でも最新rawを保護することを確認 | 成功 |
| 18 | spool/log `TestSpoolPreservesAttemptOutcomeAcrossCancellationAndTransitionRecovery`（subtest 5件） | 3回目 A/B/C | 処理fileの状態遷移の再試行を`RetryDelay: time.Nanosecond`で登録した直後に再試行を呼んでいた。期限は`time.Now()`で判定され、Windowsの時計は約15.6ms単位でしか進まないため、同じ時刻のまま期限前と判定された（`handled=false`） | 試験（時間の前提） | 同じpackageの`synctestFromWallClockForTest`（仮想時計を開始時の実時刻に揃えるsynctest）で実行し、`time.Sleep(RetryDelay)`で待機時間の経過を明示してから再試行する。検証内容は変更なし。Linuxでもsleepを除くと同じ失敗になることを確認 | 成功（各条件で10回も成功） |
| 19 | fsstore/spool/worker `TestProcessingFileExistsPreservesFilesystemErrorWrapping` | 3回目 C（A/Bはsymlink権限不足でskip） | 親directoryの自己参照symlinkでLstatを失敗させるfixtureはUnixの`ELOOP`が前提。Windowsでは`os.Lstat`がこのpathを不存在として返し、`ProcessingFileExists`が`false, nil`になった（No.21） | 試験（fixtureのOS前提） | Windowsでは`testutil.DenyWindowsAccess`でspool fileの属性の読み取りと親directoryの一覧を拒否し、`ERROR_ACCESS_DENIED`を発生させる（一般権限・管理者ともskipしない）。検証は「Lstat自体が返したPathError（Op・Path・Err）を、文脈を付けてwrapして返す」に一般化（Linuxでは従来どおりOpが`lstat`）。Linuxでerrorの握りつぶし・wrapなし・Stat使用の3種の変異を検出することを確認 | 成功（各条件で10回も成功） |
| 20 | logger/file `TestFileOverloadThresholdAcrossModesAndRoles/runner/error/drop` | 4回目 Aのみ | 試験の共通設定は書き込み遅延の閾値が1msで、Windowsでは時計の分解能（約15.6ms）などで書き込みが1ms以上と計測されると、遅延のerror通知（`log write delay`）が過負荷の通知に加わり、通知数の検証が失敗した。4回目のBで50回繰り返すと24回失敗した | 試験（時間の前提） | 過負荷試験の設定だけ書き込み遅延の閾値を1時間にする（同じpackageの他の試験と同じ方法）。同じ設定を使う`TestFileOverloadFailuresNotifyAndRecoverOnNextWrite`も同じ前提だった。Linuxで書き込み時間を15.625msに固定するoverlayでは、修正前に18件と4件が失敗し、修正後は成功 | 成功（Bで50回、Aで10回の繰り返しも成功） |
| 21 | 製品 fsstore `PathExists`（`ProcessingFileExists`、rotationのgeneration判定、圧縮元の退役処理が使用） | 3回目 C（No.19の調査で判明） | Windowsでは、親directoryのsymlinkが循環して名前を解決できないpathを、`GetFileAttributesEx`が`ERROR_FILE_NOT_FOUND`として返し、Go 1.27.3の`os.Lstat`はその結果を返す。同じpathの`CreateFile`は`ERROR_CANT_RESOLVE_FILENAME`を返すため、確認できない状態を不存在（`false, nil`）として扱っていた（下記の診断） | 製品 | Windowsでは、Lstatが不存在を返したときだけreparse pointを辿らない`CreateFile`で確かめ、不存在のerror以外は確認できないerror（`*os.PathError`）として返す。`TestPathExists`に循環symlinkを親に持つpathを追加（Windowsではsymlinkを作れる管理者で実行） | 成功（Cで`symlink-loop-parent`を含む10回も成功） |

途中の回に一度`DenyWindowsAccess`を`SetKernelObjectSecurity`へ変更しましたが、復元時に
`SE_DACL_AUTO_INHERITED`が失われ（`D:AI(...)`→`D:(...)`）、`TestDenyWindowsAccessRestoreReleasesHandle`
が失敗しました。原因がfixtureにあると分かったため、helperは元の実装へ戻しています。

### 原因確認に使った診断

途中の回と4回目に、試験とは別の診断programをguestで実行しました。

- 共有違反（途中の回）: 8並列の上書き禁止`MoveFileEx`を300回行い、既存と判定した後のopenを比べました。
  Goの通常のopenは2100回中12回`ERROR_SHARING_VIOLATION`になり、`FILE_SHARE_DELETE`付きのopenは0回でした。
- ACL（途中の回）: 旧形式の子を持つ親では、拒否中の`GetNamedSecurityInfo`が子のACEを継承なしとして表示し、
  `SetKernelObjectSecurity`で親を戻すと子の表示も元に戻りました（子の格納内容は不変）。
  旧helperの復元（WRITE_DACだけのhandleでの`SetSecurityInfo`）は、旧形式の子へ明示ACEと継承ACEを
  重複して書き込みました。自動継承形式の子ではどちらの方法でも子は変わりませんでした。
- Lstat（4回目）: 試験と同じ`StateStore`のprocessing directoryで次のpathを調べました（No.21の修正前の製品）。
  symlinkを使う行は管理者だけで実行し、ACLの行は一般権限と管理者で同じ結果でした。

| 調べたpath | `GetFileAttributesEx` | `CreateFile`（reparse pointを辿らない） | `os.Lstat` | `ProcessingFileExists` |
| --- | --- | --- | --- | --- |
| 自己参照symlink（file・directoryの両方）、または2つのsymlinkの循環を親に持つ | 2（file not found） | 1921（名前を解決できない） | 不存在 | `false, nil` |
| 存在しない親、参照先のないsymlinkの親、通常fileの親 | 3（path not found） | 3 | 不存在 | `false, nil` |
| 参照先のあるfileのsymlink・directoryのsymlinkの下 | 成功 | 成功 | 成功 | `true, nil` |
| spool fileの属性の読み取りだけ、または親directoryの一覧だけを拒否 | 成功 | 成功 | 成功 | `true, nil` |
| 両方を拒否（No.19のfixture） | 5（access denied） | 5 | `CreateFile`のaccess denied | wrapしたerror |

## 8.3短縮名TEMP（条件B）で分かったこと

- 製品は保存先を実体path（`GetFinalPathNameByHandle`）へ解決するため、`C:\HETERO~2`は
  `C:\heterolog-long-directory-name-temp`へ展開されます。多くの試験は入力側で一時directoryを
  解決しており、条件Bに固有の失敗は、短縮名の表記を期待していたNo.7だけでした（初回はline 52で失敗）。
- 初回Bだけに出たNo.13は短縮名ではなく並行処理の時機による失敗で、Aでも起こり得ます。
- 3回目・7回目・8回目・9回目の媒体ではA・Bの件数が完全に一致しました。4回目のA・Bの差は、時間依存のNo.20
  （Aだけで発生）とその件数だけです。

## 残るskip

8回目と9回目の管理者実行（C）でskipしたのは、どちらも親test 18件・subtest 14件です。7回目（親test 17件・subtest 16件）からの
差は、round 3で公開設定例から`PROJECT_ROOT`と`TEMPO_ADDR`を削除したことによる除外subtest 2件の減少と、
子processとして起動されたときだけ動く`TestSlackLoggersDefaultHTTPClientHelperProcess`の追加です。

- 実サービスとの結合試験（OpenSearch・Loki・ClickHouse・Tempo・OTel Collector・testutil）:
  親test 13件・subtest 1件。環境変数で明示した場合だけ実行する設計で、CIの通常のjobでもskipします。
  CIではLinuxの `integration` jobだけが、使い捨てのcontainerに対して実行します（2026-10-23に同じ手順を
  Linuxの開発機のDockerで実行し、13件すべて成功）。
- Windowsの通常pathは末尾の空白を保持しないため、末尾空白のdirectory名を区別する試験: subtest 9件。
- 公開設定例の各keyの効果確認から除外したkey（`OTEL_COLLECTOR_ADDR`・`ENV_PATH`）: subtest 2件。Linuxでも同じです。
- Slackの既定HTTP clientの試験が子processとして使う補助test: 親test 1件。直接の実行ではskipする設計です。
- Unix FIFOを使う試験: subtest 2件。Unix source IDの互換確認: 親test 1件。
- Linux専用のslash区切りpathの走査試験（pathidentity）: 親test 2件。
- ACLを保持しないnative filesystemのroot（`FSSTORE_NO_ACL_TEST_ROOT`）が必要な試験: 親test 1件。
  CIでもskipします。途中の回に`TESTRESULTS`（FAT）の新しいfolderを指定して実行し、親test 1件と
  subtest 2件が成功しました。試験が作った一時directoryは試験自身が削除しています。

一般権限（A・B）では、上記に加えてsymlinkの作成権限が必要な試験がskipします
（8回目・9回目は親・subtest合わせて268件。No.21の回帰試験`TestPathExists/symlink-loop-parent`を含みます）。
これらはCで実行しています。

## 所見

- 書き込み遅延のerror通知と、同じloggerへ書き込むOnError handlerの組み合わせ（解消済み）。
  全書き込みの計測時間を15.625msに固定したLinuxのoverlayで、4回目の時点の製品と試験で
  `TestFileOverloadNotificationCanRequestCloseOrReenterLogger/reenter`を実行すると、
  handler内の書き込みが新たな遅延通知を呼ぶ再帰が止まらず、stack overflowになりました
  （実際の試験はsynctestで時間が進まないため起きません）。その後、別作業でOnErrorの中から再入した
  書き込みの内部logは通知せず記録する修正が入り、現在のtreeでは同じoverlayでもこの試験は成功しました。
  7回目のWindowsでも、各loggerの再入の試験とself_notificationの配線試験がA・B・Cで10回ずつ成功しました。
- OS差（変更なし）: Windowsでは通常fileを親directoryに持つpathも、OSが`ERROR_PATH_NOT_FOUND`
  （GoのWindowsの`ENOTDIR`と同じ値）を返すため、`PathExists`は不存在と判定します。
  Unixでは`ENOTDIR`のerrorになります。

## Linuxでの確認

7回目の媒体と同じcode（123 package）で次を確認しました。8回目のtree（124 package）の確認は末尾に記します。

- `gofmt`、`go vet ./...`（linux・`GOOS=windows`・`GOOS=darwin`）が成功。
- `go test -count=1 ./...`が123 package成功。`TMPDIR`をsymlinkの下に置くmacOS相当の条件でも123 package成功。
- 変更したpackage（config、testutil、fsstore、fsstore/appendlog、fsstore/name/...、fsstore/spool/...、
  logger/...、logfile/lifecycle、compressedlogfailure、spool/log/...）の`-race`が成功。
- `testutil.Symlink`がWindowsの権限不足と同じくskipするoverlayで全packageを実行し、停止や失敗がないことを確認
  （4回目以降に変更したpackageも同じoverlayで成功）。
- CRLF改行のcheckout（Windows runnerの既定`core.autocrlf=true`相当）へ変換した複製で全packageが成功
  （3回目のtree。4回目以降に変更したpackageも同じ変換で成功）。
- No.18はsleepを除いたoverlayで、LinuxでもWindowsと同じ`handled=false`の失敗になることを確認。
  No.20は上記のoverlayで修正前の失敗と修正後の成功を確認。
- `start-vm_test.sh`（9件）、`test_control_vm.py`（10件）、既存の`build-test-media.sh`による媒体作成が成功。
  PowerShellの集計自己試験（新旧2本）はguestで`PASS`を確認しました。
- 8回目のtree（124 package）: `gofmt`、`go vet ./...`（linux・`GOOS=windows`・`GOOS=darwin`）、
  `go test -p 4 -count=1 ./...`、macOS相当の条件、上記packageの`-race`が成功（Windows実行の後に実施）。
- 9回目のtree（126 package）のLinuxでの確認は作業担当が実施し、すべて成功したと報告されています
  （こちらではcodeを変更していないため再実行していません）。

## 媒体・証跡

VM状態directory（`/path/to/virtual-machines/heterolog-windows/`）に、既存のISOを残したまま
新しい名前で保存しました。

| 媒体 | 内容 | SHA-256 |
| --- | --- | --- |
| `heterolog-windows-all-tests-20261022.iso` | 初回（修正前、121 package） | `df717ef16a880e040744454f423130fc1f0df2ab2b41afdc7b4211d283a248ff` |
| `heterolog-windows-all-tests-20261022-r2.iso` | 途中（122 package）と上記の診断program | `e4c1fca63b8e71ef315f4c5e2d156ead76a1befc8c4c60a72eb5a24a8fcb0e80` |
| `heterolog-windows-all-tests-20261023.iso` | 3回目（並行作業の反映後、122 package） | `cd9caa4898eb15cdbe74f7af7b2793df57b15fd530a4ab481d7b2feb53be5877` |
| `heterolog-windows-all-tests-20261023-r2.iso` | 4回目（No.18・No.19の修正後）とLstatの診断test | `8ebe40f4f54c87e4459cae7e44c285908f31fbb0083f4c94e5a3f1a3910b9be4` |
| `heterolog-windows-all-tests-20261023-r3.iso` | 5回目用（No.20・No.21の修正後、未実行）とLstatの診断test | `c3d2f837c7ff0b2e23b998aa7030e6b98ca79618166d3c6d1894df016317ff7c` |
| `heterolog-windows-all-tests-20261023-r4.iso` | 6回目用（123 package、ログインできず未実行） | `e73c75bc4260fe253fab2a032b01f7f1438dfc4f482c4304cb64034e13c4f6f3` |
| `heterolog-windows-all-tests-20261023-r5.iso` | 7回目（123 package。作業担当が同じscriptで作成） | `b6f1404e5ce077c2428cbb3ccf0bfc49339579fdbc682fea9c68b1f7d7f16584` |
| `heterolog-windows-all-tests-20261023-r6.iso` | 8回目（round 3の修正後、124 package。作業担当が同じscriptで作成） | `6c862c565f31b9ea7a8ba2058d7dc4d063804f80cc6225d7560dd2c85b9706b1` |
| `heterolog-windows-all-tests-20261023-r7.iso` | 9回目（round 4の修正後、126 package。作業担当が同じscriptで作成） | `6fb8cea3c2978bc099c754fe870d79c2baa17b9306c3927f7357c67fe7ad420a` |
| `heterolog-windows-all-tests-20261024.iso` | 10回目（`3473fca`、127 package。並行・反復試験。作業担当が同じscriptで作成） | `d3b021a73a72f95be2f0a4fd260dfc202ae495521ee5ee3e0b8e295e3a978abc` |

Lstatの診断test（`\diag\worker-lstat-diag.test.exe`）は、各媒体と同じtreeにrepositoryへ含めない
診断用のtest fileを加えてbuildしたものです。5回目の媒体の診断testは、No.21の修正後の製品を使います。

- guestの結果: `C:\heterolog-all-tests-20261022\`の`a`・`b`・`c`（初回）、`a2`・`b2`・`c2`（途中）、
  `a3`・`b3`・`c3`（3回目）、`a4`・`b4`・`c4`（4回目）、`a6`・`b6`・`c6`（7回目）、`a7`・`b7`・`c7`（8回目）、`a8`・`b8`・`c8`（9回目）と反復・追加試験の各directory、`C:\heterolog-all-tests-20261024\`の`p1`〜`p3`・`a1`〜`a4`（10回目）。
- `TESTRESULTS`の`heterolog-all-tests-20261022`（初回〜3回目）、`heterolog-all-tests-20261023-r2`
  （4回目と診断testの出力）、`heterolog-all-tests-20261023-r5`（7回目）、`heterolog-all-tests-20261023-r6`（8回目）、`heterolog-all-tests-20261023-r7`（9回目）、`heterolog-all-tests-20261024`（10回目）へ、各回の`summary.tsv`・`totals.txt`・`failures.txt`・`skips.txt`・
  `environment.txt`・`runs.tsv`・`logs.zip`を書き出しました。既存のfileは変更していません。
- `results-transfer.img`は、区画表のないFATとして読むとFATの不整合が出ます。実際の`TESTRESULTS`は
  sector 128から始まる区画で、hostでは停止後に複製したimageを`@@65536`付きのmtoolsで読みました。

## 停止確認と後片付け

各回の終了時にguestで`shutdown /s /t 0`を実行し（5回目・6回目はログインできなかったため、ログイン画面の
電源メニューのシャットダウン）、起動scriptの終了code 0、QEMUとswtpmのprocessがないこと、QMP・TPMの
socketとPID fileが消えたことを確認しました。7回目は利用者が起動したVMを管理者コンソールの
`shutdown /s /t 0`で停止し、起動scriptのprocessが終了し、QEMUとswtpmのprocessが残っていないことを確認しました。
8回目はこちらでofflineで起動し、同じく`shutdown /s /t 0`で停止して、起動scriptの終了code 0とprocessの終了を
確認しました。8回目の停止時には、Windowsが保留していた更新を適用する画面（「29% 完了」）が表示され、
その後に電源が切れました。この更新はこちらの操作で開始したものではありません。
9回目も同じく`shutdown /s /t 0`で停止し、起動scriptの終了code 0と、qemu-kvm・swtpmのprocessが残っていないことを
確認しました。10回目も同じく`shutdown /s /t 0`で停止し、起動scriptの終了code 0と、QEMU・swtpmのprocessが
残っていないことを確認しました。`vnc.sock`の古いfile nodeは以前の記録と同じく残っていますが、
接続先processはありません。Windowsの設定、snapshot、UEFI/TPM状態、既存のISO・結果directoryは
変更していません。

試験用に作成した次のdirectoryは残しています。不要になれば削除できます。

| 場所 | 内容 |
| --- | --- |
| guestの`C:\heterolog-all-tests-20261022` | 各回の結果（`a`〜`c8`と反復・追加試験）、展開したsource tree、条件Aの`temp-a*` |
| guestの`C:\heterolog-long-directory-name-temp` | 条件B・CのTEMP（8.3短縮名`C:\HETERO~2`）。途中の回までに失敗・強制終了した試験の一時directory 2件（`TestArchiveCheckpointFactoryClosesNotificationsOnConstructionFai…`・`TestLogFileProcessingRecoveryCompletesLaterFileWithoutCrossingUn…`）が残っています |
| guestの`C:\heterolog-all-tests-20261024` | 10回目の結果（`p1`〜`p3`・`a1`〜`a4`）、展開したsource tree、`temp-p*`・`temp-a*` |
| `TESTRESULTS`の`heterolog-all-tests-20261022`・`heterolog-all-tests-20261023-r2`・`heterolog-all-tests-20261023-r5`・`heterolog-all-tests-20261023-r6`・`heterolog-all-tests-20261023-r7`・`heterolog-all-tests-20261024` | 書き出した集計と`logs.zip` |

guestでは、コマンドプロンプトから
`rmdir /s /q C:\heterolog-all-tests-20261022 C:\heterolog-all-tests-20261024 C:\heterolog-long-directory-name-temp`で削除できます。
4回目の診断testが既定の`TEMP`に作った一時directoryは、試験終了時に削除されたことを確認しました。
