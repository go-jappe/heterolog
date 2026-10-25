# Windows filesystem試験VM

## 今回の更新と検証範囲

現在のroot packageはGoライブラリの `heterolog` です。試験媒体は公開APIを含む
root試験を `heterolog.test.exe` として、他のpackageと同じ `timetzdata` タグでビルドします。
CIの`go test ./...`（windows-latest）と同じく、test fileを持つ全package
（2026-10-24時点で127 package）をWindows nativeで実行する媒体と実行scriptも用意しています。
手順は下記の「全package媒体」と「全package試験」を参照してください。

2026-10-22〜23に、ライブラリ切り出し後のコードで全packageを一般権限（通常のTEMP／8.3短縮名の
TEMP）と管理者の3条件で実行し、見つかった失敗を修正しました。結果・原因・修正と残る項目は
[全packageのWindows native試験](results-all-packages-20261022.md)を参照してください。
2026-10-24には、CIのWindows runnerで時々落ちる試験を探すため、同じ全package試験を複数同時にCPU2つへ
制限して繰り返し、失敗がないことを確かめました（同じ記録の10回目）。
Linuxでの試験とWindows向けcross-compileは、Windows上の実行結果を意味しません。
その後に変更したpackageは、全package試験を再実行して確認してください。
以下の多くの `results-*.md` は元プロジェクトの各実行時点の記録です。
歴史的な結果ファイルはそのまま保持します。

## 以前のWindows実行記録

2026-09-10に、このLinux上で作成した専用QEMU/KVM VMへ、Windows 11 Enterprise
Evaluation 25H2 日本語版を正式にインストールしました。ローカルアカウント`vmtest`での
ログイン、インストールISOなし・offlineでの再起動、NTFS上の試験を確認済みです。
初回試験後はWindowsから正常shutdownしました。以後の起動・試験結果は下記の履歴に記録しています。

導入初日（2026-09-10）の通常Windows上での試験は104件成功・17件失敗・1件skipでした。
この時点ではVMの構築のみ完了し、製品のWindows対応は未完了でした。詳細は[Windows 11試験結果](results-windows11-20260910.md)
を参照してください。

2026-09-11に同期関連testを10回ずつ再実行しました。圧縮file本体の同期は成功し、
現在の親directory同期は`Access is denied`で失敗しました。
詳細は[Sync再試験結果](results-sync-20260911.md)を参照してください。VMは正常停止済みです。

続いてWindows専用APIの比較試験を行い、書き込み権限付きdirectory handleによる
同期へ変更しました。spool/archiveのmtime更新後のfile同期も修正しています。
詳細と残る制約は[Windows同期APIの比較・修正結果](results-native-sync-20260911.md)を
参照してください。電源断耐久性やWindows ACLの検証完了を意味するものではありません。
最終版は関連43 testの一般権限10回反復がすべて成功しました。全体試験は388件成功・
2件失敗・3件skipで、残る失敗はUnix権限bitの検証です。VMは正常停止済みです。

同日、checkpointの不要な上位directory同期とWindowsのpath比較も見直しました。
この時点の範囲・試験結果・未完了事項は
[checkpoint同期範囲とpath identity](results-checkpoint-scope-20260911.md)を参照してください。

続いてWindowsのACL対応と権限関連試験を実装しました。この時点の試験結果と適用範囲は
[Windows ACLと権限関連試験](results-acl-20260911.md)を参照してください。

その後、同期未確認の再試行とerror/report分類を実装し、Windowsで未実行だった
権限・Stat異常試験を補完しました。この時点の実装範囲と結果は
[同期未確認の再試行とWindowsスキップ解消](results-durability-20260911.md)を参照してください。
上記の古い実行結果は各時点の履歴として残しています。

同日、共有保存先の初期化競合とファイル名の大小文字対応を修正しました。
この時点の修正・試験結果は[Windows共有初期化とファイル名の大小文字対応](results-sharing-case-20260911.md)
を参照してください。

続いて大小文字の違いを設定検証でも衝突として拒否するように修正しました。
この時点の範囲と修正前後のVM試験結果は
[Windows圧縮ログ名の衝突検証](results-name-validation-20260911.md)を参照してください。

続いて、checkpointのACL設定と一覧取得が競合するWindowsの共有違反を修正しました。
この時点の範囲と両権限での修正前後の試験結果は
[Windows checkpoint一覧取得の共有違反修正](results-checkpoint-readdir-20260911.md)を参照してください。

続いて、Windowsのメモリ監視で過去最大値やRSSを現在のcommit量として使う問題を修正しました。
この時点の範囲と一般／管理者権限、amd64／386での修正前後の試験結果は
[Windowsメモリ監視の現在使用量への修正](results-memory-pressure-20260911.md)を参照してください。

続いて、通常のアプリビルドにもタイムゾーンデータを同梱しました。試験用タグに依存せず、
外部データがない条件でloggerのタイムゾーンを解決できることを確認しています。
この時点の範囲と試験結果は[アプリへのタイムゾーンデータ同梱](results-timezone-20260911.md)を参照してください。

続いてJob全体・process単体の両メモリ上限の判定と、`[]`を含むログ名のliteral照合を修正しました。
この時点の範囲と試験結果は[両メモリ上限とログ名のliteral照合](results-dual-limit-literal-names-20260911.md)
を参照してください。

続いて、failure archiveの基準パスの大小文字変更でtargetが二重に付く問題を修正しました。
この時点の範囲と修正前後のVM試験結果は
[failure archive基準パスの大小文字判定](results-archive-base-case-20260911.md)を参照してください。

続いて、ログファイル名に使用する設定値のcolonをOS共通で拒否するように修正しました。
保存先pathのドライブ指定は制限しません。この時点の範囲と試験結果は
[OS共通のログファイル名colon検証](results-log-file-name-validation-20260912.md)を参照してください。

その全体試験で残した51件について、試験準備・path比較・媒体・VMの条件を修正しました。
最新の範囲と試験結果は[Windows全体試験の51件の失敗解消](results-test-portability-20260912.md)
を参照してください。元の実運用`.env`はVMへ持ち込みません。
対象8 packageは管理者実行で1097成功・0失敗・6既存skipでした。元の失敗51件は
一般権限47件／symlink作成権限が必要な4件に分け、各10回成功しています。
最新の検証後もWindowsから正常shutdownし、VMとTPMのprocess停止を確認済みです。

2026-10-22〜23には、CIの`go test ./...`と同じ範囲の全packageを初めてWindows nativeで実行しました。
修正前は一般権限で121 package中11（8.3短縮名のTEMPと管理者では12）packageが失敗しました。
原因を製品・試験・環境に分けて修正し、7回目（123 package）、round 3の試験監査・修正後の8回目
（124 package）、round 4の修正後の9回目（126 package）では、通常のTEMP・8.3短縮名のTEMP（一般権限）と
管理者の3条件すべてで全packageが成功しました。
5回目と6回目は自動ログインのpasswordが一致せず、試験を実行していません。詳細は
[全packageのWindows native試験](results-all-packages-20261022.md)を参照してください。

既存Windows 10 ISOのセットアップ画面までの起動と、インストール前環境での予備実行は
確認しました。これは通常のWindows上での検証完了を意味しません。
WinPEは汎用OSではなく、今後の継続的な試験環境には使用しません。

VMの状態は次のdirectoryに保存しています。Gitへ仮想ディスクやISOを追加しません。

```text
/path/to/virtual-machines/heterolog-windows/
```

- QEMU 6.2.0、KVM有効（QMPの`query-kvm`で確認）
- 4 vCPU、8 GiB RAM、上限80 GiBのqcow2
- VM専用UEFI状態、TPM 2.0状態
- 既定は外部networkなし。activation/OOBE時のみuser-mode NATを明示指定
- host directory共有・port forwardingなし
- VNCとQMPはVM directory内のUnix socketのみ
- `results-transfer.img`は結果取り出し用64 MiB仮想USB。MBRの区画表があり、FAT16の
  `TESTRESULTS`はsector 128（offset 65536）から始まります。hostではVM停止中に複製した
  imageを`MTOOLS_SKIP_CHECK=1 mdir -i 複製.img@@65536 ::/`のように読みます
- `system.qcow2`にWindows 11 Enterprise Evaluationをインストール済み
- 導入済みqcow2 snapshot: `windows11-eval-installed-20260910`
- 上記snapshotと同時点のUEFI/TPM: `installed-state-20260910/`

`vmtest`のpasswordは、VM状態directory内の`login-password.txt`（permission `0600`）に
保存しています。内容をREADME、試験メディア、Git、結果reportへ転記しません。

hostの自動起動設定、libvirtの認証設定、host bridge、firewallは変更していません。

## 導入したWindows評価版

[Microsoft公式ISO（Windows 11 Enterprise Evaluation 25H2 日本語版）](https://aka.ms/Win11E-ISO-25H2-jp-jp)
を使用しています。90日期限の評価版で、評価利用に製品キーの購入は不要です。
期限後も無期限に利用できるライセンスではありません。
正規オンライン認証は成功し、guestで確認した期限は2026-12-09 15:27:42です。
snapshotの復元によって評価期限が延長されるものではありません。

取得したISOのSHA-256:

```text
d86956634d5ee8ec91ac8e7c0b28a0f7e58d3a5b661bf661a1220e6a3f4649ce
```

## 試験メディアの作成

### 全package媒体

CIの`go test ./...`と同じく、`go list`でtest fileを持つ全packageを列挙してtest exeを作ります。
Linuxの`go`、`genisoimage`、`isoinfo`、`rsync`、`tar`を使い、出力先は作業tree外の未作成directoryを指定します。

```bash
bash testenv/windows/build-all-package-test-media.sh /tmp/windows-all-tests-next
```

- toolchainはgo.modの`go`行（現在1.27.3）を`GOTOOLCHAIN`に指定します。cache済みならofflineで動きます。
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0`、`-tags=timetzdata`、`-mod=readonly`でpackageごとに
  `go test -c`を実行します。package末尾の名前は重複するため、媒体上のexe名はmodule rootからの
  directoryを`-`でつないだ名前（rootは`heterolog`）にし、`packages.tsv`へimport pathとdirectoryを記録します。
- git checkoutに含まれる範囲だけをrsyncで複製し、`source.tar`として同梱します。`.gitignore`の除外対象
  （`/.env`、`/.env.*`（`.env.example`は含める）、各種tool directory、build・test出力）と`/.claude/`、
  `/.git`は入れません。除外漏れがあれば媒体を作らず、test exeも同じ複製からbuildします。
- test fileのない`cmd/log-recovery`は、起動とusage表示を確かめる`log-recovery.exe`だけを入れます。
- 出力directoryには`heterolog-windows-all-tests.iso`のほか、ISOの全file一覧`iso-files.txt`、
  `source.tar`の全entry一覧`source-files.txt`、SHA-256一覧を残します。持ち込む前に一覧で
  `.env`や認証情報がないことを確認してください。

### 個別package媒体

Linuxの`go`（go.modの`go`行に合わせたtoolchainを`GOTOOLCHAIN`で指定）と`genisoimage`を使います。
出力先は未作成directoryを指定します。

```bash
bash testenv/windows/build-test-media.sh /tmp/windows-tests-next
```

作成するISOには、次の28 packageのテストexe、4つの実行script、
件数と終了codeだけを表示する集計scriptを含めます。

- `internal/infrastructure/fsstore`
- `internal/infrastructure/fsstore/appendlog`
- `internal/infrastructure/fsstore/failurearchive`
- `internal/infrastructure/logfile`（`logfile.test.exe`）
- `internal/infrastructure/logfile/lifecycle`（`lifecycle.test.exe`、inline・周期runner・手動CLIで共有する一巡処理）
- `internal/infrastructure/logfile/runner`（`runner.test.exe`）
- `internal/infrastructure/fsstore/checkpoint`（`checkpoint.test.exe`）
- `internal/infrastructure/fsstore/spool`（`recordspool.test.exe`、通常ログのrecord spool directory同期）
- `internal/infrastructure/fsstore/spool/store`（`recordspoolstore.test.exe`、file同期・公開・directory同期の永続化契約）
- `internal/infrastructure/fsstore/spool/objectstorage`
- `internal/infrastructure/fsstore/failurearchive/compressedlog`（`compressedlog.test.exe`）
- `pkg/path/pathidentity`
- `pkg/path`
- `internal/infrastructure/fsstore/directorylease`
- `internal/config`
- `internal/infrastructure/logger/file`
- `internal/infrastructure/logger/flushfailure/fsarchivehandler`
- `internal/infrastructure/fsstore/name/logpattern`
- `internal/infrastructure/spool/compressedlog`（`compressedlogspool.test.exe`）
- `internal/infrastructure/spool/compressedlog/worker`（`compressedlog_worker.test.exe`）
- `internal/infrastructure/spool/compressedlog/uploader`（`compressedlog_uploader.test.exe`）
- `internal/infrastructure/spool/compressedlog/uploader/s3`（`compressedlog_s3_uploader.test.exe`）
- `internal/infrastructure/objectstorage/compressedlogfailure`
- `pkg/memorypressure`
- `internal/observer/memory/pressure`
- library root（`heterolog` package、`heterolog.test.exe`）
- `pkg/fsstore`（`fsstore_config.test.exe`）
- `internal/config/validation/mapped`（`mapped_config.test.exe`）

config全体試験に必要な`.env`／`.env.example`は、公開exampleだけから
[`configfixture`](configfixture/README.md)が生成する固定dummy値のものを同梱します。
実運用`.env`、元の認証情報や私的な宛先、元のコメントは持ち込みません。
このfixtureを使うnative試験は、実運用設定の監査ではなく設定読込・mapping契約の確認です。

`run-tests.cmd`はfsstore、appendlog、failurearchive、logfile、logfile lifecycle、logfile runner、checkpoint、
通常ログrecord spoolのdirectory／file永続化、圧縮ログspoolのファイル保存、
圧縮ログfailure archive、heterolog library root、logpattern、
memorypressure、memory monitor、path builderの全体試験を実行します。
`run-path-sync-tests.cmd NEW_RESULTS_DIR`は一般権限のcommand promptで、
checkpoint同期範囲、path identity、private permissions、同期未確認の再試行・報告の
対象試験を14 packageで10回ずつ実行します。通常ログrecord spoolの2 packageも
全体試験を10回実行し、同期失敗の再試行・公開前のfile同期・公開後のdirectory同期を含めます。
後者は既存の結果directoryを拒否し、各packageの終了codeも保存します。
`run-sharing-case-tests.cmd NEW_RESULTS_DIR`は、logpattern／spool/compressedlog／compressedlogfailureの全体試験と、
Windowsの共有保存先初期化・大小文字変更の回帰試験を実行します。回帰試験は
FileLogger、logfile runner、appendlog checkpoint、FsArchiveHandlerを含む9 packageで10回ずつ繰り返します。
このscriptも既存の結果directoryを拒否します。
`summarize-results.ps1 RESULTS_DIR`はVM内で結果を集計して画面に表示します。
生ログをhostへ転送する機能はありません。

`run-logger-tests.cmd NEW_RESULTS_DIR`は、pkg/fsstore、appendlog、failurearchive、
FileLogger、logfile、logfile runner、checkpointの3 package、mapped config、config、FsArchiveHandlerの全体試験を実行します。
configだけは同梱fixtureを作業directoryにします。結果directoryは未作成のものを指定します。
`summarize-results_test.ps1 NEW_RESULTS_DIR`は合成ログで集計処理を検証します。
本物のsymlinkを作る試験にはWindowsの作成権限が必要です。永続設定を変更せず
管理者コンソールで全体試験を実行する方法と、一般権限の検証結果を最新reportに記録します。

`GOOS=windows GOARCH=amd64 CGO_ENABLED=0`でビルドします。rootを含む全packageの
試験には `timetzdata` タグでタイムゾーンデータを含めます。rootではGoライブラリの
公開APIとslog連携を含む通常のテストsuiteを実行します。
guest側にGoやzoneinfoを入れる必要はありません。
race detector付きの試験ではありません。実運用`.env`、認証情報、作業treeは持ち込みません。

前回の同期API修正の試験メディアは、VM状態directory内の`fsstore-windows-native-sync-tests-20260911-v2.iso`です。
checkpoint同期範囲/path比較の最新版は上記の新しい結果reportに記載しています。
修正前の試験メディア`fsstore-windows-sync-tests-20260911.iso`と、directory同期のみを
先に修正した`fsstore-windows-native-sync-tests-20260911.iso`も保持しています。
以前の試験メディア`fsstore-windows-tests-timetzdata.iso`も保持しています。
上記の作成scriptの出力名は`fsstore-windows-tests.iso`で、現在のメディアは予備試験用の
旧ISOと区別するため別名で保存しています。

## VMの起動・停止

### 自動キーボード入力

[`control-vm.py`](control-vm.py)は、このVMの所有者専用QMP／VNC Unix socketを使います。
外部ネットワークやguest agentは不要です。起動scriptは継続する端末（CodexではPTY）で動かし、
操作コマンドは一つずつ完了を待ってください。同時に複数のQMP操作を起動しません。

```bash
vm_state_dir=/path/to/virtual-machines/heterolog-windows
python3 testenv/windows/control-vm.py "$vm_state_dir" status
python3 testenv/windows/control-vm.py "$vm_state_dir" key ret
```

2026-10-23の5回目・6回目は、次の自動入力でpasswordが一致しませんでした（原因は未確認）。
7回目以降は利用者が手動でログインし、操作側はログインを待つ間は`screen`で画面を取得するだけで
入力せず、ログイン後のdesktopから操作しています。表示は5分の無操作で消えるため、画面が真っ暗なときだけ
`key shift`で表示を戻し、画面を確認してから次の入力を送ります。ロック画面やpassword欄が表示されたら
入力を止めてください。

自動で入力する場合は、ログイン画面のpassword欄が空で、フォーカスがあることを確認してから、次を実行します。
必要なら`click x,y,画面幅,画面高さ`で欄をクリックします。800×600の画面なら
`python3 testenv/windows/control-vm.py "$vm_state_dir" click 300,360,800,600`です。
`password`は保存済み`login-password.txt`をメモリで読み、入力します。
パスワードをコマンド引数や出力に含めず、Enterは別操作です。表示ボタンは押さないでください。

```bash
python3 testenv/windows/control-vm.py "$vm_state_dir" password
python3 testenv/windows/control-vm.py "$vm_state_dir" key ret
```

文字入力は毎回QEMUのLED状態を取得し、CapsLockをOFF、NumLockをONに揃えます。
記号はUS／JIS配列の違いを避けてAlt＋テンキーで入力するため、NumLockが必要です。
ログイン前後で状態が変わることがあるので、一度だけの切り替えには依存しません。
状態を取得・確認できない場合は入力せずエラーにします。password欄以外へ実パスワードを
入力したり、パスワードを表示して確認したりしないでください。

```bash
python3 testenv/windows/control-vm.py "$vm_state_dir" keyboard
python3 testenv/windows/control-vm.py "$vm_state_dir" key meta_l-r
python3 testenv/windows/control-vm.py "$vm_state_dir" text cmd
python3 testenv/windows/control-vm.py "$vm_state_dir" key ret
python3 testenv/windows/control-vm.py "$vm_state_dir" screen /tmp/windows-check.ppm
```

`screen`は未作成のファイルへPPMを保存します。終了はguestで`shutdown /s /t 0`を実行し、
起動scriptの正常終了とVM／TPM processの終了を確認します。`powerdown`はACPI電源button要求で、
Windowsの設定によっては終了しません。画面が暗くなっただけでは終了と判断しないでください。
パスワード不一致時に連続試行やresetを行う仕組みはありません。ログイン画面では事前の
Ctrl+A／Backspaceを自動送信せず、空欄を確認してから入力します。
入力準備の単体試験は`PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s testenv/windows -p test_control_vm.py`です。

### 上書き禁止移動・filesystem判定の実行試験

[`run-no-replace-tests.ps1`](run-no-replace-tests.ps1)へ、今回ビルドした
`fsstore.test.exe`・`archive.test.exe`・`spool.test.exe`がある媒体のdirectoryと、
既存のACL非対応試験volume（例: `TESTRESULTS`のFAT）のrootを指定します。
下記のドライブ文字は実際の`Get-Volume`の結果に合わせます。
各exeは既存の試験媒体と同じく`GOOS=windows GOARCH=amd64 go test -mod=readonly -tags=timetzdata -c`
でビルドします。対応packageは順に`internal/infrastructure/fsstore`、
`internal/infrastructure/fsstore/failurearchive/compressedlog`、`internal/infrastructure/fsstore/spool/objectstorage`です。

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File F:\run-no-replace-tests.ps1 G:\ E:\
```

通常試験はNTFS等の`TEMP`、移動処理の追加試験は指定volume内の新しい一時領域で行います。
volumeのformatや既存ファイルの削除は行いません。診断のstderrは別ファイルへ保存し、
試験の成否はexeの終了codeで判断します。各試験群に失敗があればscriptも非zeroで終了します。
pass／failは最上位test、skipはsubtestも含めて集計します。

### 起動コマンド

インストール済みVMを今後起動するコマンドです。第2引数`-`でインストールISOを
接続せず、現在の試験メディアだけを接続します。既定はofflineです。

```bash
bash testenv/windows/start-vm.sh \
  /path/to/virtual-machines/heterolog-windows \
  - \
  /path/to/virtual-machines/heterolog-windows/heterolog-windows-all-tests-20261023-r7.iso
```

第4引数は`offline|nat`です。省略時または`offline`指定時は`-nic none`で起動します。
Microsoft評価版のactivation/OOBEで通信が必要な間だけ、上記コマンドの末尾に`nat`を
追加します。NATはQEMU user-mode network（`e1000e`）を使い、port forwardingや
host bridgeは作りません。通常の試験では停止後にofflineで起動し直します。
未知のnetwork modeは、VM起動などの副作用より前に終了code 2で拒否します。

起動中の画面は、このhostに導入済みのTigerVNC Viewerから開けます。別の端末で実行します。
以下ではclipboardの送受信も無効にしています。

```bash
vncviewer -Shared -SendClipboard=0 -AcceptClipboard=0 \
  /path/to/virtual-machines/heterolog-windows/vnc.sock
```

VM内でデータを永続化する場合は、Windows自身のshutdownを使います。
launcherへのTERM/INT/HUPは緊急停止であり、guestの正常shutdownを保証しません。

launcherは手前で実行を待つ形です。KVMが使えない場合は失敗させ、TCGへ暗黙には
切り替えません。起動中のlauncher scriptそのものを書き換えないでください。

launcherの動作だけを確認するには、次を実行できます。実VMには触れず、通常終了・
異常終了・TERM/INT/HUP・起動途中の停止、およびnetwork modeの引数をmock processで
検証します。

```bash
bash testenv/windows/start-vm_test.sh
```

## Windowsでの実行

通常動作は一般権限のコマンドプロンプトで試験します。symlink作成権限がない環境では、
既存の試験によってfixtureの作成失敗またはskipになります。作成可能な権限での実行も
別途行い、そこで本物のsymlinkを使った検証が成功することを確認します。
製品の通常実行に管理者権限を要求するものではありません。
試験ISOのドライブが`G:`なら、次のように実行します。
結果保存先と一時directoryは、試験したいNTFS volumeを使ってください。
ドライブ文字は接続したメディアによって変わるため、`run-tests.cmd`の存在を確認してください。

```bat
mkdir C:\fsstore-test-temp
set TEMP=C:\fsstore-test-temp
set TMP=C:\fsstore-test-temp
G:\run-tests.cmd C:\fsstore-test-results\run-001
```

各packageのlogと終了code、全体の終了codeを保存します。失敗しても残りのpackageを実行し、
各exeに180秒のtimeoutを設定します。既存の同名結果は上書きしません。

### 全package試験（CIの`go test ./...`相当）

[`run-all-package-tests.cmd`](run-all-package-tests.cmd)は、process限定の
`-ExecutionPolicy Bypass`で[`run-all-package-tests.ps1`](run-all-package-tests.ps1)を起動します。
永続的な実行ポリシーやWindowsの設定は変更しません。scriptは次の順に処理します。

- `-ResultsDirectory`（未作成）の`src`へ`source.tar`をWindows同梱の`tar.exe`で展開します。
- `TEMP`／`TMP`を`-TempDirectory`の値に設定します。8.3短縮名を渡すと短縮名のまま使います。
- 各packageのexeを`TEMP`下の新しい`b001`へpackage末尾の名前でcopyし、そのpackageのsource directoryを
  作業directoryにして`-test.paniconexit0 -test.timeout=10m0s -test.v=true`で起動します。
  標準出力とerrorを一つのlogへ順に保存し、`go test`と同じく既定ではtimeoutの1分後にprocess treeを止めます。
- packageは一つずつ実行し、失敗しても続けます。`logs`へpackageごとのlogと終了code、`runs.tsv`へ所要時間を記録します。
- 最後に`log-recovery.exe`の起動確認、`TEMP`に残ったentry名、checkout複製へ試験が追加・削除したfileを記録し、
  [`summarize-all-package-results.ps1`](summarize-all-package-results.ps1)で集計します。
- `-ExportDirectory`（未作成）を指定すると、集計file（`summary.tsv`、`totals.txt`、`failures.txt`、
  `skips.txt`等）と全logの`logs.zip`を書き出します。64 MiBの`TESTRESULTS`へ直接出力できます。

集計はpackageごとに最上位testとsubtestを分けてPASS／FAIL／SKIPを数えます。各結果行は自身の
`=== RUN`より後だけを数えるため、試験がlogへ出した子processの出力を重複計上しません。
`failures.txt`と`skips.txt`には直前のlog行（失敗・skipの理由）も残します。
集計の自己試験は`summarize-all-package-results_test.ps1 NEW_DIR`です。

CIのwindows-latestは管理者で動き、`TEMP`は`C:\Users\RUNNER~1\AppData\Local\Temp`のような
8.3短縮名です。また、CIのcheckoutは`D:\a\heterolog\heterolog`にあり、`TEMP`（`C:`）と別のdriveです。
各packageの作業directoryから一時directoryへの相対pathは作れないため、相対pathの試験は一時directoryへ
`t.Chdir`してから作ります。runnerの一時directoryの子は、自動継承形式（`SE_DACL_AUTO_INHERITED`）でない
DACLを持ちます。次の3条件で実行します。ドライブ文字は`Get-Volume`等で確認してください。

```bat
rem A: 一般権限・通常の長いpathのTEMP
mkdir C:\heterolog-all-tests-YYYYMMDD\temp-a
G:\run-all-package-tests.cmd -ResultsDirectory C:\heterolog-all-tests-YYYYMMDD\a -TempDirectory C:\heterolog-all-tests-YYYYMMDD\temp-a -ExportDirectory E:\heterolog-all-tests-YYYYMMDD\a
rem B: 一般権限・8.3短縮名のTEMP（dir /x で短縮名を確認）
mkdir C:\heterolog-long-directory-name-temp
dir /x /ad C:\
G:\run-all-package-tests.cmd -ResultsDirectory C:\heterolog-all-tests-YYYYMMDD\b -TempDirectory C:\HETERO~2 -ExportDirectory E:\heterolog-all-tests-YYYYMMDD\b
```

C（管理者）は、Win+Rで`cmd`を入力してCtrl+Shift+Enterで開き、UACで許可した管理者
コンソールから、Bと同じ`-TempDirectory`で実行します。`fsutil 8dot3name query C:`は読み取りだけの
確認で、管理者コンソールで実行できます。8.3短縮名の作成が無効なvolumeでは、設定を変更せず
Bを実施できないことを記録してください。

再実行では`-Package`にmanifest名（例: `internal-config,internal-testutil`）、`-TestRun`と`-Count`に
`-test.run`／`-test.count`の値を指定できます。終了後はguestで`shutdown /s /t 0`を実行し、
停止を確認してから`results-transfer.img`を複製し、上記のoffsetを指定したmtoolsで読みます。

```bash
cp /path/to/virtual-machines/heterolog-windows/results-transfer.img /tmp/results-copy.img
MTOOLS_SKIP_CHECK=1 mcopy -s -n -i /tmp/results-copy.img@@65536 ::/heterolog-all-tests-YYYYMMDD /tmp/results/
```

## 初回予備試験

詳細は[2026-09-10のWinPE予備試験記録](results-20260910.md)を参照してください。
この記録は正式インストール前の結果を保持したもので、当時の「未完了の項目」やVM状態も
その時点の情報です。現在の導入状況は本READMEを参照してください。
通常のWindows上での結果は[Windows 11試験結果](results-windows11-20260910.md)です。
製品のWindows対応や電源断耐性の検証完了は主張しません。

## macOS

この非Apple製LinuxホストにはmacOS VMを作りません。Appleの標準ライセンスでは、
macOS VMの開発・試験用途はmacOSが動作する所有・管理下のMacを前提とします。
macOS側は手元Macで直接試験するか、Mac上にVMを作成する別作業になります。

参考:

- [Windows 11評価版の入手条件](https://www.microsoft.com/en-us/evalcenter/evaluate-windows-11-enterprise)
- [WinPEの用途と制限](https://learn.microsoft.com/en-us/windows-hardware/manufacture/desktop/winpe-intro)
- [macOS Tahoeのソフトウェア使用許諾契約](https://www.apple.com/legal/sla/docs/macOSTahoe.pdf)
- [Apple Silicon Mac上のmacOS VM](https://developer.apple.com/documentation/virtualization/running-macos-in-a-virtual-machine-on-apple-silicon)
