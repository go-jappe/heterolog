# Checkpoint同期範囲とWindows path identity（2026-09-11）

これは当時の試験記録です。後続の[Windows ACL試験](results-acl-20260911.md)で
ACL対応と権限関連試験を修正し、追記したSourceIDのWindows回帰試験も実行済みです。

## 対応範囲

checkpoint directoryの作成を`fsstore.EnsureDirectoryDurably`へ共通化しました。
不足するdirectoryを作成し、その新規entryの親だけを同期します。既存ancestorを
filesystem rootまで開いて同期する処理は、runner/圧縮log upload checkpointの両方から撤去。
file公開後の保存先directory自身の同期は従来どおり行います。

以前もCドライブ内の全fileをflushしていたわけではありません。checkpointの親を
たどった結果、`C:\`のdirectory情報まで同期対象にしていた問題です。

作成後にmkdir/chmod/syncが失敗した場合、未同期の親をprocess内で保持します。
同じ保存先を扱う別Storeも未完了分を引き継ぎ、同期できた親だけを集合から除きます。
初期化用lockはwriter/lifecycle lockとは別で、通常のcheckpoint更新では使いません。
既存ancestorがこのhelper以外で新規作成された場合の未同期状態は追跡しません。

path比較は`pkg/path/pathidentity`へ集約しました。

- Windowsの標準case-insensitive directoryでは大小文字と通常の`\\?\` drive/UNC aliasを統一。
- LinuxなどUnixでは大小文字を区別する従来の契約を維持。
- 設定の所有範囲検証、directory lease、guard/cache共有、purge保護、upload成功記録を同じ規則で照合。
- 比較keyと実操作pathは分離。file名、S3 object key、checkpoint JSONのPath表記は変更しない。
- 通常/error/debugの同名fileを生むprefixの大小文字違いもWindowsでは重複として拒否。

**このISOでの試験時点では、ArchiveCheckpointSourceIDの生成規則は確認待ちで未変更です。**
既存IDを無断で切り替えてcheckpointを見失わないよう、今回は比較部分だけを変更しています。
Unixの既存source IDは固定期待値の回帰試験でも確認しました。
`.env`、送信先、圧縮方式、保持期間、ACLは今回変更していません。

### 追記: 未運用の確認後のSourceID統一

利用者からアプリはまだ運用されていないとの回答を受け、既存運用データの移行を作らず、
SourceIDのfingerprintにも共通のdirectory比較規則を適用しました。Windowsでは
prefix/target/extensionの大小文字も統一し、Unixの既存IDは維持します。
sourcePrefix/classはfilesystem pathではない論理的な分類なので変更しません。

OS別の大小文字規則、Windowsのdrive/UNC extended alias、および表記を変更して
Storeを再生成してもupload成功履歴を引き継ぐ回帰試験をrepositoryへ追加しました。
この追記分はLinuxのappendlog配下/pathidentityの`go test -race`とWindows向け
試験compileを確認済みですが、まだVMで実行していません。以下のISOとWindowsの
実行件数は追記前の結果であり、追加試験の実行成功を表すものではありません。

## 旧処理の再現

### Linux

既存上位directoryをmode `0111`（通過可能・読取不可）、その下の作業directoryを
`0700`として、runnerと圧縮log uploadの両checkpointを作成する永続回帰試験を追加。
旧全ancestor同期を診断用overlayで再現すると、両方とも上位を開く時点で
`permission denied`になります。新実装は同じ試験が成功しました。

永続試験は`internal/infrastructure/fsstore/durability_directory_integration_test.go`の
`TestCheckpointCreationDoesNotOpenExistingExecuteOnlyAncestors`です。
`/tmp/checkpoint-ancestor-red.Hu5JuK/overlay.json`は旧動作との比較専用であり、
回帰試験そのものを一時directoryに置いたわけではありません。

### Windows 11 / NTFS / 一般権限

修正前の既存ISOに含まれる次の2試験が、どちらも`C:\`を同期用に開く時点で
`Access is denied`になりました。

- `TestFilesystemCheckpointStoreSeparatesSourceDestinations`
- `TestFilesystemStoreKeepsFirstDiscoveryDestinationSnapshot`

修正後は、この2試験も含む以下の一般権限の反復試験が成功しました。

## 最終版の対象反復試験

各10回、timeout 180秒。PASS/FAIL/SKIPは最上位testを数え、subtestを重複計上しません。
各packageの終了codeと全体終了codeは全て0です。

| Package | PASS | FAIL | SKIP |
| --- | ---: | ---: | ---: |
| fsstore | 90 | 0 | 0 |
| appendlog | 200 | 0 | 10 |
| runner | 70 | 0 | 0 |
| compressedlogcheckpoint | 30 | 0 | 0 |
| pathidentity | 70 | 0 | 0 |
| directorylease | 130 | 0 | 10 |
| config | 40 | 0 | 0 |
| logger/file | 40 | 0 | 0 |
| fsarchivehandler | 10 | 0 | 0 |
| 合計 | 680 | 0 | 20 |

skipはUnix限定のsource ID互換性確認とsymlinkを作成できない環境のlease試験です。
「680回成功」は68の最上位testを10回ずつ実行した数であり、680種類の試験ではありません。

## 一般権限の7 package全体試験

| Package | PASS | FAIL | SKIP |
| --- | ---: | ---: | ---: |
| fsstore | 38 | 4 | 1 |
| appendlog | 70 | 1 | 1 |
| failurearchive | 22 | 0 | 0 |
| runner | 192 | 2 | 2 |
| compressedlogcheckpoint | 23 | 0 | 0 |
| objectstorage spool | 27 | 1 | 2 |
| compressedlog archive | 18 | 1 | 4 |
| 合計 | 390 | 9 | 10 |

全体終了codeは1です。9件の失敗は次の内訳で、全体PASSとは扱いません。

- 2件: 既知のUnix permission bit期待値（checkpoint 0700/0600、archive 0640）。Windows ACL未対応。
- 7件: symlinkを試験fixtureとして作成する際の`A required privilege is not held by the client`。
  Windows一般権限でsymlinkを作る権限がないため、製品の拒否処理へ入る前に失敗。
  試験を一律skipへ変更する修正や、WindowsのDeveloper Modeを有効にする変更はしていない。

同じISOを管理者権限でも実行し、symlinkを作成できる条件で確認しました。

| Package | PASS | FAIL | SKIP |
| --- | ---: | ---: | ---: |
| fsstore | 42 | 0 | 1 |
| appendlog | 71 | 0 | 1 |
| failurearchive | 22 | 0 | 0 |
| runner | 193 | 1 | 2 |
| compressedlogcheckpoint | 23 | 0 | 0 |
| objectstorage spool | 30 | 0 | 0 |
| compressedlog archive | 22 | 1 | 0 |
| 合計 | 403 | 2 | 4 |

一般権限でのsymlink作成時の7件の失敗は解消しました。残る2件は既知のUnix権限bit検証です。
製品の通常実行に管理者権限が必要という意味ではなく、試験fixture作成の権限を切り分けた結果です。

## Linuxとビルド確認

- `go test -race`でconfig配下、fsstore配下、FileLogger、FsArchiveHandler、path helperの全体PASS。
- 同期の失敗/panic/同じ保存先の並行初期化/未完了分の引継ぎ/無関係な失敗の分離を反復確認。
- Windows向けアプリ全体buildと、変更に関連するpackageのWindows向けvetが成功。
- VM launcherの既存mock試験9ケース、shell構文、diff空白検査が成功。

## 再現・結果の保存

```bash
bash testenv/windows/build-test-media.sh /tmp/windows-path-sync-next
```

一般権限のWindows command promptで、試験ISOがG:の場合:

```bat
G:\run-path-sync-tests.cmd C:\Users\vmtest\path-sync-next
G:\run-tests.cmd C:\Users\vmtest\path-sync-full-next
```

今回のISO: `/tmp/windows-path-sync-media-20260911c/fsstore-windows-tests.iso`

SHA-256: `056f95371097c26d7c5f472dcb9f064089231cf59190c34206f04f7e5e1564ca`

生ログと終了codeはVM内の次のdirectoryに保存しています。

- `C:\Users\vmtest\checkpoint-scope-before-20260911a`
- `C:\Users\vmtest\checkpoint-scope-after-20260911b`（中間版）
- `C:\Users\vmtest\checkpoint-scope-after-20260911c`（最終反復）
- `C:\Users\vmtest\checkpoint-scope-full-20260911c`
- `C:\Users\vmtest\checkpoint-scope-admin-20260911c`

生ログの仮想USB経由でのLinuxへの回収は権限審査により拒否され、未実施です。
別経路での回収はしていません。このreportの集計はVM内で算出した件数と終了codeを画面確認したものです。
試験後はWindowsのshutdownを実行し、QEMU/TPM processの終了とsocketの撤去を確認しました。
VM diskと上記の生ログは保持しています。

## 未完了・適用範囲

- 追記したSourceID回帰試験のWindows VMでの実行。移行対応は未運用のため不要。
- Windows ACLの実装と検証。権限bitの試験を無条件にPASS扱いにはしない。
- Windowsのcase-sensitive directory、symlink/junction/8.3短縮名の実体によるalias解決は対象外。
- spool/圧縮failure archiveの別directory作成helperには、mkdir後の親同期失敗を次回に
  引き継がない既存処理がある。rootまで同期する問題とは別で、今回の変更には含めていない。
- 今回も電源断・kernel crash試験、macOS実機試験、NTFS以外のfilesystem試験はしていない。
