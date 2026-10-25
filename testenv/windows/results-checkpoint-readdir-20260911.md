# Windows checkpoint一覧取得の共有違反修正（2026-09-11）

## 修正範囲

runnerの`FilesystemCheckpointStore.listSourceCheckpointFilesFromDiskLocked`が使う
一覧取得を`fsstore.ReadDirectoryEntries`へ変更しました。変更対象の一覧取得は1箇所です。

- Windowsでは`GENERIC_READ`とdirectory用flagで開き、READ／WRITE／DELETE共有を指定。
  別sourceが同じdirectoryのACLを設定するためのhandleと共存できます。
- 読込用handleをCloseし、entryを名前順へ並べて返します。entry情報も保持します。
- LinuxなどWindows以外では、従来の`os.ReadDir`へそのまま委譲します。
- ACLの許可対象や既存childへ権限を伝播させない契約、checkpointの保存・fsync・
  memory cacheは変更していません。`.env`とメモリ監視の実装も未変更です。

`FILE_SHARE_DELETE`は他handleのDELETE accessと共存するための指定です。
読込側に削除権限を付与したり、ACLを緩めたりする処理ではありません。

## 回帰テスト

次の6 testを追加しました。試験用の引数・関数注入は製品コードに追加していません。

- `TestReadDirectoryEntriesMatchesReadDir`：名前順、元表記、directory/file区別、Infoを維持。
- `TestReadDirectoryEntriesEmptyAndInvalidPaths`：空directory、不存在、file、空path、NUL入りpath。
- `TestReadDirectoryEntriesLongPath`：通常のMAX_PATHを超えるdirectory。
- `TestReadDirectoryEntriesWindowsSharesWithPermissionHandle`：ACL設定と同じhandleを保持した読込と、ACL非変更。
- `TestReadDirectoryEntriesWindowsClosesHandle`：読込後にhandleが残らないこと。
- `TestFilesystemCheckpointStoreWindowsSharedACLHandleAllowsSourceReconciliation`：
  ACL用handle保持中の複数sourceの復元、新source初期化、source別更新の分離。

ACL競合のfixtureは`MAXIMUM_ALLOWED`のhandleを保持し、旧`os.ReadDir`が実際に
`ERROR_SHARING_VIOLATION`になることも確認します。sleepや偶然のgoroutine順序に
依存しません。別の起動通知処理が作る競合区間を、このhandle保持で固定しています。

空pathはcheckpoint入口で既に拒否されます。Windows helper単体でも既存のpath変換に
より`ErrInvalid`として拒否します。空pathを不存在扱いにする`os.ReadDir`と、全ての
入力エラー分類まで一致させる変更ではありません。

## Windows 11 / NTFSの実行結果

既存offline VMの`vmtest`一般権限と管理者権限で、同じ媒体を使用しました。
OS表示は`10.0.26200.6584`です。最上位testの件数で、subtestは重複計上しません。

| 実行 | 一般 PASS | 一般 FAIL | 一般 SKIP | 管理者 PASS | 管理者 FAIL | 管理者 SKIP |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 修正前runner＋新規回帰 | 0 | 1 | 0 | 0 | 1 | 0 |
| 修正後fsstore全体 | 59 | 0 | 1 | 60 | 0 | 0 |
| 修正後runner全体 | 199 | 0 | 1 | 200 | 0 | 0 |
| 新規fsstore 5件×10回 | 50 | 0 | 0 | 50 | 0 | 0 |
| 新規runner 1件×10回 | 10 | 0 | 0 | 10 | 0 | 0 |

修正前だけは期待した失敗で、終了codeは1です。Go overlayで一覧取得1行を旧実装へ
戻した比較用exeを使用し、作業treeを巻き戻していません。新規回帰テストは同じです。
修正後の全実行は終了code 0、全体判定も0でした。一般権限でスキップした既存2件も
管理者権限では実行・成功しています。Windows nativeはrace detectorなしです。

一般権限でスキップされたのは、symlink作成を必要とする次の2件だけです。

- `TestVerifyCompressedFileIntegrityRejectsSymlinkToValidCompressedFile`
- `TestFileSeriesManagerRunOnceDoesNotTreatCompressedPeerSymlinkAsCompleted`

修正前の失敗も、checkpoint directoryのopenが共有違反になるエラーであることを
最小限の診断画面で確認しました。

## Linux・静的確認

次の試験に成功しました（Go 1.25.0、`-mod=readonly`）。

```text
go test -race ./internal/infrastructure/fsstore/... \
  ./internal/infrastructure/logger/file \
  ./internal/infrastructure/logger/flushfailure/fsarchivehandler \
  ./internal/config/... -count=1 -timeout=180s

go test -race ./internal/infrastructure/fsstore \
  -run '^TestReadDirectoryEntries' -count=10 -timeout=60s
```

Windows向けfsstore／runnerの`go vet`、独立コードレビュー、`git diff --check`も成功しました。

## 媒体と結果

媒体は3本のtest exeと実行・集計scriptだけを含むread-only ISOです。
作業tree、`.env`、実際の資格情報は含めていません。

`/tmp/windows-checkpoint-readdir-ZwlmY4/checkpoint-readdir-tests.iso`

SHA-256:

```text
184dfa30c5e98c790ead7222d1e08a67c6e1a875bed88ac128937ad2800e6af8
```

VM内の結果保存先:

- `C:\Users\vmtest\checkpoint-readdir-normal-20260911-ZwlmY4`
- `C:\Users\vmtest\checkpoint-readdir-admin-20260911-ZwlmY4`

生ログはゲスト内に保持し、外部回収していません。件数・終了codeと必要最小限の
失敗原因だけを画面で確認しました。
終了後はWindowsの正常shutdownを実行し、launcher終了code 0とQMP／TPM socketの
終了を確認しました。仮想disk、過去の結果、今回の両権限の結果は保持しています。
