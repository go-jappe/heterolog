# Windows共有初期化とファイル名の大小文字対応（2026-09-11）

## 修正内容

今回の修正では`.env`、S3 object keyの生成規則、WindowsのACL許可対象、
fsyncの実装を変更していません。

- 圧縮ログspool／upload failure archiveの初期化を、新しい共有coordinatorを作る
  場合だけに限定しました。既存coordinatorへ参加するときは、そのStoreを共有します。
  これにより、別StoreのACL設定が進行中のdirectory readerと競合する経路をなくしました。
- 共有coordinatorの取得・解放keyへ`pathidentity.Normalize`を適用しました。
  Windowsの大小文字・通常／extended path表記が違っても同じ管理処理を共有し、
  旧ownerが終了する前に別coordinatorが一時ファイルを掃除しないようにしました。
- logpatternのWindowsでのbasename照合を、既存path identityと同じ小文字keyへ統一。
  通常／圧縮／削除待ちファイルの検索、最大index、圧縮世代の予約、canonical判定、
  世代系列分類へ適用しました。Unixは大小文字を区別します。
- runnerの最新raw保護と圧縮元判定、FsArchiveHandlerのraw回復判定も共通処理へ統一。
  FsArchiveHandlerのpurge用target-prefix選別は、従来の広い一致条件を維持しています。
- 削除待ちファイルの検索実装をlogpatternへ移動し、旧appendlog関数を削除しました。
  呼出し先を直接更新し、転送するだけのwrapperは残していません。
- Windowsで`.LOG`／`.GZ`表記を発見した場合も、圧縮ログの保存・既知codecによる
  破損検証へ正しく渡します。custom codec優先、`.jsonl`の圧縮ログ専用storeへの
  混入拒否、元のbasename・suffix・byte内容を保つ契約は維持しています。

主な変更箇所:

- `internal/infrastructure/objectstorage/compressedlogspool/coordinator.go`
- `internal/infrastructure/objectstorage/compressedlogfailure/{uploader,archive_lifecycle}.go`
- `internal/infrastructure/fsstore/name/logpattern/`
- `internal/infrastructure/fsstore/appendlog/{compress,compression_source_retirement}.go`
- `internal/infrastructure/fsstore/appendlog/runner/{file_series_manager_file_selection,file_series_manager_compression,directory_lifecycle}.go`
- `internal/infrastructure/fsstore/failurearchive/compressedlog/root.go`
- `internal/infrastructure/logger/file/archive_compression_recovery.go`
- `internal/infrastructure/logger/flushfailure/fsarchivehandler/{archive_compression_recovery,fsarchivehandler}.go`

## Windows 11 / NTFSの試験結果

件数は最上位test単位です。subtestは合計へ重複計上しません。

### 既存の関連7 package

| Package | 一般 PASS | 一般 SKIP | 管理者 PASS | 管理者 SKIP |
| --- | ---: | ---: | ---: | ---: |
| fsstore | 54 | 1 | 55 | 0 |
| appendlog | 78 | 1 | 78 | 1 |
| failurearchive | 22 | 0 | 22 | 0 |
| runner | 198 | 1 | 199 | 0 |
| compressedlogcheckpoint | 24 | 0 | 24 | 0 |
| objectstorage spool | 29 | 3 | 32 | 0 |
| compressedlog archive | 23 | 2 | 25 | 0 |
| 合計 | 428 | 8 | 435 | 1 |

一般・管理者ともFAILは0件、全packageと全体の終了codeは0です。
一般権限のsymlink試験は管理者側で実行しています。管理者で残る最上位SKIPは
Unix専用の`TestArchiveCheckpointSourceIDPreservesUnixIdentity`です。

### 追加packageの全体試験と回帰試験の反復

次の全体試験は一般・管理者とも同じ件数です。

| Package | PASS | FAIL |
| --- | ---: | ---: |
| logpattern | 35 | 0 |
| compressedlogspool | 46 | 0 |
| compressedlogfailure | 20 | 0 |
| 合計 | 101 | 0 |

一般権限ではlogpatternのdangling-symlink subtestが未実行となり、管理者側で補完しました。
さらに8 packageの33種類の対象試験を10回ずつ実行し、一般・管理者とも
330回成功・失敗0回でした。上記全体試験と合わせ、各実行の集計は431 PASS、0 FAIL、
全packageと全体の終了codeは0です。

回帰試験では次を確認しました。

- readerを保持したまま同じ保存先へ別uploaderが参加しても共有違反にならない。
- 大小文字・extended path表記が違ってもStore/coordinatorを共有する。
- 旧ownerのhandoff終了前に、次のownerが処理中のstaging fileを消さない。
- `TARGET=App`から`app`への再構築後も、既存圧縮世代を再利用しない。
- 元のraw／compressed bytesを保持し、旧表記のファイルを再送する。
- runnerが旧表記の最新rawを圧縮・upload・purgeしない。
- 通常／Deliveryのspoolで旧表記のbasenameを配送し、Doneへ遷移する。
- 大文字拡張子の破損gzipも検証し、既知形式なのに検証を省略しない。

### 途中で見つかった試験側の前提

最初の追加全体試験では`TestCompressionSourceDeletePendingPathExistsPreservesStatError`
だけが失敗しました。regular fileを親にしたpathについて、UnixのENOTDIR相当を
期待していましたが、Windowsでは「pathが存在しない」に分類されます。
製品の「不存在はfalse、それ以外のerrorは伝播」の処理は変更せず、後者の検証入力を
両OSで不正となるNUL入りpathへ変更しました。skipは追加していません。
修正後はWindows両権限で全体試験・10回反復、Linux raceで10回成功しました。

集計scriptはVMの既定PowerShell execution policyで最初の起動だけ拒否されました。
試験exeの実行結果には影響していません。読み取り専用ISO上の既知の集計scriptを、
起動するPowerShellプロセスに限って許可して集計しました。恒久policyは変更していません。

## Linuxとビルド確認

- config配下、fsstore配下、FileLogger、FsArchiveHandler、compressedlogspool、
  compressedlogfailure、path配下、pkg/fsstoreの`go test -race -count=1`成功。
- application logger packageと最終追加修正後のappendlog／compressedlog archiveもrace成功。
- logpatternの最終試験修正後、package全体のrace試験を10回実行して成功。
- Windows向けアプリbuildと変更対象のWindows向けvet成功。
- launcher mock試験9件、shell構文検査、`git diff --check`成功。

Windows nativeはrace detectorなしで実行しています。

## 試験媒体と結果の保存先

最終媒体: `/tmp/windows-sharing-case-20260911c/fsstore-windows-tests.iso`

SHA-256:

```text
09fc0618ba09e49bfd7a7647e0e21f9242df131cb4f1f6aa08547ddee5f1df4c
```

ISOは15 packageのtest exeと実行・集計scriptだけを含みます。
作業tree、`.env`、資格情報は含めません。

VM内の結果:

- 一般・既存7 package: `C:\Users\vmtest\sharing-case-normal-full-20260911c`
- 管理者・既存7 package: `C:\Users\vmtest\sharing-case-admin-full-20260911b`
- 一般・追加全体／反復: `C:\Users\vmtest\sharing-case-normal-20260911c`
- 管理者・追加全体／反復: `C:\Users\vmtest\sharing-case-admin-20260911c`

`b`から`c`への変更はlogpatternの試験fixtureと反復対象だけです。
15本中、logpattern以外の14本のexeがbyte-for-byte一致することを確認しました。
管理者の既存7 packageも最終媒体と同一exeでの結果です。
途中の`sharing-case-normal-20260911b`も上書きせず保持しています。

生ログはVM内に保持し、件数・終了code・失敗箇所の最小限の診断だけを画面で確認しました。
試験終了後はWindowsの正常shutdownで停止し、launcher終了code 0と
QMP／TPM socketの終了を確認しました。仮想diskと既存の結果は保持しています。
