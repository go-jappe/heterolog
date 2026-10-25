# 同期未確認の再試行とWindowsスキップ解消（2026-09-11）

## 修正内容

対象は圧縮log spool、圧縮log upload failure archive、共通directory作成処理と、
圧縮・checkpointの同期error分類です。この修正では`.env`を変更していません。

- mkdir成功後に親directoryの同期が失敗した場合、未完了の同期をprocess内で保持します。
  次の呼出しでは既存directoryを見ただけで成功扱いにせず、失敗した同期を再試行します。
- spool/archiveの独自directory作成処理を共通化しました。厳格な入口では全path componentの
  symlinkを拒否し、Unixの既存modeや共有ancestorの権限は変更しません。
- 作成後にWindows ACLの確認が失敗した場合も、その確認を省略しません。
  未完了ancestorの権限を確認してから子directoryを作ります。
- 同期済みの親や無関係なancestorを毎回同期しません。別Storeの初期化が共通parentの同期を
  完了しても、別の子directoryのACL未完了状態は消しません。
- `pkg/fsstore.ErrDurabilityUnconfirmed`を追加しました。同期失敗の原因errorも保持し、
  既存の`HandoffReport.HandoffErr`やarchive runnerのerrorから`errors.Is`で判別できます。
  同期後のCloseだけが失敗した場合は、同期失敗と混同せず通常のClose errorです。
- 保存先の同期未確認をhandoff成功として扱わず、元compressed fileと未完了checkpointを
  維持します。同期可能になった後の再試行成功だけを保存完了とします。

通常loggerは稼働中のarchive errorをOnErrorへ通知し、後続の通常writeを続けます。
Deliveryの圧縮・uploadは後続のrunnerが処理するため、同期未確認はその非同期reportへ
反映します。すでに返したLogのDeliveryReportを後から失敗に変更するものではありません。
これらの既存通知・保全経路は変更せず、今回のerror分類が届くことを回帰試験で確認しました。

## 前回のスキップについて

SKIPは成功でも失敗でもなく、その環境では試験を実行しなかったことです。
前回の管理者実行で残った4件のうち、次の3件にWindows固有の異常fixtureを用意しました。

- `TestPurgeByAgeIfNeededKeepsStatFailure`: delete-pendingの実fileでStat失敗を起こし、
  purgeがそのfileを成功削除として扱わないことを確認します。試験後は削除予約を取り消し、
  元byte内容を確認します。
- `TestFilesystemCheckpointStoreDefersPermissionCheckUntilCheckpointCreation`
- `TestFilesystemCheckpointStoreCommitReturnsPermissionErrorWithoutAdvancingCheckpoint`

checkpointの2件では実DACLで書込みを拒否し、error、disk/memoryのcursor維持、
不要なtemp fileが残らないこと、権限回復後の再試行を確認します。
予想外のエラーをskipする実装にはしていません。

最初のWindows実行では、権限復元用の試験handleがDELETE権限を保持していたため、
検証用の`os.ReadDir`と共有違反になり、この2件が失敗しました。製品の権限errorと
checkpoint未前進の確認は成功していました。fixture handleをREAD_CONTROLとWRITE_DAC
だけへ絞り、不要なDELETE権限を除いて再実行しています。

追加したlogger報告試験2件も、最初はWindowsで失敗しました。追記専用handleに対して
fixtureがTruncateしていたことが原因です。製品のopen権限を広げるのではなく、通常の
HandleLogEntryで1 MiBを追記してrotationを起こす試験へ修正しました。

`TestArchiveCheckpointSourceIDPreservesUnixIdentity`はUnix専用なのでWindowsでは実行しません。
Windowsのpath大小文字・extended path aliasは別のnative試験で検証します。

## Windows 11 / NTFSの全体試験結果

| Package | 一般 PASS | 一般 SKIP | 管理者 PASS | 管理者 SKIP |
| --- | ---: | ---: | ---: | ---: |
| fsstore | 54 | 1 | 55 | 0 |
| appendlog | 74 | 1 | 74 | 1 |
| failurearchive | 22 | 0 | 22 | 0 |
| runner | 196 | 1 | 197 | 0 |
| compressedlogcheckpoint | 24 | 0 | 24 | 0 |
| objectstorage spool | 29 | 3 | 32 | 0 |
| compressedlog archive | 22 | 2 | 24 | 0 |
| 合計 | 421 | 8 | 428 | 1 |

両方ともFAILは0件、各packageと全体の終了codeは0です。最上位testだけの件数で、
subtestは重複計上していません。一般権限でのsymlink関連skipは管理者実行で成功しています。

- 一般権限: `C:\Users\vmtest\durability-normal-20260911c`
- 管理者権限: `C:\Users\vmtest\durability-admin-20260911c`

前回未実行だった3件は両権限で成功しました。追加したspool/archiveの実ACLによる
親同期拒否、同じ/別Storeからの再試行、元file保持、回復後の保存も両権限で成功しました。

### 一般権限の10回反復

11 packageで93種類の対象testが10回ずつ成功しました（PASS 930、FAIL 0）。
最上位testのSKIPは20回で、Unix専用SourceID試験と、通常権限ではsymlinkを作れない
directory lease試験のそれぞれ10回です。各packageと全体の終了codeは0です。
追加した通常logger・Deliveryのreport試験2件も、実際のログ追記を入口に10回ずつ成功しています。

保存先: `C:\Users\vmtest\durability-repeat-20260911c`

### Linuxとビルド確認

- config配下、fsstore配下、FileLogger、FsArchiveHandler、compressedlogfailure、testutil、
  path配下、pkg/fsstoreの`go test -race -count=1`が成功。
- fixture最終修正後のFileLogger package全体も再度`-race`成功。
- 新規FileLogger/Delivery試験2件は`-race -count=10 -cpu=1,2,4`の60回成功。
- Windows向けアプリbuild、fsstore/testutil配下のvet、試験exeのcrosscompileが成功。
- launcher mock 9件、shell構文、`git diff --check`が成功。

## 保証の境界

- directoryの未完了状態はprocess内の共有memoryです。同じprocessで別Storeへ作り直しても
  引き継ぎますが、実process再起動をまたいで未完了の親一覧を永続化する実装ではありません。
  一方、失敗したuploadのcheckpointが未前進であることはStore再読込で確認しています。
- Windowsでの同期はNTFS上のAPI成功/失敗を検証します。電源断、kernel crash、物理storageの
  永続化、macOS、NTFS以外のfilesystemはこの試験の対象ではありません。
- 通常権限ではsymlink作成権限がない場合の試験がskipになります。管理者実行で補完します。
- Linuxでは関連範囲でrace detectorを使います。Windows native試験はrace detectorなしです。

## 試験媒体

最終試験媒体: `/tmp/windows-durability-media-20260911c/fsstore-windows-tests.iso`

SHA-256: `9416fa63a58a3028c5276aea10437d8abcaadd3842578cef53b284d5417d9526`

ISOには試験exeと実行・集計scriptのみを入れています。`.env`や資格情報は含めません。
生ログはVM内に保持し、集計件数・終了code・必要最小限の失敗診断を画面で確認します。
途中の実行結果も上書きせず、`durability-*-20260911a`と`durability-*-20260911b`に保持しています。

試験終了後はWindowsの正常shutdownでVMを停止し、launcher終了code 0を確認しました。
VMの仮想diskと生ログは保持しています。
