# Windows ACLと権限関連試験（2026-09-11）

このreportはACL修正時点の履歴です。残った権限・Stat試験のskipと同期失敗の再試行は、
後続の[同期未確認の再試行とWindowsスキップ解消](results-durability-20260911.md)で対応しています。

## 実装範囲

対象はrunner checkpoint、圧縮log upload checkpoint、圧縮log spool、
圧縮log upload failure archiveです。`.env`と送信先設定は変更していません。

- 新規directoryにはCreateDirectoryの時点でprotected DACLを指定します。
- 許可するのは実行token user、SYSTEM、builtin Administratorsです。
- Unixの0750/0640のgroup-readをWindows Users/Everyoneへ対応付けません。
- temp fileは制限済みdirectory内で作り、継承ACLでも最初から保護します。
  payloadを書き込む前にfile自身のprotected DACLも確定します。
- 既存checkpointは同じhandleで種類・owner・ACLを確認/設定してから読みます。
  空の保存先は読込だけでは作成せず、初回publishまで遅延します。
- 別の一般userがownerのobject、reparse point、非regular fileは拒否します。
  所有権を奪う処理はありません。
- 既存directoryの変更は明示されたapp所有の保存先だけです。
  共有parentと既存childへACLを再帰的に適用しません。
- ACLを設定/確認できない場合はerrorを返します。作成後の検証失敗も
  「directoryが存在するから成功」と扱いません。
- Unixのmode/umaskと、既存checkpoint読込の権限契約は維持します。

既存Windows状態のmigrationは、利用者がまだ運用していないと確認済みのため不要です。
前回追加したSourceIDの大小文字/extended path統一と、aliasを使ってStoreを再生成した
場合のupload成功履歴の引継ぎ試験も、今回のWindows mediaに含めています。

## 試験修正

Unixでは従来のmodeを検証し、Windowsでは実際のprotected DACL・principal・
access mask・継承flagを検証します。byte内容、atomic置換、temp file削除の検証は残します。
spoolのactive/processing/done移動と既存fileの再採用でも権限を確認します。

symlink fixture作成では、WindowsのERROR_PRIVILEGE_NOT_HELD（1314）だけをskipにします。
複数ケースを含む試験はsymlink部分だけをsubtestに分離しました。
予期しない失敗までskipしません。Stat失敗だけが必要な試験では、symlinkではなく
不存在pathを使います。

ACL試験には、共有parent/既存childを変更しないこと、payload書込前の継承、
rename/置換後の権限、長いpath、実際のWRITE_DAC拒否時のerror/内容保持を含めます。
Windowsの制限tokenとAccessCheckで、広い親は読めるがprivate directory/fileは読めない
ことも確認します。別の実アカウントを作成してログインする試験とは異なります。
foreign ownerの拒否はWindowsのsecurity descriptorを使った検証です。

## Windows 11 / NTFSでの結果

### 一般権限の7 package全体試験

| Package | PASS | FAIL | SKIP |
| --- | ---: | ---: | ---: |
| fsstore | 50 | 0 | 2 |
| appendlog | 74 | 0 | 1 |
| failurearchive | 22 | 0 | 0 |
| runner | 193 | 0 | 3 |
| compressedlogcheckpoint | 24 | 0 | 0 |
| objectstorage spool | 28 | 0 | 3 |
| compressedlog archive | 21 | 0 | 2 |
| 合計 | 412 | 0 | 11 |

各packageと全体の終了codeは0です。PASS/FAIL/SKIPは最上位testの件数で、
subtestを重複計上していません。

### 管理者権限の7 package全体試験

| Package | PASS | FAIL | SKIP |
| --- | ---: | ---: | ---: |
| fsstore | 51 | 0 | 1 |
| appendlog | 74 | 0 | 1 |
| failurearchive | 22 | 0 | 0 |
| runner | 194 | 0 | 2 |
| compressedlogcheckpoint | 24 | 0 | 0 |
| objectstorage spool | 31 | 0 | 0 |
| compressedlog archive | 23 | 0 | 0 |
| 合計 | 419 | 0 | 4 |

各packageと全体の終了codeは0です。symlink fixtureを作れる条件で、
通常ユーザーでは実行できなかったケースも成功しました。
残る4件はUnix固有のSourceID互換性確認と、Unix permission bitを使うStat/書込拒否fixtureです。
WindowsのACL拒否そのものは新しいnative試験でerrorと内容/ACLの保持を検証しています。

- `TestArchiveCheckpointSourceIDPreservesUnixIdentity`
- `TestPurgeByAgeIfNeededKeepsStatFailure`
- `TestFilesystemCheckpointStoreDefersPermissionCheckUntilCheckpointCreation`
- `TestFilesystemCheckpointStoreCommitReturnsPermissionErrorWithoutAdvancingCheckpoint`

### 一般権限の関連10回反復試験

| Package | PASS | FAIL | SKIP |
| --- | ---: | ---: | ---: |
| fsstore | 180 | 0 | 0 |
| appendlog | 230 | 0 | 10 |
| runner | 80 | 0 | 0 |
| compressedlogcheckpoint | 40 | 0 | 0 |
| pathidentity | 70 | 0 | 0 |
| directorylease | 130 | 0 | 10 |
| config | 40 | 0 | 0 |
| logger/file | 40 | 0 | 0 |
| fsarchivehandler | 10 | 0 | 0 |
| 合計 | 820 | 0 | 20 |

各packageと全体の終了codeは0です。82の最上位testを10回ずつ実行した件数であり、
820種類の試験ではありません。Unix限定のsource ID確認と、通常ユーザーでは
symlinkを作れないlease試験が10回ずつskipされています。

## Linux・ビルド確認

- Linux: config配下、fsstore配下、FileLogger、FsArchiveHandler、compressedlogfailure、
  testutil、path配下の`go test -race`が成功。
- Windows向けアプリbuild、fsstore/testutil配下のvetと試験compileが成功。
- VM launcher mockの9ケース、shell構文、`git diff --check`が成功。

## Mediaと保存先

最終試験media: `/tmp/windows-acl-media-20260911b/fsstore-windows-tests.iso`

SHA-256: `4c567cf34893e733acdd525534fd5823475ea8585774bca1ca7782ef73b55424`

ISOには試験exeと実行/集計scriptだけを入れ、`.env`、認証情報、checkoutは入れていません。
生ログの回収はしません。`summarize-results.ps1`でVM内のログを集計し、件数と終了codeを
画面で確認しています。

- `C:\Users\vmtest\acl-normal-20260911b`
- `C:\Users\vmtest\acl-admin-20260911b`
- `C:\Users\vmtest\acl-repeat-20260911b`

試験後にWindowsの正常shutdownを実行し、VM launcherの終了code 0とQMP/TPM socketの
撤去を確認しました。仮想diskとVM内の生ログは保持しています。

## fsync失敗の扱い

今回、同期失敗を通常loggerでは成功扱いにする、といった契約変更はしていません。
spool/archiveの別directory helperで、mkdir後の親同期失敗を次回へ引き継がない問題も、
このACL修正とは別の残件です。

OSによる遅延writebackが行われることと、error後の永続化が保証されることは別です。
同期用openの権限不足と、実際の同期中のI/O error/空き容量不足等も区別が必要です。
通常loggerを通知後に継続することは、保存成功として元fileを解放することとは異なります。
現行handoffはerrorをreportへ残し、同期未完了をdurable保存成功には数えません。

参考:

- [Linux fsync](https://man7.org/linux/man-pages/man2/fsync.2.html)
- [Windows FlushFileBuffers](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers)
- [Windows file security](https://learn.microsoft.com/en-us/windows/win32/fileio/file-security-and-access-rights)
- [SetSecurityInfo](https://learn.microsoft.com/en-us/windows/win32/api/aclapi/nf-aclapi-setsecurityinfo)

電源断・kernel crash、macOS、NTFS以外のfilesystemは今回検証していません。
