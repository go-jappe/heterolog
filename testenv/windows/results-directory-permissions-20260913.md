# Windows directory権限の再設定削除・回帰試験（2026-09-13）

## 結果

一般権限で26件の親testを各10回、symlink作成権限が必要な1件を管理者で10回実行しました。
合計270成功・失敗0で、すべてのexeが終了code 0でした。
両batchの全logに対して`^\s*--- (SKIP|FAIL)`を検索し、親・subtestを含めて
一致が0件であることも確認しています。今回選択した試験にskipや未実行はありません。

| package / exe | 権限 | 親test数 | 反復 | 成功 | 失敗 | 親skip | 終了code |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| fsstore / `fsstore.test.exe` | 一般 | 17 | 10 | 170 | 0 | 0 | 0 |
| spool/objectstorage / `objectstorage.test.exe` | 一般 | 3 | 10 | 30 | 0 | 0 | 0 |
| failurearchive/compressedlog / `compressedlog.test.exe` | 一般 | 3 | 10 | 30 | 0 | 0 | 0 |
| logger/file / `file.test.exe` | 一般 | 2 | 10 | 20 | 0 | 0 | 0 |
| objectstorage/compressedlogfailure / `compressedlogfailure.test.exe` | 一般 | 1 | 10 | 10 | 0 | 0 | 0 |
| fsstore / `fsstore.test.exe` | 管理者 | 1 | 10 | 10 | 0 | 0 | 0 |
| 合計 | | 27 | | 270 | 0 | 0 | |

## 確認した動作

- 新規directoryは作成時のprivate DACLを保持し、無関係なユーザーのアクセスを拒否します。
- 既存directoryのACLを変更・読出しできなくても、directory初期化が成功します。
  大小文字・extended path表記で繰り返しても既存ACLは変わりません。
- 子directoryの作成拒否はエラーとして返り、権限復旧後の呼出しや無関係な保存先を妨げません。
- spool/archiveの既存stagingで`WRITE_DAC`を拒否しても保存できます。
  file作成を拒否した場合は実際の作成時にエラーを返し、元fileとACLを保持します。
  外部から権限を戻すと、同じStoreで保存できます。
- 親directoryの同期失敗はその呼出しへ返り、次の初期化へ持ち越しません。
- FileLoggerのhandoff失敗はOnErrorへ通知され、元fileを保持します。
  再送loopとarchive保守loopは権限エラー後も続き、復旧後の周期で処理できます。
- 通常版の保存先自身にあるWindows symlink拒否、strict版の上位symlink拒否、
  通常版の上位symlink許可が維持されています。本物のsymlinkを使って確認しました。

## 試験選択

すべて`-test.v -test.count=10 -test.timeout=180s`を付け、以下を`-test.run`に渡しました。
一般権限のfsstoreは次のpatternで17件を選択しています。

```text
TestEnsureDirectoryDurably|TestStrictDirectoryCreation|TestPrivateDirectoryDACLIsRestrictedAtCreation|TestPrivateTempFileInheritanceAndReplacement|TestPrivatePermissionsPropagateAccessDeniedWithoutMutation|TestPrivatePermissionsPreserveLongPaths|TestSyncDirectoryWindows
```

spool/objectstorage:

```text
TestStorePreservesExistingDirectoryPermissions
TestStorePublishesAfterPreviousLayoutParentSyncFailure
TestStoreKeepsPrivatePermissionsAcrossStateMoves
```

failurearchive/compressedlog:

```text
TestStorePreservesExistingDirectoryPermissions
TestStorePublishesAfterPreviousLayoutParentSyncFailure
TestStorePublishStoresDirectCompressedFileByteForByte
```

logger/file:

```text
TestFileArchiveHandoffPermissionFailureRetainsSourceAndRetries
TestCompressedLogReplayPermissionFailureNotifiesAndContinues
```

objectstorage/compressedlogfailure:

```text
TestArchiveLifecycleReportsPermissionFailureAndContinuesNextCycle
```

管理者で実行したfsstoreのpattern:

```text
^TestDurableDirectoryPoliciesPreserveWindowsSymlinkRejection$
```

## 環境と保存先

- 既存の専用QEMU/KVM VM、Windows 11 Enterprise Evaluation 25H2 日本語版。
  guestの`ver`は`10.0.26200.6584`、OS照会はBuild 26200・64 bitでした。
- C:のNTFSを使用。一般batchはMedium integrity（S-1-16-8192）、
  symlink batchはHigh integrity（S-1-16-12288）を結果fileから確認しました。
- KVM有効、offline起動。外部network・host directory共有・新規installation・恒久設定変更なし。
- Go 1.25.0、`GOOS=windows GOARCH=amd64 CGO_ENABLED=0`、
  `go test -mod=readonly -tags=timetzdata -c`で、最終修正後に対象5exeをすべて再buildしました。
  native Windows試験にはrace detectorを付けていません。
- 媒体は`/tmp/windows-directory-acl-tests.1SLLfd-final.iso`。
  SHA-256: `f328d97d2c4bed596e22e84cbd8fcedaa03d6ccd47a1385a7820299dcd7ea8c6`。
  exeと限定batch・集計scriptだけを含み、実運用`.env`や認証情報は持ち込んでいません。
- 生logはVM内の`C:\acl13n`（一般）と`C:\acl13a`（管理者）に保持しました。
  hostでは集計・OS・権限label・skip/fail件数の画面だけを確認しました。
  画面記録は`/tmp/windows-directory-acl-control.UwgqVj/`です。

最初のconsole操作ではCD drive指定が一致せず、test開始前にcommand not foundで止まりました。
媒体label `DIR_ACL_TESTS`と実在fileをG:で確認して実行し直しています。
この操作は上記の試験件数に含めていません。

完了後はguestで`shutdown /s /t 0`を実行しました。launcher終了code 0、QEMU/TPMの
process停止、QMP/TPM socketの消滅をhostで確認しています。
