# Windows保存先directoryの実体解決試験（2026-09-13）

## 結果

今回の確定sourceから再ビルドし、専用Windows VMで46個の親テストを各10回実行しました。
親テストは460成功・0失敗・0skip、subtestを含めると1500成功・0失敗・0skipでした。
各exeの終了code、全体終了code、実行件数の検査はいずれも0です。

| テストexe | 選択した親テスト | 親テスト成功 | subtest込み成功 |
| --- | ---: | ---: | ---: |
| pathidentity | 8 | 80 | 130 |
| directorylease | 3 | 30 | 30 |
| runner | 7 | 70 | 90 |
| recordspool | 2 | 20 | 140 |
| recordspoolstore | 1 | 10 | 10 |
| objectstorage | 5 | 50 | 50 |
| compressedlog | 5 | 50 | 60 |
| compressedlogcheckpoint | 2 | 20 | 40 |
| file | 4 | 40 | 120 |
| fsarchivehandler | 1 | 10 | 30 |
| config | 5 | 50 | 140 |
| mapped_config | 3 | 30 | 660 |
| 合計 | 46 | 460 | 1500 |

`-test.list`で選択された親テスト数×10と実行された親テスト数の一致をexeごとに確認しました。
`^\s*--- (FAIL|SKIP):`でも全ログを確認し、親だけでなくsubtestにも失敗・skipがありません。

## 対象

- pathidentity：`TestResolveDirectory*`全7件と`TestWindowsDirectoryOperationPath`。
  通常symlink、未作成suffix、リンク切れ・循環・非directoryに加え、Windowsのjunction、
  junction連鎖、長いpath、Windows APIのpath表記を含みます。
- directorylease：aliasの衝突、未作成suffixを持つaliasの重複排除、dangling linkの登録拒否。
- runner：`directory_symlink_test.go`の5件とcheckpoint directory解決2件。
  aliasでのguard・checkpoint Source ID共有、error/debug保持容量の継承も含みます。
- record spool：root／祖先aliasの解決、管理下5 stateのsymlink・file拒否、
  設定alias差替え後も固定した実体へNDJSONを書き込むこと。
- compressed spool／failure archive：root末尾・祖先aliasの許可、alias差替え後の固定先へのpublish、
  rootの通常file拒否、および既存のsource／state／archive file symlink拒否。
- compressedlogcheckpoint：alias差替え後も固定した保存先へcheckpointを更新すること、通常fileの初期拒否。
- FileLogger：`directory_symlink_snapshot_test.go`の4件。lazy role・rotation・deliveryの保存先固定、
  実体pathによるrole共有判定、`TestInlineRolePurgeUsesResolvedAliasDirectoryCapacity`。
- FsArchiveHandler：`TestFailureArchivePinsSymlinkRootAcrossRotationAndUpload`。
- config：`storage_directory_symlink_test.go`の5件。保存先claim衝突、internal log mappingの実体共有、
  dangling linkの拒否、未使用保存先の除外。
- mapped config：`TestMappedFileRolePurgeUsesResolvedSharedDirectory`、
  `TestMappedFileRolePurgeSharedAliasUsesInvalidNormalCapacity`、
  `TestMappedFileRolePurgeDoesNotResolveUnusedDirectories`。

## 環境・媒体・停止確認

- Windows 11 Enterprise Evaluation、日本語版。command prompt表示は`10.0.26200.6584`、
  CIMのversionは`10.0.26200`。試験用一時directoryと結果保存先はC:のNTFSです。
- 管理者consoleのintegrity SID `S-1-16-12288`で実行しました。
  symlink fixtureをskipせず作成するための試験条件であり、製品の通常実行に管理者権限が必要という意味ではありません。
- 既存専用QEMU/KVM VMを`offline`（`-nic none`）で起動。host directory共有やport forwardingなし。
- Go 1.25.0、`GOOS=windows GOARCH=amd64 CGO_ENABLED=0`、library試験は`timetzdata`同梱。
  Windows実行はrace detector付きではありません。
- `build-test-media.sh`の公開example由来dummy fixtureを使用し、実運用`.env`や認証情報は持ち込んでいません。
  通常record spoolの2 exeだけ追加ビルドしました。媒体内の全exeを実行した結果ではなく、上表の選択試験です。
- 媒体：`/tmp/windows-directory-resolution.xyPxcq/directory-resolution-tests.iso`
- SHA-256：`4576e4e8a6549ef79ed89b295d32044ef049871eee333d1e1da4a95f96a2c6e1`
- 実行script：媒体内`run-directory-tests.cmd`、追加集計`check-all-results.ps1`。
  各exeに`-test.v -test.count=10 -test.timeout=300s`と対象正規表現を指定しました。
- guest結果：`C:\dirresolve13a`。生ログはhostへコピーせず、件数・終了code・OS情報のconsole表示だけ確認しました。
  集計画面：`/tmp/windows-directory-resolution-control.Zao2zA/batch-results.png`。
- 試験後にWindowsの`shutdown /s /t 0`で正常終了し、launcher終了code 0、
  QEMU／swtpm process停止、QMP／TPM socketおよびPID fileの消失をhostで確認しました。

稼働中の外部path置換に対する安全性全般や電源断耐久性の検証ではありません。
通常record spoolの時間bucket内部に対する新たなリンク検査は今回の実装・検証範囲に含めていません。
