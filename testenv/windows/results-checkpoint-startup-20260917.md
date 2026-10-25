# checkpointの起動前準備とdirectory初期化（2026-09-17）

既存のWindows 11 Enterprise Evaluation VMをofflineで起動し、ローカルNTFSで
対象28試験を各10回実行しました。最終結果は280成功・0失敗・0 skipです。
全packageの終了コードも0でした。件数は親test単位で、子testを重複計上していません。

## 確認範囲

- checkpoint保存先をlogger稼働前に準備し、Store構築では未作成の保存先を拒否する。
- 保存先が途中で削除されても、checkpoint保存処理が勝手に再作成しない。
- directory初期化のglobal lockを除去しても、作成前のキャンセルと途中まで作成した親の同期を維持する。
- 別保存先の初期化は、無関係な親directoryの同期完了を待たない。
- WindowsのACLを新規作成時に設定し、既存ACLは変更しない。拒否された操作はエラーを返し、復旧後に再試行できる。
- 親directoryのaliasは実体へ固定し、checkpoint保存先自身のリンクは拒否する。
- loggerのlazy file openより前に、使用するcheckpoint保存先だけを準備する。準備失敗時には通常ログも開かない。

| package | 一般権限の試験数 | 管理者の試験数 | 10回反復後の成功数 |
| --- | ---: | ---: | ---: |
| fsstore | 6 | 1 | 70 |
| appendlog | 3 | 1 | 40 |
| compressedlogcheckpoint | 5 | 2 | 70 |
| appendlog runner | 5 | 2 | 70 |
| logger | 3 | 0 | 30 |
| 合計 | 22 | 6 | 280 |

管理者実行は実際のsymlinkを作る6試験だけです。一般権限では220回、管理者では
60回成功しました。親test・子testともskipはありません。

## 実試験で見つけた準備処理の修正

`TestFilesystemCheckpointStoreUsesPrivatePermissionsAndLeavesNoTempFile`の初回実行は
10回とも、保存先のDACLが継承から保護されていないという検査で失敗しました。
試験が既存の`TempDir`へ`EnsureDirectory`を呼び、Windowsでも既存ACLが変更されると
期待していたためです。現在の「作成時に設定し、既存ACLは変更しない」契約と合っていませんでした。

試験準備を、新規の子directoryを`EnsureDirectory`で作成する形に修正しました。
製品のACL契約は変更していません。再ビルド後、同じ一般ユーザーで10回すべて成功し、
checkpoint fileの置換後の権限とtmp fileが残らないことも確認しました。

追加試験の手入力コマンドでは、最初にPowerShellの引数引用不足による終了コード2も
ありました。これは試験開始前のコマンド解釈エラーで、成功件数へ含めていません。
引数を引用したコマンド／scriptで再実行しています。

## 実行条件と証跡

Go 1.25.0、`GOOS=windows GOARCH=amd64 CGO_ENABLED=0`、
`-mod=readonly -tags=timetzdata`で対象test exeだけをビルドしました。
Windows側のrace detector試験や全体試験ではありません。
実`.env`・アプリ認証情報・作業treeはVMへ持ち込まず、host共有、新規install、
ネットワーク接続、恒久的なWindows設定変更も行っていません。

生ログは次のVM内directoryに保存しました。hostでは件数・終了コードと必要なassertionの画面だけを確認しました。

- 一般権限17試験：`C:\Users\vmtest\checkpoint-normal-Nzwevv`
- 管理者のリンク6試験：`C:\Users\vmtest\checkpoint-links-Nzwevv`
- 修正後の権限2試験とlogger 3試験：`C:\Users\vmtest\checkpoint-confirm-PlW9z8`
- 集計画面：`/tmp/windows-checkpoint-setup.Nzwevv/normal-complete.png`、`links.png`、`confirm.png`
- 初回権限失敗の画面：同directoryの`private-corrected.png`
- 最初の媒体：`/tmp/windows-checkpoint-setup.Nzwevv.iso`
  - SHA-256：`b9f547648c0305d6746cd18b70da1a0d9ff6bc4a5a0c78befa8516e855461489`
- 修正版runnerとloggerの媒体：`/tmp/windows-checkpoint-confirm.PlW9z8.iso`
  - SHA-256：`3472db060b66a6351abc09d9867b17ed1444edc426bf5b83911390a88d4903d6`

終了時はWindowsの`shutdown /s /t 0`で正常shutdownしました。
launcherの終了コード0、QEMU／TPM processとQMP／TPM socket・PID fileの消失を確認しました。
