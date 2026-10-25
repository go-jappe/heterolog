# Windows上書き禁止移動・保存先初期化の実行試験（2026-09-28）

## 試験結果

既存のWindows 11評価版VMをKVM・ネットワークなしで使用しました。
今回の試験対応では製品コードを変更していません。前回実装したOS別の非置換移動と、
Windows保存先のACL対応確認を、実際のWindowsで検証しました。

| 試験 | 成功した最上位test | 失敗 | skip（subtest含む） |
| --- | ---: | ---: | ---: |
| fsstore全体 | 133 | 0 | 0 |
| 圧縮ログfailure archive全体 | 51 | 0 | 0 |
| 圧縮ログspool store全体 | 57 | 0 | 1 |
| NTFSで非置換移動・初期化判定を10回 | 120 | 0 | 0 |
| FATで非置換移動を10回 | 60 | 0 | 0 |

通常試験は241件、重点試験は180回、全実行ファイルと実行scriptの終了codeは0です。
spoolのskipは、Windowsに存在しないUnix FIFOを作るsubtestだけです。
directory・symlink・不正名の拒否、キャンセル、保存失敗の試験は実行しています。

NTFSでは長いパス・大文字小文字違いの衝突・既存file／directory／symlinkを上書きしないこと、
32並列の移動で一つだけ成功することを確認しました。FATでも通常移動・衝突・並列移動を確認しました。
FATはACLを保持しないため、アプリ保存先としては初期化時に拒否し、子directoryを作らないことも確認しています。
FATで移動helperが動くことと、アプリ保存先としてFATを許可することは別です。
exFAT／ReFSの実volumeでは未実行です。filesystem名ではなくACL対応flagの判定は単体試験しています。

最終結果画面: [final-results2.png](/tmp/noreplace-filesystem.Sptc1E/final-results2.png)。
guest内の最終ログは`C:\Users\vmtest\AppData\Local\Temp\noreplace-check-64c1ccd6110a453ebedd4b8230ff2f12`です。
secretを表示・出力せず、アカウントやパスワードの変更も行っていません。
修正後の入力scriptでNumLock OFFからのロック解除も確認しました。
[再ログイン結果](/tmp/noreplace-filesystem.Sptc1E/clean-login-result.png)。
試験後はguestの`shutdown /s /t 0`で通常終了し、起動scriptの終了code 0を確認しました。

## 試験中に直した問題

- VM入力: NumLock OFFでAlt＋テンキーの記号入力が欠落していました。入力ごとにguestのLED状態を確認します。
  追加した事前のCtrl+A／Backspaceを使う入力では再ログインできなかったため、この操作を除去しました。
  空のpassword欄に直接入力する最終版で成功を確認しています。
- パス表示の比較3件: `%q`でescapeされたWindowsパスを、生のパスと比較していました。期待値もquoteします。
- 移動試験: `SameFile`による移動前後のID不変を要求していました。Windowsの遅延ID取得に加え、
  FATでは名前変更自体でIDが変わり得ます。移動の契約である元パス消失・内容一致を検証します。
  [Microsoftのfile ID仕様](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/ns-fileapi-by_handle_file_information)。
- コピー中断試験: directory entryの古いサイズを見て中断が遅れていました。開いたfile handleからサイズを取得します。
- 実行script: PowerShellのstderr解釈で中断しないよう、stdout／stderrを別々に保存し終了codeで判定します。
  Windows PowerShell 5.1向けにUTF-8 BOMを付けています。
- 私の途中の再ビルドで`timetzdata`指定を落とし、timezone試験2件が失敗しました。
  既存のビルド手順と同じ指定へ戻して全件を再実行しています。製品のtimezone処理の変更ではありません。

## その他の確認

- Linux: 関連25packageの`go test -race`成功。
- Linux: 非置換移動・削除・該当purge試験を`-race -count=20`で成功。
- Darwin arm64／FreeBSD amd64: fsstore試験のクロスコンパイル成功。実行試験ではありません。
- VM入力helper: VM・認証情報不要の単体試験10件成功。

## 今回変更した全ファイル

前回の製品変更とは区別した、このVM確認作業の一覧です。
[今回の全差分](/tmp/windows-vm-control.IUM33L/changes.diff)は作業開始前の内容との比較です。

| ファイル | 変更内容 |
| --- | --- |
| [control-vm.py](control-vm.py) | 新規。guestキーボード状態確認、非表示のcredential入力、画面・クリック・終了要求。 |
| [test_control_vm.py](test_control_vm.py) | 新規。VM不要の入力準備・クリック単体試験。 |
| [run-no-replace-tests.ps1](run-no-replace-tests.ps1) | 新規。上記5群の実行、exit code判定、結果保存。 |
| [README.md](README.md) | 操作・試験再実行手順を追加。 |
| [rename_file_noreplace_test.go](../../internal/infrastructure/fsstore/rename_file_noreplace_test.go) | 移動後のID不変という誤った前提を除去。 |
| [remove_file_test.go](../../internal/infrastructure/fsstore/remove_file_test.go) | エラー中のquote済みパスを比較。 |
| [remove_regular_file_test.go](../../internal/infrastructure/fsstore/remove_regular_file_test.go) | エラー中のquote済みパスを比較。 |
| [purge_filename_test.go](../../internal/infrastructure/fsstore/purge_filename_test.go) | エラー中のquote済みパスを比較。 |
| [store_publish_cancel_test.go](../../internal/infrastructure/fsstore/spool/objectstorage/store_publish_cancel_test.go) | handleから途中のコピー量を確認。 |
| [results-no-replace-20260928.md](results-no-replace-20260928.md) | 新規。この試験・変更報告。 |

既存の他の変更は戻していません。前回の製品変更18ファイルは
[前回の変更一覧](/tmp/noreplace-filesystem.Sptc1E/changes.md)・[前回の差分](/tmp/noreplace-filesystem.Sptc1E/changes.diff)にあります。

## 最終Windows実行ファイル

`GOOS=windows GOARCH=amd64 go test -mod=readonly -tags=timetzdata -c`で作成しました。

```text
fsstore.test.exe dc296f8fa1350a08376f9c218b08d514068f4c85f8f168e16a68b6bb174547be
archive.test.exe d82cdc0bb180b34fb6ca5763a1200bfa4c4888e50fbbea94b0dc95a97f850d71
spool.test.exe   e3cd6d1ad3d2d92f4bcd3c60e734b1f65877df0860f48bcce39f68858955550c
```
