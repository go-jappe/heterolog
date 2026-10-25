# Windows failure archive基準パスの大小文字判定（2026-09-11）

## 修正範囲

`pkg/path.BuildApplicationDataDirectoryPath`のtarget存在判定を、Windowsでは既存の
`pathidentity`と同じ小文字化による照合へ変更しました。比較にだけ使用し、返すパスの
元表記は変更しません。Linux/macOSの大小文字を区別する既存動作は維持しています。

例えばtargetが`service`で基準パスが`C:\data\SERVICE`の場合、修正前は末尾へさらに
`service`を追加していました。修正後は追加せず、大小文字だけの設定変更でfailure archiveの
保存先が変わることを防ぎます。targetが祖先directoryにある場合も同じです。

- 部分一致する名前はtargetとみなしません。
- パスのcleanと、targetがなければ追加する既存契約は変更しません。
- 既存directoryの移動・統合や、保存形式・`.env`の変更は行いません。
- UNC共有先の空き容量取得と、アプリ起動時のDBエラー処理は今回の修正対象外です。

## 回帰試験

`pkg/path/builder_test.go`では、末尾／祖先の完全一致と大小文字違い、Unicode文字、
部分一致の除外、返却表記、二重適用時の安定性、pathのcleanを確認します。

`failurearchive/directory_case_windows_test.go`では、実際のWindows filesystem上で
targetを末尾／祖先に持つ2パターンとretryable／permanentの両保存先を確認します。
大小文字だけ変更した設定で既存ファイルを列挙し、`os.SameFile`と内容比較により
同じファイルへ到達できることを確認します。製品への試験用API追加はありません。

## Linux・ビルド検証

Go 1.25.0、`-mod=readonly`で実施しました。

- `pkg/path/...`、`failurearchive/...`、`internal/config/...`の関連全体で`-race`成功。
- path／failure archiveは追加で`-race -count=10 -cpu=1,2,4`成功。
- Windows amd64向けpath／failure archiveの`go vet`成功。
- 継続試験媒体に`pkg/path`を追加し、19 packageの媒体生成成功。
- build scriptの構文検証と`git diff --check`成功。

## Windows 11 VMでの修正前後比較

offlineのWindows 11 Enterprise Evaluation 25H2（`10.0.26200.6584`）、一般権限で
amd64バイナリをnative実行しました。Windowsではrace detectorを使用していません。
修正前バイナリは`builder.go`だけを保存済み旧sourceへGo overlayで差し替え、試験コードは
修正後と同一です。作業treeは巻き戻していません。

| 試験 | PASS | FAIL | SKIP | 終了code |
| --- | ---: | ---: | ---: | ---: |
| 修正前pathの大小文字回帰 | 0 | 1（期待した失敗） | 0 | 1 |
| 修正前failure archiveの回帰 | 0 | 1（期待した失敗） | 0 | 1 |
| 修正後path全体 | 2 | 0 | 0 | 0 |
| 修正後path全体10回 | 20 | 0 | 0 | 0 |
| 修正後failure archive全体 | 23 | 0 | 0 | 0 |
| 修正後failure archive回帰10回 | 10 | 0 | 0 | 0 |

件数は最上位testだけを数えています。修正後は計55件成功、最上位・subtestともに
SKIP 0、全体判定0です。failure archive全体は`-test.list`の23件とも一致しました。

## 媒体・結果保存先

比較媒体：`/tmp/windows-archive-base-case.16r9xJ/archive-base-case-tests.iso`

SHA-256：`757ecb3812948d386e0f74e4815d735055fd2f928c474612414b6ad6c3d49f5f`

VM内結果：`C:\Users\vmtest\archive-base-case-normal-20260911-16r9xJ`

集計画面：`/tmp/windows-archive-base-case-control.KWmdIC/summary.png`

継続試験媒体：`/tmp/windows-archive-base-case.16r9xJ/full-media/fsstore-windows-tests.iso`
（19 packageのbuildを確認した媒体で、今回のVM比較には上記の対象限定媒体を使用）

媒体には明示したtest exeと実行・集計scriptだけを含めました。作業tree、`.env`、
資格情報は含めず、host directory共有・外部networkも使用しません。
生ログはVM内に保持し、件数・終了codeと合成試験データによる失敗理由を画面で確認しました。

終了後はWindowsを正常shutdownし、launcher終了code 0とQMP／TPM socketの消失を
確認しました。仮想diskと各試験結果は保持しています。
