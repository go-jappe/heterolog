# Windows圧縮ログ名の衝突検証（2026-09-11）

## 修正範囲

`internal/config/validation/mapped/compressed_log_file_upload_name.go`で、
Windowsの比較用identityだけを小文字へ統一しました。再送matcherが使用する
`logpattern.fileNameKey`と同じ規則です。Linuxの大小文字区別、設定値の元表記、
実ファイル名、S3 object key、`.env`は変更していません。

例えば、base error系列の`error_app_*`とinternal default系列の`ERROR_APP_*`を
別のS3宛先へ設定した場合、Windowsでは起動時の設定検証で衝突を拒否します。
同じspoolファイルが複数宛先に一致して再送できなくなる設定を防止します。

今回変更した製品コードは上記の比較処理のみです。別途指摘したcheckpointの
ACL／ReadDir競合と、Job Objectのメモリ監視は今回の修正には含みません。

## 回帰テスト

`internal/config/validation/mapped/compressed_log_file_upload_name_case_test.go`を追加。

- sync／asyncのinline・runner、Deliveryのrunnerを確認。
- default同士とerror／internal default間の大小文字衝突を確認。
- prefix・target・extensionの大小文字、別target・別extensionを区別。
- エラーの診断には設定した元表記を残す。
- Linuxでは大小文字だけが異なる設定を引き続き許可。
- 既存テストで無効系列と別serviceを衝突対象から除く契約も確認。

## Windows 11での実行

既存のoffline Windows 11 VM、NTFS、一般ユーザー`vmtest`で実行しました。
OS表示は`10.0.26200.6584`です。件数は最上位test単位で、subtestを重複計上しません。

| 実行 | PASS | FAIL | SKIP | 終了code |
| --- | ---: | ---: | ---: | ---: |
| 修正前比較処理＋新規回帰テスト | 0 | 3 | 0 | 1（期待した失敗） |
| 修正後mapped package全体 | 46 | 0 | 0 | 0 |
| 修正後の関連8 testを各10回 | 80 | 0 | 0 | 0 |

修正前の比較用exeはGoのoverlayでidentity処理だけを旧実装へ戻して構築しました。
作業treeは巻き戻さず、回帰テストは修正後と同じものを使用しています。
修正前の検証漏れを再現し、修正後の実行は全て成功しました。
修正前の失敗原因も、衝突があるのに検証errorが`nil`になることを画面で確認しました。

試験媒体:

`/tmp/windows-name-validation-QZksoA/mapped-case-tests.iso`

SHA-256:

```text
2d70c25ab4ef14ea7f77960ed66ef896ab13b33b1bfcf053dd8cb1bbbc7d1f10
```

媒体には2本のtest exeと実行・集計scriptだけを含め、`.env`や実際の資格情報は
含めていません。生ログはVM内の次のdirectoryに保持しています。

`C:\Users\vmtest\mapped-name-case-20260911-QZksoA`

生ログの外部回収は行わず、件数・終了code・必要最小限の失敗原因を画面で確認しました。
Windows native実行はrace detectorなしです。
終了後はWindowsの正常shutdownを実行し、launcher終了code 0とQMP／TPM socketの
終了を確認しました。仮想diskと過去の試験結果は保持しています。

## Linux・静的確認

次の試験に成功しました。

```text
go test -mod=readonly -race ./internal/config/... \
  ./internal/infrastructure/fsstore/name/logpattern \
  ./internal/infrastructure/objectstorage/compressedlogspool -count=1 -timeout=180s

go test -mod=readonly -race ./internal/config/validation/mapped \
  -run 'CompressedLogFile.*(Name|Series)' -count=10 -timeout=60s
```

Windows向けmapped packageの`go vet`、独立レビューと対象テストのrace反復、
`git diff --check`も成功しました。

新規テストの初回Linux実行では、error系列を有効にしたfixtureに保持設定が不足して
失敗しました。正常な保持値をfixtureへ追加して再実行済みで、製品の保持仕様や
バリデーションを緩める変更はしていません。
