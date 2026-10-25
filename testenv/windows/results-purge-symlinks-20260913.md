# Windows purgeの非通常file除外試験（2026-09-13）

## 最新判定の仕様訂正

以下は訂正前のコードをWindowsで試験した記録です。その後、最新判定はファイル名の
日付・世代番号を正本とする仕様に戻しました。リンクを除外して古い通常ログを追加保護する
`TestFileSeriesManagerPurgeProtectsLatestRegularLog`は、ファイル名で最新を選ぶ
`TestFileSeriesManagerPurgeUsesLatestFileName`へ置き換えています。
この記録のWindows実行結果は、置き換え後のテストの実行を証明するものではありません。
リンクを容量計算・purge対象から除外する処理は維持しています。

## 結果

製品・fixture修正の最終確定後に4 packageを再ビルドし、専用Windows VMで
27個の親テストを各10回実行しました。親テストは270成功・0失敗・0skip、
subtest込みでは660成功・0失敗・0skipです。
全exeの終了code、全体終了code、選択件数と実行件数の検査はいずれも0でした。

| テストexe | 選択した親テスト | 親テスト成功 | subtest込み成功 |
| --- | ---: | ---: | ---: |
| fsstore | 17 | 170 | 330 |
| file | 2 | 20 | 100 |
| runner | 4 | 40 | 190 |
| fsarchivehandler | 4 | 40 | 40 |
| 合計 | 27 | 270 | 660 |

`-test.list`の選択数×10と親テストの実行件数がexeごとに一致し、未実行はありません。
`^\s*--- (FAIL|SKIP):`でも全ログを確認し、親だけでなくsubtestにも失敗・skipがありません。

## 対象

- fsstore：`^Test(GetFilesInfo|RemoveObservedFile|Purge)`に一致する17件。
  新規の`TestGetFilesInfoExcludesNonRegularPaths`、`TestPurgeIgnoresLinkedFileMetadata`、
  `TestRemoveObservedFileRejectsNonRegularPaths`、Windows専用の
  `TestPurgeExcludesWindowsJunction`を含みます。
  file／directory symlink、dangling／loop link、通常directory、junctionの除外と、
  リンク先の容量・mtimeが正常ログのpurge判断に混入しないことを検証しました。
- file：`TestInlinePurgeIgnoresNonRegularLogEntries`、
  `TestFileLoggerCleanupInternalErrorHandlersCanCloseSameLogger`。
- runner：`TestRunnerPurgeIgnoresNonRegularLogEntries`、
  `TestFileSeriesManagerPurgeProtectsLatestRegularLog`、
  `TestFileSeriesManagerRunOnceReturnsGenerationPurgeFailureAsError`、
  `TestFileSeriesManagerStopsLaterPurgeStepsAfterAgeDeleteFailure`。
  最新の通常ログ保護はsymlink／dangling symlink／directory、当期／次期、
  age／directory-sizeの12 subcaseを各10回実行しました。
- fsarchivehandler：`TestDirectoryPurgeFailureDoesNotConsumeUploadRetrySchedule`、
  `TestAgeDeleteFailureStopsBeforeDirectorySizePurge`、
  `TestGenerationDeleteFailureStopsBeforeDirectorySizePurge`、
  `TestInlineGenerationPurgeFailureRetriesAtNextNaturalObservation`。

既存の削除失敗テスト7件は、通常fileを読めるままfileのDELETEと親directoryの
FILE_DELETE_CHILDを拒否するWindows ACL fixtureへ変更した最終版で実行しました。
失敗通知、後続purgeの停止、自然な次回観測での再試行、Closeの既存契約も対象です。

## 環境・媒体・停止確認

- Windows 11 Enterprise Evaluation、日本語版。command promptは`10.0.26200.6584`、
  CIMのversionは`10.0.26200`。試験用一時directoryと結果はC:のNTFSへ保存しました。
- 管理者consoleのintegrity SID `S-1-16-12288`で実行しました。
  symlink fixtureをskipせず作るための条件で、製品の通常実行に管理者権限を要求するものではありません。
- 既存専用QEMU/KVM VMのみを`offline`（`-nic none`）で起動し、KVM有効を確認しました。
  host directory共有、port forwarding、新規installationはありません。
- Go 1.25.0、`GOOS=windows GOARCH=amd64 CGO_ENABLED=0`、`timetzdata`同梱。
  Windows実行はrace detector付きではありません。
- 媒体には対象4 exeと実行・集計scriptだけを収録し、`.env`、実運用設定、認証情報は持ち込んでいません。
- 最終媒体：`/tmp/windows-purge-symlink.ntKO7r/purge-symlink-tests-v2.iso`
- SHA-256：`4cf14d108ecc0e8bce89d7b482036e080f2085c47bd73dce23e595258b8d5da3`
- 媒体内の`run-purge-tests.cmd`で各exeを`-test.v -test.count=10 -test.timeout=300s`と
  対象正規表現付きで実行し、`check-all-results.ps1`でもsubtestと実行件数を検査しました。
- guest結果：`C:\purgesymlink13a`。生ログはhostへコピーせず、終了code・件数・OS情報のconsole表示を確認しました。
  集計画面：`/tmp/windows-purge-symlink-control.QwrEdm/batch-results-v2.png`。
- 最初の媒体では試験開始前にfixture修正を待つためWindowsを正常停止し、試験は実行していません。
  上記結果は最終媒体だけのものです。
- 最終試験後もWindowsの`shutdown /s /t 0`で正常終了しました。launcher終了code 0、
  QEMU／swtpm process停止、QMP／TPM socketとPID fileの消失をhostで確認しました。

今回の選択試験の結果であり、全packageのWindows試験ではありません。
Lstat後から削除直前までの外部差替え競合を含む安全性全般や、電源断耐久性の検証ではありません。
