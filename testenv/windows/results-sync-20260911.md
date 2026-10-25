# Windows VMでのSync試験（2026-09-11）

## 結果

最新の作業treeからWindows用exeを再ビルドし、既存の同期関連21 testを10回ずつ実行しました。
**圧縮file本体の実`Sync()`は成功し、現在の親directory同期は`Access is denied`で失敗しました。**
製品コード・既存test・`.env`は、この再試験では変更していません。

| 確認対象 | 結果 |
| --- | --- |
| appendlogの圧縮file本体の実`Sync()` | 10回すべて成功 |
| failurearchiveの圧縮file本体の実`Sync()` | 10回すべて成功 |
| `TestSyncParentDirectory/existing_parent`の実directory同期 | 10回すべて`Access is denied` |
| appendlogの圧縮公開後の実directory同期 | 10回すべて`Access is denied` |
| failurearchiveの圧縮公開後の実directory同期 | 10回すべて`Access is denied` |
| 削除待ちsourceの削除前の実directory同期 | 10回すべて`Access is denied`。削除処理には未到達 |

本体同期の成功を確認したのは、次の2 testです。

- `TestCompressOldFileFinalDirectorySyncFailureKeepsRawAndPublishedPeer`
- `TestCompressOldFileDurabilityPublishedFileDirectorySyncFailureKeepsSourceAndFinal`

この2 testは`(*os.File).Sync`を実行し、その後のdirectory同期だけに所定のerrorを注入します。
注入errorとの一致まで確認してPASSしているため、本体の実同期が成功したことを区別できます。
単に「同期失敗をmockしたtestがPASSした」ことを実同期成功の根拠にしているわけではありません。

圧縮公開後の実directory同期を含むのは、次の2 testです。

- `TestCompressOldFileDurabilityOperationsRunInCommitOrder`
- `TestCompressOldFileDurabilitySyncsTemporaryFileBeforePublishingAndRetiresSource`

両方とも本体同期を通過し、公開後の`SyncParentDirectory`で失敗しています。
起点は`internal/infrastructure/fsstore/durability.go`の`directory.Sync()`です。

```text
SyncParentDirectory() error = sync parent directory ...: Access is denied.
```

## 全体集計

subtestを重複計上しない最上位testの延べ実行数です。全testが10回とも同じ成否でした。

| Package | test名の数 | PASS | FAIL | SKIP | exe終了code |
| --- | ---: | ---: | ---: | ---: | ---: |
| fsstore | 1 | 0 | 10 | 0 | 1 |
| appendlog | 17 | 100 | 70 | 0 | 1 |
| failurearchive | 3 | 20 | 10 | 0 | 1 |
| 合計 | 21 | 120 | 90 | 0 | — |

失敗90回のうち、80回（8 test名）は実directory同期の`Access is denied`です。
残る10回（1 test名）は、`TestRetireCompressedSourceWithDirectorySyncReturnsCleanupErrorWhenSyncAfterRemoveFails`
で、`%q`によりescapeしたWindows pathと生pathを部分一致で比較する既存assertionの問題です。
この10回はOSの実同期失敗として数えていません。

圧縮source retirementの2回目・3回目のdirectory同期へerrorを注入するtestもありますが、
Windowsでは、その前の圧縮file公開後の実directory同期で停止しています。
これを「意図した注入地点まで到達した結果」とは解釈していません。

panic・test timeoutは観測されませんでした。同じ対象・回数をLinuxではrace detector付きで
実行し、3 packageすべて成功しました。

## 実行条件

- Windows 11 Enterprise Evaluation 25H2、`ver`: `10.0.26200.6584`
- 既存QEMU/KVM VM、4 vCPU・8 GiB、offline、インストールISOなし
- 管理者コマンドプロンプトで実行。`fsutil`でも`C:`が読み書き可能なNTFSであることを確認
- `TEMP` / `TMP`: `C:\fsstore-test-temp`
- 結果保存先: `C:\fsstore-sync-20260911`
- Go 1.25.0、`GOOS=windows GOARCH=amd64 CGO_ENABLED=0`、`-tags=timetzdata`
- Windows側のrace detectorはなし。今回の試験は電源断試験ではない
- 全packageの再試験ではなく、以下の正規表現に一致する同期関連testが対象

Windowsで実行した引数は3 exeとも次のとおりです。

```text
-test.v -test.count=10 -test.timeout=180s -test.run="Sync|Durability|RetireCompressedSource"
```

比較用のLinuxコマンド:

```bash
GOCACHE=/tmp/go-build-cache GOTOOLCHAIN=go1.25.0 \
  go test -mod=readonly -race \
  ./internal/infrastructure/fsstore \
  ./internal/infrastructure/fsstore/appendlog \
  ./internal/infrastructure/fsstore/failurearchive \
  -run 'Sync|Durability|RetireCompressedSource' -count=10 -timeout=180s
```

## 保存先

再現用ISO:

```text
/path/to/virtual-machines/heterolog-windows/fsstore-windows-sync-tests-20260911.iso
SHA-256: f77ea5f56b7b7b77b7297749d81bc574dbb676d8f8f39024658cac008d70f8d7
```

生ログ、終了code、引数、OS/NTFS情報の9 file:

```text
/path/to/virtual-machines/heterolog-windows/results-sync-20260911/sync-results-20260911/
```

Windowsから専用仮想USBへcopyし、通常の`shutdown /s /t 0`で終了後にhostへ回収しました。
QEMU終了code 0とQMP/TPM socketの消滅を確認しています。VMは停止中です。
以前のISO・試験結果・snapshotは上書きしていません。

## 判断の範囲

この環境では、管理者権限でも現在の親directory同期が失敗することを再確認しました。
Windowsの同期APIがすべて使えない、または圧縮file本体の同期も失敗する、という結果ではありません。
Windows向けのdirectory同期処理の検討・修正は別途必要です。異なる同期APIやアクセス権での
比較、実機Windows/macOS、電源断・kernel crash・物理storageの耐久性は今回未検証です。
