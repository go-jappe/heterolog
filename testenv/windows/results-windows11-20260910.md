# Windows 11 filesystem試験（2026-09-10）

## 結論

通常のWindows 11を専用VMへインストールし、filesystem関連の3 packageを実行しました。
最上位test単位で104件成功・17件失敗・1件skipです。製品のWindows対応は未完了です。
インストール用ISOとnetworkを外して再起動した後の再実行も、同じtest名・成否でした。

## 環境

- Windows 11 Enterprise Evaluation 25H2 日本語版、`ver`: `10.0.26200.6584`
- QEMU 6.2.0 / KVM、4 vCPU、8 GiB RAM、UEFI、TPM 2.0
- 専用80 GiB qcow2。Windowsは空き領域へインストールし、既存の予備試験用2 GiB NTFS領域は保持
- 管理者コマンドプロンプトで実行。`TEMP` / `TMP` はNTFS上の `C:\fsstore-test-temp`
- Go 1.25.0、`GOOS=windows GOARCH=amd64 CGO_ENABLED=0`、`-tags=timetzdata`
- 各exeは `-test.v -test.timeout=180s`。Windows側ではrace detectorなし
- テストexeと実行用cmdだけをread-only ISOで渡し、hostのsource tree、`.env`、認証情報は非共有
- 初回のみactivation用NATあり。再起動後の2回目は `-nic none`、port forwardingなし
- HEADは `d4e387d8327ee1951ce7711980a7aa374c15fc1a`。既存の未commit変更を含む作業treeからビルド

[Microsoft公式ISO](https://aka.ms/Win11E-ISO-25H2-jp-jp)を使用し、
[公式SHA-256一覧](https://cdn-dynmedia-1.microsoft.com/is/content/microsoftcorp/microsoft/bade/documents/products-and-services/en-us/owned-and-operated/Verify-Download-Win11-Enterprise.pdf)
との一致を確認しました。

```text
d86956634d5ee8ec91ac8e7c0b28a0f7e58d3a5b661bf661a1220e6a3f4649ce
```

評価版の正規オンライン認証は成功しました。`slmgr.vbs /xpr`で確認した期限は
2026-12-09 15:27:42です。Microsoftアカウントは作成せず、Windowsの正式なローカル
アカウント作成経路を使用しました。ハードウェア要件・認証の回避操作は行っていません。

## 試験結果

subtestを重複計上しない、最上位test単位の件数です。2回とも以下の結果でした。

| Package | PASS | FAIL | SKIP | exe終了code |
| --- | ---: | ---: | ---: | ---: |
| fsstore | 30 | 1 | 1 | 1 |
| appendlog | 54 | 14 | 0 | 1 |
| failurearchive | 20 | 2 | 0 | 1 |
| 合計 | 104 | 17 | 1 | — |

全体終了codeは1です。panic・test timeoutは観測されませんでした。

### 製品側：親ディレクトリの同期

15件は親ディレクトリの同期失敗に起因します。起点は
`internal/infrastructure/fsstore/durability.go` の `directory.Sync()` です。

```text
SyncParentDirectory() error = sync parent directory ...: Access is denied.
```

通常のWindows・管理者権限・NTFSでも再現しました。WinPE固有の失敗でも、単に
一般ユーザーで実行したための失敗でもありません。圧縮file公開、圧縮source retirement、
checkpoint同期などへ影響しており、Windows向けの同期処理を別途検討・修正する必要があります。
このVM構築・検証作業では製品処理を変更していません。

### 試験側：Windows pathのescape

2件はerror messageへ `%q` で埋め込んだpathを、escape前の生pathで部分一致検査する
assertionに問題があります。

- `compression_source_conflict_test.go` の `TestRecoverCompressionSourceDeletePendingPathQuarantinesDifferentContent`
- `compression_source_retirement_durability_test.go` の `TestRetireCompressedSourceWithDirectorySyncReturnsCleanupErrorWhenSyncAfterRemoveFails`

前者にはdirectory同期errorも併合されます。後者は試験で注入したerrorだけでも失敗するため、
Windows pathの期待値の問題は同期失敗とは独立しています。既存testのassertionは変更していません。

### skip・予備試験との差

`TestPurgeByAgeIfNeededKeepsStatFailure`は、Windowsのsymlink-loopの挙動差により
既存testが明示的にskipしています。

[WinPEでの予備試験](results-20260910.md)で失敗したtimezone関連の2件は、
`timetzdata`を含めたことで成功しました。残る失敗名は予備試験と一致しています。
ただし、失敗地点より後の検証は未到達なので「ほかに問題がない」ことを保証する結果ではありません。

## 保存先・再現

VMとログは以下に保存しています。

```text
/path/to/virtual-machines/heterolog-windows/
├─ system.qcow2
├─ fsstore-windows-tests-timetzdata.iso
├─ results-windows11-20260910/
│  ├─ windows11-results/         # 初回: 3 log、4 exit code、OS/NTFS/認証情報の12 file
│  └─ windows11-offline-results/ # 再起動後: 3 log、4 exit codeの7 file
└─ installed-state-20260910/    # 停止時のUEFI/TPM状態
```

試験ISOのSHA-256:

```text
c99ec6b505b4053a0a702b57fd1bea9e243bdccfdea6d5222a1e8b9c5b229f74
```

Windows側の結果directoryは `C:\fsstore-results\windows11-20260910` と
`C:\fsstore-results\windows11-offline-20260910` です。
専用仮想USBへcopyし、Windowsを正常shutdownしてからhostへ回収しました。

ローカルアカウント`vmtest`で、保存したpasswordによるロック解除・再ログインを確認済みです。
初期入力の相違は、設定済みの秘密の質問を使うWindows標準のpassword再設定で解消しました。
passwordおよび秘密の質問の回答はownerだけが読めるVM状態directoryに保存し、reportへ記載しません。

最終状態は正常shutdown済みです。QEMUの終了code 0とQMP/TPM socketの消滅を確認しました。
停止状態のqcow2 snapshot `windows11-eval-installed-20260910` と、対応するUEFI/TPMの
copyを保存しています。snapshotの復元は行っていません。

再利用手順は[README](README.md)に記載しています。launcherの9つのmock scenario、
shell構文検証、`git diff --check`も成功しました。

## 対象外・残作業

- 製品のWindows向け同期処理と、既存testのWindows path assertionの修正
- macOSでの試験（この非Apple製Linux上にmacOS VMは作成していない）
- 全applicationのWindows対応確認、および全packageの試験
- VM・hostの強制停止、電源断、kernel crash、物理storageを含む永続性保証の検証

今回の成功判定は「Windows VMを構築して対象試験を実施・回収できた」ことです。
Windows上で製品の全試験が成功した、またはfsyncの電源断耐性を実証したという意味ではありません。
