# macOS実機試験

CIのmacOS（GitHub-hosted runner）は、既定の大小文字を区別しないAPFSだけで全packageを実行します。
この手順は、macOSの実機で次の条件を追加で確かめます。

- 既定の一時ディレクトリでの全packageと、CIのLinuxだけで実行しているrace試験の一覧
- 大小文字を区別するAPFSを試験の一時ディレクトリにした全package
- ExFAT・FAT32を試験の一時ディレクトリにした、大小文字の判定の試験
  （`pkg/path/pathidentity` の `TestDarwinDirectoryCaseMatchesTemporaryFilesystem`）と、
  大小文字を区別しない一時ディレクトリでだけ動くfile loggerの試験
- 同じExFAT・FAT32での `pkg/path/pathidentity` と `internal/infrastructure/logger/file` のpackage全体（参考）
- Docker上の結合試験（[testenv/integration](../integration/README.md) の全group）

判定の試験は、macOSの判定（`pathconf` の `_PC_CASE_SENSITIVE`）が、作成したファイルを別表記で
開けるかという実際の振る舞いと一致することを確かめます。CIのmacOSでも毎回実行します。

2026-10-24に利用者のMacで実行した結果は、[実行記録](results-20261024.md)にあります。

## 前提

- macOS（Apple Silicon・Intelのどちらでも可）。root権限は使いません。
- Xcode Command Line Tools（`git` と、race試験に必要な `clang`）。
- Go 1.21以降。`go.mod` の版より古くても、1.21以降なら既定の `GOTOOLCHAIN=auto` で必要な版を
  自動で取得します。1.21より前のGoは `go.mod` を読めず、全段階が `invalid go version` で失敗します。
  その場合は新しいGoを入れてください（例: `brew install go`）。`go version` が古い版を表示するときは、
  `PATH` で先に見つかるGoを確かめてください。
- このリポジトリの最新のmain。
- 結合試験を行う場合は、Docker（Docker Desktopなど）が起動していること。使えなければ結合試験だけ
  `SKIP` と記録します。結合試験は使い捨てのlocal container向けの `testenv/integration/ci.env` だけを
  使い、開発者の設定の宛先へは送りません。
- disk imageはsparse imageで、書き込んだ分だけdiskを使います。大小文字を区別するAPFSのimageは、
  設定例の `HETEROLOG_MIN_AVAILABLE_DISK_SPACE_GB`（1GB）を満たす16GBの容量で作りますが、実際の使用は
  試験が書き込む分（数百MB程度）です。既定の一時ディレクトリにも1GB以上の空きが必要です。
- 初回は、ターミナルからdisk image上のファイルへのアクセス許可を求めるダイアログが出ることがあります。
  許可してください。

## 実行

```sh
bash testenv/macos/run-real-machine-tests.sh             # 結果は ~/heterolog-macos-results-日時/
bash testenv/macos/run-real-machine-tests.sh ~/results-1 # 結果directoryを指定（存在しないpath）
```

各段階は失敗しても次の段階へ進みます。終了コードは、参考の段階を除くいずれかが失敗すると1、
すべて通ると0、macOS以外や必要なcommandがない場合は2です。所要時間は、結合試験のimage取得を
含めて15〜30分程度です。

## 結果

結果directoryに次のファイルを残します。

| ファイル | 内容 |
|---|---|
| `summary.txt` | 段階ごとの `PASS`・`FAIL`・`SKIP` と、必須の段階の失敗数 |
| `environment.txt` | macOS・CPU・Go・Docker・commit・既定の一時ディレクトリの空き容量 |
| 段階名の `.log` | 各段階の `go test` の出力 |
| `volumes.log` | disk imageの作成・mountと、大小文字の扱いの確認 |
| `integration-start.log`・`integration-live-tests.log`・`integration-stop.log` | 結合試験の起動・試験・停止 |

段階名の末尾が `-reference` の段階は参考で、全体の結果に含めません。macOSはFAT32・ExFATに拡張属性を
`._` で始まる別のファイルとして保存するため、ディレクトリの中身を数で確かめる試験などが失敗します。
macOS 26のExFATは、置換しないrename（`renamex_np` の `RENAME_EXCL`）にも対応していません
（[2026-10-24の実行記録](results-20261024.md)）。失敗の内容を確かめ、これら以外の大小文字や
保存先の扱いに関わるものを調べてください。

## 後片付け

終了時（失敗・中断を含む）に、disk imageをdetachして削除し、結合試験のcontainerも停止します。
強制終了などで残った場合は、`hdiutil info` で `heterolog-macos.` を含むimageを確かめて
`hdiutil detach <mount point>` で外し、`$TMPDIR/heterolog-macos.*` を削除してください。
結合試験のcontainerとnetworkは `bash testenv/integration/stop-services.sh` で削除できます。
