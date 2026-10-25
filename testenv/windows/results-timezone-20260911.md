# アプリへのタイムゾーンデータ同梱（2026-09-11）

## 修正範囲

製品変更は`main.go`への`import _ "time/tzdata"`と説明コメントの追加だけです。
通常の`go build`でもIANAタイムゾーンデータをexeに含め、外部のデータがない場合に
`Asia/Tokyo`等を解決できるようにしました。実行時のダウンロードはありません。

Windows限定のimportではなく、同じデータ不足が起こり得るLinux等でも有効です。
library packageが埋め込みを強制する設計にはせず、アプリのmainが同梱を決めます。
`.env`、メモリ上限の判定、globによるファイル検索は変更していません。

## 回帰テスト

`timezone_windows_test.go`に`TestApplicationTimezonesWithoutExternalData`を追加しました。

- 新しい子processを起動し、他のtestや環境変数の初期化cacheから独立して確認。
- `GOROOT`と`ZONEINFO`にはそれぞれ専用の不存在pathを指定し、実際に不存在かも確認。
- WindowsにはGoが探索するUnix形式のsystem zoneinfo directoryがないため、
  この条件ではmainの埋め込みデータが必要です。
- 実際のlogger formatter生成経路で、既定のAsia/TokyoとNew Yorkの冬・夏のoffsetを確認。
- 不正なtimezone名をerrorとして拒否することも確認。

修正前の比較用exeは、Go overlayでmain.goの追加importを除いた内容を使用しました。
同じ回帰テストを含み、作業treeは巻き戻していません。両exeとも`timetzdata`タグは
使っていません。testだけに埋め込みを追加して製品の欠落を隠す構成ではありません。

## Windows VMでの結果

既存のWindows 11 Enterprise Evaluation 25H2、offline VMの一般権限で実行しました。
最上位testの件数で、子processやsubtestの出力は重複計上しません。

| 実行 | PASS | FAIL | SKIP | 終了code |
| --- | ---: | ---: | ---: | ---: |
| 修正前＋新規回帰 | 0 | 1 | 0 | 1（期待した失敗） |
| 修正後application root全体 | 4 | 0 | 0 | 0 |
| 修正後の新規回帰×10回 | 10 | 0 | 0 | 0 |

修正後は延べ14件成功、失敗・skipなし、全体判定も0でした。
修正前は`unknown time zone Asia/Tokyo`と`unknown time zone America/New_York`により、
Tokyo・New York冬・New York夏の3 subtestが失敗したことを確認しました。
Windows nativeはrace detectorなしです。

アプリ全体をDB・外部サービス込みで起動する試験ではなく、mainのimportを含むtest exeで
実際のloggerのタイムゾーン解決・formatter生成を確認した結果です。

## Linux・ビルド確認

Go 1.25.0、`-mod=readonly`で次のLinux試験が成功しました。

```text
go test -race . ./internal/infrastructure/logger/logging \
  ./internal/infrastructure/logger/timestamp ./internal/infrastructure/logger/file \
  ./internal/config/... -count=1 -timeout=180s
```

通常条件のWindows向け`go build`、root packageの`go vet`、独立レビュー、
`git diff --check`にも成功しました。Linux試験だけを「外部データなし」の証明とはせず、
その条件はWindowsの子processで検証しています。

## 今後の試験媒体

`build-test-media.sh`にもapplication rootのexeを追加しました。既存15 library packageの
試験は`timetzdata`タグ付きのままですが、rootだけは`-tags=`でタグを明示的に除外します。
これにより`GOFLAGS=-tags=timetzdata`を継承しても、製品のmainからimportが消えた場合に
回帰を検出できます。`run-tests.cmd`にもapplicationの実行・結果の上書き保護を追加しました。

`GOFLAGS=-tags=timetzdata`を指定した状態で最終scriptによる16本の媒体生成に成功しました。
そのapplication exeは、今回nativeで実行したタグなしexeとSHA-256が一致しています。
16 packageすべてを今回VMで再実行した、という意味ではありません。

## 実行媒体と結果保存先

今回のnative比較には、2本のtest exeと実行・集計scriptだけのread-only ISOを使用しました。
`.env`、資格情報、作業tree、host共有、外部networkは使用していません。

`/tmp/windows-timezone.SmK8qi/timezone-tests.iso`

SHA-256:

```text
16df3e01bac5005730e17046e6a0f9223f767873cb131b92dfaee4790bf15b1d
```

VM内の結果保存先は`C:\Users\vmtest\timezone-normal-20260911-SmK8qi`です。
生ログはゲスト内に保持し、hostへ回収せず、件数と必要最小限の失敗原因だけを画面で確認しました。
終了後はWindowsを正常shutdownし、launcher終了code 0とQMP／TPM socketの消失を確認しました。
