# 開発の決まり

heterologのコードを書くときの決まりです。

## コメントと文書の言語

- 公開APIの説明は英語で書きます。対象は、root packageと、`internal` でない `pkg/` のpackageにある、公開された名前（型、関数、メソッド、変数、定数、structのfield）のdoc commentです。package docと、Example関数と `examples/` の説明も英語にします。
- ほかのコメントは日本語で書きます。対象は次のコメントです。
  - `internal` のpackage（公開された名前も含みます）
  - 公開されていない名前と、公開されていない型のメソッド
  - テストと関数の中
  - scriptとCIの設定
- 日本語は「です・ます」で書き、英語の技術用語はそのまま使います（例: 「loggerを作ります」）。
- コードを変えたら、コメントも合わせて直します。
- `README.md` と `README.ja.md`、`docs/reference.md` と `docs/reference.ja.md` は、英語と日本語の組です。片方を変えたら、もう片方も同じ内容にします。

## packageの分け方

- root packageは公開の窓口だけにし、処理を置きません。
- 入口（HTTP、CLI、slog、環境変数）は `internal/presentation` に置き、処理は `internal/application` に置きます。
- ただし、ログファイルの手動復旧（`log-recovery`）のように、infrastructure層だけで完結する処理には、application層のpackageを作りません。入口は、`cmd/` から渡された `internal/bootstrap` の関数で処理を呼び、infrastructure層の型を使います。
- 出力先ごとのloggerは `internal/infrastructure` にあり、環境変数から設定を組み立てる処理は `internal/config` にあります。
- `internal/application` がimportするのは、`internal/application` のほかのpackage、`internal/domain`、`pkg/` だけです。`internal/infrastructure`・`internal/config`・`internal/presentation` はimportしません。テストも同じですが、`internal/testutil/observertest` のような試験用の部品は使えます。
- application層は、infrastructure層の都合に関わりません。再試行、待つ間隔、既定値、設定の値の範囲、file・checkpoint・spoolの扱いは、infrastructure層が受け持ちます。application層が扱う設定は、稼働中に変えるlevelとsamplingのように、application層の処理が使うものだけです。
- application層の処理にinfrastructure層の実装が要る場合は、application層が必要とする形のinterfaceをapplication層に置き、infrastructure層がそれを実装します。両方をimportして組み合わせるのは、`internal/bootstrap` と `cmd/` だけです。root packageは、loggerと既定の設定を `internal/bootstrap` から得ます。
- 組み立てたloggerや実際のfileで動きを確かめるテストは、`internal/bootstrap` かinfrastructure層に置きます。
- 実装が `internal` にある公開の型（`Runtime` など）は、root packageにstructとして定義します。中の実装はpointerで持ち、メソッドはそれを呼ぶだけにします。
- ほかの公開の型は、`internal` でない `pkg/` のpackageに定義し、root packageでは別名（type alias）にします。root packageが `internal` の型を別名にするのは `Option` だけです。
- 同じ規則（検証など）を2か所に書きません。1か所に置き、ほかからはそれを呼びます。

## 設定（環境変数）

- キーはすべて `HETEROLOG_` で始めます。例外は、AWS SDKの標準の変数（`AWS_REGION` など）と、テストだけで使う切り替え（`*_INTEGRATION_TEST` など）です。
- loggerの設定の値の範囲（負の数を許さない、など）は `pkg/logger` の検証に書き、環境変数の値を型に変える処理（`pkg/env`）には足しません。コードで作った設定にも、同じ検証が働くようにするためです。
- retryの回数は、最初の送信の後に送り直す回数です。0なら送り直しません。
- キーの名前を変えたら、古い名前を `internal/config/renamedkey` に足します。古い名前が設定に残っていると、新しい名前を示すエラーになります。
- キーを足したり変えたりしたら、`.env.example` と文書も直します。

## テスト

- 動きのテストは、その動きを実装したpackageに置きます。root packageには、公開の窓口のテストだけを置きます。
- pushの前に、`gofmt` で整形し、`go vet ./...` と `go test ./...` を通します。
- 並行に動くコードを足したら、そのpackageを `.github/workflows/test.yml` のraceテストの一覧に足し、`go test -race` でも確かめます。
- 環境変数のキーや、出力先とのやり取りを変えたら、結合テストも実行します（`testenv/integration/README.md`）。

## commitとpush

- 種類の違う変更（機能、リファクタリング、コメントだけの変更）は、別のcommitにします。どのcommitでもbuildとテストが通るようにします。
- pushの前に見つけた直しは、直す元のcommitにまとめます。
- commit messageは英語で、何をするかを書きます（例: `Reject admin APIs whose outputs share live settings`）。
- pushとtagは `tools/publish/publish.sh` で行い、`git push` は使いません。手順は `tools/publish/README.md` にあります。
