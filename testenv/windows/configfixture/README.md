# Windows config試験用の公開fixture

リポジトリの公開 `.env.example` だけを入力に、offlineでconfig試験を実行するための読み取り用rootを生成します。実運用の `.env` は読み込みません。

```sh
go run ./testenv/windows/configfixture .env.example /tmp/new-config-fixture
```

出力先は存在していないdirectoryを指定してください。生成物は `.env`、`.env.example`、root検出用の最小 `go.mod`、由来を記した `README.txt` の4ファイルです。

認証情報、header、宛先、channel、tenant、indexなどの自由値は、固定のダミー値・loopback・予約された `.invalid` ドメインへ置き換えます。コメントは除去し、その他の文字列はallowlistで確認します。未知の値、重複key、不正な構文、実運用 `.env` の入力、既存出力先はエラーにします。元の値はエラーへ含めません。

合成した両ファイルでは、各backendの設定を検証するためにfile・stdout・Slack・OpenSearch・Lokiを有効にします。認証情報もテスト専用のダミー値で補います。endpointやindexにはsample/example別のダミー値を使い、読み分けも検証します。

`HETEROLOG_SYSTEM_LOG_DIR`、`HETEROLOG_SYSTEM_LIB_DIR`、`HETEROLOG_APP_STATE_DIR` は、元の値やOSの個人用directoryを参照せず、それぞれ `fixture-storage/logs`、`fixture-storage/spool`、`fixture-storage/state` という固定相対pathを出力します。このrootでは設定の読み取り・変換だけを行い、実際のwriter試験は自分の一時directoryを使用します。

Windowsでは、この読み取り用rootを作業directoryとして `config.exe` を起動してください。`TEMP` と `TMP` は媒体の外にある書き込み可能なguest専用directoryへ設定します。実環境の認証情報を媒体へ追加してはいけません。

このrootでのPASSは、公開fixtureのloader／mapper契約を検証した結果です。privateな実運用 `.env` の監査結果ではありません。通常のGoテストも同じ `internal/testutil/configfixture` の生成処理を使い、テストごとの一時ディレクトリから設定を読み込みます。
