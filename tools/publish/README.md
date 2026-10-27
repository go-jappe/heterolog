# GitHubへのpush

このrepository（`go-jappe/heterolog`）のcommitとtagは、すべて公開用の名前 `go-jappe` と
GitHub Appのbotのemail（`<番号>+<slug>[bot]@users.noreply.github.com`）で作り、日時はUTCで
記録します。GitHubへの書き込みは、GitHub Appのinstallation tokenだけで行います。`publish.sh`
は、そのための検査とpushを行います。

- GitHubへの接続には、Appのtokenだけを使います。利用者のgit設定（認証情報helper、URLの
  書き換え、hookなど）と `.netrc` は使いません。
- pushする前に、GitHubのmainにまだないcommitをすべて検査し、一つでも満たさなければpushしません。
  - 作者とcommitterが公開用の名前とemailで、日時がUTCであること。
  - 署名（gpgsig）やmergeで取り込んだtag（mergetag）などの追加のheaderがないこと。
  - messageに、`Co-authored-by:` や `Signed-off-by:` など人の名前を書くtrailerがないこと。
  - message・file名・fileの内容に禁止語がないこと。途中のcommitだけにある語も対象です。
  - `.env` や秘密鍵などの設定fileを含まないこと。
- 禁止語は、gitの名前とemail（global設定とこのcloneの設定）、GitHubのnoreply emailの利用者
  番号、実行userの名前とhome directory、`forbidden-words` の語です。公開用の名前とemail、
  repository名に含まれる語と、4文字未満の語は使いません。
- mainはfast-forwardだけで進め、force pushはしません。
- GitHubの画面やAPIからの書き込み（pull requestのmerge、fileの編集、tagやReleaseの作成、
  workflowの再実行など）は、操作したaccountとして記録されます。このrepositoryへの書き込みは、
  このscriptだけで行います。

## 準備（端末ごとに一度）

必要なcommandは、git 2.31以降、bash（macOSのbash 3.2でも動きます）、curl、jq、opensslです。

`~/.config/heterolog-publish/` に次のfileを置きます。どれもrepositoryには入れません。

- `app.pem`: GitHub Appの秘密鍵です。`chmod 600` にします。
- `config`: 次の内容です。`BOT_USER_ID` は、
  `https://api.github.com/users/<APP_SLUG>%5Bbot%5D` の `id` です。

  ```text
  APP_ID=<AppのApp ID>
  APP_SLUG=go-jappe-publisher
  APP_KEY=~/.config/heterolog-publish/app.pem
  PUBLIC_REPO=go-jappe/heterolog
  PUBLISH_NAME=go-jappe
  BOT_USER_ID=<botの利用者番号>
  ```

- `forbidden-words`: 公開してはいけない語を1行に1つ書きます（他の端末の利用者名など、自動では
  集めない語）。`#` で始まる行はコメントです。

repositoryをcloneしたら、そのcloneで一度だけ次を実行します。

```sh
tools/publish/publish.sh setup
```

`setup` は、このcloneを次のように設定します。

- commitの名前とemailを公開用にし、commitとtagの署名を止めます。
- `origin` のpush先を使えない値にして、`git push origin` が失敗するようにします。
- pre-push hookを置き、別のremote名やURLを使ったこのrepositoryへの `git push` も止めます。

repositoryがprivateの間は、`git fetch` や `git pull` に個人の認証情報が要ります。代わりに
`tools/publish/publish.sh fetch` を使います。

## commitとpush

commitは普段どおり作ります。日時をUTCで記録するには `TZ=UTC git commit` とします。UTCでない
commitは、pushの前に `fix` で直せます。

```sh
tools/publish/publish.sh push --dry-run   # 検査して、pushするcommitを表示するだけ
tools/publish/publish.sh push             # 検査してpushする
tools/publish/publish.sh fix              # まだpushしていないcommitの名前・email・日時を直す
```

`fix` は、まだpushしていないcommitの作者とcommitterを公開用の名前とemailにし、日時を同じ時刻の
UTCにして作り直します。内容とmessageは変えず、署名などの追加のheaderは除きます。merge commitは
作り直さないため、先にmainをorigin/mainへrebaseします。messageの問題は、`git commit --amend`
や `git rebase -i` で直します。

他の端末がpushしていた場合は、mainをGitHubのmainへrebaseしてからpushします。rebaseしたcommitは
日時が変わるため、`fix` も実行します。

```sh
tools/publish/publish.sh fetch
git rebase origin/main
tools/publish/publish.sh fix
tools/publish/publish.sh push
```

## release

pushしたmainにversionのtagを付けます。tagはannotated tagで、作者は公開用の名前とemail、日時は
UTCです。手元のmainとGitHubのmainが同じでなければ、tagを付けません。

```sh
tools/publish/publish.sh release --dry-run v0.5.0
tools/publish/publish.sh release v0.5.0
```

## GitHub側の設定

organizationの所有者が一度だけ行います。

1. organizationでGitHub Appを作ります（Settings → Developer settings → GitHub Apps → New GitHub
   App）。
   - Webhookは使わないため、Activeのcheckを外します。
   - Repository permissionsは、Contentsを「Read and write」、Workflowsを「Read and write」に
     します（Metadataの「Read-only」は自動で付きます）。
   - 「Where can this GitHub App be installed?」は「Only on this account」にします。
2. AppのPrivate keyを生成し、pushする端末の `~/.config/heterolog-publish/app.pem` に保存します。
3. Appをこのrepositoryだけにinstallします（Install App → Only select repositories）。
4. rulesetを作り、Appだけが書き込めるようにします。対象はすべてのbranchとtagで、Restrict
   creations・Restrict updates・Restrict deletions・Block force pushesを有効にし、Bypass list
   にはこのAppだけを入れます。Freeのplanでは、rulesetはrepositoryがpublicの間だけ働きます。
