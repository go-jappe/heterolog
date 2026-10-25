# Windows同期APIの比較と修正（2026-09-11）

## 結論

Windows 11/NTFSで、directory同期とmtime更新後の同期の`Access is denied`を解消しました。
最終版の関連43 testを一般権限で10回ずつ実行し、430回すべて成功しています。
全体試験は388 PASS・2 FAIL・3 SKIPで、残る2件はUnix権限bitのassertionです。
Windows ACLの設定・検証と電源断試験は未実施なので、Windows全対応完了とは扱いません。
VMはWindowsの正常shutdownで停止し、ログを回収済みです。

## 今回の変更

- directoryのopenだけをOS別に分離。Windowsでは`CreateFileW`に
  `GENERIC_WRITE`と`FILE_FLAG_BACKUP_SEMANTICS`を指定し、共通の`File.Sync`/`Close`へ渡す。
  Linux/macOSのopenと同期経路は従来どおり。
- checkpoint・spool・compressed log file archiveのdirectory同期を共通の
  `fsstore.SyncDirectory`へ統一。仮の子pathを作って親を同期する迂回も廃止。
- spoolの再試行・同一内容の再引受けなどでmtime更新後に同期するfileを、
  `O_RDWR`で開く。作成・切り詰めは行わず、内容不変・symlink拒否・同一fileの確認を維持。
- Windowsのパス表記に合わなかったtestの期待値を修正。FIFOのtestはUnix用実装へ
  分離し、Windowsではそのsubtestだけ理由付きskip。Unix権限bitのassertionは変更しない。

`.env`、upload先、保持期間、圧縮方式、checkpointの形式は変更していません。

## 専用APIの比較

[診断コード](directory-sync-probe/main_windows.go)を製品から独立したexeとしてビルドし、
Windows 11 Enterprise Evaluation 25H2のNTFS上で実行しました。OS buildは26200.6584です。
一般権限と管理者権限の2回実行し、JSONの`elevated`がそれぞれfalse/trueであることも確認。

各方式につき「file作成後・rename後・remove後」を10回ずつ、合計30回比較しています。

| directoryの開き方 | 一般権限 成功/失敗 | 管理者 成功/失敗 |
| --- | ---: | ---: |
| 従来の`os.Open`（読み取りのみ） | 0/30 | 0/30 |
| `CreateFileW`＋`GENERIC_READ` | 0/30 | 0/30 |
| `CreateFileW`＋`GENERIC_WRITE` | 30/0 | 30/0 |
| `CreateFileW`＋`GENERIC_READ \| GENERIC_WRITE` | 30/0 | 30/0 |
| 上記read/write＋`FILE_FLAG_WRITE_THROUGH` | 30/0 | 30/0 |

native方式はすべて`FILE_FLAG_BACKUP_SEMANTICS`と、read/write/deleteの共有flagを指定。
失敗した方式もopen自体は成功し、flushでWindows error 5 (`Access is denied`)でした。
作成した通常file本体の同期は、一般・管理者それぞれ50回すべて成功しました。
JSON保存file自身の同期は、この50回に含めていません。

実装には成功した最小構成の`GENERIC_WRITE`を採用しました。管理者昇格は必要ありませんが、
対象directoryへの書き込み権限は必要です。
[FlushFileBuffersの権限要件](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers)、
[CreateFileWのdirectory用flag](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew)。

診断はAPI失敗後も次のstageを観測します。製品のcommit手順ではありません。
終了code 0は診断完了を表すだけで、各同期の成功はJSONのopen/sync/close errorで判定します。

## directory同期修正直後の試験

7 packageの全testを管理者権限で1回実行した初回結果は、366 PASS・24 FAIL・3 SKIPでした。
directory同期は解消しましたが、fileのmtime更新後の読み取り専用Syncが12件で失敗しました。
この結果に基づいて上記の`O_RDWR`修正を追加しています。

ほかはWindowsのパス表記・空slice・Unix permission bitに関するtestの前提でした。
同期エラーを無視したり、失敗したtestを一律skipしたりしていません。

一般権限でのdirectory同期・圧縮・source retirementの反復試験は次のとおりです。
正規表現は`Sync|Durability|RetireCompressedSource`、各10回、timeout 180秒です。

| Package | test名数 | 延べPASS | FAIL |
| --- | ---: | ---: | ---: |
| fsstore | 5 | 50 | 0 |
| appendlog | 17 | 170 | 0 |
| failurearchive | 3 | 30 | 0 |

## 最終版の全体試験

mtime同期とOS依存のtest期待値を修正後、管理者権限で7 packageを再実行しました。
各packageを全test1回、timeout 180秒で実行しています。
集計は最上位のtest単位で、subtestを重複計上していません。

| Package | PASS | FAIL | SKIP | 終了code |
| --- | ---: | ---: | ---: | ---: |
| fsstore | 35 | 0 | 1 | 0 |
| appendlog | 68 | 0 | 0 | 0 |
| failurearchive | 22 | 0 | 0 | 0 |
| appendlog/runner | 190 | 1 | 2 | 1 |
| appendlog/compressedlogcheckpoint | 21 | 0 | 0 | 0 |
| spool/objectstorage | 30 | 0 | 0 | 0 |
| failurearchive/compressedlog | 22 | 1 | 0 | 1 |
| 合計 | 388 | 2 | 3 | 1 |

残した失敗は次の2件です。同期の失敗ではありません。

- `TestFilesystemCheckpointStoreUsesPrivatePermissionsAndLeavesNoTempFile`:
  directoryのUnix mode 0700を期待し、Windowsの`FileMode`表示0777と不一致。
- `TestStorePublishStoresDirectCompressedFileByteForByte`:
  archiveのUnix mode 0640を期待し、Windowsの表示0666と不一致。
  内部の5圧縮形式のsubtestが同じassertionで失敗しています。

既存のskipは`TestPurgeByAgeIfNeededKeepsStatFailure`と、runnerの2 permission testです。
さらにspoolのFIFO subtestが理由付きskipですが、上記最上位集計には追加していません。

## 最終版の一般権限・反復試験

通常のコマンドプロンプトで実行し、`whoami /groups`の
`S-1-16-8192`（Medium integrity）を`integrity.txt`に記録しました。
以下の正規表現に一致するtestを各10回、timeout 180秒で実行しています。

```text
Sync|Durability|RetireCompressedSource|Requeue|RecoverProcessing|Equivalent|Concurrent|SameBasename|Idempotent
```

| Package | test名数 | 延べPASS | FAIL |
| --- | ---: | ---: | ---: |
| fsstore | 5 | 50 | 0 |
| appendlog | 19 | 190 | 0 |
| failurearchive | 3 | 30 | 0 |
| spool/objectstorage | 12 | 120 | 0 |
| failurearchive/compressedlog | 4 | 40 | 0 |
| 合計 | 43 | 430 | 0 |

すべてのpackage log末尾がPASSで、panic・timeoutはありません。
この反復実行は各exeの数値終了codeを別fileへ保存していないため、codeの記録とは区別します。

Linuxでは`go test -race ./internal/infrastructure/fsstore/...`の15 package全体、
file logger・fsarchivehandler・compressedlogspool・compressedlogfailureの4 package全体、
上記正規表現の10回反復試験がすべて成功しました。
Windows向け7 packageの`go vet`、Darwin arm64向けfsstoreのcross-compile、
試験scriptの構文・mock試験、差分の空白検査も成功しました。

## 確認範囲の制約

- file作成・rename・remove後の同期API正常完了、製品の圧縮公開・source削除経路を確認する試験です。
  VM強制停止・kernel crash・実電源断の試験はしていません。
- 対象は今回のWindows 11/NTFSです。ReFS、FAT/exFAT、SMB/UNCの実共有は未試験です。
  APIが成功したことから、すべてのfilesystemやstorageで同一の瞬断耐久性を推定しません。
- WindowsではGoの`Chmod(0700/0600/0640)`がUnixと同じaccess controlを設定するわけではありません。
  `FileMode`の0666/0777表示だけで実ACLが公開状態かどうかも判断できません。
  今回はACLを変更・検証していません。[GoのChmod仕様](https://pkg.go.dev/os#Chmod)。
- Windowsではrace detectorを使用していません。Linuxでは関連packageを`-race`付きで試験済みです。
  macOSはDarwin arm64向けのコンパイル確認のみで、実行試験ではありません。

## 再現と保存先

全体試験exeは次でビルドします。出力先は未作成directoryを指定します。

```bash
bash testenv/windows/build-test-media.sh /tmp/windows-tests-next
```

Windowsでは試験ISOが`G:`の場合、次のように新しい結果directoryを指定します。

```bat
G:\run-tests.cmd C:\Users\vmtest\windows-tests-next
```

診断exeのビルドは次のとおりです。これをISOへ入れる場合も認証情報やcheckoutを含めません。

```bash
probe_build_dir=$(mktemp -d /tmp/windows-directory-sync-probe.XXXXXX)
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 GOTOOLCHAIN=go1.25.0 \
  go build -mod=readonly -o "$probe_build_dir/directory-sync-probe.exe" \
  ./testenv/windows/directory-sync-probe
```

VMに診断exeを持ち込んだ後、`directory-sync-probe.exe NEW_TEST_DIRECTORY`で実行します。
既存directoryを拒否し、その実行が新規作成したfileだけをrename/removeします。

VM本体・ISO・生の結果は、Git外の専用directoryに保存します。

```text
/path/to/virtual-machines/heterolog-windows/
├─ directory-sync-probe-20260911.iso
├─ fsstore-windows-native-sync-tests-20260911.iso
├─ fsstore-windows-native-sync-tests-20260911-v2.iso
└─ results-native-sync-20260911/
   ├─ native-sync-20260911/{normal,admin}.json
   ├─ native-product-admin-20260911/
   ├─ native-sync-repeat-20260911/
   ├─ native-product-final-20260911/
   └─ native-final-repeat-v2-20260911/
```

ISO SHA-256:

- 診断: `9644ec9fba3e8c271019fdc3195b5dc0ae095a2c25d4fedffa71828c60eedd34`
- directory同期修正直後: `518bdcbad946000aa42a91e02125370c12d9ace7f5bd3fe4117519de173b144d`
- mtime同期・test期待値修正後: `17c3726706a7eedc33144dacfced5c55f1bba2fdca3aa81537818359765436ed`
