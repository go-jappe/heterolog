# Filesystem耐久保存とfsync 設計メモ

## 状態

この文書は、filesystem上のdurable storeへ`fsync`を導入する際の設計メモです。
下記の「導入検討時点の実装状況」は当時の記録で、現在の実装の網羅的な一覧ではありません。

### 現在のcheckpoint保存先の初期化

top-levelの`NewLogger`・`NewAsyncLogger`・`NewDeliveryLogger`は、設定検証後、有効な
object storage upload経路がある場合だけ、内部で共通のcheckpoint保存先を準備します。
workerや下位loggerの構築を始める前に`checkpoint.PrepareDirectory`を
呼び出すため、利用側の`main`やintegration helperによる個別の初期化は不要です。
upload無効、lifecycle無効、未選択のbackendやfile系列に残ったupload設定だけでは作成しません。
保存先は`<HETEROLOG_APP_STATE_DIR>/<HETEROLOG_TARGET>/archive-checkpoints`の一つで、通常ログ・内部ログ・
failure archiveが共有します。各top-level constructorは有効な経路が使う共通保存先を
一度準備し、sourceごとの保存処理では繰り返しません。
同じ保存先を共有するloggerの初回構築は、呼び出し側で順番に行います。
作成・権限設定・新規directoryの親の同期は、worker起動やlazy fallbackの初回利用より前に
完了させます。失敗は構築エラーとして返し、checkpointファイル本体はまだ作りません。

FileLoggerSetやrunnerなどの下位constructorとcheckpoint保存処理は、directoryを
作成・再初期化しません。top-level loggerを経由せず部品を直接利用する場合は、
従来どおり呼び出し側が使用前に保存先を準備します。
upload無効なら保存先とcheckpointファイルが既存でもStoreを作らず、過去の記録を読みません。
設定した保存先ディレクトリの検証は続け、記録はupload再有効化時に読みます。
共通のdirectory作成処理は呼び出し同士を排他しません。新規directoryの親の同期に失敗した場合は、その親をprocess内で記録し、その親が保存先自身か上位にある後の準備では、既存directoryでも検査・作成より先に同期を再試行します。成功するまでは`ErrDurabilityUnconfirmed`を返し、その下に新しいdirectoryも作りません。同期できた記録は解除し、記録数には上限を設けます。

## 背景

`MkdirAll`、`Write`、`Rename`が成功すると、変更は実行中のprocessから参照できます。しかし、
それだけでは電源断やmachine crash後にもdirectory entryやfile内容が残ることを保証できません。

一方、通常のappendlogはbest-effortなlog用途であり、すべてのfilesystem操作に`fsync`を
強制すると、必要のないI/O待ちまで増やします。そのため、`mkdir`を使用する全処理へ
一律に`fsync`を追加せず、成功を「耐久保存完了」として報告する経路だけを対象にします。

## 対象となる契約

次の処理は、成功を返す前に必要な`fsync`がすべて成功していなければなりません。

- appendlog lifecycleのarchive upload checkpoint
- backend送信失敗recordを保存するspool
- backend送信失敗recordを保存するfailure archive
- 上記のdurable fileを公開、移動、削除、圧縮する処理

通常のappendlog本体は現在durableを契約していないため対象外です。将来その契約を変更する
場合は、directory作成だけでなく、通常書き込みとrotationを含めて別途設計します。

## 導入検討時点の実装状況

導入検討時点ではproduction codeの`MkdirAll`は6箇所でした。

### 対応済み

- `internal/infrastructure/fsstore/appendlog/runner/filesystem_checkpoint_store.go`
  - checkpoint directoryの作成後に親directoryを`fsync`します。
  - checkpoint fileは一時fileへの書き込み、fileの`fsync`、rename、checkpoint directoryの
    `fsync`が成功してからcommit成功になります。

### 未対応

- `internal/infrastructure/fsstore/spool/store/spool.go`
  - spool file自体は`Sync`しますが、rename後の保存先directoryと、`MkdirAll`が作成した
    bucket directoryの親directoryを同期していません。
- `internal/infrastructure/fsstore/spool/worker/common.go`
  - spoolのstate間rename後に、移動先と移動元のdirectoryを同期していません。
  - processing fileの分割結果を公開する処理も、rename後のdirectoryを同期していません。
- `internal/infrastructure/logger/flushfailure/fsarchivehandler/archive.go`
  - failure archive directoryを作成した後の親directoryを同期していません。
  - JSONL追記後のfile `Sync`を実行せず、`Close`のerrorも確認していませんが、呼び出し元は
    `DurableSucceeded`を返し得ます。
- `internal/infrastructure/fsstore/fsstore.go`
  - 通常appendlog、failure archive、lifecycle scanで共有されています。durableと
    best-effortの契約が混在するため、この共通処理へ無条件に`fsync`を追加しません。

また、`appendlog/compress.go`と`failurearchive/compress.go`の圧縮file公開にも、fileの
`fsync`、rename後のdirectory `fsync`、元file削除後のdirectory `fsync`が不足しています。
`mkdir`だけを修正しても、durable処理全体のcrash consistencyは成立しません。

## 操作ごとの完了条件

### 保存先directoryの作成（現在のcheckpoint初期化）

1. 必要なdirectoryを作成する。
2. 必要なpermissionを設定する。
3. 新しく作成された各directory entryを保持する親directoryを`fsync`する。
4. すべて成功した場合だけ、初期化済みdirectoryを使うStoreの構築へ進む。

失敗した場合、作成済みdirectoryは削除せず、構築エラーを返します。次の初期化では
未作成のdirectoryを作成します。同期に失敗した親はprocess内で記録し、依存する次の準備では
先に同期を再試行します。同期できるまでは作成済みのdirectoryでも成功を返しません。

### 新規fileのatomic publish

1. 最終fileと同じdirectoryに一時fileを作成する。
2. permissionを設定する。
3. 内容を書き込む。
4. 一時fileを`fsync`する。
5. 一時fileを閉じ、`Close`のerrorを確認する。
6. 一時fileを最終名へrenameする。
7. 最終fileを保持するdirectoryを`fsync`する。

permissionをfileへ永続化するため、`Chmod`はfileの`fsync`より前に行います。

### 既存fileへのappend

1. recordをすべて追記する。
2. fileを`fsync`する。
3. 呼び出しごとにfileを開いた場合は、`Close`のerrorも確認する。

failure archiveのJSONL追記を再試行すると、`fsync`失敗前に書き込まれたrecordが重複する
可能性があります。重複を厳密に避ける必要が生じた場合は、追記fileではなくbatchごとの
immutable fileをatomic publishする方式を検討します。

### directory間のrename

1. 移動先directoryをdurableに作成する。
2. fileをrenameする。
3. 移動先directoryを先に`fsync`する。
4. 移動元directoryを`fsync`する。

移動先を先に同期することで、途中失敗時はfile消失よりも移動元と移動先の重複を選びます。
spool backendは冪等または重複許容を前提としているため、この順序と整合します。同一directory
内のrenameでは、そのdirectoryを一度だけ同期します。

### fileの削除

削除をdurableなstate変更として扱う場合は、`Remove`後に親directoryを`fsync`します。
purgeのように、crash後に削除対象が再び見えても次回処理で回収できる処理は、性能との
兼ね合いからbest-effortにする余地があります。その区別を呼び出し側の契約で明示します。

### 圧縮fileへの置き換え

1. 一時圧縮fileへ最後まで書き込む。
2. 一時圧縮fileを`fsync`して閉じる。
3. 最終圧縮file名へrenameする。
4. 最終圧縮fileを保持するdirectoryを`fsync`する。
5. 元fileを削除する。
6. 元fileを保持していたdirectoryを`fsync`する。

最終圧縮fileのdirectory同期が成功する前に元fileを削除しません。元file削除後の同期に失敗
した場合は、crash後に元fileが再出現する可能性を許容し、圧縮fileの消失を避けます。

## fsync失敗時の扱い

必要な`fsync`が一つでも失敗した場合は、durable成功として報告しません。ただし、rename後は
最終fileが実行中のfilesystemへすでに公開されているため、単純に処理前の状態へ戻しません。

将来の共通処理では、少なくとも次の二状態を区別できる結果を返します。

- filesystem上への公開が済んでいるか
- 必要な`fsync`まで成功し、永続化を確認できたか

rename後のdirectory `fsync`に失敗した場合は、公開済みfileを削除・再作成せず、同じ
directoryの`fsync`を再試行します。通常の`error`だけを見て最初から再実行すると、spoolや
archiveを重複作成する可能性があります。

retryは無制限にその場で待たず、contextと設定された上限に従います。最終失敗は内部errorと
metricsへ記録します。`EIO`、`EROFS`、`ENOSPC`、filesystemがdirectory `fsync`を
サポートしない場合などを黙って成功扱いにしません。

## 共通化方針

既存の`ensureLogDir`へ一律に同期処理を追加せず、次の責務を持つdurable filesystem操作を
共通化します。公開API名と配置は実装時に決定します。

- 保存先directoryの作成と親directory同期
- 一時fileから最終fileへのdurableなatomic publish
- 移動元・移動先directoryを同期するdurable rename
- 必要に応じて親directoryを同期するdurable remove

通常appendlogなどのbest-effort処理は、これらを強制的に経由させません。

## 試験方針

- directory作成、file sync、rename後のdirectory syncをそれぞれ失敗させ、成功扱いに
  ならないことを確認する。
- rename前の失敗では最終fileを公開せず、元データを維持することを確認する。
- rename後のdirectory sync失敗では、公開済みfileを削除・上書きしないことを確認する。
- state間renameでは、移動先directoryを移動元より先に同期することを確認する。
- 同一directory内のrenameでは重複して同期しないことを確認する。
- failure archiveのfile `Sync`と`Close`のerrorが`DurableFailed`へ反映されることを確認する。
- 圧縮fileのdirectory同期前には元fileを削除しないことを確認する。
- permission、`EIO`、`EROFS`、`ENOSPC`相当のerrorを`errors.Is`で判定できる形で返す。
- 通常appendlogへ不要な`fsync`が追加されていないことを回帰試験で確認する。

実際のpower-lossをunit testだけで完全に再現することは難しいため、I/O操作を差し替えた
failure injectionで呼び出し順序と各失敗境界を固定し、必要に応じてfilesystemを用いた
integration testを追加します。

## 実装順序

1. failure archiveのappendとdirectory作成をdurable成功契約へ合わせる。
2. spool新規保存と分割fileのatomic publishを修正する。
3. spool state間renameを移動先、移動元の順に同期する。
4. appendlogとfailure archiveの圧縮commit順序を修正する。
5. 共通化後もcheckpointの既存耐久性が維持されることを確認する。

## 完了条件

- durable保存経路は、必要なfileとdirectoryの`fsync`が完了するまで成功を返さない。
- rename後の同期失敗を、rename前の未公開失敗と区別できる。
- spool state遷移中のcrashで、データ消失より重複を選ぶ順序になっている。
- failure archiveが`DurableSucceeded`を返したrecordは、必要な同期まで完了している。
- 圧縮済みfileの公開が永続化される前に元fileを削除しない。
- 通常appendlogのbest-effort契約と性能を不要に変更しない。

## 関連文書

- `docs/design/archive-checkpoint-repair.md`
