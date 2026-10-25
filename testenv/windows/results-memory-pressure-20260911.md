# Windowsメモリ監視の現在使用量への修正（2026-09-11）

## 修正範囲

製品の変更は`pkg/memorypressure/windows.go`だけです。
Linux・macOSの取得処理、共通monitorの製品コード、設定、`.env`は変更していません。

- Job全体の上限との比較には、過去最大の`PeakJobMemoryUsed`ではなく、現在のcommit量を使います。
- process単位の上限との比較には、RSSではなく現在の`PrivateUsage`を使います。
  RSSは引き続き補助指標として返します。
- 所属・上限・現在量の取得に失敗した観測は`Reliable=false`とし、hostやpeakで代用しません。
  共通monitorはこの観測を受けて、古いcritical判定と連続回数を解除します。
- Job非所属、または照会したJobにmemory上限がない場合は、従来どおりhostの物理メモリを使います。
  host APIの失敗も不明な観測として扱います。
- 上限情報の構造体には既存の`x/sys/windows`の型を使います。
  旧独自定義で不足していたWindows/386のpaddingも補われます。

取得不能を「実際の使用量が0だった」と扱う変更ではありません。
使用量0を正常に取得できた場合は`Reliable=true`、取得不能なら`false`です。

## APIと適用範囲

Jobの現在量はMicrosoftのhcsshimでも利用される`JobObjectMemoryUsageInformation`
（class 28）の`JobMemory`から取得します。全Windows環境での対応を前提にはせず、
APIが失敗した場合は判定対象から外します。

- [Microsoft hcsshimの構造体定義](https://github.com/microsoft/hcsshim/blob/main/internal/winapi/jobobject.go)
- [PROCESS_MEMORY_COUNTERS_EXのPrivateUsage](https://learn.microsoft.com/en-us/windows/win32/api/psapi/ns-psapi-process_memory_counters_ex)

`hJob=NULL`の照会は現在processの即時Jobを対象とします。祖先Jobを列挙したり、
複数の独立した上限をすべて比較したりする変更ではありません。
Job上限を優先し、なければprocess上限を使う既存の選択順は維持しています。

Windows APIへの構造体pointerは`LazyProc.Call`へ直接渡します。
amd64／386のescape診断で、製品・回帰試験ともAPIへ渡す構造体がheapに配置されることを
確認しました。試験用の関数注入や差し替え可能な製品APIは追加していません。

## 回帰テスト

Windows限定の次の試験を追加しました。

- 現在量、正常な0、上限超過時のclamp、取得失敗、再取得成功、上限不明の計算。
- amd64／386それぞれのWindows API構造体サイズ。
- 専用の子processにJobまたはprocessの256 MiB上限を設定し、48 MiBをcommitして解放。
  使用量とratioが下がり、critical判定が解除されることを確認。
- 上記allocationは実メモリに触れず、commitの増加をRSSだけでは検出できないことも確認。
  peakが高いまま残っていても現在量は低下することを確認。
- 不明な観測を受けた共通monitorが、前回のcritical判定と履歴を解除すること。

子processはJobへの所属が完了するまでstdinで待ち合わせます。親process自体は
Jobへ入れず、終了期限と後始末を設定しています。

## Linux・静的確認

Go 1.25.0、`-mod=readonly`で次の試験に成功しました。

```text
go test -race ./pkg/memorypressure ./internal/observer/memory/pressure \
  ./internal/infrastructure/logger/flushfailure/spoolstorehandler \
  ./internal/config/... -count=1 -timeout=180s
```

LinuxではWindows限定テストは対象外です。共通monitor、利用側、設定の回帰確認を行いました。
Windows向けの`go vet`はamd64でmemorypressure／monitor、386でmemorypressureに成功しました。
独立レビューでも追加指摘はありません。

## Windows 11 VMでの結果

既存offline VM（Windows 11 Enterprise Evaluation 25H2、`10.0.26200.6584`）で、
一般権限と管理者権限の両方から同じ媒体を実行しました。386版はWOW64で実行しています。
最上位testの件数で、subtestや子processの出力は重複計上しません。

| 実行 | 一般 PASS | 一般 FAIL | 一般 SKIP | 管理者 PASS | 管理者 FAIL | 管理者 SKIP |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 修正前＋同じnative回帰 | 0 | 1 | 0 | 0 | 1 | 0 |
| 修正後amd64全体 | 4 | 0 | 0 | 4 | 0 | 0 |
| 修正後386全体 | 4 | 0 | 0 | 4 | 0 | 0 |
| 共通monitor全体 | 17 | 0 | 0 | 17 | 0 | 0 |
| 修正後amd64全体×10回 | 40 | 0 | 0 | 40 | 0 | 0 |
| 修正後386全体×10回 | 40 | 0 | 0 | 40 | 0 | 0 |
| monitorの新規回帰×10回 | 10 | 0 | 0 | 10 | 0 | 0 |

修正後は各権限115件、合計230件成功、失敗・スキップは0です。修正前の終了code 1は
期待した失敗で、修正後の全実行と全体判定は終了code 0でした。Windows nativeは
race detectorなしです。

修正前比較ではGo overlayでWindows providerを修正前の内容に差し替えました。
作業treeは巻き戻しておらず、native回帰テストは修正後と同じです。
修正前にはない計算helperを使用する新規unit testだけは、比較用buildから除外しています。

一般権限のamd64実測値は次のとおりです（単位bytes、上限はすべて268,435,456）。

| 対象 | 確保前の使用量 | 48 MiB確保後 | 解放後の使用量 | 解放後のpeak |
| --- | ---: | ---: | ---: | ---: |
| 修正前・Job上限 | 14,131,200 | 64,561,152 | 64,561,152 | 64,561,152 |
| 修正後・Job上限 | 14,299,136 | 64,729,088 | 14,299,136 | 64,729,088 |
| 修正前・process上限 | 7,835,648 | 7,843,840 | 7,847,936 | 64,729,088 |
| 修正後・process上限 | 14,295,040 | 64,724,992 | 14,295,040 | 64,724,992 |

修正前のJobケースは「解放しても使用量が下がらない」、processケースは「未参照の
commitが計上されない」という期待した原因で失敗しました。修正後はpeakが高いままでも
現在使用量は確保前へ戻っています。386版でも確保・解放に伴う増減を確認しました。

媒体内での実行コマンドは次です。既存の結果directoryへの上書きは拒否します。

```text
run-memory-pressure-tests.cmd NEW_RESULTS_DIRECTORY
powershell -NoProfile -ExecutionPolicy Bypass -File summarize-results.ps1 RESULTS_DIRECTORY
```

## 媒体と結果保存先

媒体は4本のtest exeと実行・集計scriptだけを含むread-only ISOです。
`.env`、実際の資格情報、作業tree、host共有、外部networkは使用していません。

`/tmp/windows-memory-pressure-final.OKo0EP/memory-pressure-tests.iso`

SHA-256:

```text
dd7cadc2c499b2bb5d09792cfc86b487de38115e555a071ee683919eaf232168
```

VM内の結果保存先:

- `C:\Users\vmtest\memory-pressure-normal-20260911-OKo0EP`
- `C:\Users\vmtest\memory-pressure-admin-20260911-OKo0EP`

生ログはゲスト内に保持し、hostへ回収していません。件数・終了codeと、この試験が
出力する使用量・失敗原因だけを画面で確認しました。

終了後はWindowsの`shutdown /s /t 0`で正常停止し、launcher終了code 0とQMP／TPM
socketの消失を確認しました。仮想disk、過去の結果、今回の両権限の結果は保持しています。
