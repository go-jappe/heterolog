package pressure

import (
	"context"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
)

// MemoryPressureMonitor は、共有されたメモリ逼迫状態の参照口です。
//
// 責務:
//   - 現在の pressure を返す
//   - critical 判定結果を返す
//   - 直近状態を snapshot として返す
//   - 状態をリセットする
//
// 呼び出し側は、この monitor が lazy + TTL か、常駐監視かを知る必要はありません。
type MemoryPressureMonitor interface {
	// Current は、現在使うべき MemoryPressure を返します。
	//
	// 実装は必要に応じて内部状態を更新してよく、
	// 呼び出し側は「今の pressure」を得る用途で使います。
	Current(ctx context.Context) (memorypressure.MemoryPressure, error)

	// IsCritical は、現在のメモリ状態が critical overload かを返します。
	//
	// drop 判定や負荷遮断判定に使います。
	IsCritical(ctx context.Context) (bool, error)

	// Last は、直近の内部状態 snapshot を返します。
	//
	// provider は呼ばず、現在保持している状態だけを返します。
	// デバッグ、ログ出力、観測用途向けです。
	Last() memorypressure.MonitorSnapshot

	// Reset は、monitor 内部状態を初期化します。
	Reset()

	// Start は、バックグラウンドでの定期取得を開始します。
	//
	// ctx が終了したら、内部の監視 loop も停止します。
	// 既に開始済みの場合は重複起動せず、初回起動結果を返します。
	//
	// 実装によっては、起動時に現在値を 1 回取得してから
	// 定期監視を開始してよいです。
	Start(ctx context.Context) error
}
