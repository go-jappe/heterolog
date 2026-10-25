package lifecycle

import "time"

// StatePurgeTracker は、state ごとの purge 履歴とサイズ推定値を保持します。
type StatePurgeTracker struct {
	// LastAgePurgeAt は、最後に by-age purge を実行した時刻です。
	LastAgePurgeAt time.Time

	// LastSizeCheckAt は、最後に by-size check に成功した時刻です。
	//
	// full scan または diff scan が成功した場合にだけ更新します。
	LastSizeCheckAt time.Time

	// LastSizeFullScanAt は、最後に full scan に成功した時刻です。
	//
	// zero 値の場合、EstimatedTotalSize はまだ初期化されていません。
	LastSizeFullScanAt time.Time

	// SizeCheckCountSinceFullScan は、前回 full scan 成功後の by-size check 成功回数です。
	SizeCheckCountSinceFullScan int

	// EstimatedTotalSize は、state 配下の managed spool file の推定総サイズです。
	//
	// LastSizeFullScanAt が zero でない場合に使える値です。
	// 外部プロセスや手動操作で spool 配下を変更した場合、その差分は次回 full scan まで反映されません。
	EstimatedTotalSize uint64
}

// SizePurgeMode は、by-size 処理の選択肢です。
type SizePurgeMode int

const (
	// SizePurgeSkip は、by-size 処理を実行しない選択です。
	SizePurgeSkip SizePurgeMode = iota

	// SizePurgeDiffScan は、前回 check 以降の増加分だけを走査する選択です。
	SizePurgeDiffScan

	// SizePurgeFullScan は、filesystem 全体を走査してサイズを確認する選択です。
	SizePurgeFullScan
)
