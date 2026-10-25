// Package failure はbackend共通のdurable failure設定を提供します。
package failure

import "github.com/go-jappe/heterolog/pkg/logger/durable"

const (
	DefaultEnableDegrade = true
	DefaultMaxConcurrent = 8

	DefaultAsyncEnableDegrade = true
	DefaultMaxAsyncConcurrent = 8

	// DurableTypeFile はローカルストレージにファイルで保存します
	DurableTypeFsstore = durable.DurableTypeFileArchive
	DurableTypeSpool   = durable.DurableTypeSpool

	OverloadPolicyDrop    = durable.OverloadPolicyDrop
	OverloadPolicyDegrade = durable.OverloadPolicyDegrade

	// (1)でファイルシステムに保存すると選択した場合に、
	// 実行環境のファイルシステムがオーバーロード時で
	// OverloadPolicy　が　degradeの場合の退避先
	SpillDestinationS3     = durable.SpillDestinationS3
	SpillDestinationStdout = durable.SpillDestinationStdout
	SpillDestinationNfs    = durable.SpillDestinationNFS

	// failure storeを使わずにdegradeする場合の直接出力先。
	// durable storeがoverloadしたときのSpillDestinationとは別の設定である。
	// 現在実装済みなのはstdoutだけで、s3/nfsは将来実装向けの予約値。
	// s3/nfsを選択した場合はfailure handler factoryが起動時エラーを返す。
	DegradeDestinationS3     durable.SpillType = "s3"
	DegradeDestinationStdout durable.SpillType = "stdout"
	DegradeDestinationNfs    durable.SpillType = "nfs"
)

var DefaultDurableTypes = []durable.DurableType{DurableTypeSpool}
var DefaultSpillTypes = []durable.SpillType{DegradeDestinationStdout}

var DefaultAsyncDurableTypes = []durable.DurableType{DurableTypeSpool}
var DefaultAsyncSpillTypes = []durable.SpillType{DegradeDestinationStdout}

var DefaultDeliveryDurableTypes = []durable.DurableType{DurableTypeFsstore}
