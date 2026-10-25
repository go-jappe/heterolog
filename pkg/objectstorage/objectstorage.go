package objectstorage

import (
	"slices"
	"time"

	"github.com/go-jappe/heterolog/pkg/failurestore"
)

type UploadTarget string

const (
	UploadTargetS3 UploadTarget = "s3"
)

// DestinationIDは、checkpointやupload結果で一つの物理送信先を安定して識別します。
// providerの種類だけでなく、実際の保存先を決めるendpointやbucket、prefixなどを
// 含むidentityから生成します。認証情報やtimeout設定は送信先を変えないため含めません。
type DestinationID string

type UploadTargetConfig struct {
	Enabled bool
	Targets []UploadTarget
	S3      S3Config
	// S3DestinationsはS3の送信先一覧です。指定時はS3の一件指定を置き換えます。
	// 各要素に接続先・認証情報・bucket・prefixを持ち、宛先ごとに進捗を管理します。
	// nilの場合だけS3を使います。明示的な空一覧は設定エラーです。
	S3Destinations []S3Config
	// CompressedLogFileUploadFailureStoreはobject storageへの直接uploadを完了できなかった圧縮log fileを、
	// producerが解放する前に引き渡すdurable storeを選択します。
	// 選択が空またはnoneの場合は、producerがlocal fileを保持します。
	CompressedLogFileUploadFailureStore CompressedLogFileUploadFailureStoreConfig
	// CompressedLogFileUploadSpoolは、この送信経路が使う再送用spoolの設定です。
	// 同じlogger所有者のbackend送信失敗archiveと通常ログで、保存先と運用設定が
	// 一致する場合は同じruntimeを使用します。
	CompressedLogFileUploadSpool CompressedLogFileUploadSpoolConfigProvider
}

type LogFileUploadConfig struct {
	Enabled bool
	Default UploadTargetConfig
	Error   UploadTargetConfig
	Debug   UploadTargetConfig
	// CompressedLogFileUploadSpoolはrole間で共通に使用する再送用spoolの設定です。
	// runtimeの共有はproviderの同一性ではなく、保存先と運用設定の一致で決まります。
	CompressedLogFileUploadSpool CompressedLogFileUploadSpoolConfigProvider
}

// UsesCompressedLogFileUploadSpoolは実際に使用するroleのupload設定だけから、
// 共有compressed-log spoolを選択しているか判定します。
func (cfg LogFileUploadConfig) UsesCompressedLogFileUploadSpool(separateError, separateDebug bool) bool {
	if !cfg.Enabled {
		return false
	}
	targets := []UploadTargetConfig{cfg.Default}
	if separateError {
		targets = append(targets, cfg.Error)
	}
	if separateDebug {
		targets = append(targets, cfg.Debug)
	}
	for _, target := range targets {
		if target.Enabled && slices.Contains(target.CompressedLogFileUploadFailureStore.DurableType, failurestore.DurableTypeSpool) {
			return true
		}
	}
	return false
}

type S3Config struct {
	// Regionは前後の空白を除いた値でSigV4署名します。大文字小文字は変更しません。
	// 宛先IDでは、大文字小文字だけが異なるregionを同じ保存先として扱います。
	Region string
	// Endpointはhttp/httpsのS3互換endpointです。空ならRegionのAWS S3 endpointを使います。
	// queryとfragmentは受け付けません。
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Bucket          string
	Prefix          string
	ForcePathStyle  bool
	// TimeoutはS3 HTTP clientの上限です。0はdefault timeoutの未指定sentinel、負値は不正です。
	Timeout time.Duration
}

// S3DestinationConfigsは実際に使うS3宛先を設定順に返します。返り値は変更しないでください。
func (cfg UploadTargetConfig) S3DestinationConfigs() []S3Config {
	if cfg.S3Destinations != nil {
		return cfg.S3Destinations
	}
	return []S3Config{cfg.S3}
}

func (cfg UploadTargetConfig) Has(target UploadTarget) bool {
	for _, current := range cfg.Targets {
		if current == target {
			return true
		}
	}
	return false
}
