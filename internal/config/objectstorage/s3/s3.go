package s3

import "time"

const (
	DefaultUploadEnabled       = false
	DefaultUploadTargetEnabled = true

	DefaultRegion         = "ap-northeast-1"
	DefaultEndpoint       = ""
	DefaultBucket         = ""
	DefaultPrefix         = ""
	DefaultForcePathStyle = false
	// 圧縮log fileは数十MBになるため、通常のbackend record送信より余裕を持たせます。
	// S3 clientはretryせず、一回のPUTごとにこの上限を適用します。
	DefaultTimeout = 60 * time.Second
)
