package slack

import "time"

// スラック1通の最大の長さ
const (
	MaxMessageLength   = 4000
	DefaultHTTPTimeout = 2 * time.Second
)
