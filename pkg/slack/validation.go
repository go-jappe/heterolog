package slack

import (
	"errors"
	"strings"
)

var (
	// ErrTokenRequiredは、Slack tokenが空または空白だけであることを表します。
	ErrTokenRequired = errors.New("slack token is required")
	// ErrChannelRequiredは、Slack channelが空または空白だけであることを表します。
	ErrChannelRequired = errors.New("slack channel is required")
)

// ConfigはSlack APIへ投稿するために必要な宛先設定です。
type Config struct {
	Token   string
	Channel string
}

// ValidateConfigはSlack clientの生成に必要な設定を検証します。
// HTTP通信は行わず、空文字だけでなく空白文字だけの値も未設定として扱います。
// 欠落はErrTokenRequired、ErrChannelRequiredとerrors.Isで判別できます。
func ValidateConfig(cfg Config) error {
	if strings.TrimSpace(cfg.Token) == "" {
		return ErrTokenRequired
	}
	if strings.TrimSpace(cfg.Channel) == "" {
		return ErrChannelRequired
	}
	return nil
}
