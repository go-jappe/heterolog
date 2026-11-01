package cipherfile

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/pkg/fsstore"
)

// ErrRecipientRequiredは、暗号化を有効にしたのに公開鍵がないことを表します。
var ErrRecipientRequired = errors.New("encryption is enabled but no recipient (public key) is configured")

// ResolveRecipientは、設定から公開鍵を読みます。暗号化が無効ならnilを返します。有効で
// 公開鍵がないか読めなければerrorです。loggerの構築で一度だけ呼びます。
func ResolveRecipient(cfg fsstore.EncryptionConfig) (*Recipient, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if cfg.Recipient == "" {
		return nil, ErrRecipientRequired
	}
	recipient, err := ParseRecipient(cfg.Recipient)
	if err != nil {
		return nil, fmt.Errorf("encryption recipient: %w", err)
	}
	return recipient, nil
}

// CompressionSuppressedWarningは、暗号化のために圧縮の設定を無視するときの警告の文です。
const CompressionSuppressedWarning = "file compression is disabled because the files are encrypted; encrypted data does not compress"
