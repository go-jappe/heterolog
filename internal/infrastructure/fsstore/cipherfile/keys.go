// Package cipherfileは、log file・failure archive・OTLP/JSONのfileをdiskに書くときの
// 暗号化です。書く側は公開鍵（Recipient）だけを持ち、読むのは秘密鍵（Identity）を持つ
// 復号のCLIだけです。processが読み戻すfile（spool）には使いません。
//
// fileは、segmentの並びです。segmentは、fileを開くたび（起動とrotation）に始まり、
// header（magic、鍵のID、HPKEのカプセル化した鍵）と、record 1件ごとのchunk（4 byteの
// 長さと暗号文）が続きます。秘密鍵がなくても、新しいsegmentを足せば追記できます。
// chunkのnonceはHPKEの文脈が順に数えるため保存せず、並べ替えや欠落は復号で分かります。
// 途中で切れたchunkがあっても、読む側は次のmagicまで読み飛ばして続きを読めます。
//
// 暗号は、Goの標準libraryのHPKE（RFC 9180）で、DHKEM(X25519, HKDF-SHA256)・
// HKDF-SHA256・AES-256-GCMです。
package cipherfile

import (
	"crypto/ecdh"
	"crypto/hpke"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// 鍵の文字列のprefixです。公開鍵は環境変数に、秘密鍵は0600のfileに置きます。
const (
	recipientPrefix = "hlog-pub1-"
	identityPrefix  = "hlog-sec1-"
)

// KeyIDSizeは、鍵のIDのbyte数です。公開鍵のSHA-256の先頭です。
const KeyIDSize = 8

var (
	// ErrInvalidRecipientは、公開鍵の文字列の形が違うことを表します。
	ErrInvalidRecipient = errors.New("invalid encryption recipient (public key)")
	// ErrInvalidIdentityは、秘密鍵の文字列の形が違うことを表します。
	ErrInvalidIdentity = errors.New("invalid encryption identity (private key)")
)

// Recipientは、書く側が持つ公開鍵です。
type Recipient struct {
	public *ecdh.PublicKey
	keyID  [KeyIDSize]byte
}

// Identityは、復号する側が持つ秘密鍵です。
type Identity struct {
	private   *ecdh.PrivateKey
	recipient *Recipient
}

// GenerateIdentityは、新しい鍵の組を作ります。
func GenerateIdentity() (*Identity, error) {
	private, err := ecdh.X25519().GenerateKey(nil)
	if err != nil {
		return nil, fmt.Errorf("generate encryption key: %w", err)
	}
	return newIdentity(private), nil
}

func newIdentity(private *ecdh.PrivateKey) *Identity {
	return &Identity{private: private, recipient: newRecipient(private.PublicKey())}
}

func newRecipient(public *ecdh.PublicKey) *Recipient {
	sum := sha256.Sum256(public.Bytes())
	recipient := &Recipient{public: public}
	copy(recipient.keyID[:], sum[:KeyIDSize])
	return recipient
}

// ParseRecipientは、公開鍵の文字列（"hlog-pub1-..."）を読みます。前後の空白は無視します。
func ParseRecipient(text string) (*Recipient, error) {
	raw, err := decodeKey(text, recipientPrefix, ErrInvalidRecipient)
	if err != nil {
		return nil, err
	}
	public, err := ecdh.X25519().NewPublicKey(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidRecipient, err)
	}
	return newRecipient(public), nil
}

// ParseIdentityは、秘密鍵の文字列（"hlog-sec1-..."）を読みます。前後の空白は無視します。
func ParseIdentity(text string) (*Identity, error) {
	raw, err := decodeKey(text, identityPrefix, ErrInvalidIdentity)
	if err != nil {
		return nil, err
	}
	private, err := ecdh.X25519().NewPrivateKey(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidIdentity, err)
	}
	return newIdentity(private), nil
}

func decodeKey(text, prefix string, invalid error) ([]byte, error) {
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, prefix) {
		return nil, fmt.Errorf("%w: it must start with %q", invalid, prefix)
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(text, prefix))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", invalid, err)
	}
	return raw, nil
}

// Stringは、環境変数に置く公開鍵の文字列です。
func (r *Recipient) String() string {
	return recipientPrefix + base64.RawURLEncoding.EncodeToString(r.public.Bytes())
}

// KeyIDは、鍵のIDの16進の文字列です。headerに入り、復号のCLIが鍵を選ぶのに使います。
func (r *Recipient) KeyID() string {
	return hex.EncodeToString(r.keyID[:])
}

func (r *Recipient) hpkePublicKey() (hpke.PublicKey, error) {
	return hpke.NewDHKEMPublicKey(r.public)
}

// Stringは、fileに置く秘密鍵の文字列です。
func (i *Identity) String() string {
	return identityPrefix + base64.RawURLEncoding.EncodeToString(i.private.Bytes())
}

// Recipientは、この秘密鍵に対応する公開鍵です。
func (i *Identity) Recipient() *Recipient {
	return i.recipient
}

// KeyIDは、対応する公開鍵の鍵のIDです。
func (i *Identity) KeyID() string {
	return i.recipient.KeyID()
}

func (i *Identity) hpkePrivateKey() (hpke.PrivateKey, error) {
	return hpke.NewDHKEMPrivateKey(i.private)
}
