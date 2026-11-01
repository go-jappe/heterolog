package cipherfile

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/go-jappe/heterolog/pkg/fsstore"
)

// spoolの暗号化は、processが送り直しのために読み戻すfileのためのもので、process自身が持つ
// 対称鍵を使います。processの権限を取られれば鍵も取られるため、守れるのはdiskの盗難・
// backup・snapshotなど、processの外にあるfileだけです。spool fileは一度に書き切り、一度に
// 読むため、file全体を1つのAES-256-GCMの暗号文にします。
const (
	// SymmetricMagicは、対称鍵で暗号化したfileの先頭の8 byteです。
	SymmetricMagic      = "HLOGSYM1"
	keyPrefix           = "hlog-key1-"
	keySize             = 32
	nonceSize           = 12
	symmetricHeaderSize = len(SymmetricMagic) + KeyIDSize + nonceSize
)

var (
	// ErrInvalidKeyは、対称鍵の文字列の形が違うことを表します。
	ErrInvalidKey = errors.New("invalid spool encryption key")
	// ErrNoKeyInFileは、対称鍵のfileに鍵の行がないことを表します。
	ErrNoKeyInFile = errors.New("the key file has no key line")
	// ErrKeyFileRequiredは、spoolの暗号化を有効にしたのに鍵のfileがないことを表します。
	ErrKeyFileRequired = errors.New("spool encryption is enabled but no key file is configured")
	// ErrSpoolKeyRequiredは、暗号化したspool fileを読むのに鍵がないことを表します。
	ErrSpoolKeyRequired = errors.New("the spool file is encrypted but no spool encryption key is configured")
	// ErrUnknownSpoolKeyは、暗号化したspool fileの鍵のIDが、持っている鍵のどれでもないことを表します。
	ErrUnknownSpoolKey = errors.New("no spool encryption key matches the key ID of the file")
	// ErrSpoolFileDamagedは、暗号化したspool fileが短すぎるか、認証に失敗したことを表します。
	ErrSpoolFileDamaged = errors.New("the encrypted spool file is damaged")
)

// Keyは、spoolの対称鍵です。
type Key struct {
	raw   [keySize]byte
	keyID [KeyIDSize]byte
}

// GenerateKeyは、新しい対称鍵を作ります。
func GenerateKey() (*Key, error) {
	var raw [keySize]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return nil, fmt.Errorf("generate spool encryption key: %w", err)
	}
	return newKey(raw), nil
}

func newKey(raw [keySize]byte) *Key {
	sum := sha256.Sum256(raw[:])
	key := &Key{raw: raw}
	copy(key.keyID[:], sum[:KeyIDSize])
	return key
}

// ParseKeyは、対称鍵の文字列（"hlog-key1-..."）を読みます。
func ParseKey(text string) (*Key, error) {
	raw, err := decodeKey(text, keyPrefix, ErrInvalidKey)
	if err != nil {
		return nil, err
	}
	if len(raw) != keySize {
		return nil, fmt.Errorf("%w: %d bytes, want %d", ErrInvalidKey, len(raw), keySize)
	}
	var fixed [keySize]byte
	copy(fixed[:], raw)
	return newKey(fixed), nil
}

// Stringは、fileに置く対称鍵の文字列です。
func (k *Key) String() string {
	return keyPrefix + base64.RawURLEncoding.EncodeToString(k.raw[:])
}

// KeyIDは、鍵のIDの16進の文字列です。
func (k *Key) KeyID() string {
	return hex.EncodeToString(k.keyID[:])
}

// LoadKeyFileは、対称鍵のfileを読みます。"#"で始まる行と空行は飛ばし、残りの各行を鍵として
// 読みます。最初の鍵で書き、どの鍵でも読めます。
func LoadKeyFile(path string) ([]*Key, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read the spool key file: %w", err)
	}
	var keys []*Key
	for i, line := range strings.Split(string(contents), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, err := ParseKey(line)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, i+1, err)
		}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("%s: %w", path, ErrNoKeyInFile)
	}
	return keys, nil
}

// WriteKeyFileは、対称鍵を新しいfileに0600で書きます。すでにあるfileは上書きしません。
func WriteKeyFile(path string, key *Key) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create the spool key file: %w", err)
	}
	_, writeErr := fmt.Fprintf(file, "# heterolog spool encryption key\n# key id: %s\n%s\n", key.KeyID(), key)
	if err := errors.Join(writeErr, file.Close()); err != nil {
		return fmt.Errorf("write the spool key file: %w", err)
	}
	return nil
}

// SpoolCipherは、spool fileを書く鍵と読める鍵の組です。nilは暗号化なしを表し、nilの
// SpoolCipherでもSealとOpenを呼べます。
type SpoolCipher struct {
	current *Key
	keys    map[[KeyIDSize]byte]*Key
}

// NewSpoolCipherは、keysの最初の鍵で書き、全部の鍵で読むSpoolCipherを返します。
func NewSpoolCipher(keys []*Key) (*SpoolCipher, error) {
	if len(keys) == 0 {
		return nil, ErrNoKeyInFile
	}
	c := &SpoolCipher{current: keys[0], keys: make(map[[KeyIDSize]byte]*Key, len(keys))}
	for _, key := range keys {
		c.keys[key.keyID] = key
	}
	return c, nil
}

// ResolveSpoolCipherは、設定から鍵のfileを読みます。暗号化が無効ならnilを返します。
func ResolveSpoolCipher(cfg fsstore.SpoolEncryptionConfig) (*SpoolCipher, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if cfg.KeyFile == "" {
		return nil, ErrKeyFileRequired
	}
	keys, err := LoadKeyFile(cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("spool encryption: %w", err)
	}
	return NewSpoolCipher(keys)
}

// Enabledは、暗号化して書くことを返します。
func (c *SpoolCipher) Enabled() bool {
	return c != nil
}

// Sealは、spool fileの本文全体を1つの暗号文にします。cがnilならbodyをそのまま返します。
func (c *SpoolCipher) Seal(body []byte) ([]byte, error) {
	if c == nil {
		return body, nil
	}
	return SealSymmetric(c.current, body)
}

// Openは、暗号化したspool fileの本文を復号します。先頭がSymmetricMagicでなければ、暗号化
// 前のfileとしてそのまま返します。
func (c *SpoolCipher) Open(body []byte) ([]byte, error) {
	if !IsSymmetric(body) {
		return body, nil
	}
	if c == nil {
		return nil, ErrSpoolKeyRequired
	}
	var keyID [KeyIDSize]byte
	copy(keyID[:], body[len(SymmetricMagic):])
	key, ok := c.keys[keyID]
	if !ok {
		return nil, fmt.Errorf("%w: %x", ErrUnknownSpoolKey, keyID[:])
	}
	return OpenSymmetric(key, body)
}

// IsSymmetricは、bodyが対称鍵で暗号化したfileかを返します。
func IsSymmetric(body []byte) bool {
	return len(body) >= len(SymmetricMagic) && string(body[:len(SymmetricMagic)]) == SymmetricMagic
}

// SealSymmetricは、plaintextをkeyでfile全体の暗号文（magic、鍵のID、nonce、AES-256-GCMの
// 暗号文）にします。headerはAADとして認証します。
func SealSymmetric(key *Key, plaintext []byte) ([]byte, error) {
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	header := make([]byte, 0, symmetricHeaderSize+len(plaintext)+aeadOverhead)
	header = append(header, SymmetricMagic...)
	header = append(header, key.keyID[:]...)
	nonce := make([]byte, nonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("encrypt a spool file: %w", err)
	}
	header = append(header, nonce...)
	return aead.Seal(header, nonce, plaintext, header), nil
}

// OpenSymmetricは、SealSymmetricの出力をkeyで復号します。鍵のIDは確かめません。
func OpenSymmetric(key *Key, body []byte) ([]byte, error) {
	if len(body) < symmetricHeaderSize+aeadOverhead {
		return nil, ErrSpoolFileDamaged
	}
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	header := body[:symmetricHeaderSize]
	nonce := header[len(SymmetricMagic)+KeyIDSize:]
	plaintext, err := aead.Open(nil, nonce, body[symmetricHeaderSize:], header)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSpoolFileDamaged, err)
	}
	return plaintext, nil
}

// KeyIDOfSymmetricは、暗号化したspool fileの鍵のIDの16進の文字列です。
func KeyIDOfSymmetric(body []byte) string {
	if len(body) < len(SymmetricMagic)+KeyIDSize {
		return ""
	}
	return hex.EncodeToString(body[len(SymmetricMagic) : len(SymmetricMagic)+KeyIDSize])
}

func newAEAD(key *Key) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key.raw[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
