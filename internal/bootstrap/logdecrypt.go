package bootstrap

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/cipherfile"
)

// GenerateEncryptionIdentityは、fileの暗号化の鍵の組を作り、秘密鍵をidentityPathに0600で
// 書きます。環境変数HETEROLOG_ENCRYPTION_RECIPIENTに置く公開鍵と、鍵のIDを返します。
func GenerateEncryptionIdentity(identityPath string) (recipient, keyID string, err error) {
	identity, err := cipherfile.GenerateIdentity()
	if err != nil {
		return "", "", err
	}
	if err := cipherfile.WriteIdentityFile(identityPath, identity); err != nil {
		return "", "", err
	}
	return identity.Recipient().String(), identity.KeyID(), nil
}

// GenerateSpoolEncryptionKeyは、spoolの暗号化の対称鍵を作り、keyPathに0600で書きます。
// 鍵のIDを返します。
func GenerateSpoolEncryptionKey(keyPath string) (keyID string, err error) {
	key, err := cipherfile.GenerateKey()
	if err != nil {
		return "", err
	}
	if err := cipherfile.WriteKeyFile(keyPath, key); err != nil {
		return "", err
	}
	return key.KeyID(), nil
}

// DecryptFileは、inputPathの暗号化したfileを復号し、平文をoutputに書きます。公開鍵で
// 暗号化したfile（log・archive・OTLP）はidentityPathの秘密鍵で、spool fileはkeyPathの対称鍵で
// 読み、どちらかは空でも構いません。壊れた箇所や鍵のないsegmentは読み飛ばし、Reportに
// 記録します。
func DecryptFile(identityPath, keyPath, inputPath string, output io.Writer) (cipherfile.Report, error) {
	var identities []*cipherfile.Identity
	var keys []*cipherfile.Key
	var err error
	if identityPath != "" {
		if identities, err = cipherfile.LoadIdentityFile(identityPath); err != nil {
			return cipherfile.Report{}, err
		}
	}
	if keyPath != "" {
		if keys, err = cipherfile.LoadKeyFile(keyPath); err != nil {
			return cipherfile.Report{}, err
		}
	}
	input, err := os.Open(inputPath)
	if err != nil {
		return cipherfile.Report{}, fmt.Errorf("open the encrypted file: %w", err)
	}
	report, err := cipherfile.DecryptAny(input, identities, keys, output)
	if err != nil {
		err = fmt.Errorf("decrypt %s: %w", inputPath, err)
	}
	return report, errors.Join(err, input.Close())
}
