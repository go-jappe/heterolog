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

// DecryptFileは、identityPathの秘密鍵でinputPathの暗号化したfileを復号し、平文をoutputに
// 書きます。壊れた箇所や鍵のないsegmentは読み飛ばし、Reportに記録します。
func DecryptFile(identityPath, inputPath string, output io.Writer) (cipherfile.Report, error) {
	identities, err := cipherfile.LoadIdentityFile(identityPath)
	if err != nil {
		return cipherfile.Report{}, err
	}
	input, err := os.Open(inputPath)
	if err != nil {
		return cipherfile.Report{}, fmt.Errorf("open the encrypted file: %w", err)
	}
	report, err := cipherfile.Decrypt(input, identities, output)
	if err != nil {
		err = fmt.Errorf("decrypt %s: %w", inputPath, err)
	}
	return report, errors.Join(err, input.Close())
}
