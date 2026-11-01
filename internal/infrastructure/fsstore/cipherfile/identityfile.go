package cipherfile

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// ErrNoIdentityInFileは、秘密鍵のfileに鍵の行がないことを表します。
var ErrNoIdentityInFile = errors.New("the identity file has no key line")

// LoadIdentityFileは、秘密鍵のfileを読みます。"#"で始まる行と空行は飛ばし、残りの各行を
// 秘密鍵として読みます。1つのfileに複数の鍵（切り替え前の鍵など）を置けます。
func LoadIdentityFile(path string) ([]*Identity, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read the identity file: %w", err)
	}
	var identities []*Identity
	for i, line := range strings.Split(string(contents), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		identity, err := ParseIdentity(line)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, i+1, err)
		}
		identities = append(identities, identity)
	}
	if len(identities) == 0 {
		return nil, fmt.Errorf("%s: %w", path, ErrNoIdentityInFile)
	}
	return identities, nil
}

// WriteIdentityFileは、秘密鍵を新しいfileに0600で書きます。すでにあるfileは上書きしません。
// 公開鍵と鍵のIDをcommentで添えます。
func WriteIdentityFile(path string, identity *Identity) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create the identity file: %w", err)
	}
	_, writeErr := fmt.Fprintf(file, "# heterolog encryption identity\n# public key: %s\n# key id: %s\n%s\n",
		identity.Recipient(), identity.KeyID(), identity)
	if err := errors.Join(writeErr, file.Close()); err != nil {
		return fmt.Errorf("write the identity file: %w", err)
	}
	return nil
}
