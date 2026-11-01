package cipherfile

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

// DecryptAnyは、fileの先頭のmagicで形式を見分けて復号します。公開鍵で暗号化したfile
// （Magic）はidentitiesで、対称鍵で暗号化したspool file（SymmetricMagic）はkeysで読みます。
// どちらのmagicでもなければErrNotEncryptedです。
func DecryptAny(r io.ReadSeeker, identities []*Identity, keys []*Key, w io.Writer) (Report, error) {
	head := make([]byte, len(Magic))
	n, err := io.ReadFull(r, head)
	if err != nil && n < len(Magic) {
		if n == 0 && err == io.EOF {
			return Report{}, nil
		}
		return Report{}, ErrNotEncrypted
	}
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return Report{}, err
	}
	if string(head) == SymmetricMagic {
		return decryptSymmetric(r, keys, w)
	}
	return Decrypt(r, identities, w)
}

// decryptSymmetricは、spool fileを全部読んで復号し、平文をwに書きます。
func decryptSymmetric(r io.Reader, keys []*Key, w io.Writer) (Report, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return Report{}, err
	}
	var cipher *SpoolCipher
	if len(keys) > 0 {
		if cipher, err = NewSpoolCipher(keys); err != nil {
			return Report{}, err
		}
	}
	plain, err := cipher.Open(body)
	if err != nil {
		// 鍵がない・合わない・壊れているfileは、全体を読み飛ばしとして報告します。
		report := Report{Skipped: []Skip{{Offset: 0, Length: int64(len(body)), Reason: err.Error()}}}
		if errors.Is(err, ErrSpoolKeyRequired) || errors.Is(err, ErrUnknownSpoolKey) {
			report.UnknownKeyIDs = []string{KeyIDOfSymmetric(body)}
		}
		return report, nil
	}
	if _, err := w.Write(plain); err != nil {
		return Report{}, fmt.Errorf("write the decrypted file: %w", err)
	}
	return Report{Segments: 1, Records: bytes.Count(plain, []byte{'\n'}), PlaintextBytes: int64(len(plain))}, nil
}
