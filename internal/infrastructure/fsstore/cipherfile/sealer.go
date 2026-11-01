package cipherfile

import (
	"crypto/hpke"
	"fmt"
	"io"
)

// Sealerは、一つのsegmentの暗号化の文脈です。fileを開くたびにNewSealerで作り、headerを
// fileの末尾に書いてから、recordごとにSealした結果を順に書きます。並行には使えないため、
// 呼び出し元がfileへの書き込みと同じlockで直列にします。
type Sealer struct {
	sender *hpke.Sender
}

// NewSealerは、recipientへの新しいsegmentを始め、fileに書くheaderとSealerを返します。
func NewSealer(recipient *Recipient) ([]byte, *Sealer, error) {
	public, err := recipient.hpkePublicKey()
	if err != nil {
		return nil, nil, fmt.Errorf("start an encrypted segment: %w", err)
	}
	kdf, aead := suite()
	enc, sender, err := hpke.NewSender(public, kdf, aead, infoOf(recipient.keyID))
	if err != nil {
		return nil, nil, fmt.Errorf("start an encrypted segment: %w", err)
	}
	header, err := encodeHeader(recipient.keyID, enc)
	if err != nil {
		return nil, nil, err
	}
	return header, &Sealer{sender: sender}, nil
}

// Sealは、plaintextを1つのchunk（長さの欄と暗号文）にします。
func (s *Sealer) Seal(plaintext []byte) ([]byte, error) {
	if len(plaintext) > MaxChunkLength-aeadOverhead {
		return nil, fmt.Errorf("%w: %d bytes", ErrChunkTooLong, len(plaintext))
	}
	ciphertext, err := s.sender.Seal(nil, plaintext)
	if err != nil {
		return nil, fmt.Errorf("encrypt a record: %w", err)
	}
	return frameChunk(ciphertext), nil
}

// aeadOverheadは、AES-GCMの認証tagのbyte数です。
const aeadOverhead = 16

// Writerは、Writeの1回を1つのchunkとして暗号化し、wに書くio.Writerです。Writeの戻り値は、
// 全部書けたときにlen(p)で、書けなかったときは0です。暗号文の長さが要る呼び出し元は、
// Sealerを直接使います。
type Writer struct {
	sealer *Sealer
	w      io.Writer
}

// NewWriterは、wへ書くWriterを返します。headerは呼び出し元がすでに書いています。
func NewWriter(w io.Writer, sealer *Sealer) *Writer {
	return &Writer{sealer: sealer, w: w}
}

// Writeは、pを1つのchunkにして書きます。
func (w *Writer) Write(p []byte) (int, error) {
	framed, err := w.sealer.Seal(p)
	if err != nil {
		return 0, err
	}
	n, err := w.w.Write(framed)
	if err != nil {
		return 0, err
	}
	if n != len(framed) {
		return 0, io.ErrShortWrite
	}
	return len(p), nil
}
