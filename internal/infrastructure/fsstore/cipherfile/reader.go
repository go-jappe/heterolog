package cipherfile

import (
	"bytes"
	"crypto/hpke"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Skipは、復号で読み飛ばしたbyte列です。
type Skip struct {
	Offset int64
	Length int64
	Reason string
}

// 読み飛ばす理由です。
const (
	SkipNotEncrypted    = "not an encrypted segment"
	SkipDamagedHeader   = "damaged segment header"
	SkipUnknownKey      = "no identity for the key ID"
	SkipDamagedChunk    = "damaged or truncated chunk"
	SkipUnreadableChunk = "chunk could not be decrypted"
)

// Reportは、復号の結果です。
type Report struct {
	Segments       int
	Records        int
	PlaintextBytes int64
	// Skippedは、読み飛ばしたbyte列です。空なら、fileを全部復号できました。
	Skipped []Skip
	// UnknownKeyIDsは、渡した秘密鍵のどれにも合わない鍵のIDです。
	UnknownKeyIDs []string
}

// Complete は、読み飛ばしがなかったことを返します。
func (r Report) Complete() bool {
	return len(r.Skipped) == 0
}

// Decryptは、rの暗号化したfileをidentitiesの秘密鍵で復号し、平文をwに書きます。壊れた
// 箇所や鍵のないsegmentは読み飛ばして続け、Reportに記録します。fileの先頭にmagicがなければ
// ErrNotEncryptedを返します。読み書きのerrorだけがerrorになります。
func Decrypt(r io.ReadSeeker, identities []*Identity, w io.Writer) (Report, error) {
	d := &decrypter{r: r, w: w, identities: map[string]*Identity{}}
	for _, identity := range identities {
		d.identities[identity.KeyID()] = identity
	}
	size, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		return d.report, err
	}
	d.size = size
	if err := d.run(); err != nil {
		return d.report, err
	}
	return d.report, nil
}

type decrypter struct {
	r          io.ReadSeeker
	w          io.Writer
	identities map[string]*Identity
	size       int64
	report     Report
	seenKeyIDs map[string]bool
}

func (d *decrypter) run() error {
	pos := int64(0)
	for pos < d.size {
		if ok, err := d.hasMagicAt(pos); err != nil {
			return err
		} else if !ok {
			reason := SkipDamagedChunk
			if pos == 0 {
				reason = SkipNotEncrypted
			}
			next, err := d.skip(pos, pos+1, reason)
			if err != nil {
				return err
			}
			if pos == 0 && next == d.size {
				return ErrNotEncrypted
			}
			pos = next
			continue
		}
		next, err := d.readSegment(pos)
		if err != nil {
			return err
		}
		pos = next
	}
	return nil
}

// readSegmentは、startにあるsegmentを読み、次の位置を返します。
func (d *decrypter) readSegment(start int64) (int64, error) {
	prefix, err := d.readAt(start, headerPrefixSize)
	if err != nil {
		return 0, err
	}
	if len(prefix) < headerPrefixSize {
		return d.skip(start, start+1, SkipDamagedHeader)
	}
	var keyID [KeyIDSize]byte
	copy(keyID[:], prefix[len(Magic):])
	encLen := int(binary.BigEndian.Uint16(prefix[len(Magic)+KeyIDSize:]))
	enc, err := d.readAt(start+int64(headerPrefixSize), encLen)
	if err != nil {
		return 0, err
	}
	if len(enc) < encLen {
		return d.skip(start, start+1, SkipDamagedHeader)
	}
	pos := start + int64(headerPrefixSize) + int64(encLen)
	id := fmt.Sprintf("%x", keyID[:])
	identity, ok := d.identities[id]
	if !ok {
		if d.seenKeyIDs == nil {
			d.seenKeyIDs = map[string]bool{}
		}
		if !d.seenKeyIDs[id] {
			d.seenKeyIDs[id] = true
			d.report.UnknownKeyIDs = append(d.report.UnknownKeyIDs, id)
		}
		return d.skip(start, pos, SkipUnknownKey)
	}
	private, err := identity.hpkePrivateKey()
	if err != nil {
		return 0, err
	}
	kdf, aead := suite()
	recipient, err := hpke.NewRecipient(enc, private, kdf, aead, infoOf(keyID))
	if err != nil {
		return d.skip(start, start+1, SkipDamagedHeader)
	}
	d.report.Segments++
	return d.readChunks(pos, recipient)
}

// readChunksは、posから次のsegmentかfileの終わりまでのchunkを復号します。
func (d *decrypter) readChunks(pos int64, recipient *hpke.Recipient) (int64, error) {
	for pos < d.size {
		if ok, err := d.hasMagicAt(pos); err != nil {
			return 0, err
		} else if ok {
			return pos, nil
		}
		lengthBytes, err := d.readAt(pos, chunkLengthSize)
		if err != nil {
			return 0, err
		}
		if len(lengthBytes) < chunkLengthSize {
			return d.skip(pos, pos+1, SkipDamagedChunk)
		}
		length := int64(binary.BigEndian.Uint32(lengthBytes))
		if length < aeadOverhead || length > MaxChunkLength {
			return d.skip(pos, pos+1, SkipDamagedChunk)
		}
		ciphertext, err := d.readAt(pos+chunkLengthSize, int(length))
		if err != nil {
			return 0, err
		}
		if int64(len(ciphertext)) < length {
			return d.skip(pos, pos+1, SkipDamagedChunk)
		}
		plaintext, err := recipient.Open(nil, ciphertext)
		if err != nil {
			return d.skipUnreadableChunk(pos, chunkLengthSize+length)
		}
		if _, err := d.w.Write(plaintext); err != nil {
			return 0, fmt.Errorf("write the decrypted record: %w", err)
		}
		d.report.Records++
		d.report.PlaintextBytes += int64(len(plaintext))
		pos += chunkLengthSize + length
	}
	return pos, nil
}

// hasMagicAtは、posにmagicがあるかを返します。
func (d *decrypter) hasMagicAt(pos int64) (bool, error) {
	head, err := d.readAt(pos, len(Magic))
	if err != nil {
		return false, err
	}
	return string(head) == Magic, nil
}

// readAtは、posからn byteまでを読みます。fileの終わりでは短い結果を返します。
func (d *decrypter) readAt(pos int64, n int) ([]byte, error) {
	if pos >= d.size || n == 0 {
		return nil, nil
	}
	if remaining := d.size - pos; int64(n) > remaining {
		n = int(remaining)
	}
	if _, err := d.r.Seek(pos, io.SeekStart); err != nil {
		return nil, err
	}
	buf := make([]byte, n)
	read, err := io.ReadFull(d.r, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return buf[:read], nil
}

// skipは、startから始まる壊れた範囲を読み飛ばし、searchFrom以降の次のmagicの位置を
// 返します。直前の読み飛ばしと理由が同じで続いていれば、一つにまとめて記録します。
func (d *decrypter) skip(start, searchFrom int64, reason string) (int64, error) {
	next, err := d.findMagic(searchFrom)
	if err != nil {
		return 0, err
	}
	return d.record(start, next, reason), nil
}

// recordは、startからnextまでの読み飛ばしを記録し、nextを返します。
func (d *decrypter) record(start, next int64, reason string) int64 {
	if n := len(d.report.Skipped); n > 0 && d.report.Skipped[n-1].Offset+d.report.Skipped[n-1].Length == start && d.report.Skipped[n-1].Reason == reason {
		d.report.Skipped[n-1].Length = next - d.report.Skipped[n-1].Offset
	} else {
		d.report.Skipped = append(d.report.Skipped, Skip{Offset: start, Length: next - start, Reason: reason})
	}
	return next
}

// skipUnreadableChunkは、復号できなかったchunkを読み飛ばします。次のmagicがそのchunkの
// 中にあれば、chunkは途中で切れていて、その後に新しいsegmentが足されたものです。
func (d *decrypter) skipUnreadableChunk(pos, size int64) (int64, error) {
	next, err := d.findMagic(pos + 1)
	if err != nil {
		return 0, err
	}
	reason := SkipUnreadableChunk
	if next < pos+size {
		reason = SkipDamagedChunk
	}
	return d.record(pos, next, reason), nil
}

// findMagicは、from以降で最初のmagicの位置を返します。なければfileの終わりです。
func (d *decrypter) findMagic(from int64) (int64, error) {
	const window = 64 << 10
	magic := []byte(Magic)
	pos := from
	var carry []byte
	for pos < d.size {
		buf, err := d.readAt(pos, window)
		if err != nil {
			return 0, err
		}
		if len(buf) == 0 {
			break
		}
		joined := append(carry, buf...)
		if i := bytes.Index(joined, magic); i >= 0 {
			return pos - int64(len(carry)) + int64(i), nil
		}
		keep := len(magic) - 1
		if len(joined) < keep {
			keep = len(joined)
		}
		carry = append([]byte(nil), joined[len(joined)-keep:]...)
		pos += int64(len(buf))
	}
	return d.size, nil
}
