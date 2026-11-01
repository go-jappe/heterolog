package cipherfile

import (
	"crypto/hpke"
	"encoding/binary"
	"errors"
	"fmt"
)

// fileの形式の定数です。
const (
	// Magicは、segmentの先頭の8 byteです。fileの先頭にあれば、暗号化したfileです。
	Magic = "HLOGENC1"
	// chunkLengthSizeは、chunkの長さの欄のbyte数（uint32、big endian）です。
	chunkLengthSize = 4
	// encLengthSizeは、headerのカプセル化した鍵の長さの欄のbyte数（uint16、big endian）です。
	encLengthSize = 2
	// MaxChunkLengthは、chunkの暗号文の長さの上限です。これより大きい長さは、壊れた箇所と
	// して読み飛ばします。
	MaxChunkLength = 64 << 20
	// headerPrefixSizeは、headerのうち長さが決まっている部分（magic、鍵のID、encの長さ）です。
	headerPrefixSize = len(Magic) + KeyIDSize + encLengthSize
)

// hpkeInfoは、HPKEの文脈のinfoです。鍵のIDを続けて、segmentのheaderに結び付けます。
const hpkeInfo = "heterolog cipherfile v1 "

var (
	// ErrNotEncryptedは、fileの先頭がmagicでないことを表します。
	ErrNotEncrypted = errors.New("the file does not start with an encrypted segment")
	// ErrChunkTooLongは、1回に暗号化する内容がMaxChunkLengthを超えることを表します。
	ErrChunkTooLong = errors.New("the record is too long to encrypt as one chunk")
)

func suite() (hpke.KDF, hpke.AEAD) {
	return hpke.HKDFSHA256(), hpke.AES256GCM()
}

func infoOf(keyID [KeyIDSize]byte) []byte {
	return append([]byte(hpkeInfo), keyID[:]...)
}

// encodeHeaderは、segmentのheaderを作ります。
func encodeHeader(keyID [KeyIDSize]byte, enc []byte) ([]byte, error) {
	if len(enc) > 0xFFFF {
		return nil, fmt.Errorf("encapsulated key of %d bytes does not fit the header", len(enc))
	}
	header := make([]byte, 0, headerPrefixSize+len(enc))
	header = append(header, Magic...)
	header = append(header, keyID[:]...)
	header = binary.BigEndian.AppendUint16(header, uint16(len(enc)))
	return append(header, enc...), nil
}

// frameChunkは、暗号文に長さの欄を付けます。
func frameChunk(ciphertext []byte) []byte {
	framed := make([]byte, 0, chunkLengthSize+len(ciphertext))
	framed = binary.BigEndian.AppendUint32(framed, uint32(len(ciphertext)))
	return append(framed, ciphertext...)
}
