package fsstore

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"

	"github.com/ulikunitz/xz"
)

// verifyXZFraming requires a complete index and footer for every stream before
// trusting the decoder's EOF. The current XZ decoder can return a normal EOF
// from a truncated block header, including immediately after a stream header.
// Walk backwards using each index's block sizes so a complete final stream
// cannot hide an incomplete preceding stream. ReadAt preserves the position
// for the subsequent full decode, which also verifies block data and checksums.
func verifyXZFraming(in *os.File) error {
	start, err := in.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	info, err := in.Stat()
	if err != nil {
		return err
	}
	end := info.Size()
	if end <= start || (end-start)%4 != 0 {
		return io.ErrUnexpectedEOF
	}
	for {
		end, err = skipXZStreamPadding(in, start, end)
		if err != nil {
			return err
		}
		// Header (12), empty index (8), and footer (12) are all required.
		if end-start < 32 {
			return io.ErrUnexpectedEOF
		}
		var footer [12]byte
		if _, err := in.ReadAt(footer[:], end-12); err != nil {
			return err
		}
		if footer[10] != 'Y' || footer[11] != 'Z' ||
			binary.LittleEndian.Uint32(footer[:4]) != crc32.ChecksumIEEE(footer[4:10]) {
			return errors.New("invalid xz stream footer")
		}
		indexSize := (int64(binary.LittleEndian.Uint32(footer[4:8])) + 1) * 4
		if indexSize < 8 || indexSize > end-start-24 {
			return errors.New("xz index exceeds stream bounds")
		}
		indexStart := end - 12 - indexSize
		blocksSize, err := readXZIndexBlocksSize(in, indexStart, indexSize, indexStart-start-12, nil)
		if err != nil {
			return err
		}
		streamStart := indexStart - blocksSize - 12
		var header [12]byte
		if _, err := in.ReadAt(header[:], streamStart); err != nil {
			return err
		}
		if !xz.ValidHeader(header[:]) || header[6] != footer[8] || header[7] != footer[9] {
			return errors.New("xz stream header and footer do not match")
		}
		// Re-read the bounded index after locating the header. This avoids
		// allocating a block table from untrusted input. Check the actual LZMA2
		// boundaries too: an early end marker can otherwise make the decoder
		// mistake block data for a new, truncated block header and accept EOF.
		if _, err := readXZIndexBlocksSize(in, indexStart, indexSize, blocksSize, func(relative, unpadded int64) error {
			return verifyXZBlockFraming(in, streamStart+12+relative, unpadded, header[7])
		}); err != nil {
			return err
		}
		if streamStart == start {
			return nil
		}
		end = streamStart
	}
}

// Stream padding is zero bytes in multiples of four, after a complete stream.
// Read it in bounded chunks, including when an archive has a large padding area.
func skipXZStreamPadding(in io.ReaderAt, start, end int64) (int64, error) {
	var chunk [4096]byte
	for end > start {
		n := min(int64(len(chunk)), end-start)
		if _, err := in.ReadAt(chunk[:n], end-n); err != nil {
			return 0, err
		}
		for i := n; i > 0; i -= 4 {
			if binary.LittleEndian.Uint32(chunk[i-4:i]) != 0 {
				return end - n + i, nil
			}
		}
		end -= n
	}
	return end, nil
}

// The index is read without allocating from its untrusted record count. Each
// record describes the unpadded size of one block and its uncompressed size.
func readXZIndexBlocksSize(in io.ReaderAt, offset, size, blockBudget int64, verifyBlock func(relative, unpadded int64) error) (int64, error) {
	checksum := crc32.NewIEEE()
	r := bufio.NewReader(io.TeeReader(io.NewSectionReader(in, offset, size-4), checksum))
	indicator, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	if indicator != 0 {
		return 0, errors.New("invalid xz index indicator")
	}
	count, err := readXZIndexInteger(r)
	if err != nil {
		return 0, err
	}
	if count > uint64((size-6)/2) {
		return 0, errors.New("xz index record count exceeds index bounds")
	}
	var blocksSize int64
	for range count {
		unpadded, err := readXZIndexInteger(r)
		if err != nil {
			return 0, err
		}
		if _, err := readXZIndexInteger(r); err != nil {
			return 0, err
		}
		padded := (unpadded + 3) &^ uint64(3)
		if unpadded == 0 || padded > uint64(blockBudget-blocksSize) {
			return 0, errors.New("xz block sizes exceed stream bounds")
		}
		if verifyBlock != nil {
			if err := verifyBlock(blocksSize, int64(unpadded)); err != nil {
				return 0, err
			}
		}
		blocksSize += int64(padded)
	}
	padding, err := io.ReadAll(io.LimitReader(r, 4))
	if err != nil {
		return 0, err
	}
	if len(padding) > 3 {
		return 0, errors.New("excess xz index padding")
	}
	for _, b := range padding {
		if b != 0 {
			return 0, errors.New("nonzero xz index padding")
		}
	}
	var storedCRC [4]byte
	if _, err := in.ReadAt(storedCRC[:], offset+size-4); err != nil {
		return 0, err
	}
	if binary.LittleEndian.Uint32(storedCRC[:]) != checksum.Sum32() {
		return 0, errors.New("invalid xz index checksum")
	}
	return blocksSize, nil
}

// The built-in decoder supports LZMA2 blocks. Validate chunk lengths without
// decoding their payload; the decoder still owns filters, checksums, state
// transitions, and decoded-size checks. A block must end exactly where its
// index record says, rather than drifting into the next block/index/footer.
func verifyXZBlockFraming(in io.ReaderAt, offset, unpadded int64, check byte) error {
	var checkSize int64
	switch check {
	case xz.None:
	case xz.CRC32:
		checkSize = 4
	case xz.CRC64:
		checkSize = 8
	case xz.SHA256:
		checkSize = 32
	default:
		return errors.New("unsupported xz checksum")
	}
	var header [6]byte
	if _, err := in.ReadAt(header[:1], offset); err != nil {
		return err
	}
	headerSize := (int64(header[0]) + 1) * 4
	if header[0] == 0 || headerSize >= unpadded-checkSize {
		return errors.New("xz block header exceeds indexed block bounds")
	}
	end := offset + unpadded - checkSize
	for pos := offset + headerSize; pos < end; {
		if _, err := in.ReadAt(header[:1], pos); err != nil {
			return err
		}
		control := header[0]
		if control == 0 {
			if pos+1 != end {
				return errors.New("xz block ends before its indexed boundary")
			}
			return nil
		}
		var chunkHeaderSize int64
		switch {
		case control == 1 || control == 2:
			chunkHeaderSize = 3
		case control >= 0xc0:
			chunkHeaderSize = 6
		case control >= 0x80:
			chunkHeaderSize = 5
		default:
			return errors.New("invalid lzma2 chunk control byte")
		}
		if chunkHeaderSize > end-pos {
			return io.ErrUnexpectedEOF
		}
		if _, err := in.ReadAt(header[:chunkHeaderSize], pos); err != nil {
			return err
		}
		sizeBytes := header[1:3]
		if control >= 0x80 {
			sizeBytes = header[3:5]
		}
		chunkSize := int64(binary.BigEndian.Uint16(sizeBytes)) + 1
		// Leave at least one byte for the end marker (or the next chunk).
		if chunkSize >= end-pos-chunkHeaderSize {
			return errors.New("lzma2 chunk exceeds indexed block bounds")
		}
		pos += chunkHeaderSize + chunkSize
	}
	return io.ErrUnexpectedEOF
}

// XZ variable-length integers use at most nine bytes and a canonical encoding.
func readXZIndexInteger(r io.ByteReader) (uint64, error) {
	var value uint64
	for i := range 9 {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		value |= uint64(b&0x7f) << (7 * i)
		if b&0x80 == 0 {
			if i > 0 && b == 0 {
				return 0, errors.New("noncanonical xz index integer")
			}
			return value, nil
		}
	}
	return 0, errors.New("xz index integer exceeds 63 bits")
}
