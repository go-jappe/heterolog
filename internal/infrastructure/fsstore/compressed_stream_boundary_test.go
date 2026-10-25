package fsstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/ulikunitz/xz"
)

func TestCompressedIntegrityRejectsEveryIncompletePrefix(t *testing.T) {
	for _, format := range []pkgfsstore.CompressFormat{
		pkgfsstore.CompressGzip, pkgfsstore.CompressZstd, pkgfsstore.CompressLz4, pkgfsstore.CompressXz,
	} {
		t.Run(string(format), func(t *testing.T) {
			dir := t.TempDir()
			source, path := filepath.Join(dir, "raw"), filepath.Join(dir, "compressed")
			if err := os.WriteFile(source, bytes.Repeat([]byte("preserve every record\n"), 50), 0o600); err != nil {
				t.Fatal(err)
			}
			codec, err := ResolveCompressionCodec(format, nil)
			if err != nil {
				t.Fatal(err)
			}
			in, out, err := OpenFilesForCompression(source, path)
			if err != nil {
				t.Fatal(err)
			}
			compressErr := codec.Compress(in, out)
			if err := errors.Join(compressErr, in.Close(), out.Close()); err != nil {
				t.Fatal(err)
			}
			if err := VerifyCompressedFileIntegrity(path, codec); err != nil {
				t.Fatalf("complete stream: %v", err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			// A nonempty prefix may have a valid header or complete blocks while
			// still missing the index/footer needed to prove compression complete.
			for size := 1; size < len(data); size++ {
				if err := os.WriteFile(path, data[:size], 0o600); err != nil {
					t.Fatal(err)
				}
				if err := VerifyCompressedFileIntegrity(path, codec); !errors.Is(err, pkgfsstore.ErrCorruptCompressedFile) {
					t.Errorf("prefix %d/%d: error = %v, want corruption", size, len(data), err)
				}
			}
		})
	}
}

func TestXZIntegrityAcceptsCompleteStreamsAndPadding(t *testing.T) {
	payload := bytes.Repeat([]byte("multiple blocks\n"), 30)
	for _, check := range []byte{xz.None, xz.CRC32, xz.CRC64, xz.SHA256} {
		data := makeXZIntegrityFixture(t, payload, xz.WriterConfig{CheckSum: check, NoCheckSum: check == xz.None, BlockSize: 64})
		assertXZIntegrity(t, data, false)
	}
	empty := makeXZIntegrityFixture(t, nil, xz.WriterConfig{})
	data := makeXZIntegrityFixture(t, payload, xz.WriterConfig{})
	for name, contents := range map[string][]byte{
		"empty stream":              empty,
		"trailing padding":          bytes.Join([][]byte{data, make([]byte, 8196)}, nil),
		"concatenated":              bytes.Join([][]byte{data, empty, data}, nil),
		"concatenated with padding": bytes.Join([][]byte{data, make([]byte, 12), empty, make([]byte, 4), data, make([]byte, 8)}, nil),
	} {
		t.Run(name, func(t *testing.T) { assertXZIntegrity(t, contents, false) })
	}
}

func TestXZIntegrityAcceptsMultipleChunks(t *testing.T) {
	// Exercise both compressed continuations and uncompressed continuations
	// within a single block, rather than only the first LZMA2 chunk.
	random := make([]byte, 150000)
	if _, err := rand.New(rand.NewSource(1)).Read(random); err != nil {
		t.Fatal(err)
	}
	for name, payload := range map[string][]byte{
		"compressed":   bytes.Repeat([]byte("a"), 3<<20),
		"uncompressed": random,
	} {
		t.Run(name, func(t *testing.T) {
			assertXZIntegrity(t, makeXZIntegrityFixture(t, payload, xz.WriterConfig{}), false)
		})
	}
}

func TestXZIntegrityRejectsIncompleteStreamsAroundValidPeers(t *testing.T) {
	data := makeXZIntegrityFixture(t, []byte("records"), xz.WriterConfig{BlockSize: 2})
	for name, contents := range map[string][]byte{
		"truncated first stream":             bytes.Join([][]byte{data[:12], data}, nil),
		"truncated last stream":              bytes.Join([][]byte{data, data[:12]}, nil),
		"truncated last stream with padding": bytes.Join([][]byte{data, data[:12], make([]byte, 8)}, nil),
		"leading padding":                    bytes.Join([][]byte{make([]byte, 4), data}, nil),
		"only padding":                       make([]byte, 32),
		"one padding byte":                   append(bytes.Clone(data), 0),
		"three padding bytes":                append(bytes.Clone(data), 0, 0, 0),
		"nonzero trailing data":              append(bytes.Clone(data), 1, 2, 3, 4),
	} {
		t.Run(name, func(t *testing.T) { assertXZIntegrity(t, contents, true) })
	}
	for size := 1; size < len(data); size++ {
		assertXZIntegrity(t, data[:size], true)
	}
}

func TestXZIntegrityRejectsDamagedFramingAndPayload(t *testing.T) {
	data := makeXZIntegrityFixture(t, []byte("records"), xz.WriterConfig{})
	footerOffset := len(data) - 12
	indexSize := (int(binary.LittleEndian.Uint32(data[footerOffset+4:])) + 1) * 4
	indexOffset := footerOffset - indexSize
	for _, mutation := range []struct {
		name  string
		apply func([]byte)
	}{
		{"footer checksum", func(b []byte) { b[footerOffset] ^= 1 }},
		{"oversized index", func(b []byte) {
			binary.LittleEndian.PutUint32(b[footerOffset+4:], ^uint32(0))
			binary.LittleEndian.PutUint32(b[footerOffset:], crc32.ChecksumIEEE(b[footerOffset+4:footerOffset+10]))
		}},
		{"index checksum", func(b []byte) { b[footerOffset-1] ^= 1 }},
		{"impossible record count", func(b []byte) {
			b[indexOffset+1] = 127
			binary.LittleEndian.PutUint32(b[footerOffset-4:], crc32.ChecksumIEEE(b[indexOffset:footerOffset-4]))
		}},
		{"header flags mismatch", func(b []byte) {
			b[footerOffset+9] = xz.CRC32
			binary.LittleEndian.PutUint32(b[footerOffset:], crc32.ChecksumIEEE(b[footerOffset+4:footerOffset+10]))
		}},
		{"oversized block header", func(b []byte) { b[12] = 255 }},
		{"block data", func(b []byte) { b[32] ^= 1 }},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			corrupt := bytes.Clone(data)
			mutation.apply(corrupt)
			assertXZIntegrity(t, corrupt, true)
		})
	}
}

func TestXZIntegrityRejectsEarlyBlockEndBeforeIndexedBoundary(t *testing.T) {
	data := makeXZIntegrityFixture(t, []byte("records"), xz.WriterConfig{NoCheckSum: true})
	assertXZIntegrity(t, data, false)
	compressedStart := 12 + (int(data[12])+1)*4
	// The stream's original index and footer remain valid. The decoder can
	// otherwise accept this shortened block and swallow EOF while reading the
	// phantom 1024-byte block header immediately after its zero padding.
	data[compressedStart] = 0
	clear(data[compressedStart+1 : compressedStart+4])
	data[compressedStart+4] = 255
	assertXZIntegrity(t, data, true)
}

func makeXZIntegrityFixture(t *testing.T, payload []byte, cfg xz.WriterConfig) []byte {
	t.Helper()
	var out bytes.Buffer
	w, err := cfg.NewWriter(&out)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := w.Write(payload)
	if err := errors.Join(writeErr, w.Close()); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func assertXZIntegrity(t *testing.T, contents []byte, corrupt bool) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "archive.xz")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	codec, err := ResolveCompressionCodec(pkgfsstore.CompressXz, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = VerifyCompressedFileIntegrity(path, codec)
	if corrupt {
		if !errors.Is(err, pkgfsstore.ErrCorruptCompressedFile) {
			t.Fatalf("%d byte input: error = %v, want corruption", len(contents), err)
		}
	} else if err != nil {
		t.Fatalf("complete xz stream: %v", err)
	}
}

func TestXZIntegrityPreservesFilesystemErrors(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "closed.xz")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	codec, err := ResolveCompressionCodec(pkgfsstore.CompressXz, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = codec.VerifyCompressedIntegrity(f)
	if !errors.Is(err, os.ErrClosed) || errors.Is(err, pkgfsstore.ErrCorruptCompressedFile) {
		t.Fatalf("closed file classified as corruption: %v", err)
	}
}

func TestXZIntegrityStartsAtCurrentFilePosition(t *testing.T) {
	data := makeXZIntegrityFixture(t, []byte("records"), xz.WriterConfig{})
	path := filepath.Join(t.TempDir(), "embedded.xz")
	if err := os.WriteFile(path, append([]byte("prefix"), data...), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Seek(6, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if err := decodeXzCompressed(f, io.Discard); err != nil {
		t.Fatal(err)
	}
}
