package fsstore

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/ulikunitz/xz"
)

var errInvalidUserDefinedCompressedFile = errors.New("invalid user-defined compressed file")

const userDefinedCompressionHeader = "test-codec-v1\n"

type userDefinedCompressionCodec struct{}

type unavailableVerificationCompressionCodec struct{}

func (unavailableVerificationCompressionCodec) Format() pkgfsstore.CompressFormat {
	return pkgfsstore.CompressFormat("unavailable")
}

func (unavailableVerificationCompressionCodec) Compress(*os.File, *os.File) error {
	return nil
}

func (unavailableVerificationCompressionCodec) VerifyCompressedIntegrity(*os.File) error {
	return &os.PathError{Op: "read", Path: "compressed.log.unavailable", Err: syscall.EIO}
}

func (userDefinedCompressionCodec) Format() pkgfsstore.CompressFormat {
	return pkgfsstore.CompressFormat("br")
}

func (userDefinedCompressionCodec) Compress(in, out *os.File) error {
	if _, err := io.WriteString(out, userDefinedCompressionHeader); err != nil {
		return err
	}
	_, err := io.Copy(out, in)
	return err
}

func (userDefinedCompressionCodec) VerifyCompressedIntegrity(in *os.File) error {
	content, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	if !bytes.HasPrefix(content, []byte(userDefinedCompressionHeader)) {
		return fmt.Errorf(
			"%w: %w: header is missing",
			pkgfsstore.ErrCorruptCompressedFile,
			errInvalidUserDefinedCompressedFile,
		)
	}
	return nil
}

type genericFailureCompressionCodec struct {
	err error
}

func (genericFailureCompressionCodec) Format() pkgfsstore.CompressFormat {
	return pkgfsstore.CompressFormat("generic")
}

func (genericFailureCompressionCodec) Compress(*os.File, *os.File) error {
	return nil
}

func (c genericFailureCompressionCodec) VerifyCompressedIntegrity(*os.File) error {
	return c.err
}

func decodeGzipForTest(input io.Reader) ([]byte, error) {
	reader, err := gzip.NewReader(input)
	if err != nil {
		return nil, err
	}
	content, readErr := io.ReadAll(reader)
	return content, errors.Join(readErr, reader.Close())
}

func decodeZstdForTest(input io.Reader) ([]byte, error) {
	reader, err := zstd.NewReader(input)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func decodeLz4ForTest(input io.Reader) ([]byte, error) {
	return io.ReadAll(lz4.NewReader(input))
}

func decodeXzForTest(input io.Reader) ([]byte, error) {
	reader, err := xz.NewReader(input)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(reader)
}

func TestVerifyCompressedFileIntegrityAcceptsEverySupportedFormat(t *testing.T) {
	testCases := []struct {
		format pkgfsstore.CompressFormat
		decode func(io.Reader) ([]byte, error)
	}{
		{format: pkgfsstore.CompressGzip, decode: decodeGzipForTest},
		{format: pkgfsstore.CompressZstd, decode: decodeZstdForTest},
		{format: pkgfsstore.CompressLz4, decode: decodeLz4ForTest},
		{format: pkgfsstore.CompressXz, decode: decodeXzForTest},
	}
	sourceContent := []byte("verified compressed content\n")

	for _, testCase := range testCases {
		t.Run(string(testCase.format), func(t *testing.T) {
			dir := t.TempDir()
			sourcePath := filepath.Join(dir, "source.log")
			compressedPath := GenerateCompressedPath(sourcePath, testCase.format)
			if err := os.WriteFile(sourcePath, sourceContent, 0o600); err != nil {
				t.Fatalf("write source: %v", err)
			}
			codec, err := ResolveCompressionCodec(testCase.format, nil)
			if err != nil {
				t.Fatalf("ResolveCompressionCodec(%s) error = %v", testCase.format, err)
			}
			if codec.Format() != testCase.format {
				t.Fatalf(
					"ResolveCompressionCodec(%s).Format() = %s",
					testCase.format,
					codec.Format(),
				)
			}

			in, out, err := OpenFilesForCompression(sourcePath, compressedPath)
			if err != nil {
				t.Fatalf("open compression files: %v", err)
			}
			if err := codec.Compress(in, out); err != nil {
				t.Fatalf("compress %s: %v", testCase.format, err)
			}
			if err := in.Close(); err != nil {
				t.Fatalf("close source: %v", err)
			}
			if err := out.Close(); err != nil {
				t.Fatalf("close compressed file: %v", err)
			}

			compressed, err := os.Open(compressedPath)
			if err != nil {
				t.Fatalf("open compressed file for independent decode: %v", err)
			}
			decoded, decodeErr := testCase.decode(compressed)
			closeErr := compressed.Close()
			if err := errors.Join(decodeErr, closeErr); err != nil {
				t.Fatalf("decode %s with its standard decoder: %v", testCase.format, err)
			}
			if !bytes.Equal(decoded, sourceContent) {
				t.Fatalf("decoded content = %q, want %q", decoded, sourceContent)
			}

			if err := VerifyCompressedFileIntegrity(compressedPath, codec); err != nil {
				t.Fatalf("VerifyCompressedFileIntegrity() error = %v", err)
			}
		})
	}
}

func TestVerifyCompressedFileIntegrityRejectsCorruptDataForEverySupportedFormat(t *testing.T) {
	for _, format := range []pkgfsstore.CompressFormat{
		pkgfsstore.CompressGzip,
		pkgfsstore.CompressZstd,
		pkgfsstore.CompressLz4,
		pkgfsstore.CompressXz,
	} {
		t.Run(string(format), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "corrupt"+format.Extension())
			if err := os.WriteFile(path, []byte("not compressed data"), 0o600); err != nil {
				t.Fatalf("write corrupt file: %v", err)
			}

			codec, err := ResolveCompressionCodec(format, nil)
			if err != nil {
				t.Fatalf("ResolveCompressionCodec(%s) error = %v", format, err)
			}
			err = VerifyCompressedFileIntegrity(path, codec)
			if !errors.Is(err, ErrInvalidCompressedFile) {
				t.Fatalf("VerifyCompressedFileIntegrity() error = %v, want ErrInvalidCompressedFile", err)
			}
			if !errors.Is(err, pkgfsstore.ErrCorruptCompressedFile) {
				t.Fatalf("VerifyCompressedFileIntegrity() error = %v, want ErrCorruptCompressedFile", err)
			}
		})
	}
}

func TestVerifyCompressedFileIntegrityAcceptsUserDefinedCodec(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.log")
	if err := os.WriteFile(sourcePath, []byte("user-defined content\n"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	custom := userDefinedCompressionCodec{}
	codec, err := ResolveCompressionCodec(pkgfsstore.CompressGzip, custom)
	if err != nil {
		t.Fatalf("ResolveCompressionCodec() error = %v", err)
	}
	if codec != custom {
		t.Fatalf("ResolveCompressionCodec() codec = %T, want supplied custom codec", codec)
	}
	compressedPath := GenerateCompressedPath(sourcePath, codec.Format())
	in, out, err := OpenFilesForCompression(sourcePath, compressedPath)
	if err != nil {
		t.Fatalf("open compression files: %v", err)
	}
	if err := codec.Compress(in, out); err != nil {
		t.Fatalf("compress with user-defined codec: %v", err)
	}
	if err := in.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}
	if err := out.Close(); err != nil {
		t.Fatalf("close compressed file: %v", err)
	}

	if err := VerifyCompressedFileIntegrity(compressedPath, codec); err != nil {
		t.Fatalf("VerifyCompressedFileIntegrity() error = %v", err)
	}
}

func TestVerifyCompressedFileIntegrityWrapsUserDefinedCodecCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.br")
	if err := os.WriteFile(path, []byte("missing custom header"), 0o600); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}

	err := VerifyCompressedFileIntegrity(path, userDefinedCompressionCodec{})
	if !errors.Is(err, ErrInvalidCompressedFile) {
		t.Fatalf("VerifyCompressedFileIntegrity() error = %v, want ErrInvalidCompressedFile", err)
	}
	if !errors.Is(err, errInvalidUserDefinedCompressedFile) {
		t.Fatalf("VerifyCompressedFileIntegrity() error = %v, want codec verification cause", err)
	}
	if !errors.Is(err, pkgfsstore.ErrCorruptCompressedFile) {
		t.Fatalf("VerifyCompressedFileIntegrity() error = %v, want ErrCorruptCompressedFile", err)
	}
}

func TestVerifyCompressedFileIntegrityDoesNotInferCustomCodecCorruption(t *testing.T) {
	for _, verificationErr := range []error{
		errors.New("custom verifier could not classify failure"),
		context.Canceled,
		context.DeadlineExceeded,
	} {
		path := filepath.Join(t.TempDir(), "unverified.log.generic")
		if err := os.WriteFile(path, []byte("opaque"), 0o600); err != nil {
			t.Fatal(err)
		}

		err := VerifyCompressedFileIntegrity(
			path,
			genericFailureCompressionCodec{err: verificationErr},
		)
		if !errors.Is(err, ErrInvalidCompressedFile) || !errors.Is(err, verificationErr) {
			t.Fatalf("VerifyCompressedFileIntegrity() error = %v, want invalid/%v", err, verificationErr)
		}
		if errors.Is(err, pkgfsstore.ErrCorruptCompressedFile) {
			t.Fatalf("custom codec error was inferred as corruption: %v", err)
		}
	}
}

func TestVerifyCompressedFileIntegrityDoesNotClassifyFilesystemIOAsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unavailable.log.unavailable")
	if err := os.WriteFile(path, []byte("opaque"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := VerifyCompressedFileIntegrity(path, unavailableVerificationCompressionCodec{})
	if !errors.Is(err, ErrInvalidCompressedFile) || !errors.Is(err, syscall.EIO) {
		t.Fatalf("VerifyCompressedFileIntegrity() error = %v, want invalid/EIO", err)
	}
	if errors.Is(err, pkgfsstore.ErrCorruptCompressedFile) {
		t.Fatalf("filesystem EIO was classified as corruption: %v", err)
	}
}

func TestResolveCompressionCodecRejectsUnsupportedFormatWithoutCustomCodec(t *testing.T) {
	codec, err := ResolveCompressionCodec(pkgfsstore.CompressFormat("unsupported"), nil)
	if !errors.Is(err, ErrUnsupportedCompressionFormat) {
		t.Fatalf("ResolveCompressionCodec() error = %v, want ErrUnsupportedCompressionFormat", err)
	}
	if codec != nil {
		t.Fatalf("ResolveCompressionCodec() codec = %T, want nil", codec)
	}
}
