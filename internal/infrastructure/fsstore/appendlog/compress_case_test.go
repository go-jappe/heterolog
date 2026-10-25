package appendlog

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// compressionNameCaseTestsは、保存先directoryが大小文字を区別する場合・しない場合を、
// OS名に依存せず両方試験するための規則です。
var compressionNameCaseTests = []struct {
	name     string
	nameCase pathidentity.NameCase
}{
	{name: "case-sensitive", nameCase: pathidentity.NameCaseSensitive},
	{name: "case-insensitive", nameCase: pathidentity.NameCaseInsensitive},
}

func TestCompressionCodecForExtensionCaseFollowsDirectoryRule(t *testing.T) {
	t.Parallel()
	for _, rule := range compressionNameCaseTests {
		for _, format := range []pkgfsstore.CompressFormat{
			pkgfsstore.CompressGzip, pkgfsstore.CompressZstd, pkgfsstore.CompressLz4, pkgfsstore.CompressXz,
		} {
			for _, uppercase := range []bool{false, true} {
				extension := format.Extension()
				if uppercase {
					extension = strings.ToUpper(extension)
				}
				t.Run(rule.name+extension, func(t *testing.T) {
					codec, supported, err := compressionCodecForExtension(extension, nil, rule.nameCase)
					wantSupported := !uppercase || rule.nameCase == pathidentity.NameCaseInsensitive
					if err != nil || supported != wantSupported {
						t.Fatalf("compressionCodecForExtension(%q) = %v, %v, %v, want supported=%v", extension, codec, supported, err, wantSupported)
					}
					if wantSupported && (codec == nil || codec.Format() != format) {
						t.Fatalf("compressionCodecForExtension(%q) = %v, want format %q", extension, codec, format)
					}
				})
			}
		}
	}
}

func TestCompressionCodecForExtensionCaseKeepsCustomCodecPriority(t *testing.T) {
	t.Parallel()
	for _, rule := range compressionNameCaseTests {
		for _, format := range []pkgfsstore.CompressFormat{pkgfsstore.CompressGzip, "GZ", "br", "BR"} {
			custom := &userDefinedAppendLogCompressionCodec{format: format}
			for _, extension := range []string{
				strings.ToLower(format.Extension()), strings.ToUpper(format.Extension()),
			} {
				t.Run(rule.name+string(format)+extension, func(t *testing.T) {
					codec, _, err := compressionCodecForExtension(extension, custom, rule.nameCase)
					if err != nil {
						t.Fatal(err)
					}
					wantCustom := rule.nameCase == pathidentity.NameCaseInsensitive || extension == format.Extension()
					if (codec == custom) != wantCustom {
						t.Fatalf("compressionCodecForExtension(%q, custom=%q) selected %T, want custom=%v", extension, format, codec, wantCustom)
					}
				})
			}
		}
	}
}

func TestVerifyExistingCompressedFileIntegrityCaseRejectsCorruptKnownFormat(t *testing.T) {
	t.Parallel()
	for _, rule := range compressionNameCaseTests {
		for _, suffix := range []string{".log.gz", ".log.GZ", ".LOG.GZ"} {
			t.Run(rule.name+suffix, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "app_20260911_1"+suffix)
				if err := os.WriteFile(path, []byte("not gzip"), 0o600); err != nil {
					t.Fatal(err)
				}
				err := verifyExistingCompressedFileIntegrity(path, nil, rule.nameCase)
				wantCorrupt := rule.nameCase == pathidentity.NameCaseInsensitive || suffix == ".log.gz"
				wantInvalid := wantCorrupt
				if errors.Is(err, pkgfsstore.ErrCorruptCompressedFile) != wantCorrupt ||
					errors.Is(err, infrafsstore.ErrInvalidCompressedFile) != wantInvalid {
					t.Fatalf("verifyExistingCompressedFileIntegrity(%q) = %v, want corrupt=%v invalid=%v", suffix, err, wantCorrupt, wantInvalid)
				}
				if !wantInvalid && !errors.Is(err, infrafsstore.ErrUnsupportedCompressionFormat) {
					t.Fatalf("case-distinct extension in a case-sensitive directory must prevent source retirement: %v", err)
				}
			})
		}
	}
}

func TestVerifyExistingCompressedFileIntegrityCaseUsesDirectoryRule(t *testing.T) {
	t.Parallel()
	for _, suffix := range []string{".log.gz", ".log.GZ"} {
		t.Run(suffix, func(t *testing.T) {
			directory := t.TempDir()
			path := filepath.Join(directory, "app_20260911_1"+suffix)
			if err := os.WriteFile(path, []byte("not gzip"), 0o600); err != nil {
				t.Fatal(err)
			}
			store := FileStore{Config: NewConfigAdapter(pkgfsstore.Config{
				Lifecycle: pkgfsstore.FileLifecycleConfig{CompressFormat: pkgfsstore.CompressZstd},
			})}
			// 規則を指定しない公開処理は、ComparisonKeyと共用する保存先directoryの判定を使います。
			want := verifyExistingCompressedFileIntegrity(path, nil, pathidentity.DirectoryNameCase(directory))
			err := store.VerifyExistingCompressedFileIntegrity(path)
			for _, target := range []error{
				pkgfsstore.ErrCorruptCompressedFile,
				infrafsstore.ErrInvalidCompressedFile,
				infrafsstore.ErrUnsupportedCompressionFormat,
			} {
				if errors.Is(err, target) != errors.Is(want, target) {
					t.Fatalf("VerifyExistingCompressedFileIntegrity(%q) = %v, want directory rule result %v", suffix, err, want)
				}
			}
		})
	}
}

func TestVerifyExistingCompressedFileIntegrityCaseUsesCustomCodec(t *testing.T) {
	t.Parallel()
	for _, rule := range compressionNameCaseTests {
		t.Run(rule.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "app_20260911_1.log.BR")
			if err := os.WriteFile(path, []byte("not custom compression"), 0o600); err != nil {
				t.Fatal(err)
			}
			custom := userDefinedAppendLogCompressionCodec{format: "br"}
			err := verifyExistingCompressedFileIntegrity(path, custom, rule.nameCase)
			if rule.nameCase == pathidentity.NameCaseInsensitive {
				if !errors.Is(err, errInvalidUserDefinedAppendLogCompressedFile) ||
					!errors.Is(err, pkgfsstore.ErrCorruptCompressedFile) {
					t.Fatalf("uppercase custom extension must use current codec: %v", err)
				}
			} else if !errors.Is(err, infrafsstore.ErrUnsupportedCompressionFormat) {
				t.Fatalf("case-distinct extension in a case-sensitive directory must prevent source retirement: %v", err)
			}
		})
	}
	t.Run("directory rule", func(t *testing.T) {
		directory := t.TempDir()
		path := filepath.Join(directory, "app_20260911_1.log.BR")
		if err := os.WriteFile(path, []byte("not custom compression"), 0o600); err != nil {
			t.Fatal(err)
		}
		custom := userDefinedAppendLogCompressionCodec{format: "br"}
		store := FileStore{Config: newCompressionTestConfigWithCodec(custom)}
		want := verifyExistingCompressedFileIntegrity(path, custom, pathidentity.DirectoryNameCase(directory))
		err := store.VerifyExistingCompressedFileIntegrity(path)
		for _, target := range []error{
			errInvalidUserDefinedAppendLogCompressedFile,
			pkgfsstore.ErrCorruptCompressedFile,
			infrafsstore.ErrUnsupportedCompressionFormat,
		} {
			if errors.Is(err, target) != errors.Is(want, target) {
				t.Fatalf("VerifyExistingCompressedFileIntegrity = %v, want directory rule result %v", err, want)
			}
		}
	})
}
