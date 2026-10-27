package fsstore

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
	"unicode"
)

// ErrCorruptCompressedFile is the classification sentinel returned when a codec
// has checked the decoding, checksum, footer or the like and established that
// the compressed content is corrupt, or when the compressed result of a
// non-empty source log has been confirmed to be 0 bytes and so cannot
// represent the source log.
//
// Implementations of CompressionCodec must not use this sentinel to wrap
// filesystem or resource errors, context cancellation, failures to initialize
// the verifier, or generic errors whose cause cannot be identified as
// corruption.
var ErrCorruptCompressedFile = errors.New("compressed file is corrupt")

// FileRotationType is the rotation unit that determines the period boundaries
// in appendlog file names.
type FileRotationType string

// CompressFormat is a persistent ID that identifies a compression format.
// Besides the built-in formats, it also represents custom formats that a
// CompressionCodec provides.
type CompressFormat string

const (
	// CompressGzip represents the gzip format.
	CompressGzip CompressFormat = "gzip"
	// CompressZstd represents the Zstandard format.
	CompressZstd CompressFormat = "zstd"
	// CompressLz4 represents the LZ4 format.
	CompressLz4 CompressFormat = "lz4"
	// CompressXz represents the XZ format.
	CompressXz CompressFormat = "xz"

	// ExtensionCompressGzip is the standard file extension of the gzip format.
	ExtensionCompressGzip = ".gz"
	// ExtensionCompressZstd is the standard file extension of the Zstandard format.
	ExtensionCompressZstd = ".zst"
	// ExtensionCompressLz4 is the file extension of the LZ4 format.
	ExtensionCompressLz4 = ".lz4"
	// ExtensionCompressXz is the file extension of the XZ format.
	ExtensionCompressXz = ".xz"

	// FileNameFormat is the persistent naming format of normal appendlog files.
	// Its arguments are the prefix, the target, the rotation period and the
	// index, in that order.
	FileNameFormat = "/%s%s_%s_%d.log"
	// FailureArchiveFileNameFormat is the persistent naming format of failure
	// archive JSONL files. Its arguments are in the same order as those of
	// FileNameFormat.
	FailureArchiveFileNameFormat = "/%s%s_%s_%d.jsonl"

	// FileDateFormat is the YYYYMMDD format used for the rotation period in file
	// names and for archive checkpoint cursors. Changing it requires migrating
	// existing files and checkpoints.
	FileDateFormat = "20060102"

	// FileRotationDaily represents a daily rotation period.
	FileRotationDaily FileRotationType = "daily"
	// FileRotationWeekly represents a weekly rotation period. A period starts at
	// 00:00 on the configured rotation weekday, so a new file starts on that day,
	// and the file names carry the date of that day.
	FileRotationWeekly FileRotationType = "weekly"
	// FileRotationMonthly represents a monthly rotation period.
	FileRotationMonthly FileRotationType = "monthly"
)

// CompressionCodec provides, for the same format, the compression of appendlog
// files and the integrity verification of the compressed result.
// VerifyCompressedIntegrity is used both to verify a newly created temporary
// file before it is committed and to decide, after a restart, how to recover a
// published compressed file that coexists with its source. To add a custom
// format, set a codec that implements this contract in the typed config. There
// is no need to add the format to .env.
//
// Format is also used as the single-level extension of compressed files, so it
// returns a canonical ID, such as `br`, that satisfies
// IsValidCompressionFileExtension("."+format). The ID is the persistent
// identity of the format, used to decode existing files even after a restart.
// Decoding compatibility for the same ID must not be lost while the system is
// in operation.
//
// VerifyCompressedIntegrity must not close the input, and must decode it to EOF
// and complete the codec-specific integrity verification, including checksums
// and footers. Files whose names have a `.tmp` suffix are also passed to it
// before a commit, so it verifies the input as this codec's own format
// instead of guessing the format from the file name. Wrap
// ErrCorruptCompressedFile only when decoding, a checksum or a footer
// establishes that the content is
// corrupt. I/O errors, resource shortages, context cancellation,
// initialization failures and errors of unknown cause are not judged to be
// corruption and are returned as they are. One codec instance may be shared
// among the default/error/debug and retryable/permanent Managers, so
// implementations must be safe for concurrent calls to Compress and
// VerifyCompressedIntegrity.
type CompressionCodec interface {
	Format() CompressFormat
	Compress(in, out *os.File) error
	VerifyCompressedIntegrity(in *os.File) error
}

// Extension returns the extension appended to compressed files. For formats
// other than the built-in ones it prefixes the canonical ID with a dot, and it
// returns an empty string for an ID that cannot be used in file names.
func (f CompressFormat) Extension() string {
	switch f {
	case CompressGzip:
		return ExtensionCompressGzip
	case CompressZstd:
		return ExtensionCompressZstd
	case CompressLz4:
		return ExtensionCompressLz4
	case CompressXz:
		return ExtensionCompressXz
	default:
		// 外部Compressorが追加したformatは、そのcanonical IDをfile extensionとして使います。
		// 例: CompressFormat("br") -> ".br"
		extension := "." + strings.TrimPrefix(strings.TrimSpace(string(f)), ".")
		if !IsValidCompressionFileExtension(extension) {
			return ""
		}
		return extension
	}
}

// IsValidCompressionFileExtension reports whether extension is a single-level
// extension that can be appended to compressed file names. `.tmp` is reserved
// for the temporary files of atomic compression, and values that contain
// additional dots or path or glob characters are rejected.
func IsValidCompressionFileExtension(extension string) bool {
	if len(extension) < 2 || extension[0] != '.' || strings.EqualFold(extension, ".tmp") {
		return false
	}
	for _, character := range extension[1:] {
		if unicode.IsLetter(character) || unicode.IsDigit(character) || character == '-' || character == '_' {
			continue
		}
		return false
	}
	return true
}

// ParseFileRotation converts a canonical string to a FileRotationType. It does
// not correct variant spellings, and it returns an error for an unsupported
// value. Falling back to the default value is the responsibility of the config
// loader.
func ParseFileRotation(rotationStr string) (FileRotationType, error) {
	rotation := FileRotationType(rotationStr)
	switch rotation {
	case FileRotationDaily, FileRotationWeekly, FileRotationMonthly:
		return rotation, nil
	default:
		return "", fmt.Errorf("invalid log rotation type: %s", rotationStr)
	}
}

// ErrInvalidFileRotationWeekday reports that the weekday of a weekly rotation
// is outside the valid range of time.Weekday.
var ErrInvalidFileRotationWeekday = errors.New("file rotation weekday must be between Sunday and Saturday")

// ValidateFileRotationWeekday validates that the weekday used by a weekly
// rotation is within the valid range of time.Weekday. Daily, monthly and
// unknown rotations do not use the weekday, so it is not validated for them.
// At run time, an unknown rotation itself is treated as daily.
func ValidateFileRotationWeekday(rotation FileRotationType, dayOfWeek time.Weekday) error {
	if rotation != FileRotationWeekly {
		return nil
	}
	if dayOfWeek < time.Sunday || dayOfWeek > time.Saturday {
		return fmt.Errorf("%w: %d", ErrInvalidFileRotationWeekday, dayOfWeek)
	}
	return nil
}

// String returns the string representation of the FileRotationType.
func (r FileRotationType) String() string {
	return string(r)
}
