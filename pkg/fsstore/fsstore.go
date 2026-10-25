package fsstore

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
	"unicode"
)

// ErrCorruptCompressedFileは、codecがdecode、checksum、footer等を検査して
// 圧縮内容の破損を確定できた場合、または非空の元ログに対する圧縮結果が0 byteで
// 元ログを表現できないと確認した場合に返す分類sentinelです。
//
// CompressionCodecの実装は、filesystem/resource error、context cancellation、
// verifier初期化不能、原因を破損と断定できないgeneric errorをこのsentinelで
// wrapしてはいけません。
var ErrCorruptCompressedFile = errors.New("compressed file is corrupt")

// FileRotationTypeは、appendlog file名の期間境界を決めるrotation単位です。
type FileRotationType string

// CompressFormatは、圧縮形式を識別する永続的なIDです。組み込み形式に加えて、
// CompressionCodecが提供する独自形式も表現します。
type CompressFormat string

const (
	// CompressGzipはgzip形式を表します。
	CompressGzip CompressFormat = "gzip"
	// CompressZstdはZstandard形式を表します。
	CompressZstd CompressFormat = "zstd"
	// CompressLz4はLZ4形式を表します。
	CompressLz4 CompressFormat = "lz4"
	// CompressXzはXZ形式を表します。
	CompressXz CompressFormat = "xz"

	// ExtensionCompressGzipはgzip形式の標準的なfile extensionです。
	ExtensionCompressGzip = ".gz"
	// ExtensionCompressZstdはZstandard形式の標準的なfile extensionです。
	ExtensionCompressZstd = ".zst"
	// ExtensionCompressLz4はLZ4形式のfile extensionです。
	ExtensionCompressLz4 = ".lz4"
	// ExtensionCompressXzはXZ形式のfile extensionです。
	ExtensionCompressXz = ".xz"

	// FileNameFormatは通常appendlogの永続的な命名形式です。
	// 引数はprefix、target、rotation期間、indexの順です。
	FileNameFormat = "/%s%s_%s_%d.log"
	// FailureArchiveFileNameFormatはfailure archive JSONLの永続的な命名形式です。
	// 引数の順序はFileNameFormatと同じです。
	FailureArchiveFileNameFormat = "/%s%s_%s_%d.jsonl"

	// FileDateFormatはfile名のrotation期間とarchive checkpoint cursorに使用する
	// YYYYMMDD形式です。変更する場合は既存fileとcheckpointのmigrationが必要です。
	FileDateFormat = "20060102"

	// FileRotationDailyは日単位のrotation期間を表します。
	FileRotationDaily FileRotationType = "daily"
	// FileRotationWeeklyは週単位のrotation期間を表します。
	FileRotationWeekly FileRotationType = "weekly"
	// FileRotationMonthlyは月単位のrotation期間を表します。
	FileRotationMonthly FileRotationType = "monthly"
)

// CompressionCodecは、appendlog fileの圧縮と圧縮結果の完全性検証を、同じ形式について
// 提供します。VerifyCompressedIntegrityは新しく生成した一時fileのcommit前検証と、再起動後に
// sourceと共存する公開済み圧縮fileの回復判定の両方で使用します。独自形式を追加する場合は、
// この契約を実装したcodecをtyped configへ設定します。.envへ形式を追加する必要はありません。
//
// Formatは圧縮fileの一段extensionにも使うため、`br`のような
// IsValidCompressionFileExtension("."+format)を満たすcanonical IDを返します。
// このIDは既存fileを再起動後にもdecodeするための永続的な形式identityです。同じIDの
// decode互換性を運用途中で失わせてはいけません。
//
// VerifyCompressedIntegrityはinputをcloseせず、EOFまでdecodeしてchecksumやfooterを含む
// codec固有の完全性検証を完了しなければなりません。commit前には`.tmp` suffixのpathも
// 渡されるため、file名から形式を推測せず、このcodec自身の形式として検証します。
// decode/checksum/footerにより内容の破損を確定できた場合だけ
// ErrCorruptCompressedFileをwrapしてください。I/O、resource不足、context cancellation、
// 初期化不能、原因不明のerrorは破損と断定せず、そのまま返します。
// 一つのcodec instanceはdefault/error/debugやretryable/permanentのManager間で共有され得るため、
// 実装はCompressとVerifyCompressedIntegrityの並行呼び出しに安全である必要があります。
type CompressionCodec interface {
	Format() CompressFormat
	Compress(in, out *os.File) error
	VerifyCompressedIntegrity(in *os.File) error
}

// Extensionは圧縮済みfileへ付加するextensionを返します。組み込み以外の形式では
// canonical IDの先頭へdotを付け、file名に使用できないIDでは空文字を返します。
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

// IsValidCompressionFileExtensionは、圧縮済みfile名へ付加できる一段extensionかを返します。
// `.tmp`はatomic圧縮の一時file用に予約し、追加dotやpath/glob文字を含む値は拒否します。
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

// ParseFileRotationはcanonicalな文字列をFileRotationTypeへ変換します。表記揺れは
// 補正せず、未対応の値はerrorにします。既定値へのfallbackはconfig loaderの責務です。
func ParseFileRotation(rotationStr string) (FileRotationType, error) {
	rotation := FileRotationType(rotationStr)
	switch rotation {
	case FileRotationDaily, FileRotationWeekly, FileRotationMonthly:
		return rotation, nil
	default:
		return "", fmt.Errorf("invalid log rotation type: %s", rotationStr)
	}
}

// ErrInvalidFileRotationWeekdayは、weekly rotationの曜日がtime.Weekdayの有効範囲外であることを表します。
var ErrInvalidFileRotationWeekday = errors.New("file rotation weekday must be between Sunday and Saturday")

// ValidateFileRotationWeekdayは、weekly rotationで使用する曜日がtime.Weekdayの
// 有効範囲内かを検証します。daily、monthly、未知のrotationでは曜日を使用しないため
// 検証しません。未知のrotation自体は実行側がdailyとして扱います。
func ValidateFileRotationWeekday(rotation FileRotationType, dayOfWeek time.Weekday) error {
	if rotation != FileRotationWeekly {
		return nil
	}
	if dayOfWeek < time.Sunday || dayOfWeek > time.Saturday {
		return fmt.Errorf("%w: %d", ErrInvalidFileRotationWeekday, dayOfWeek)
	}
	return nil
}

// StringはFileRotationTypeの文字列表現を返します。
func (r FileRotationType) String() string {
	return string(r)
}
