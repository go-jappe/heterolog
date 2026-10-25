package fsstore

import "fmt"

// CompressedContentMismatchKindは、完全な圧縮済みfileを展開した内容と元ログの食い違い方です。
type CompressedContentMismatchKind int

const (
	// CompressedContentDiffersは、Offsetのbyteが元ログと異なることを表します。
	CompressedContentDiffers CompressedContentMismatchKind = iota + 1
	// CompressedContentSourceEndsFirstは、元ログがOffsetで終わり、展開内容が続くことを表します。
	CompressedContentSourceEndsFirst
	// CompressedContentDecodedEndsFirstは、展開内容がOffsetで終わり、元ログが続くことを表します。
	CompressedContentDecodedEndsFirst
)

// CompressedContentMismatchErrorは、完全な圧縮済みfileを展開した内容が元ログと一致しない
// 最初の位置を表します。ErrCompressedContentMismatchをwrapするため、errors.Isで不一致を判定し、
// errors.Asで食い違い方と位置を得られます。
type CompressedContentMismatchError struct {
	CompressedPath string
	SourcePath     string
	Kind           CompressedContentMismatchKind
	// Offsetは、先頭から最初に食い違うbyte位置です。片方が先に終わる場合はその終端の位置です。
	Offset int64
}

func (e *CompressedContentMismatchError) Error() string {
	return fmt.Sprintf("%v: compressed=%q source=%q: %s",
		ErrCompressedContentMismatch, e.CompressedPath, e.SourcePath, e.detail())
}

func (e *CompressedContentMismatchError) Unwrap() error {
	return ErrCompressedContentMismatch
}

func (e *CompressedContentMismatchError) detail() string {
	switch e.Kind {
	case CompressedContentSourceEndsFirst:
		return fmt.Sprintf("source ends at byte %d before the decoded content", e.Offset)
	case CompressedContentDecodedEndsFirst:
		return fmt.Sprintf("decoded content ends at byte %d before the source", e.Offset)
	default:
		return fmt.Sprintf("decoded content differs from the source at byte %d", e.Offset)
	}
}
