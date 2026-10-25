package logconfig

import (
	"errors"
	"strings"

	"github.com/go-jappe/heterolog/internal/config/configerror"
	"github.com/go-jappe/heterolog/pkg/fsstore"
)

// ParseLogCompressFormatはenvironmentで指定できる組み込み圧縮形式を解釈します。
// 独自CompressionCodecはtyped configから注入する契約であり、未知の文字列をgzipへ
// 補正しません。
func ParseLogCompressFormat(value string) (fsstore.CompressFormat, error) {
	format := normalizeCompressFormat(value)

	switch format {
	case "gzip", "gz":
		return fsstore.CompressGzip, nil
	case "zstd", "zst", "zstandard":
		return fsstore.CompressZstd, nil
	case "lz4":
		return fsstore.CompressLz4, nil
	case "xz":
		return fsstore.CompressXz, nil
	default:
		return "", configerror.New(ErrUnsupportedLogCompressFormat, "unsupported log compression format: %q", value)
	}
}

// ErrUnsupportedLogCompressFormatは、ログの圧縮形式が対応していない形式であることを表します。
var ErrUnsupportedLogCompressFormat = errors.New("unsupported log compression format")

func normalizeCompressFormat(format string) string {
	v := strings.TrimSpace(format)
	v = strings.ToLower(v)

	// 先頭の . を除去
	v = strings.TrimPrefix(v, ".")

	// 拡張子付きファイル名が来た場合の吸収
	// 例: "app.log.gz", "archive.tar.zst"
	if idx := strings.LastIndex(v, "."); idx != -1 {
		ext := v[idx+1:]
		if isKnownExt(ext) {
			v = ext
		}
	}

	// ハイフン・アンダースコア除去（z-standard 等の吸収）
	v = strings.ReplaceAll(v, "-", "")
	v = strings.ReplaceAll(v, "_", "")

	return v
}

func isKnownExt(ext string) bool {
	switch ext {
	case "gz", "gzip", "zst", "zstd", "lz4", "xz", "lzma", "lzma2":
		return true
	default:
		return false
	}
}

func ParseLogFileLifecycleMode(value string, defaultMode fsstore.FileLifecycleMode) fsstore.FileLifecycleMode {
	mode := strings.ToLower(strings.TrimSpace(value))
	switch mode {
	case "":
		return defaultMode
	case string(fsstore.FileLifecycleModeDisable), "disabled", "off", "none":
		return fsstore.FileLifecycleModeDisable
	case string(fsstore.FileLifecycleModeInline):
		return fsstore.FileLifecycleModeInline
	case string(fsstore.FileLifecycleModeRunner):
		return fsstore.FileLifecycleModeRunner
	default:
		// 明示された未知値はmapper/受け取り側のvalidationで起動時errorにするため保持します。
		return fsstore.FileLifecycleMode(mode)
	}
}
