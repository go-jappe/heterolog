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
	"strings"
	"syscall"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/ulikunitz/xz"
)

var (
	// ErrInvalidCompressedFileは、指定されたfileの完全性を確認できなかったことを表します。
	// filesystem I/O等の一時的な検証不能も含むため、破損確定の判定には
	// pkgfsstore.ErrCorruptCompressedFileを使用します。
	ErrInvalidCompressedFile = errors.New("invalid compressed file")
	// ErrUnsupportedCompressionFormatは、組み込みcodecが存在せず、独自codecも
	// 注入されていない圧縮形式を表します。
	ErrUnsupportedCompressionFormat = errors.New("unsupported compression format")
	// ErrCompressedContentMismatchは、圧縮済みfileを最後まで展開でき完全性も確認できたものの、
	// 展開した内容が共存する元ログと一致しないことを表します。圧縮済みfileの破損ではないため、
	// ErrInvalidCompressedFileやpkgfsstore.ErrCorruptCompressedFileはwrapしません。
	// 呼び出し元はこのfileを元ログの圧縮完了の証明にせず、元ログを削除しません。
	ErrCompressedContentMismatch = errors.New("compressed file content does not match its source")
)

// compressedContentComparisonBufferSizeは、展開した内容と元ログを比較する一回分の読み取りbyte数です。
// 比較はこの大きさのbufferで逐次行い、どちらの内容もfile全体をmemoryへ保持しません。
const compressedContentComparisonBufferSize = 128 * 1024

// compressionSourceMismatchNamePatternは、内容の一致しない圧縮済みfileと共存した元ログを
// .preservedへ退避するときのfile名patternです。先頭には元ログのfile名を付けます。
const compressionSourceMismatchNamePattern = ".compressed-content-mismatch-*.preserved"

// OpenFilesForCompressionは圧縮元を読み取り用、圧縮先を新規作成・上書き用に開きます。
// 圧縮元がリンクではない通常ファイルであることを確認してから、圧縮先を作成します。
// Unixでは元ファイルのpermission bitsより出力を広げず、umaskや既存出力の追加制限も
// 維持します。Windowsでは既存のprivate-file DACL方針を適用します。ACLの複製は行いません。
// 権限設定は既存内容の切り詰め・圧縮データの書き込みより前に完了します。
func OpenFilesForCompression(srcPath, dstPath string) (*os.File, *os.File, error) {
	inFile, sourceInfo, err := OpenRegularFile(srcPath, os.O_RDONLY)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file %v: %w", srcPath, err)
	}

	// New files must start with restricted mode bits. Existing files are opened
	// without truncation so they can be inspected and restricted before mutation.
	outFile, err := os.OpenFile(dstPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, sourceInfo.Mode().Perm())
	if errors.Is(err, os.ErrExist) {
		outFile, _, err = OpenRegularFile(dstPath, os.O_WRONLY)
	}
	if err != nil {
		return nil, nil, errors.Join(fmt.Errorf("failed to open compression output %v: %w", dstPath, err), inFile.Close())
	}
	outputInfo, err := outFile.Stat()
	if err == nil && os.SameFile(sourceInfo, outputInfo) {
		err = fmt.Errorf("compression output %q refers to the source file", dstPath)
	}
	if err == nil {
		err = SetPrivateFilePermissions(outFile, sourceInfo.Mode().Perm()&outputInfo.Mode().Perm())
	}
	if err == nil {
		err = outFile.Truncate(0)
	}
	if err != nil {
		return nil, nil, errors.Join(fmt.Errorf("prepare compression output %v: %w", dstPath, err), outFile.Close(), inFile.Close())
	}

	return inFile, outFile, nil
}

// 圧縮 gzip
func CompressGzip(in *os.File, out *os.File) (err error) {
	gzWriter := gzip.NewWriter(out)
	defer func() {
		cerr := gzWriter.Close()
		if err == nil && cerr != nil {
			err = fmt.Errorf("gzip close failed: %w", cerr)
		}
	}()

	if _, err = io.Copy(gzWriter, in); err != nil {
		return fmt.Errorf("copy failed during gzip compression: %w", err)
	}
	return nil
}

// 圧縮 zstd
func CompressZstd(in *os.File, out *os.File) (err error) {
	enc, err := zstd.NewWriter(out)
	if err != nil {
		return fmt.Errorf("failed to create zstd writer: %w", err)
	}

	defer func() {
		cerr := enc.Close()
		if err == nil && cerr != nil {
			err = fmt.Errorf("zstd close failed: %w", cerr)
		}
	}()

	if _, err = io.Copy(enc, in); err != nil {
		return fmt.Errorf("copy failed during zstd compression: %w", err)
	}

	return nil
}

// 圧縮 lz4
func CompressLz4(in *os.File, out *os.File) (err error) {
	lzWriter := lz4.NewWriter(out)

	defer func() {
		cerr := lzWriter.Close()
		if err == nil && cerr != nil {
			err = fmt.Errorf("lz4 close failed: %w", cerr)
		}
	}()

	if _, err = io.Copy(lzWriter, in); err != nil {
		return fmt.Errorf("copy failed during lz4 compression: %w", err)
	}

	return nil
}

// 圧縮 xz
func CompressXz(in *os.File, out *os.File) (err error) {
	xzWriter, err := xz.NewWriter(out)
	if err != nil {
		return fmt.Errorf("failed to create xz writer: %w", err)
	}

	defer func() {
		cerr := xzWriter.Close()
		if err == nil && cerr != nil {
			err = fmt.Errorf("xz close failed: %w", cerr)
		}
	}()

	if _, err = io.Copy(xzWriter, in); err != nil {
		return fmt.Errorf("copy failed during xz compression: %w", err)
	}

	return nil
}

// builtInCompressionCodecは、ResolveCompressionCodecが一度だけ選択した
// 組み込み圧縮形式と、対応する圧縮・展開処理を保持します。
// 各methodはformatを再判定せず、解決済みの処理を実行します。
// VerifyCompressedFileMatchesSourceは、この型のcodecだけを展開して元ログと比較します。
type builtInCompressionCodec struct {
	format   pkgfsstore.CompressFormat
	compress func(*os.File, *os.File) error
	// decodeは、inの現在位置から圧縮streamをEOFまで展開した内容をoutへ渡し、
	// checksumやfooterを含む形式固有の完全性検証も同じ走査で完了します。
	decode func(in *os.File, out io.Writer) error
}

func (c builtInCompressionCodec) Format() pkgfsstore.CompressFormat {
	return c.format
}

func (c builtInCompressionCodec) Compress(in, out *os.File) error {
	if c.compress == nil {
		return fmt.Errorf(
			"%w: built-in format %q has no compression implementation",
			ErrUnsupportedCompressionFormat,
			c.format,
		)
	}
	return c.compress(in, out)
}

func (c builtInCompressionCodec) VerifyCompressedIntegrity(in *os.File) error {
	return c.decodeTo(in, io.Discard)
}

// decodeToは、展開した内容をoutへ渡しながら完全性を検証します。
// outのWrite errorもdecoderのerrorとして返るため、outで内容を比較する呼び出し元は、
// out側で記録した読み取りerrorをこの戻り値より先に確認してください。
func (c builtInCompressionCodec) decodeTo(in *os.File, out io.Writer) error {
	if c.decode == nil {
		return fmt.Errorf(
			"%w: built-in format %q has no integrity verification implementation",
			ErrUnsupportedCompressionFormat,
			c.format,
		)
	}
	err := c.decode(in, out)
	if err == nil || isOperationalCompressedFileVerificationError(err) {
		return err
	}
	// 組み込みcodecのdecoderが返したformat/checksum/footer errorだけは、
	// 実装を所有するこの境界で内容破損と確定できます。独自codecのerrorは
	// ResolveCompressionCodecから直接返るため、ここで推測分類されません。
	return fmt.Errorf("%w: %w", pkgfsstore.ErrCorruptCompressedFile, err)
}

func copyDecodedStream(
	format pkgfsstore.CompressFormat,
	reader io.Reader,
	closeReader func() error,
	out io.Writer,
) error {
	_, readErr := io.Copy(out, reader)
	var closeErr error
	if closeReader != nil {
		closeErr = closeReader()
	}
	if readErr != nil {
		return fmt.Errorf("decode %s: %w", format, readErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close %s reader: %w", format, closeErr)
	}
	return nil
}

func decodeGzipCompressed(in *os.File, out io.Writer) error {
	reader, err := gzip.NewReader(in)
	if err != nil {
		return fmt.Errorf("initialize gzip reader: %w", err)
	}
	return copyDecodedStream(pkgfsstore.CompressGzip, reader, reader.Close, out)
}

func decodeZstdCompressed(in *os.File, out io.Writer) error {
	reader, err := zstd.NewReader(in)
	if err != nil {
		return fmt.Errorf("initialize zstd reader: %w", err)
	}
	return copyDecodedStream(pkgfsstore.CompressZstd, reader, func() error {
		reader.Close()
		return nil
	}, out)
}

func decodeLz4Compressed(in *os.File, out io.Writer) error {
	return copyDecodedStream(pkgfsstore.CompressLz4, lz4.NewReader(in), nil, out)
}

func decodeXzCompressed(in *os.File, out io.Writer) error {
	if err := verifyXZFraming(in); err != nil {
		return fmt.Errorf("verify xz framing: %w", err)
	}
	reader, err := xz.NewReader(in)
	if err != nil {
		return fmt.Errorf("initialize xz reader: %w", err)
	}
	return copyDecodedStream(pkgfsstore.CompressXz, reader, nil, out)
}

// ResolveCompressionCodecは独自codecが指定されていればそれを返し、nilならformatに
// 対応する組み込みcodecを返します。独自codecのFormatは設定上のformatより優先され、
// 圧縮path、圧縮処理、復旧時検証のすべてで同じidentityを使用します。
func ResolveCompressionCodec(
	format pkgfsstore.CompressFormat,
	custom pkgfsstore.CompressionCodec,
) (pkgfsstore.CompressionCodec, error) {
	if custom != nil {
		if extension := custom.Format().Extension(); extension == "" {
			return nil, fmt.Errorf(
				"%w: custom codec format %q has no valid file extension",
				ErrUnsupportedCompressionFormat,
				custom.Format(),
			)
		}
		return custom, nil
	}

	switch format {
	case pkgfsstore.CompressGzip:
		return builtInCompressionCodec{format: format, compress: CompressGzip, decode: decodeGzipCompressed}, nil
	case pkgfsstore.CompressZstd:
		return builtInCompressionCodec{format: format, compress: CompressZstd, decode: decodeZstdCompressed}, nil
	case pkgfsstore.CompressLz4:
		return builtInCompressionCodec{format: format, compress: CompressLz4, decode: decodeLz4Compressed}, nil
	case pkgfsstore.CompressXz:
		return builtInCompressionCodec{format: format, compress: CompressXz, decode: decodeXzCompressed}, nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedCompressionFormat, format)
	}
}

// ResolveExistingCompressionCodecは、既存の圧縮済みfileを検証するcodecを、そのfileの
// extensionから選びます。現在の独自codecとextensionが一致すれば独自codecを優先し、
// 組み込み形式のextensionなら設定変更前の形式でも組み込みcodecを返します。
// extensionの大小文字は、pathの保存先directoryの規則（pathidentity.DirectoryNameCase）で
// 比較し、file pathやobject keyの表記は変更しません。対応するcodecがない形式は、
// 検証を省略せずErrUnsupportedCompressionFormatを返します。
func ResolveExistingCompressionCodec(
	path string,
	custom pkgfsstore.CompressionCodec,
) (pkgfsstore.CompressionCodec, error) {
	codec, supported, err := ResolveCompressionCodecForExtension(
		filepath.Ext(path),
		custom,
		pathidentity.DirectoryNameCase(filepath.Dir(path)),
	)
	if err != nil {
		return nil, err
	}
	if !supported {
		return nil, fmt.Errorf("%w: cannot verify existing compressed file %q", ErrUnsupportedCompressionFormat, path)
	}
	return codec, nil
}

// ResolveCompressionCodecForExtensionは、判定済みの規則nameCaseでextensionを比較し、
// ResolveExistingCompressionCodecと同じ規則で既存の圧縮済みfileを検証するcodecを選びます。
// 大小文字を区別しないdirectoryで受理する大小文字違いも、既知codecの検証を省略しません。
// 対応するcodecがない形式ではsupported=falseを返します。
func ResolveCompressionCodecForExtension(
	extension string,
	custom pkgfsstore.CompressionCodec,
	nameCase pathidentity.NameCase,
) (codec pkgfsstore.CompressionCodec, supported bool, err error) {
	extension = nameCase.Key(extension)
	if custom != nil && nameCase.Key(custom.Format().Extension()) == extension {
		return custom, true, nil
	}
	var format pkgfsstore.CompressFormat
	switch extension {
	case pkgfsstore.ExtensionCompressGzip:
		format = pkgfsstore.CompressGzip
	case pkgfsstore.ExtensionCompressZstd:
		format = pkgfsstore.CompressZstd
	case pkgfsstore.ExtensionCompressLz4:
		format = pkgfsstore.CompressLz4
	case pkgfsstore.ExtensionCompressXz:
		format = pkgfsstore.CompressXz
	default:
		return nil, false, nil
	}
	codec, err = ResolveCompressionCodec(format, nil)
	if err != nil {
		return nil, false, err
	}
	return codec, true, nil
}

// 圧縮ファイル名を作る
func GenerateCompressedPath(filePath string, format pkgfsstore.CompressFormat) string {
	extension := format.Extension()
	if extension == "" {
		return ""
	}
	if strings.HasSuffix(filePath, extension) {
		return filePath
	}
	return filePath + extension
}

// VerifyCompressedFileIntegrityは、pathを指定codecでEOFまでdecodeできることを確認します。
// 新しい圧縮結果をcommitする前などに、壊れたfileを完了済みの圧縮結果と誤認しないために使用します。
// final path自体が通常fileであることも必須とし、symlinkの参照先がdecodeできても
// 圧縮完了の証明にはしません。元ログとの内容一致は確認しないため、appendlogのsourceと
// 圧縮済みfileが共存する回復経路では、VerifyCompressedFileMatchesSourceで内容も確認します。
func VerifyCompressedFileIntegrity(path string, codec pkgfsstore.CompressionCodec) error {
	if codec == nil {
		return fmt.Errorf("%w: compression codec is nil for %s", ErrInvalidCompressedFile, path)
	}
	if _, err := InspectRegularFile(path); err != nil {
		return fmt.Errorf("%w: inspect %s: %w", ErrInvalidCompressedFile, path, err)
	}
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("%w: open %s: %w", ErrInvalidCompressedFile, path, err)
	}
	defer file.Close()

	if err := codec.VerifyCompressedIntegrity(file); err != nil {
		return fmt.Errorf(
			"%w: verify integrity of %s as %s: %w",
			ErrInvalidCompressedFile,
			path,
			codec.Format(),
			err,
		)
	}
	return nil
}

// VerifyExistingCompressedFileMatchesSourceは、元ログsourcePathと共存する既存の
// 圧縮済みfileを、ResolveExistingCompressionCodecで選んだそのfile自身のcodecで検証し、
// 組み込みcodecでは展開した内容がsourcePathと一致することも同じ走査で確認します。
// 結果の分類はVerifyCompressedFileMatchesSourceと同じです。
func VerifyExistingCompressedFileMatchesSource(
	sourcePath string,
	compressedPath string,
	custom pkgfsstore.CompressionCodec,
) error {
	codec, err := ResolveExistingCompressionCodec(compressedPath, custom)
	if err != nil {
		return err
	}
	return VerifyCompressedFileMatchesSource(compressedPath, codec, sourcePath)
}

// VerifyCompressedFileMatchesSourceは、pathをcodecでEOFまで展開して完全性を確認し、
// 同じ走査で展開した内容がsourcePathの内容と一致することも確認します。元ログと共存する
// 圧縮済みfileを根拠に元ログを削除・退避する前に使用し、完全でも別の内容を持つfileを
// 圧縮完了の証明にしません。比較は固定長のbufferで逐次行います。
//
// 展開できない、またはchecksum等が一致しない場合はVerifyCompressedFileIntegrityと同じ
// errorを返します。内容の不一致より破損の判定を優先するため、不一致を見つけても最後まで
// 展開します。完全なstreamの内容が異なる場合だけErrCompressedContentMismatchをwrapした
// *CompressedContentMismatchErrorを返し、最初の不一致の位置を示します。
// sourcePathが存在しなければ比較対象がないため、完全性だけを確認します。sourcePathが
// 通常fileでない、読み取れない、比較中に変更された場合は、破損とも不一致とも判定せずに
// errorを返します。内容を比較するのは、このpackageが展開処理を持つ組み込みcodecだけです。
// 独自codecは、展開に使えるmethodを持っていても内容を比較せず、従来どおりcodec自身の
// 完全性検証だけを行います。
func VerifyCompressedFileMatchesSource(path string, codec pkgfsstore.CompressionCodec, sourcePath string) error {
	builtIn, decodable := codec.(builtInCompressionCodec)
	if !decodable {
		return VerifyCompressedFileIntegrity(path, codec)
	}
	if _, err := InspectRegularFile(path); err != nil {
		return fmt.Errorf("%w: inspect %s: %w", ErrInvalidCompressedFile, path, err)
	}
	source, sourceInfo, err := OpenRegularFile(sourcePath, os.O_RDONLY)
	if errors.Is(err, os.ErrNotExist) {
		return VerifyCompressedFileIntegrity(path, codec)
	}
	if err != nil {
		return fmt.Errorf("open compression source %s to compare with %s: %w", sourcePath, path, err)
	}
	defer source.Close()
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("%w: open %s: %w", ErrInvalidCompressedFile, path, err)
	}
	defer file.Close()

	comparison := decodedContentComparison{
		source: source,
		buffer: make([]byte, compressedContentComparisonBufferSize),
	}
	decodeErr := builtIn.decodeTo(file, &comparison)
	// 元ログの読み取り失敗はdecoderのerrorとしても返るため、破損と分類する前に区別します。
	if comparison.readErr != nil {
		return fmt.Errorf("read compression source %s to compare with %s: %w", sourcePath, path, comparison.readErr)
	}
	if decodeErr != nil {
		return fmt.Errorf(
			"%w: verify integrity of %s as %s: %w",
			ErrInvalidCompressedFile,
			path,
			codec.Format(),
			decodeErr,
		)
	}
	if err := comparison.finish(); err != nil {
		return fmt.Errorf("read compression source %s to compare with %s: %w", sourcePath, path, err)
	}
	after, err := source.Stat()
	if err != nil {
		return fmt.Errorf("inspect compression source %s after comparison: %w", sourcePath, err)
	}
	if after.Size() != sourceInfo.Size() || !after.ModTime().Equal(sourceInfo.ModTime()) {
		return fmt.Errorf("compression source %s changed while comparing with %s", sourcePath, path)
	}
	if comparison.mismatch != nil {
		mismatch := *comparison.mismatch
		mismatch.CompressedPath, mismatch.SourcePath = path, sourcePath
		return &mismatch
	}
	return nil
}

// decodedContentComparisonは、decoderが書き出す展開内容を元ログと先頭から順に比較します。
// 不一致を見つけた後も展開は止めず、残りを読み捨てて圧縮streamの完全性検証を完了させます。
// 元ログの読み取りに失敗した場合だけWrite errorを返して展開を中止します。
type decodedContentComparison struct {
	source io.Reader
	buffer []byte
	// comparedは、展開内容と一致を確認した先頭からのbyte数です。
	compared int64
	// mismatchは最初に見つけた不一致です。nilなら、比較済みの範囲は一致しています。
	mismatch *CompressedContentMismatchError
	// readErrは元ログの読み取りerrorです。設定後は比較結果を判定に使いません。
	readErr error
}

func (c *decodedContentComparison) Write(decoded []byte) (int, error) {
	if c.readErr != nil {
		return 0, c.readErr
	}
	written := len(decoded)
	for c.mismatch == nil && len(decoded) > 0 {
		chunk := decoded[:min(len(decoded), len(c.buffer))]
		read, err := io.ReadFull(c.source, c.buffer[:len(chunk)])
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			c.readErr = err
			return 0, err
		}
		if index := firstDifferentByte(c.buffer[:read], chunk[:read]); index >= 0 {
			c.mismatch = &CompressedContentMismatchError{Kind: CompressedContentDiffers, Offset: c.compared + int64(index)}
			break
		}
		c.compared += int64(read)
		if read < len(chunk) {
			c.mismatch = &CompressedContentMismatchError{Kind: CompressedContentSourceEndsFirst, Offset: c.compared}
			break
		}
		decoded = decoded[read:]
	}
	return written, nil
}

// finishは、展開内容をすべて比較した後に、元ログ側に比較されていない内容が残っていないことを確認します。
// 残っていれば不一致として記録し、読み取りerrorだけを返します。
func (c *decodedContentComparison) finish() error {
	if c.readErr != nil {
		return c.readErr
	}
	if c.mismatch != nil {
		return nil
	}
	var next [1]byte
	read, err := io.ReadFull(c.source, next[:])
	if read > 0 {
		c.mismatch = &CompressedContentMismatchError{Kind: CompressedContentDecodedEndsFirst, Offset: c.compared}
		return nil
	}
	if err != nil && !errors.Is(err, io.EOF) {
		c.readErr = err
		return err
	}
	return nil
}

// firstDifferentByteは、同じ長さのleftとrightで最初に異なるbyteの位置を返します。
// 全て一致すれば-1を返します。
func firstDifferentByte(left, right []byte) int {
	if bytes.Equal(left, right) {
		return -1
	}
	for index := range left {
		if left[index] != right[index] {
			return index
		}
	}
	return -1
}

// PreserveCompressionSourceWithMismatchedPeerは、内容の一致しない圧縮済みfileと共存する
// 元ログを、同じログdirectoryの.preservedへrenameで退避します。
// CreateTempで専用の空fileを排他的に確保してから置き換え、過去の保全fileは上書きしません。
// 保全先は通常の圧縮・upload・purge・削除待ち回復の対象外で、管理者の確認へ残します。
// 元ログの内容は変更せず、圧縮済みfileは操作しません。退避後は圧縮済みfileだけが残り、
// その世代の通常の圧縮済みfileとして扱われます。
// 戻り値のpathはrename成功後だけ設定し、その後のdirectory同期に失敗した場合も返します。
// 呼び出し元は元ログと圧縮済みfileを保護し、同じdirectoryのlifecycle lockを保持してください。
func PreserveCompressionSourceWithMismatchedPeer(sourcePath string) (string, error) {
	if _, err := InspectRegularFile(sourcePath); err != nil {
		return "", fmt.Errorf("inspect compression source with mismatched compressed peer: %w", err)
	}
	directory := filepath.Join(filepath.Dir(sourcePath), PreservedDirectoryName)
	if err := EnsureDirectoryNoSymlinks(directory, 0o750); err != nil {
		return "", fmt.Errorf("prepare compression source preservation directory: %w", err)
	}
	reserved, err := os.CreateTemp(directory, filepath.Base(sourcePath)+compressionSourceMismatchNamePattern)
	if err != nil {
		return "", fmt.Errorf("reserve compression source preservation path: %w", err)
	}
	preservedPath := reserved.Name()
	renamed := false
	defer func() {
		if !renamed {
			// この呼出しが確保した空fileだけを片付け、元ログは削除しません。
			_ = os.Remove(preservedPath)
		}
	}()
	if err := reserved.Close(); err != nil {
		return "", fmt.Errorf("close reserved compression source preservation file: %w", err)
	}
	if err := os.Rename(sourcePath, preservedPath); err != nil {
		return "", fmt.Errorf("preserve compression source with mismatched compressed peer: %w", err)
	}
	renamed = true
	var errs []error
	for _, changedDirectory := range []string{directory, filepath.Dir(sourcePath)} {
		if err := SyncDirectory(changedDirectory); err != nil {
			errs = append(errs, fmt.Errorf("sync compression source preservation directory %q: %w", changedDirectory, err))
		}
	}
	return preservedPath, errors.Join(errs...)
}

// isOperationalCompressedFileVerificationError keeps filesystem/resource
// failures out of the corruption classification. Decoder format/checksum
// errors do not normally carry an OS error in their chain.
func isOperationalCompressedFileVerificationError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrNotExist) ||
		errors.Is(err, os.ErrPermission) ||
		errors.Is(err, os.ErrClosed) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrUnsupportedCompressionFormat) {
		return true
	}
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return true
	}
	var linkErr *os.LinkError
	if errors.As(err, &linkErr) {
		return true
	}
	var syscallErr *os.SyscallError
	if errors.As(err, &syscallErr) {
		return true
	}
	var errno syscall.Errno
	return errors.As(err, &errno)
}
