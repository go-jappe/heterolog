package checkpoint

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// RecordedGenerationは、checkpointが送信・退避の完了位置、登録時の対象外、
// 送信途中経過のいずれかとして記録した世代です。Periodが空なら記録はありません。
type RecordedGeneration struct {
	// Periodはファイル名の期間（YYYYMMDD）、Indexはその期間内の世代番号です。
	Period string
	Index  uint64
}

// LatestRecordedGenerationは、cfgの保存先にあるclass・fileNameのログ系列のcheckpoint文書を
// 読み取りだけで開き、記録済みの最も新しい世代を返します。登録済みの全宛先を対象にし、
// 現在の有効宛先に限定しません。
// writerは起動時にこの世代より後から書き込みます。手動復旧で最新fileまで送信・削除した後や、
// log保存先だけが失われた後に、記録済みの世代番号を新しい内容へ再利用しないためです。
// 保存先・source prefixのどちらかが未設定の場合と、保存先・文書が存在しない場合は記録なしです。
// Storeの構築、欠落時の復旧、保存は行いません。読み取りや検証に失敗した場合はエラーを返します。
func LatestRecordedGeneration(
	cfg pkgfsstore.ArchiveCheckpointConfig,
	class string,
	fileName logpattern.FileNamePattern,
) (RecordedGeneration, error) {
	if strings.TrimSpace(cfg.Directory) == "" || strings.TrimSpace(cfg.SourcePrefix) == "" {
		return RecordedGeneration{}, nil
	}
	sourceID := ArchiveCheckpointSourceID(cfg.SourcePrefix, class, fileName)
	if err := ValidateSourceID(sourceID); err != nil {
		return RecordedGeneration{}, err
	}
	directory, err := filepath.Abs(cfg.Directory)
	if err != nil {
		return RecordedGeneration{}, fmt.Errorf("%w: resolve log file upload checkpoint directory: %w", ErrInvalidDirectory, err)
	}
	// Storeと同じ規則で保存先を解決し、同じ文書を読みます。
	directory, err = pathidentity.ResolveAndValidateManagedDirectory(filepath.Dir(directory), filepath.Base(directory))
	if err != nil {
		return RecordedGeneration{}, fmt.Errorf("%w: resolve log file upload checkpoint directory: %w", ErrInvalidDirectory, err)
	}
	path := filepath.Join(directory, sourceID+documentFileSuffix)
	payload, err := readCheckpointFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return RecordedGeneration{}, nil
	}
	if err != nil {
		return RecordedGeneration{}, fmt.Errorf("%w: %w", ErrUnreadableCheckpoint, err)
	}
	document, err := decodeDocument(payload)
	if err == nil {
		err = validateDocument(document, sourceID)
	}
	if err != nil {
		return RecordedGeneration{}, fmt.Errorf("%w: path=%q: %w", ErrCorruptCheckpoint, path, err)
	}
	latest := ""
	consider := func(generation string) {
		if generation != "" && (latest == "" || CompareGenerations(generation, latest) > 0) {
			latest = generation
		}
	}
	// 対象外の範囲は完了位置以前にあるため、完了位置と途中経過だけを比較します。
	for _, cursor := range document.Cursors {
		consider(cursor.Generation)
	}
	for _, progress := range document.UploadProgress {
		consider(progress.Key.Generation)
	}
	if latest == "" {
		return RecordedGeneration{}, nil
	}
	period, index, ok := splitGeneration(latest)
	if !ok {
		// 読み込み時に検証済みのため通常は到達しません。
		return RecordedGeneration{}, fmt.Errorf("%w: path=%q: invalid generation %q", ErrCorruptCheckpoint, path, latest)
	}
	return RecordedGeneration{Period: period, Index: index}, nil
}
