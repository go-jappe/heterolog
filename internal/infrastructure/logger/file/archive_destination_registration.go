package file

import (
	"context"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	uploadercompose "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/uploader/compose"
	s3uploaderbuilder "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/uploader/s3"
	"github.com/go-jappe/heterolog/pkg/clock"
)

// archiveDestinationCursorsは、起動時の宛先登録で取得したログ系列ごとの完了cursorを保持します。
// FileLogger未生成のログ系列について、共有ディレクトリのpurge時に送信待ちの世代を判定します。
// 保持するcursorの数は、各ログ系列に登録された宛先数で決まります。
type archiveDestinationCursors map[loggerType][]checkpointstore.DestinationCursor

// archiveGenerationPendingは、FileLogger未生成のログ系列では初期cursorを使い、
// 生成済みのログ系列では現在のStoreの完了情報を確認します。送信・退避が完了した
// 未圧縮ログも保存先に残るため、共有ディレクトリのpurge時にこの判定を使います。
func (s *LoggerSet) archiveGenerationPending(role loggerType, key checkpointstore.CheckpointKey) bool {
	s.mu.Lock()
	if s.closeState != common.LoggerCloseStateOpen {
		s.mu.Unlock()
		return true
	}
	logger := s.loggers[role]
	cursors, registered := s.archiveDestinationCursors[role]
	s.mu.Unlock()
	if !registered {
		return true
	}
	if logger != nil && logger.archiveUploadCheckpoints != nil {
		// peerのCloseと競合しても、Storeを閉じる前にこの読取が完了します。
		if !logger.beginBackgroundOperation() {
			return true
		}
		defer logger.endBackgroundOperation()
		checkpoint, err := logger.archiveUploadCheckpoints.Load(context.Background(), key)
		if err != nil {
			return true
		}
		for _, cursor := range cursors {
			if !checkpoint.DestinationComplete(cursor.DestinationID) {
				return true
			}
		}
		return false
	}
	for _, cursor := range cursors {
		if !cursor.Covers(key) {
			return true
		}
	}
	return false
}

// registerLoggerSetArchiveDestinationsは、FileLogger未生成のログ系列の未圧縮ログが
// 別系列の処理で圧縮される前に、各ログ系列の初期完了cursorを確定します。
// ログ保存先ディレクトリ・ログファイル・送信worker・FileLoggerは作成せず、
// 呼び出し元が準備済みのcheckpoint保存先だけを更新します。
func registerLoggerSetArchiveDestinations(
	cfg FileLoggerConfigProvider,
	directories loggerDirectoryPaths,
) (archiveDestinationCursors, error) {
	if !usesInlineLifecycle(cfg.LifecycleMode()) || !cfg.ObjectStorageUpload().Enabled() {
		return nil, nil
	}
	layout := cfg.Fsstore().Layout()
	// 未生成のroleも、writerの既定のUTC時計と同じ規則で現在のrotation期間を登録境界にします。
	activePeriod, err := logpattern.ResolveDateStringForFileNameAt(
		appendlog.NewNameLogPatternConfigAdapter(cfg.Fsstore()),
		clock.NewUTCClock().Now(),
	)
	if err != nil {
		return nil, fmt.Errorf("resolve archive destination registration period: %w", err)
	}
	roles := []loggerType{DefaultLogger}
	if layout.IsSeparateErrorLogsEnabled() {
		roles = append(roles, ErrorLogger)
	}
	if layout.IsSeparateDebugLogsEnabled() {
		roles = append(roles, DebugLogger)
	}
	cursors := make(archiveDestinationCursors)
	for _, role := range roles {
		targetCfg := uploadTargetConfigForLoggerType(cfg.ObjectStorageUpload(), role)
		if !targetCfg.Enabled() {
			continue
		}
		pattern := logpattern.FileNamePattern{
			Dir: directories[role], Target: layout.Target(), Date: activePeriod, Extension: LogFileExtension,
		}
		switch role {
		case ErrorLogger:
			pattern.Prefix = layout.SeparateErrorLogsPrefix()
		case DebugLogger:
			pattern.Prefix = layout.SeparateDebugLogsPrefix()
		}
		destinationIDs, err := uploadercompose.DestinationIDsFromTargets(targetCfg, s3uploaderbuilder.DestinationIDResolvers())
		if err != nil {
			return nil, fmt.Errorf("create %s archive destination registration: %w", role, err)
		}
		// 起動時の宛先登録だけに使ってすぐ閉じるStoreです。失敗は戻り値で返します。
		store, err := newArchiveCompressedLogCheckpointStore(cfg.ArchiveCheckpoint(), role, pattern, true, destinationIDs, nil)
		if err != nil {
			return nil, fmt.Errorf("register %s archive destinations: %w", role, err)
		}
		if store == nil {
			continue
		}
		registered, err := store.DestinationCursors(context.Background())
		closeErr := store.Close()
		if err != nil {
			return nil, fmt.Errorf("snapshot %s archive destinations: %w", role, err)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close %s archive destinations: %w", role, closeErr)
		}
		cursors[role] = nil
		for _, cursor := range registered {
			for _, destinationID := range destinationIDs {
				if cursor.DestinationID == destinationID {
					cursors[role] = append(cursors[role], cursor)
					break
				}
			}
		}
	}
	return cursors, nil
}
