package file

import (
	"fmt"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// advancePastRecordedGenerationは、起動時に解決したfile名patternを、checkpointへ
// 送信・退避の完了位置や途中経過として記録済みの世代より後へ進めます。
// 手動復旧で最新fileまで送信・削除した後や、log保存先だけが失われた後の再起動で、
// 記録済みの世代番号へ新しい内容を書かないためです。そうした世代は送信済みと判定され、
// 送信されないまま削除され、object storageの同じobjectも上書きします。
// system clockが記録済みの期間より前へ戻った場合は、その期間を続けます。
// uploadRequiredがtrueの系列では、checkpointを読めない場合にエラーを返します。
// uploadが無効な系列では残存checkpointの不備で書き込みを止めず、読めない記録は使いません。
func advancePastRecordedGeneration(
	fileName *logpattern.FileNamePattern,
	checkpointConfig pkgfsstore.ArchiveCheckpointConfig,
	role loggerType,
	uploadRequired bool,
) error {
	recorded, err := checkpointstore.LatestRecordedGeneration(checkpointConfig, string(role), *fileName)
	if err != nil {
		if uploadRequired {
			return fmt.Errorf("read recorded log file generations: %w", err)
		}
		return nil
	}
	if err := logpattern.AdvancePastRecordedGeneration(fileName, recorded.Period, recorded.Index); err != nil {
		return fmt.Errorf("advance log file past recorded generation: %w", err)
	}
	return nil
}

// roleUploadEnabledは、全体のupload設定とroleの送信先設定の両方が有効かを返します。
func roleUploadEnabled(uploadCfg objectstorage.LogFileUploadConfigProvider, role loggerType) bool {
	return uploadCfg != nil && uploadCfg.Enabled() && uploadTargetConfigForLoggerType(uploadCfg, role).Enabled()
}
