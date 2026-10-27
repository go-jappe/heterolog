package failurearchive

import (
	"errors"
	"fmt"
	"os"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
)

// ErrIncompleteJSONLTailは、既存のfailure archiveが改行で終わらず、最後のJSONL recordが
// 途中で切れていることを表します。追記せず、運用者が保全・修復するまで残します。
var ErrIncompleteJSONLTail = errors.New("incomplete JSONL tail")

// 追記用handleを開くときに、最後の1 byteを一度だけ確認します。前のprocessが、
// 追記の境界を復元しないままrecordの途中で停止した場合があります。そのbyte列は
// 黙って捨てたり、次に成功したrecordを未完了のJSONL行へ連結したりせず、
// 運用者が復旧できるよう保全します。
func validateArchiveAppendBoundary(active *os.File) error {
	info, err := active.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		return nil
	}
	reader, openedInfo, err := fsstore.OpenRegularFile(active.Name(), os.O_RDONLY)
	if err != nil {
		return fmt.Errorf("inspect failure archive append boundary: %w", err)
	}
	defer reader.Close()
	if !os.SameFile(info, openedInfo) {
		return errors.New("failure archive path changed while inspecting append boundary")
	}
	var last [1]byte
	if _, err := reader.ReadAt(last[:], info.Size()-1); err != nil {
		return fmt.Errorf("read failure archive append boundary: %w", err)
	}
	if last[0] != '\n' {
		return fmt.Errorf("failure archive %q has an %w; preserve and repair it before appending", active.Name(), ErrIncompleteJSONLTail)
	}
	return nil
}
