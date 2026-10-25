package checkpoint

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

var (
	ErrInvalidSourceID      = errors.New("log file upload checkpoint source ID is invalid")
	ErrInvalidGroupKey      = errors.New("log file upload checkpoint group key is invalid")
	ErrInvalidDestinationID = errors.New("log file upload checkpoint destination ID is invalid")
)

// CheckpointKeyは、一つの世代の未圧縮・圧縮ファイルを区別します。
// Generationには期間と世代番号を渡し、圧縮済みのファイルではCompressedをtrueにします。
// Storeへの入力とJSON読み込み時に形式を検証します。空白の補正は行いません。
type CheckpointKey struct {
	// Generationは「期間-世代番号」です。例: app_20260915_3.log、
	// app_20260915_3.log.gzはいずれも"20260915-3"です。
	Generation string `json:"generation"`
	Compressed bool   `json:"compressed"`
}

// validateCheckpointGenerationは入口で世代キーを検証します。日付は命名規則と同じ
// YYYYMMDD、番号は先頭ゼロのないuint64です（0自体は許可）。入力値は変更しません。
func validateCheckpointGeneration(generation string) error {
	period, indexText, found := strings.Cut(generation, "-")
	if !found || len(period) != len(pkgfsstore.FileDateFormat) {
		return fmt.Errorf("%w: generation %q must be YYYYMMDD-index", ErrInvalidGroupKey, generation)
	}
	if _, err := time.Parse(pkgfsstore.FileDateFormat, period); err != nil {
		return fmt.Errorf("%w: generation %q has an invalid date: %w", ErrInvalidGroupKey, generation, err)
	}
	index, err := strconv.ParseUint(indexText, 10, 64)
	if err != nil || strconv.FormatUint(index, 10) != indexText {
		return fmt.Errorf("%w: generation %q must have a decimal uint64 index without leading zeros", ErrInvalidGroupKey, generation)
	}
	return nil
}

// UnmarshalJSONは形式の欠落を未圧縮ファイルと誤認しないよう、両fieldを必須にします。
// 呼び出し元のdecoderでは検証できない、このobject内の未知fieldも拒否します。
func (key *CheckpointKey) UnmarshalJSON(payload []byte) error {
	var decoded struct {
		Generation *string `json:"generation"`
		Compressed *bool   `json:"compressed"`
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	if decoded.Generation == nil || decoded.Compressed == nil {
		return errors.New("checkpoint key generation and compressed fields are required and must not be null")
	}
	if err := validateCheckpointGeneration(*decoded.Generation); err != nil {
		return err
	}
	*key = CheckpointKey{Generation: *decoded.Generation, Compressed: *decoded.Compressed}
	return nil
}

// CompareGenerationsはファイル名の期間、数値の世代番号の順に比較します。
// 入力は生成・受け入れ時に検証済みの世代キーを渡します。ここでは再検証しません。
func CompareGenerations(left, right string) int {
	leftPeriod, leftIndex, leftOK := splitGeneration(left)
	rightPeriod, rightIndex, rightOK := splitGeneration(right)
	if leftOK && rightOK {
		if order := cmp.Compare(leftPeriod, rightPeriod); order != 0 {
			return order
		}
		return cmp.Compare(leftIndex, rightIndex)
	}
	if leftOK != rightOK {
		if leftOK {
			return -1
		}
		return 1
	}
	return cmp.Compare(left, right)
}

func splitGeneration(value string) (string, uint64, bool) {
	separator := strings.IndexByte(value, '-')
	if separator <= 0 {
		return "", 0, false
	}
	index, err := strconv.ParseUint(value[separator+1:], 10, 64)
	return value[:separator], index, err == nil
}

// compareCheckpointKeysは世代、未圧縮・圧縮の順で照合の順序を揃えます。
func compareCheckpointKeys(left, right CheckpointKey) int {
	if order := CompareGenerations(left.Generation, right.Generation); order != 0 {
		return order
	}
	if left.Compressed == right.Compressed {
		return 0
	}
	if left.Compressed {
		return 1
	}
	return -1
}

// Checkpointは指定世代について、これ以上のuploadが不要な宛先を返すsnapshotです。
// 送信・退避が完了した宛先と、宛先の登録前から存在したため対象外とした宛先を区別します。
// 未圧縮・圧縮は同じ世代のデータであり、一方が完了したら他方も再送しません。
type Checkpoint struct {
	// Keyは照会対象のファイル名の期間・世代番号と、物理ファイルの圧縮状態です。
	// 例: app_20260915_3.log.gzならGeneration="20260915-3", Compressed=trueです。
	// Compressedは呼び出し元のファイル操作用で、完了判定には使用しません。
	Key CheckpointKey `json:"key"`
	// CompletedDestinationIDsは初回対象外、upload成功、または全指定退避先への
	// 保存成功により、この世代のuploadが不要な宛先です。送信要否はこの一覧で判断します。
	// spoolからの再送結果はここへ書き戻しません。
	CompletedDestinationIDs []objectstorage.DestinationID `json:"completed_destination_ids"`
	// OutOfScopeDestinationIDsはCompletedDestinationIDsのうち、宛先の登録前から存在したため
	// 送信対象外とした宛先です。送信・退避はしていないため、元ファイルを削除する根拠にしません。
	// 登録時に対象外の世代がなかった宛先と、この区別がない旧形式のcursorの宛先は含みません。
	OutOfScopeDestinationIDs []objectstorage.DestinationID `json:"out_of_scope_destination_ids,omitempty"`
}

// PendingDestinationIDsは現在有効な宛先から完了済みを除き、重複を取り除いて設定順で返します。
func (c Checkpoint) PendingDestinationIDs(active []objectstorage.DestinationID) []objectstorage.DestinationID {
	completed := destinationSet(c.CompletedDestinationIDs)
	pending := make([]objectstorage.DestinationID, 0, len(active))
	for _, destination := range active {
		if _, done := completed[destination]; !done {
			pending = append(pending, destination)
			completed[destination] = struct{}{}
		}
	}
	return pending
}

// Completeは有効な宛先が一つ以上あり、そのすべてでuploadが不要かを返します。
// 登録前から存在した対象外の宛先も完了に含めます。元ファイルを削除できるかは、
// DeliveredToAnyで実際に送信・退避した宛先があることも確認します。
func (c Checkpoint) Complete(active []objectstorage.DestinationID) bool {
	if len(active) == 0 {
		return false
	}
	completed := destinationSet(c.CompletedDestinationIDs)
	for _, destination := range active {
		if _, done := completed[destination]; !done {
			return false
		}
	}
	return true
}

// DestinationCompleteはdestinationに対する、この世代のuploadが不要かを返します。
func (c Checkpoint) DestinationComplete(destination objectstorage.DestinationID) bool {
	return slices.Contains(c.CompletedDestinationIDs, destination)
}

// DestinationDeliveredはdestinationへ、この世代の送信または全指定退避先への保存が完了したかを返します。
// 登録前から存在したため対象外とした宛先は、uploadが不要でもfalseです。
func (c Checkpoint) DestinationDelivered(destination objectstorage.DestinationID) bool {
	return c.DestinationComplete(destination) && !slices.Contains(c.OutOfScopeDestinationIDs, destination)
}

// DeliveredToAnyはactiveの少なくとも一つの宛先へ、この世代を送信・退避したかを返します。
// 元ファイルはCompleteとこの結果の両方がtrueの場合だけ削除できます。登録前から存在した
// 対象外の宛先だけで完了した世代はどこにも送っていないため、通常の保持・purgeへ任せます。
func (c Checkpoint) DeliveredToAny(active []objectstorage.DestinationID) bool {
	completed := destinationSet(c.CompletedDestinationIDs)
	outOfScope := destinationSet(c.OutOfScopeDestinationIDs)
	for _, destination := range active {
		_, done := completed[destination]
		_, skipped := outOfScope[destination]
		if done && !skipped {
			return true
		}
	}
	return false
}

// DestinationCursorは一つの宛先で、これ以上uploadする必要がない最後の世代です。
// Generationが空なら完了世代はありません。未圧縮・圧縮は同じ完了位置を共有します。
type DestinationCursor struct {
	DestinationID objectstorage.DestinationID `json:"destination_id"`
	Generation    string                      `json:"generation"`
	// OutOfScopeGenerationは、宛先の登録前から存在したため送信対象外とした最後の世代です。
	// この世代以前はuploadが不要ですが、送信・退避済みではありません。常にGeneration以前です。
	// 空なら対象外の世代はなく、Generation以前はすべて送信・退避済みです。
	// この項目がない旧形式のcursorも、従来どおりGeneration以前を送信・退避済みとして読みます。
	OutOfScopeGeneration string `json:"out_of_scope_generation,omitempty"`
}

// 空のgenerationは初期状態です。保存内容の欠落・nullとは区別します。
// out_of_scope_generationは省略可能で、省略時は対象外の世代なしとして読みます。
func (cursor *DestinationCursor) UnmarshalJSON(payload []byte) error {
	var decoded struct {
		DestinationID        *objectstorage.DestinationID `json:"destination_id"`
		Generation           *string                      `json:"generation"`
		OutOfScopeGeneration json.RawMessage              `json:"out_of_scope_generation"`
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	if decoded.DestinationID == nil || decoded.Generation == nil {
		return errors.New("destination_id and generation are required and must not be null")
	}
	// 空文字だけは「完了世代なし」を表すcursor固有の値です。
	if *decoded.Generation != "" {
		if err := validateCheckpointGeneration(*decoded.Generation); err != nil {
			return err
		}
	}
	outOfScope := ""
	if len(decoded.OutOfScopeGeneration) > 0 {
		// 欠落は旧形式として受け入れますが、明示されたnullは保存内容の破損として扱います。
		if bytes.Equal(decoded.OutOfScopeGeneration, []byte("null")) {
			return errors.New("out_of_scope_generation must not be null")
		}
		if err := json.Unmarshal(decoded.OutOfScopeGeneration, &outOfScope); err != nil {
			return fmt.Errorf("out_of_scope_generation: %w", err)
		}
	}
	if outOfScope != "" {
		if err := validateCheckpointGeneration(outOfScope); err != nil {
			return err
		}
		// 対象外の範囲は完了位置に含まれます。完了位置より先の対象外は記録できません。
		if *decoded.Generation == "" || CompareGenerations(outOfScope, *decoded.Generation) > 0 {
			return fmt.Errorf("out_of_scope_generation %q must not be after generation %q", outOfScope, *decoded.Generation)
		}
	}
	cursor.DestinationID, cursor.Generation = *decoded.DestinationID, *decoded.Generation
	cursor.OutOfScopeGeneration = outOfScope
	return nil
}

// Coversは指定世代が完了位置以前かを返します。圧縮状態は判定に使用しません。
// 登録前から存在した対象外の世代も含みます。
func (cursor DestinationCursor) Covers(key CheckpointKey) bool {
	return cursor.Generation != "" && CompareGenerations(key.Generation, cursor.Generation) <= 0
}

// OutOfScopeは指定世代が、宛先の登録前から存在したため送信対象外とした範囲にあるかを返します。
// 圧縮状態は判定に使用しません。trueの世代はCoversもtrueですが、送信・退避済みではありません。
func (cursor DestinationCursor) OutOfScope(key CheckpointKey) bool {
	return cursor.OutOfScopeGeneration != "" && CompareGenerations(key.Generation, cursor.OutOfScopeGeneration) <= 0
}

// sortedDestinationIDsは検証済みの宛先ID一覧を辞書順に並べたコピーを返します。
func sortedDestinationIDs(destinations []objectstorage.DestinationID) []objectstorage.DestinationID {
	result := make([]objectstorage.DestinationID, len(destinations))
	copy(result, destinations)
	slices.Sort(result)
	return result
}

// destinationSetは、追加時の重複防止やループ内での繰り返し照合に使う宛先集合を作ります。
// 照合用のmapはループの外で一度作り、単発の存在確認にはslices.Containsを使います。
// 返すmapを変更しても、元の宛先一覧には影響しません。
func destinationSet(
	destinations []objectstorage.DestinationID,
) map[objectstorage.DestinationID]struct{} {
	result := make(map[objectstorage.DestinationID]struct{}, len(destinations))
	for _, destination := range destinations {
		result[destination] = struct{}{}
	}
	return result
}

func cloneCheckpoint(checkpoint Checkpoint) Checkpoint {
	return Checkpoint{
		Key:                      checkpoint.Key,
		CompletedDestinationIDs:  cloneDestinationIDs(checkpoint.CompletedDestinationIDs),
		OutOfScopeDestinationIDs: cloneDestinationIDs(checkpoint.OutOfScopeDestinationIDs),
	}
}

func cloneDestinationIDs(
	destinations []objectstorage.DestinationID,
) []objectstorage.DestinationID {
	if destinations == nil {
		return nil
	}
	cloned := make([]objectstorage.DestinationID, len(destinations))
	copy(cloned, destinations)
	return cloned
}
