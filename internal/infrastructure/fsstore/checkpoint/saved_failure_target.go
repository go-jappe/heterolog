package checkpoint

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// SavedFailureTargetは保存が完了した退避先です。KindとRootを使い、設定変更後も
// 別の保存先への成功と取り違えません。記録の検証・比較・コピーは各実装が担当します。
// 保存結果が矛盾した場合の.preservedへの保全は、この記録とは別に利用側が行います。
// この型は通常の退避先の記録であり、保全コピーの保存先・内容を表すものではありません。
// 新しい種類を永続化する場合は、typeを含むJSONへの変換と復元処理も用意します。
type SavedFailureTarget interface {
	// 現在はspoolまたはfile archiveです。
	Kind() failurestore.DurableType
	Root() string
	// Validateは種類固有の保存結果を検証します。Rootの必須判定は共通処理で行います。
	Validate() error
	// Equalは保存先と保存結果が一致するかを返します。
	// 同じKind・Rootでfalseになった場合、Storeは保存結果の矛盾として記録します。
	// 比較自体は実ファイルの検査・移動・保全を行いません。
	Equal(SavedFailureTarget) bool
	// Cloneは元の記録と変更を共有しないコピーを返します。
	Clone() SavedFailureTarget
}

// SavedArchiveTargetはarchiveが実際に置かれた分類を保持します。
// 既存archiveを再利用すると、今回の送信失敗の分類とは異なる場合があります。
type SavedArchiveTarget struct {
	Target          string                   `json:"target"`
	FailureCategory uploader.FailureCategory `json:"archive_failure_category"`
}

func (SavedArchiveTarget) Kind() failurestore.DurableType { return failurestore.DurableTypeFileArchive }
func (target SavedArchiveTarget) Root() string            { return target.Target }

func (target SavedArchiveTarget) Validate() error {
	if !target.FailureCategory.IsValid() {
		return errors.New("saved archive target must have a failure category")
	}
	return nil
}

func (target SavedArchiveTarget) Equal(other SavedFailureTarget) bool {
	switch other := other.(type) {
	case SavedArchiveTarget:
		return target == other
	case *SavedArchiveTarget:
		return other != nil && target == *other
	default:
		return false
	}
}

func (target SavedArchiveTarget) Clone() SavedFailureTarget { return target }

func (target SavedArchiveTarget) MarshalJSON() ([]byte, error) {
	return json.Marshal(savedArchiveTargetJSON{
		Type: target.Kind(), Target: target.Target, ArchiveFailureCategory: target.FailureCategory,
	})
}

// SavedSpoolTargetはspoolに保存した時点の状態を保持します。
type SavedSpoolTarget struct {
	Target string         `json:"target"`
	State  pkgspool.State `json:"spool_state"`
}

func (SavedSpoolTarget) Kind() failurestore.DurableType { return failurestore.DurableTypeSpool }
func (target SavedSpoolTarget) Root() string            { return target.Target }

func (target SavedSpoolTarget) Validate() error {
	if !target.State.IsValid() {
		return errors.New("saved spool target must have a valid spool state")
	}
	return nil
}

func (target SavedSpoolTarget) Equal(other SavedFailureTarget) bool {
	switch other := other.(type) {
	case SavedSpoolTarget:
		return target == other
	case *SavedSpoolTarget:
		return other != nil && target == *other
	default:
		return false
	}
}

func (target SavedSpoolTarget) Clone() SavedFailureTarget { return target }

func (target SavedSpoolTarget) MarshalJSON() ([]byte, error) {
	return json.Marshal(savedSpoolTargetJSON{
		Type: target.Kind(), Target: target.Target, SpoolState: target.State,
	})
}

type savedArchiveTargetJSON struct {
	Type                   failurestore.DurableType `json:"type"`
	Target                 string                   `json:"target"`
	ArchiveFailureCategory uploader.FailureCategory `json:"archive_failure_category"`
}

type savedSpoolTargetJSON struct {
	Type       failurestore.DurableType `json:"type"`
	Target     string                   `json:"target"`
	SpoolState pkgspool.State           `json:"spool_state"`
}

// JSONのtypeで具体型を選び、それ以外の種類のfieldを含む記録は拒否します。
func decodeSavedFailureTarget(payload json.RawMessage) (SavedFailureTarget, error) {
	var header struct {
		Type failurestore.DurableType `json:"type"`
	}
	if err := json.Unmarshal(payload, &header); err != nil {
		return nil, err
	}
	switch header.Type {
	case failurestore.DurableTypeFileArchive:
		var record savedArchiveTargetJSON
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&record); err != nil {
			return nil, err
		}
		return SavedArchiveTarget{Target: record.Target, FailureCategory: record.ArchiveFailureCategory}, nil
	case failurestore.DurableTypeSpool:
		var record savedSpoolTargetJSON
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&record); err != nil {
			return nil, err
		}
		return SavedSpoolTarget{Target: record.Target, State: record.SpoolState}, nil
	default:
		return nil, fmt.Errorf("saved failure target type %q is invalid", header.Type)
	}
}

// interfaceに入ったnil pointerなども、メソッドを呼ぶ前に拒否します。
func isNilSavedFailureTarget(target SavedFailureTarget) bool {
	if target == nil {
		return true
	}
	value := reflect.ValueOf(target)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
