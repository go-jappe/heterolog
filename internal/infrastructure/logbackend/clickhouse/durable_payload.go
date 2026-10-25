package clickhouse

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"

	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
)

const (
	// DurablePayloadFieldRecordIDは、durable保存後も一件のlog recordを安定して
	// 識別するIDのflat payload field名です。
	DurablePayloadFieldRecordID = "record_id"

	// DurablePayloadFieldReplayModeは、元batchのまま再送するか、一件ずつ隔離して
	// 再送するかを固定するflat payload field名です。
	DurablePayloadFieldReplayMode = "replay_mode"

	recordIsolationTokenDomain = "clickhouse-record-isolation-v1"
)

// DeriveRecordIsolationTokenが拒否するroot tokenとrecord IDの不正です。
var (
	// ErrEmptyRootInsertDeduplicationTokenは、root INSERT tokenが空であることを表します。
	ErrEmptyRootInsertDeduplicationToken = errors.New("root insert deduplication token is empty")
	// ErrRootInsertDeduplicationTokenWhitespaceは、root INSERT tokenの前後に空白があることを表します。
	ErrRootInsertDeduplicationTokenWhitespace = errors.New("root insert deduplication token contains surrounding whitespace")
	// ErrRootInsertDeduplicationTokenNULは、root INSERT tokenがNULを含むことを表します。
	ErrRootInsertDeduplicationTokenNUL = errors.New("root insert deduplication token contains NUL")
	// ErrEmptyDurableRecordIDは、record IDが空であることを表します。
	ErrEmptyDurableRecordID = errors.New("clickhouse durable record ID is empty")
	// ErrDurableRecordIDWhitespaceは、record IDの前後に空白があることを表します。
	ErrDurableRecordIDWhitespace = errors.New("clickhouse durable record ID contains surrounding whitespace")
	// ErrDurableRecordIDNULは、record IDがNULを含むことを表します。
	ErrDurableRecordIDNUL = errors.New("clickhouse durable record ID contains NUL")
)

// DurableReplayModeは、ClickHouse failureをdurable storeから再送する単位です。
// failure発生時にpayloadへ保存し、再送途中で暗黙に変更しません。
type DurableReplayMode string

const (
	// DurableReplayModeBatchは、初回INSERTと同じtokenで元batchを再送します。
	DurableReplayModeBatch DurableReplayMode = "batch"

	// DurableReplayModeRecordIsolationは、root tokenとrecord IDから導出したtokenで
	// 一件だけを再送します。
	DurableReplayModeRecordIsolation DurableReplayMode = "record_isolation"

	// DurableReplayModeNoneは、再送対象ではないpermanent / local invalid recordです。
	DurableReplayModeNone DurableReplayMode = "none"
)

// DurablePayloadは、ClickHouse failure storeが保存するflat documentです。
// LogRecordを埋め込むことで、再送用metadataと送信対象rowを同じJSON階層に保ちます。
type DurablePayload struct {
	pkgclickhouse.LogRecord
	InsertDeduplicationToken string            `json:"insert_deduplication_token"`
	RecordID                 string            `json:"record_id"`
	ReplayMode               DurableReplayMode `json:"replay_mode"`
}

// IsKnownは、durable payload contractで定義済みのmodeかを返します。
func (m DurableReplayMode) IsKnown() bool {
	switch m {
	case DurableReplayModeBatch,
		DurableReplayModeRecordIsolation,
		DurableReplayModeNone:
		return true
	default:
		return false
	}
}

// DeriveRecordIsolationTokenは、root INSERT tokenと永続的なrecord IDから、
// singleton再送に使用する安定したdeduplication tokenを生成します。
func DeriveRecordIsolationToken(rootToken, recordID string) (string, error) {
	trimmedRootToken := strings.TrimSpace(rootToken)
	if trimmedRootToken == "" {
		return "", ErrEmptyRootInsertDeduplicationToken
	}
	if trimmedRootToken != rootToken {
		return "", ErrRootInsertDeduplicationTokenWhitespace
	}
	trimmedRecordID := strings.TrimSpace(recordID)
	if trimmedRecordID == "" {
		return "", ErrEmptyDurableRecordID
	}
	if trimmedRecordID != recordID {
		return "", ErrDurableRecordIDWhitespace
	}
	if strings.ContainsRune(rootToken, '\x00') {
		return "", ErrRootInsertDeduplicationTokenNUL
	}
	if strings.ContainsRune(recordID, '\x00') {
		return "", ErrDurableRecordIDNUL
	}

	sum := sha256.Sum256([]byte(
		recordIsolationTokenDomain + "\x00" + rootToken + "\x00" + recordID,
	))
	return hex.EncodeToString(sum[:]), nil
}
