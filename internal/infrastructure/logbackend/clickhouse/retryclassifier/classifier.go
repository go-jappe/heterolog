// Package retryclassifierはClickHouse送信失敗のretry可否を分類します。
package retryclassifier

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
)

const (
	// RetryReasonRecordIsolationRequiredは、複数recordを含むClickHouse INSERTが
	// record固有の入力値、またはaggregate request sizeに起因し得るerrorで失敗し、
	// durable再送時にrecordを分けて原因を隔離する必要があることを表します。
	RetryReasonRecordIsolationRequired retryable.RetryReason = "clickhouse_record_isolation_required"
)

// ErrMissingInsertResponseは、clientがerrorを返さずにInsertResponseも返さなかったことを表します。
// HTTP応答を確認できないためINSERTの成否は不明です。通常送信・spool再送ともに即時retryせず、
// 内容と配送状況を確認するまでquarantineへ隔離します。
var ErrMissingInsertResponse = errors.New("clickhouse client returned nil InsertResponse without error")

// ClassifyRetryはClickHouse固有のerror typeを考慮して送信失敗を分類します。
// 応答本文の読込失敗と応答の欠落は結果不明として、即時retryしません。
func ClassifyRetry(ctx context.Context, err error) retryable.RetryDecision {
	if err != nil && (ctx == nil || ctx.Err() == nil) {
		var decodeErr *pkgclickhouse.ResponseDecodeError
		if errors.As(err, &decodeErr) || errors.Is(err, ErrMissingInsertResponse) {
			return retryable.RetryDecision{Reason: retryable.RetryReasonUnknownOutcome}
		}
	}
	decision := retryable.ClassifyRetry(ctx, err)
	if err == nil || (ctx != nil && ctx.Err() != nil) {
		return decision
	}

	var statusErr *pkgclickhouse.HTTPStatusError
	if !errors.As(err, &statusErr) {
		return decision
	}

	errorType := statusErr.ClickHouseErrorType()
	if !isPermanentClickHouseErrorType(errorType) {
		// ClickHouseはheader送信後の失敗をHTTP 2xxのbodyへ返す場合があります。
		// 未知のexceptionからrecordごとの結果は確定できないため、
		// 即時再送せずdurable分類でquarantineへ隔離します。
		if statusErr.StatusCode() >= http.StatusOK && statusErr.StatusCode() < http.StatusMultipleChoices {
			return retryable.RetryDecision{
				Reason:     retryable.RetryReasonUnknownOutcome,
				Code:       statusErr.StatusCode(),
				RetryAfter: statusErr.RetryAfter(),
			}
		}
		return decision
	}

	return retryable.RetryDecision{
		Retry:      false,
		Reason:     retryable.RetryReasonHTTPStatus,
		Code:       statusErr.StatusCode(),
		RetryAfter: statusErr.RetryAfter(),
	}
}

// ClassifyDurableInsertFailureは、失敗したClickHouse INSERTをdurable storeから
// 将来再送する際の分類を返します。Circuit Breaker openは将来再送、結果不明errorはquarantineへ残します。
// HTTP送信前のrecord marshal error、recordの値に起因する入力値error、HTTP 413は、
// 複数recordなら一件ずつ再送して正常recordを救済するためrecord isolation対象とし、
// 一件でも拒否されたrecordは入力不備または上限超過としてquarantineへ隔離します。
// query・schema・settingなどrecord内容に依存しないrequest全体の不備は配送不能とします。
// 明示的な応答の分類は、呼び出し元contextが終了した後も維持します。
func ClassifyDurableInsertFailure(
	ctx context.Context,
	err error,
	inputCount int,
) retryable.RetryDecision {
	if err == nil {
		return retryable.RetryDecision{}
	}
	// marshal errorは既知の入力不備であり、配送結果が不明なものではありません。
	var marshalErr *pkgclickhouse.LogRecordMarshalError
	if errors.As(err, &marshalErr) && marshalErr != nil {
		return recordScopedInsertFailure(inputCount, retryable.RetryReasonRecordRejected, 0, 0)
	}

	decision := ClassifyRetry(retryable.DurableClassificationContext(ctx), err)
	var statusErr *pkgclickhouse.HTTPStatusError
	// 呼び出し元の停止と同時に返ったerrorは、どのrequestの結果か確定できないため、
	// record単位の拒否として扱わず結果不明の分類へ進めます。
	if !decision.Retry && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) &&
		errors.As(err, &statusErr) && statusErr != nil {
		switch {
		case statusErr.StatusCode() == http.StatusRequestEntityTooLarge:
			return recordScopedInsertFailure(
				inputCount,
				retryable.RetryReasonRequestTooLarge,
				statusErr.StatusCode(),
				statusErr.RetryAfter(),
			)
		case isRecordContentClickHouseErrorType(statusErr.ClickHouseErrorType()):
			return recordScopedInsertFailure(
				inputCount,
				retryable.RetryReasonRecordRejected,
				statusErr.StatusCode(),
				statusErr.RetryAfter(),
			)
		}
	}
	return retryable.ForDurableReplay(ctx, err, decision)
}

// recordScopedInsertFailureは、recordごとに結果が異なり得る拒否を分類します。
// 複数recordのINSERTは原因のrecordを特定するため一件ずつの再送を要求し、
// 一件のINSERTはreasonを付けてquarantineへ隔離します。
func recordScopedInsertFailure(
	inputCount int,
	reason retryable.RetryReason,
	code int,
	retryAfter time.Duration,
) retryable.RetryDecision {
	if inputCount > 1 {
		return retryable.RetryDecision{
			Retry:      true,
			Reason:     RetryReasonRecordIsolationRequired,
			Code:       code,
			RetryAfter: retryAfter,
		}
	}
	return retryable.RetryDecision{
		Quarantine: true,
		Reason:     reason,
		Code:       code,
	}
}

// isPermanentClickHouseErrorTypeは、即時retryでは解消しないClickHouseのerror typeを返します。
// recordの値に起因するerrorと、record内容に依存しないrequest全体の不備の両方を含みます。
func isPermanentClickHouseErrorType(errorType pkgclickhouse.ErrorType) bool {
	return isRecordContentClickHouseErrorType(errorType) || isRequestWideClickHouseErrorType(errorType)
}

// isRequestWideClickHouseErrorTypeは、table・column・関数・形式・settingなど、送信する
// recordの内容に依存しない宛先・query・schemaの不備を表すerror typeを返します。
// 同じrequestを分けて再送しても解消しないため、durable処理では配送不能とします。
// BAD_ARGUMENTSやILLEGAL_TYPE_OF_ARGUMENTは関数・式の不備、BAD_CASTは内部の型変換の
// 不備に使われるため、ここに含めます。
func isRequestWideClickHouseErrorType(errorType pkgclickhouse.ErrorType) bool {
	switch errorType {
	case pkgclickhouse.ErrorTypeUnknownTable,
		pkgclickhouse.ErrorTypeUnknownDatabase,
		pkgclickhouse.ErrorTypeUnknownIdentifier,
		pkgclickhouse.ErrorTypeUnknownFunction,
		pkgclickhouse.ErrorTypeUnknownType,
		pkgclickhouse.ErrorTypeNoSuchColumnInTable,
		pkgclickhouse.ErrorTypeThereIsNoColumn,
		pkgclickhouse.ErrorTypeSyntaxError,
		pkgclickhouse.ErrorTypeIllegalTypeOfArgument,
		pkgclickhouse.ErrorTypeBadArguments,
		pkgclickhouse.ErrorTypeBadCast,
		pkgclickhouse.ErrorTypeUnknownFormat,
		pkgclickhouse.ErrorTypeBlocksHaveDifferentStructure,
		pkgclickhouse.ErrorTypeTooFewColumns,
		pkgclickhouse.ErrorTypeTooManyColumns,
		pkgclickhouse.ErrorTypeNumberOfColumnsDoesntMatch,
		pkgclickhouse.ErrorTypeUnknownSetting,
		pkgclickhouse.ErrorTypeReadonlySetting,
		pkgclickhouse.ErrorTypeSettingConstraintViolation,
		pkgclickhouse.ErrorTypeIncorrectNumberOfColumns,
		pkgclickhouse.ErrorTypeSizesOfColumnsDoesntMatch,
		pkgclickhouse.ErrorTypeNotFoundColumnInBlock,
		pkgclickhouse.ErrorTypeDuplicateColumn,
		pkgclickhouse.ErrorTypeIllegalColumn,
		pkgclickhouse.ErrorTypeFormatIsNotSuitableForInput,
		pkgclickhouse.ErrorTypeEmptyDataPassed,
		pkgclickhouse.ErrorTypeIncompatibleColumns,
		pkgclickhouse.ErrorTypeBadQueryParameter,
		pkgclickhouse.ErrorTypeInvalidSettingValue,
		pkgclickhouse.ErrorTypeIncompatibleSchema:
		return true
	default:
		return false
	}
}

// isRecordContentClickHouseErrorTypeは、JSONEachRowの一件の値をparse・型変換・検証する際に
// 生じるerror typeを返します。複数recordのINSERTでは正常recordを救済するため一件ずつ再送し、
// 一件でも拒否されたrecordは入力不備としてquarantineへ隔離します。
// TYPE_MISMATCHとCANNOT_CONVERT_TYPEは、入力値を列の型へ変換できない場合に使われるため含めます。
func isRecordContentClickHouseErrorType(errorType pkgclickhouse.ErrorType) bool {
	switch errorType {
	case pkgclickhouse.ErrorTypeCannotParseInputAssertionFailed,
		pkgclickhouse.ErrorTypeCannotParseQuotedString,
		pkgclickhouse.ErrorTypeCannotParseNumber,
		pkgclickhouse.ErrorTypeCannotParseDate,
		pkgclickhouse.ErrorTypeCannotParseDateTime,
		pkgclickhouse.ErrorTypeIncorrectData,
		pkgclickhouse.ErrorTypeCannotParseText,
		pkgclickhouse.ErrorTypeSizeOfFixedStringDoesntMatch,
		pkgclickhouse.ErrorTypeCannotParseEscapeSequence,
		pkgclickhouse.ErrorTypeCannotInsertValueOfDifferentSizeIntoTuple,
		pkgclickhouse.ErrorTypeCannotReadArrayFromText,
		pkgclickhouse.ErrorTypeTooLargeStringSize,
		pkgclickhouse.ErrorTypeCannotParseUUID,
		pkgclickhouse.ErrorTypeCannotParseBool,
		pkgclickhouse.ErrorTypeViolatedConstraint,
		pkgclickhouse.ErrorTypeCannotParseIPv4,
		pkgclickhouse.ErrorTypeCannotParseIPv6,
		pkgclickhouse.ErrorTypeUnknownElementOfEnum,
		pkgclickhouse.ErrorTypeTypeMismatch,
		pkgclickhouse.ErrorTypeCannotConvertType:
		return true
	default:
		return false
	}
}
