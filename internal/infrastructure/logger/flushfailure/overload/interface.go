// Package overload は、backend送信失敗のdurable保存先が過負荷になった場合の
// 共通退避境界を提供します。
package overload

import (
	"context"

	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
)

type Handler interface {
	Handle(ctx context.Context, result loggerfailure.SendResult) error
}

// SpillDestinationConfigProvider は、overload時の二次退避先だけを受け取る
// backend-neutralな設定境界です。
type SpillDestinationConfigProvider interface {
	SpillDestination() durable.SpillDestination
}
