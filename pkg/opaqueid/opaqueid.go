package opaqueid

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type ctxKeyOpaqueID struct{}

func GenerateOpaqueID(prefix string) string {
	return fmt.Sprintf("%s-%d-%s", prefix, time.Now().UnixNano(), uuid.NewString())
}

// OpaqueIDGenerator はOpaqueIDを生成するインターフェース
type OpaqueIDGenerator interface {
	GenerateOpaqueID(prefix string) string
}

// DefaultOpaqueIDGenerator はデフォルトのOpaqueIDジェネレーター
type DefaultOpaqueIDGenerator struct{}

// GenerateOpaqueID はOpaqueIDを生成します
func (g *DefaultOpaqueIDGenerator) GenerateOpaqueID(prefix string) string {
	return GenerateOpaqueID(prefix)
}

func WithOpaqueID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyOpaqueID{}, id)
}
func OpaqueID(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	id, ok := ctx.Value(ctxKeyOpaqueID{}).(string)
	return id, ok && id != ""
}
