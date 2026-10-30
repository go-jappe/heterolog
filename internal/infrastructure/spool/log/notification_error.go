package logspool

import (
	"context"
	"errors"
)

// NotificationErrorは、runnerが返したerrorのうち、通知するものを返します。runtimeへの
// 中止要求だけは異常通知から除きます。送信timeoutや、まだ稼働中のruntimeに戻った
// context errorは通知し、Joinされた別の障害も捨てません。
// Unwrapがnilや空の原因を返すerror（例: Errがnilのos.PathError）は辿れる原因を
// 持たないため、それ自体を障害として判定し、中止要求でなければ通知します。
func NotificationError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var remaining []error
		causes := 0
		for _, cause := range joined.Unwrap() {
			if cause == nil {
				continue
			}
			causes++
			if cause = NotificationError(ctx, cause); cause != nil {
				remaining = append(remaining, cause)
			}
		}
		if causes > 0 {
			return errors.Join(remaining...)
		}
	} else if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		if cause := wrapped.Unwrap(); cause != nil {
			if NotificationError(ctx, cause) == nil {
				return nil
			}
			return err
		}
	}
	if errors.Is(ctx.Err(), context.Canceled) && errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
