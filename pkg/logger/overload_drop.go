package logger

import "errors"

// ErrLogDroppedは、保存先の過負荷によりoverload policyのdropを適用し、ログを保存せずに
// 破棄したことを表します。破棄した場合もdrop件数のmetricとspan eventを記録します。
//
// 同期APIは、送信先への送信に失敗したログをfailure storeの過負荷でdropした場合に、
// このerrorを包んだerrorを返します。errors.Is(err, ErrLogDropped)で判定してください。
// 送信先の元の原因も同じerrorからerrors.Is／errors.Asで判別できます。
// degradeによる退避に成功した場合は、ログを破棄していないためこのerrorを返しません。
// 非同期APIはbest effortのため、呼び出し元へerrorを返しません。
// Delivery APIでは、errorではなくDeliveryReport.HasDropで確認してください。
var ErrLogDropped = errors.New("log was dropped by overload policy")
