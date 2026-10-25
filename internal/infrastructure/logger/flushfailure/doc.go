// Package flushfailureは、logger backendへの送信失敗後に実行する共通処理を提供します。
//
// backend固有packageは、backendの応答をfailure.SendResultへ変換し、permanent recordの
// 保存先をdeadまたはquarantineへ分類するための情報をこのpackageへ渡します。
// このpackageは、spool、file archive、direct degradeの構築、overload処理、
// failure処理の同時実行制御と完了待ち、およびDeliveryReportの集約を担当します。
package flushfailure
