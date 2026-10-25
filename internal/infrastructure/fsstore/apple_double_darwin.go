//go:build darwin

package fsstore

// macOSは拡張属性を保存できないfilesystemにAppleDoubleの付随fileを作るため、付随fileを
// 管理対象のentryから除きます。
const hidesAppleDoubleCompanions = true
