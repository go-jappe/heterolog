package file

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// constructorがerrorを返した場合は、DirectoryGuardを持たない部分構築instanceを
// 呼び出し元へ渡しません。成功して返るloggerだけがlifecycle invariantを満たします。
func TestFileLoggerConstructorsDoNotReturnPartialInstances(t *testing.T) {
	root := t.TempDir()
	blockingFile := filepath.Join(root, "not-a-directory")
	require.NoError(t, os.WriteFile(blockingFile, []byte("block mkdir"), 0o600))

	fileCfg := newTestFileLoggerConfig(blockingFile, "logs", "constructor-invariant")
	fileAdapter := NewConfigAdapter(fileCfg)
	deliveryCfg := newTestDeliveryFileLoggerConfig(
		blockingFile,
		"delivery_logs",
		"delivery-constructor-invariant",
	)
	deliveryAdapter := NewDeliveryFileLoggerConfigAdapter(deliveryCfg)
	ctx := context.Background()

	tests := []struct {
		name      string
		construct func() (any, error)
	}{
		{
			name: "default",
			construct: func() (any, error) {
				return newStandaloneFileLogger(ctx, fileAdapter, nil, nil)
			},
		},
		{
			name: "error",
			construct: func() (any, error) {
				return newStandaloneErrorFileLogger(ctx, fileAdapter, nil, nil)
			},
		},
		{
			name: "debug",
			construct: func() (any, error) {
				return newStandaloneDebugFileLogger(ctx, fileAdapter, nil, nil)
			},
		},
		{
			name: "delivery default",
			construct: func() (any, error) {
				return NewDeliveryFileLogger(ctx, deliveryAdapter, nil, nil)
			},
		},
		{
			name: "delivery error",
			construct: func() (any, error) {
				return NewDeliveryErrorFileLogger(ctx, deliveryAdapter, nil, nil)
			},
		},
		{
			name: "delivery debug",
			construct: func() (any, error) {
				return NewDeliveryDebugFileLogger(ctx, deliveryAdapter, nil, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, err := tt.construct()
			require.Error(t, err)
			require.Nil(t, logger)
		})
	}
}
