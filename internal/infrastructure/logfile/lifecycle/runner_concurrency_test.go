package lifecycle

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"

	"github.com/stretchr/testify/require"
)

func TestDirectoryLifecycleConcurrentOperationsShareInitializedDependencies(t *testing.T) {
	for _, initializeAtConstruction := range []bool{true, false} {
		t.Run(fmt.Sprintf("initialize_at_construction=%t", initializeAtConstruction), func(t *testing.T) {
			root := resolvedTempDirForTest(t)
			const target = "concurrent-runner"
			directory := filepath.Join(root, target, "logs")
			require.NoError(t, os.MkdirAll(directory, 0o700))
			manager := NewFileSeriesManager(FileSeriesManagerOptions{
				FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
				Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
			})
			lifecycle := &DirectoryLifecycle{fileSeriesManagers: []*FileSeriesManager{manager}}
			if initializeAtConstruction {
				var err error
				lifecycle, err = NewDirectoryLifecycle(manager)
				require.NoError(t, err)
			}
			ctx := context.Background()
			operations := []func() (string, error){
				func() (string, error) {
					report, err := lifecycle.RunOnce(ctx)
					return report.DirectoryPath, err
				},
				func() (string, error) {
					report, err := lifecycle.ReconcileObjectStorageUploadDestinations(ctx)
					return report.DirectoryPath, err
				},
				lifecycle.DirectoryPath,
			}

			// 同じDirectoryLifecycleへの公開APIを重ね、validateによる共有依存の
			// 再代入と実行中の参照が競合しないことをrace detectorでも確認します。
			// manager.RunOnceは毎回別のDirectoryLifecycleを作るため使用しません。
			start := make(chan struct{})
			results := make(chan error, len(operations))
			for _, operation := range operations {
				go func() {
					<-start
					for range 100 {
						got, err := operation()
						if err != nil {
							results <- err
							return
						}
						if got != directory {
							results <- fmt.Errorf("directory = %q, want %q", got, directory)
							return
						}
					}
					results <- nil
				}()
			}
			close(start)
			for range operations {
				if err := boundedwait.Receive(t, results, "concurrent lifecycle operation"); err != nil {
					t.Error(err)
				}
			}
		})
	}
}
