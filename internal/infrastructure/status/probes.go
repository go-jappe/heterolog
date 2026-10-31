// Package statusは、管理用APIのstatusの報告に使う、OSとfile systemの読み取りです。
// processのCPU時間、memory、directoryのfileの数とdiskの空きを読み、application層の
// statusのProbesとして渡します。
package status

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	appstatus "github.com/go-jappe/heterolog/internal/application/status"
	"github.com/go-jappe/heterolog/pkg/disk"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
)

// Probesは、このOSの読み取りを返します。
func Probes() appstatus.Probes {
	return appstatus.Probes{Process: processProbe{}, Memory: memorypressure.New(), Scanner: scanner{}}
}

// processProbeは、このprocessのCPU時間を読みます。OSごとのfileで実装します。
type processProbe struct{}

// scannerは、directoryのfileを数え、diskの空きを読みます。
type scanner struct{}

// ScanDirectoryは、path以下の通常のfileの数とbyte数を返します。symbolic linkは辿らず、
// 数えません。pathがなければ0を返します。ctxが終わると、そこまでの数とctxのerrorを
// 返します。
func (scanner) ScanDirectory(ctx context.Context, path string) (appstatus.Usage, error) {
	var usage appstatus.Usage
	err := filepath.WalkDir(path, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			// 途中のdirectoryを読めなくても、読めた分は数えます。rootがなければ空です。
			if errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrPermission) {
				return nil
			}
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return nil
		}
		usage.Files++
		usage.Bytes += uint64(max(info.Size(), 0))
		return nil
	})
	return usage, err
}

// DiskSpaceは、pathのあるfile systemの空きと総量を返します。pathがまだなければ、ある
// 親のdirectoryで読みます。
func (scanner) DiskSpace(path string) (uint64, uint64, error) {
	return disk.FreeAndTotal(nearestExistingPath(path))
}

// nearestExistingPathは、pathか、その一番近い親のうち存在するものを返します。
func nearestExistingPath(path string) string {
	for {
		if _, err := os.Stat(path); err == nil {
			return path
		}
		parent := filepath.Dir(path)
		if parent == path {
			return path
		}
		path = parent
	}
}
