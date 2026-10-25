package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

const (
	spoolDirectoryDepth = 5 // YYYY/MM/DD/HH/mm
	spoolReadBatchSize  = 128
)

type spoolDirectoryReader interface {
	ReadDir(int) ([]os.DirEntry, error)
	Close() error
}

// spoolCandidateScanner はディレクトリを分割読み込みし、状態配下の候補を列挙する。
// ディレクトリごとに読み込み用ハンドルを開き、列挙が終わるたびに閉じる。
type spoolCandidateScanner struct {
	openDirectory func(string) (spoolDirectoryReader, error)
}

func (s *StateStore) listStateFileCandidatesAfter(
	ctx context.Context,
	state pkgspool.State,
	afterRelativePath string,
	limit int,
) ([]pkgspool.FileCandidate, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	after, err := spoolBoundaryComponents(afterRelativePath)
	if err != nil {
		return nil, err
	}
	if limit < 1 {
		return []pkgspool.FileCandidate{}, nil
	}
	stateDir, err := s.stateDir(state)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve state dir: %w", err)
	}
	scanner := spoolCandidateScanner{
		openDirectory: func(path string) (spoolDirectoryReader, error) {
			return fsstore.OpenDirectoryForRead(path)
		},
	}
	return scanner.list(ctx, stateDir.Path(), after, limit)
}

// 境界は比較用の文字列としてのみ扱い、存在確認やシンボリックリンク解決をしない。
func spoolBoundaryComponents(relativePath string) ([]string, error) {
	if relativePath == "" {
		return nil, nil
	}
	if !filepath.IsLocal(relativePath) || filepath.Clean(relativePath) != relativePath {
		return nil, fmt.Errorf("invalid spool continuation relative path: %q", relativePath)
	}
	parts := strings.Split(relativePath, string(os.PathSeparator))
	if len(parts) != spoolDirectoryDepth+1 || filepath.Ext(parts[len(parts)-1]) != spooldir.SpoolExt {
		return nil, fmt.Errorf("invalid spool continuation relative path: %q", relativePath)
	}
	return parts, nil
}

func (s spoolCandidateScanner) list(ctx context.Context, root string, after []string, limit int) ([]pkgspool.FileCandidate, error) {
	paths := make([]pkgspool.FileCandidate, 0, limit)
	if err := s.walk(ctx, root, "", 0, after, limit, &paths); err != nil {
		return nil, err
	}
	return paths, nil
}

// 各階層のディレクトリ候補を固定件数に制限し、ファイルは残り limit 件だけ保持する。
// ファイルシステムの列挙順によらず辞書順の候補を選ぶため、候補を補充するときは
// ディレクトリ全体を固定件数ずつ分割読み込みする。同一の分に多数のファイルが
// ある場合も全件の列挙 I/O は残るが、保持メモリは O(limit + 階層数 * 固定件数) となる。
func (s spoolCandidateScanner) walk(
	ctx context.Context,
	root, relativeDir string,
	depth int,
	after []string,
	limit int,
	paths *[]pkgspool.FileCandidate,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dir := filepath.Join(root, relativeDir)
	if depth == spoolDirectoryDepth {
		lowerBound := ""
		if len(after) != 0 {
			lowerBound = after[depth]
		}
		names, err := s.smallestNames(ctx, dir, lowerBound, false, limit-len(*paths), true)
		if err != nil {
			return err
		}
		for _, name := range names {
			relativePath := filepath.Join(relativeDir, name)
			*paths = append(*paths, pkgspool.FileCandidate{
				Path:         filepath.Join(root, relativePath),
				RelativePath: relativePath,
			})
		}
		return nil
	}

	lowerBound, includeEqual := "", false
	if len(after) != 0 {
		lowerBound, includeEqual = after[depth], true
	}
	for {
		names, err := s.smallestNames(ctx, dir, lowerBound, includeEqual, spoolReadBatchSize, false)
		if err != nil {
			return err
		}
		for _, name := range names {
			// 境界と同じ階層名のときだけ、次の階層にも境界を引き継ぐ。
			// 古い配下のディレクトリは smallestNames で除外され、開かれない。
			var childAfter []string
			if len(after) != 0 && name == after[depth] {
				childAfter = after
			}
			if err := s.walk(ctx, root, filepath.Join(relativeDir, name), depth+1, childAfter, limit, paths); err != nil {
				return err
			}
			if len(*paths) == limit {
				return nil
			}
		}
		if len(names) < spoolReadBatchSize {
			return nil
		}
		lowerBound, includeEqual = names[len(names)-1], false
	}
}

func (s spoolCandidateScanner) smallestNames(
	ctx context.Context,
	dir, lowerBound string,
	includeEqual bool,
	limit int,
	files bool,
) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reader, err := s.openDirectory(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to open spool directory %s: %w", dir, err)
	}
	defer reader.Close()

	names := boundedSpoolNames{limit: limit}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entries, readErr := reader.ReadDir(spoolReadBatchSize)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			if errors.Is(readErr, os.ErrNotExist) {
				return nil, nil
			}
			return nil, fmt.Errorf("failed to read spool directory %s: %w", dir, readErr)
		}
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			name := entry.Name()
			if name < lowerBound || (name == lowerBound && !includeEqual) {
				continue
			}
			// macOSのAppleDoubleの付随fileは、spool fileやbucket directoryとして扱いません。
			if fsstore.IsAppleDoubleCompanion(dir, name) {
				continue
			}
			if files {
				if entry.IsDir() || filepath.Ext(name) != spooldir.SpoolExt {
					continue
				}
				// Info は最後のパス要素のシンボリックリンクを辿らない。
				info, err := entry.Info()
				if err != nil {
					if errors.Is(err, os.ErrNotExist) {
						continue
					}
					return nil, fmt.Errorf("failed to stat %s: %w", filepath.Join(dir, name), err)
				}
				if !info.Mode().IsRegular() {
					continue
				}
			} else if !entry.IsDir() {
				continue
			}
			names.add(name)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if errors.Is(readErr, io.EOF) {
			slices.Sort(names.values)
			return names.values, nil
		}
	}
}

// 最大ヒープの根を差し替え、辞書順で最小の limit 件だけを保持する。
// スライスの配列容量も limit を超えない。
type boundedSpoolNames struct {
	values []string
	limit  int
}

func (h *boundedSpoolNames) add(name string) {
	if h.limit < 1 {
		return
	}
	if len(h.values) < h.limit {
		if len(h.values) == cap(h.values) {
			values := make([]string, len(h.values), min(h.limit, max(8, 2*cap(h.values))))
			copy(values, h.values)
			h.values = values
		}
		h.values = append(h.values, name)
		for i := len(h.values) - 1; i > 0; {
			parent := (i - 1) / 2
			if h.values[parent] >= h.values[i] {
				break
			}
			h.values[parent], h.values[i] = h.values[i], h.values[parent]
			i = parent
		}
		return
	}
	if name >= h.values[0] {
		return
	}
	h.values[0] = name
	for i := 0; ; {
		child := 2*i + 1
		if child >= len(h.values) {
			return
		}
		if right := child + 1; right < len(h.values) && h.values[right] > h.values[child] {
			child = right
		}
		if h.values[i] >= h.values[child] {
			return
		}
		h.values[i], h.values[child] = h.values[child], h.values[i]
		i = child
	}
}
