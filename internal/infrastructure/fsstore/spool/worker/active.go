package worker

import (
	"context"

	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// ClaimOldestActiveFile は、active 配下の最古 spool file を1件だけ processing へ移動し、
// 移動後の processing path を返す。
//
// 対象がなければ ok=false を返す。
func (s *StateStore) ClaimOldestActiveFile(
	ctx context.Context,
) (processingPath string, ok bool, err error) {
	res, ok, err := s.MoveOldestActiveToProcessing(ctx)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, nil
	}

	return res.ToPath, true, nil
}

// active 配下の最古 spool file を1件だけ processing へ移動する。
// 対象がなければ ok=false を返す。
// 成功時の MoveResult.ToPath は、移動後の processing file path を表す。
func (s *StateStore) MoveOldestActiveToProcessing(
	ctx context.Context,
) (MoveResult, bool, error) {
	paths, err := s.listOldestStatePaths(ctx, pkgspool.StateActive, 1)
	if err != nil {
		return MoveResult{}, false, err
	}
	if len(paths) == 0 {
		return MoveResult{}, false, nil
	}

	res, err := s.moveActiveToProcessing(ctx, paths[0])
	if err != nil {
		return MoveResult{}, false, err
	}

	return res, true, nil
}

// active から processing へ移動する。
// 外部から任意 active path を移動させないため private にする。
func (s *StateStore) moveActiveToProcessing(
	ctx context.Context,
	activePath string,
) (MoveResult, error) {
	return s.moveToState(ctx, activePath, pkgspool.StateActive, pkgspool.StateProcessing)
}
