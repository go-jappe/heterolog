package worker

import (
	"errors"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

type StateStore struct {
	RootDir spool.Dir
	Config  ConfigProvider

	OnError logging.LoggerErrorHandler // 通知用の関数　内部エラー発生時にハンドル
	OnWarn  logging.LoggerHandlerFunc
	OnInfo  logging.LoggerHandlerFunc
	OnDebug logging.LoggerHandlerFunc
	Tracer  pkgobserver.Tracer // observebility計装用
	Meter   pkgobserver.Meter  // observebility計装用

}

func NewStateStore(cfg ConfigProvider) (*StateStore, error) {
	if cfg == nil {
		return nil, errors.New("spool worker config is nil")
	}
	rootDir, err := spool.NewRootDir(
		cfg.SpoolPath(),
		cfg.Target(),
		cfg.Dir(),
	)
	if err != nil {
		return nil, err
	}

	return &StateStore{
		RootDir: rootDir,
		Config:  cfg,
	}, nil
}
