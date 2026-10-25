package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

const tmpFileSuffix = ".tmp"

// ErrInvalidStateは、保存先として扱えないspool stateが指定されたことを表します。
var ErrInvalidState = errors.New("invalid spool state")

// spoolFileModeはspool fileのUnix権限です。os.CreateTempが作成時に使う0600を維持し、
// 他のstoreと同じく、保存したrecordを所有者以外へ公開しません。
const spoolFileMode = 0o600

// ファイル保存先ディレクトリ例:
//
//	ex.) /var/lib/heterolog/spool/opensearch/active/{yyyy}/{mm}/{dd}/{hh}/{mm}
//
// ファイル名例:
//
//	ex.) 05321_123456789.hlogspool
//
// ファイル名は「秒2桁 + ミリ秒3桁 + 一意サフィックス + 拡張子」とする。
//
//	05        = 秒
//	321       = ミリ秒
//	_123...   = os.CreateTemp が付与する一意サフィックス
//	.hlogspool = spool ファイル拡張子
type SpoolStore struct {
	clock   clock.Clock
	Config  SpoolStoreConfigProvider
	RootDir spool.Dir // 保存先ルートパス ex.) /var/lib/hetelolog/spool/opensearch

	OnError logging.LoggerErrorHandler
	OnWarn  logging.LoggerHandlerFunc
	OnInfo  logging.LoggerHandlerFunc
	OnDebug logging.LoggerHandlerFunc
	Tracer  observer.Tracer
	Meter   observer.Meter
}

func (s *SpoolStore) stateDir(state pkgspool.State) (spool.StateDir, error) {
	switch state {
	case pkgspool.StateActive:
		return s.RootDir.ActiveDir(), nil
	case pkgspool.StateProcessing:
		return s.RootDir.ProcessingDir(), nil
	case pkgspool.StateDone:
		return s.RootDir.DoneDir(), nil
	case pkgspool.StateDead:
		return s.RootDir.DeadDir(), nil
	case pkgspool.StateQuarantine:
		return s.RootDir.QuarantineDir(), nil
	default:
		return spool.StateDir{}, fmt.Errorf("%w: %q", ErrInvalidState, state)
	}
}

func NewSpoolStore(
	timeSource clock.Clock,
	cfg SpoolStoreConfigProvider,
	tracer observer.Tracer,
	meter observer.Meter,
) (*SpoolStore, error) {
	if timeSource == nil {
		return nil, errors.New("spool store clock is nil")
	}
	rootDir, err := spool.NewRootDir(
		cfg.SpoolPath(),
		cfg.Target(),
		cfg.Dir(),
	)
	if err != nil {
		return nil, err
	}

	return &SpoolStore{
		clock:   timeSource,
		Config:  cfg,
		RootDir: rootDir,
		Tracer:  tracer,
		Meter:   meter,
	}, nil
}

// StoreByStateはrecordsをstate配下へ1ファイルのNDJSONとして保存します。
// ファイル本体と公開先ディレクトリの同期が完了した場合だけnilを返します。
// ErrDurabilityUnconfirmedでも公開済みファイルが残ることがあり、呼出し側の
// 再試行で同じrecordが複数保存され得ます。入力recordsは変更しません。
func (s *SpoolStore) StoreByState(
	ctx context.Context,
	state pkgspool.State,
	records []pkgspool.SpoolRecord,
) error {
	if len(records) == 0 {
		return nil
	}

	// Clockの時刻とタイムゾーンをそのまま日時別保存ディレクトリに使います。
	storedAt := s.clock.Now()

	body, err := buildSpoolNDJSON(records)
	if err != nil {
		return fmt.Errorf("failed to build spool ndjson: %w", err)
	}

	return s.writeStateBody(ctx, state, storedAt, body)
}

// EnsureCurrentTimePartitionDirectoryは、state配下にClockの現在時刻で区切った保存先を用意します。
// ディレクトリの形式はYYYY/MM/DD/HH/mmです。
// 保存先を塞ぐファイルやリンクを.preservedへ隔離してOnWarnへ通知し、正常な
// ディレクトリ・root自身・リンク先のデータは動かしません。過負荷検査の前に呼び、
// 不正な項目を正常なspoolの使用量と誤認してログ保存を拒否することを防ぎます。
func (s *SpoolStore) EnsureCurrentTimePartitionDirectory(ctx context.Context, state pkgspool.State) error {
	directory, err := s.timePartitionDirectory(state, s.clock.Now())
	if err != nil {
		return err
	}
	return s.ensureDirectory(ctx, directory)
}

func (s *SpoolStore) ensureDirectory(ctx context.Context, directory string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	preserved, err := spool.EnsureDirectoryWithPreservation(ctx, s.RootDir.String(), directory)
	// 検査・隔離・再作成の排他は既に解放済みです。同期失敗時も移動済みの項目を通知します。
	for _, result := range preserved {
		if s.OnWarn != nil {
			s.OnWarn(context.WithoutCancel(ctx), &logging.LogEntry{
				OccurredAt: s.clock.Now(),
				Level:      pkglogger.LogLevelWarn,
				Message:    "preserved an entry obstructing the spool directory",
				Err:        result.Reason,
				Internal:   true,
				Source:     "failure_spool",
				Fields: map[string]any{
					"original_path": result.OriginalPath,
					"retained_path": result.RetainedPath,
				},
			}, s.Tracer, s.Meter)
		}
	}
	return err
}

func (s *SpoolStore) timePartitionDirectory(state pkgspool.State, storedAt time.Time) (string, error) {
	stateDir, err := s.stateDir(state)
	if err != nil {
		return "", fmt.Errorf("failed to resolve state dir: %w", err)
	}
	return stateDir.TimePartitionDir(
		storedAt.Format("2006"), // YYYY
		storedAt.Format("01"),   // mm
		storedAt.Format("02"),   // dd
		storedAt.Format("15"),   // hh
		storedAt.Format("04"),   // ii
	), nil
}

func (s *SpoolStore) writeStateBody(
	ctx context.Context,
	state pkgspool.State,
	storedAt time.Time,
	body []byte,
) error {
	dir, err := s.timePartitionDirectory(state, storedAt)
	if err != nil {
		return err
	}

	// 保存先を塞ぐ不正な項目は隔離し、今回のbodyの保存をそのまま続けます。
	// 権限・I/Oエラーや安全に隔離できない場合は、書き込みを始めずエラーを返します。
	if err := s.ensureDirectory(ctx, dir); err != nil {
		return fmt.Errorf("failed to create spool directory: %w", err)
	}

	_, err = s.writeUniqueFilePath(
		dir,                          // ディレクトリ
		fileNameTimePrefix(storedAt), // 秒2桁 + ミリ秒3桁。ex.) 05321
		spool.SpoolExt,               // 拡張子
		body,                         // 本文
		(*os.File).Sync,
		infrafsstore.SyncDirectory,
	)
	if err != nil {
		return fmt.Errorf("failed to write spool file: %w", err)
	}

	return nil
}

// fileNameTimePrefix は、ファイル名の時刻 prefix を返す。
//
// 形式:
//
//	ssmmm
//
// 例:
//
//	05321
//
// 意味:
//
//	05  = 秒
//	321 = ミリ秒
func fileNameTimePrefix(t time.Time) string {
	return t.Format("05") + fmt.Sprintf("%03d", t.Nanosecond()/1e6)
}

// records を 1 行 1 レコードの NDJSON に変換する。
func buildSpoolNDJSON(records []pkgspool.SpoolRecord) ([]byte, error) {
	var buf bytes.Buffer

	for _, rec := range records {
		line, err := json.Marshal(rec)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal spool record: %w", err)
		}

		buf.Write(line)
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
}

// writeUniqueFilePath は、一意な spool ファイルを作成して、その最終パスを返す。
//
// 書き込み中のファイル名形式:
//
//	ssmmm_*.hlogspool.tmp
//
// 完成後のファイル名形式:
//
//	ssmmm_*.hlogspool
//
// 例:
//
//	書き込み中: 05321_123456789.hlogspool.tmp
//	完成後:     05321_123456789.hlogspool
//
// os.CreateTemp は pattern 内の "*" を一意な文字列に置換する。
// まず .tmp として権限設定 / Write / Sync / Close まで完了させ、.hlogspool へ rename する。
// 権限はos.CreateTempの0600を維持し、Windowsでは本文を書き込む前に実行ユーザー・
// SYSTEM・Administratorsだけへアクセスを許可する。
// これにより、worker が .hlogspool のみを処理すれば、書き込み途中のファイルを処理対象にしない。
// rename後は親ディレクトリを同期してから成功を返す。同期失敗時も公開済みファイルは
// 削除せず、そのpathとErrDurabilityUnconfirmedを返す。呼出し側は保存成功として扱わない。
func (s *SpoolStore) writeUniqueFilePath(
	dir string,
	timePrefix string,
	ext string,
	body []byte,
	syncFile func(*os.File) error,
	syncDirectory func(string) error,
) (string, error) {
	tmpPattern := fmt.Sprintf("%s_*%s%s", timePrefix, ext, tmpFileSuffix)

	// ファイル名にランダムな文字列を使用するためにCreateTempを使用
	f, err := os.CreateTemp(dir, tmpPattern)
	if err != nil {
		return "", fmt.Errorf("failed to create temp spool file: %w", err)
	}

	tmpPath := f.Name()
	finalPath := strings.TrimSuffix(tmpPath, tmpFileSuffix)
	// 本文より先に権限を確定し、record全体を所有者以外が読める期間を作りません。
	if err := infrafsstore.SetPrivateFilePermissions(f, spoolFileMode); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("failed to set spool file permissions: %w", err)
	}

	if _, err := f.Write(body); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("failed to write spool file body: %w", err)
	}

	if err := syncFile(f); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("%w: failed to sync spool file: %w", pkgfsstore.ErrDurabilityUnconfirmed, err)
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("failed to close spool file: %w", err)
	}

	// Unix 系の os.Rename は既存ファイルを置換し得るため、念のため事前に確認する。
	// CreateTemp の一意サフィックスにより通常は衝突しないが、既存ファイルを誤って上書きしないための防御。
	if _, err := os.Lstat(finalPath); err == nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("final spool file already exists: %s", finalPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("failed to check final spool file: %w", err)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("failed to rename temp spool file: %w", err)
	}
	if err := syncDirectory(dir); err != nil {
		return finalPath, fmt.Errorf("%w: failed to sync published spool file directory: %w", pkgfsstore.ErrDurabilityUnconfirmed, err)
	}

	return finalPath, nil
}
