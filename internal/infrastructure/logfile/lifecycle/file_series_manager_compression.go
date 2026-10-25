package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// cleanupCompressionSourceDeletePendingPathsは、過去の圧縮でcanonical名から退避済みだが
// process停止やRemove失敗で残ったsourceを、このfile系列の次周期で再発見します。
// rawと内容が異なるpendingは削除せず隔離し、保全先を含むerrorを返します。
// pending pathは通常のmanaged file globから意図的に除外されるため、専用patternを使います。
// 戻り値のpathsは、今回の実行でcleanupを完了できたpending pathだけを含みます。
func (m *FileSeriesManager) cleanupCompressionSourceDeletePendingPaths(
	ctx context.Context,
	reports ...*RunReport,
) (recoveredPaths []string, err error) {
	fileName, err := m.fileNamePattern()
	if err != nil {
		return nil, err
	}
	paths, err := logpattern.FindCompressionSourceDeletePendingPaths(
		fileName,
		logpattern.GlobByTarget,
	)
	if err != nil {
		return nil, err
	}

	var errs []error
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		// sourceも残る場合は内容一致を確認してpendingを削除し、異なる場合は隔離します。
		// sourceがない通常の回復ではraw→pending renameを同期してから削除します。
		if _, err := appendlog.RecoverCompressionSourceDeletePendingPath(path); err != nil {
			for _, report := range reports {
				m.recordRecoveryError(report, path, err)
			}
			// 通常周期で受理した既知の削除待ちだけを個別に引き継ぎます。
			// 競合・非通常fileは管理者確認へ残し、終了時に新たな走査はしません。
			_, inspectionErr := fsstore.InspectRegularFile(path)
			if !errors.Is(err, appendlog.ErrCompressionSourceDeletePendingConflict) &&
				(inspectionErr == nil || errors.Is(err, appendlog.ErrCompressionSourceCleanup)) {
				if source, ok := logpattern.CompressionSourcePathFromDeletePendingPath(path); ok {
					m.scheduleCompressionSourceCleanup(source, err)
				}
			}
			errs = append(errs, err)
			continue
		}
		recoveredPaths = append(recoveredPaths, path)
	}
	return recoveredPaths, errors.Join(errs...)
}

// isCompressionSourceは、このFileSeriesManagerのFileNamePatternに一致する未圧縮fileかを判定します。
// 通常logは`.log`、failure archiveは`.jsonl`を使うため、拡張子を固定しません。
func (m *FileSeriesManager) isCompressionSource(path string) bool {
	return m.Config.Lifecycle().EnableFileCompress() &&
		m.hasUncompressedExtension(path)
}

// correspondingCompressedFileExistsは、sourceに対応する読み取り可能な
// 圧縮済み通常fileが存在するかを返します。
// sourceと同じpathになる不正な圧縮形式は未commitとして扱い、実際の圧縮処理で
// unsupported format errorを返します。存在確認不能や通常file以外のpeerは、既存pathの
// 上書きとsourceのpurgeをどちらも避けるためerrorにします。
// 通常fileをdecodeできない場合もerrorを返します。内容破損と確定できたpeerは
// 呼び出し元で削除し、その世代の再圧縮を次周期へ回します。他の世代は継続します。
// この判定method自体はfilesystemを変更しません。
// upload済みかは圧縮commitの条件に含めません。通常経路でもrenameをcommit境界として
// sourceを削除してからuploadするため、圧縮済みfileのobject storage upload状態は別に管理します。
func (m *FileSeriesManager) correspondingCompressedFileExists(path string) (bool, error) {
	if !m.isCompressionSource(path) {
		return false, nil
	}
	format := m.store.GetCompressFormat()
	compressedPath := fsstore.GenerateCompressedPath(path, format)
	return m.existingCompressedFileIsValid(path, compressedPath)
}

// existingCompressedFileIsValidは観測で解決済みのpeerを、そのfile自身のcodecで
// 検証します。組み込み形式では展開した内容がsourceと一致することも確認し、
// 完全でも内容の異なるpeerはfsstore.ErrCompressedContentMismatchのerrorにします。
// sourceが退避済みで存在しなければ、peerの完全性だけを確認します。
// 未知の旧codecは破損とも完了済みとも見なさずerrorで保全します。
func (m *FileSeriesManager) existingCompressedFileIsValid(path, compressedPath string) (bool, error) {
	if compressedPath == "" {
		return false, fmt.Errorf(
			"compression format %q has no valid file extension",
			m.store.GetCompressFormat(),
		)
	}
	info, err := appendlog.InspectCompressedFile(path, compressedPath)
	if err != nil {
		if errors.Is(err, appendlog.ErrCompressedPeerNotRegular) || errors.Is(err, pkgfsstore.ErrCorruptCompressedFile) {
			m.forgetVerifiedCompressedPeer(compressedPath)
		}
		return false, err
	}
	if info == nil {
		m.forgetVerifiedCompressedPeer(compressedPath)
		return false, nil
	}
	var source compressionSourceFingerprint
	if sourceInfo, err := os.Lstat(path); err == nil {
		source = compressionSourceFingerprint{
			exists:          true,
			size:            sourceInfo.Size(),
			modTimeUnixNano: sourceInfo.ModTime().UnixNano(),
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("inspect source %s for compressed peer %s: %w", path, compressedPath, err)
	}
	fingerprint := compressedPeerFingerprint{
		size:            info.Size(),
		modTimeUnixNano: info.ModTime().UnixNano(),
		source:          source,
	}
	verificationKey := pathidentity.ComparisonKey(compressedPath)
	m.compressedPeerCacheMu.Lock()
	cached, cachedExists := m.verifiedCompressedPeers[verificationKey]
	if cachedExists && cached != fingerprint {
		// 変更前の成功記録は、今回の再検証が失敗しても残しません。
		delete(m.verifiedCompressedPeers, verificationKey)
	}
	m.compressedPeerCacheMu.Unlock()
	if cachedExists && cached == fingerprint {
		return true, nil
	}

	var custom pkgfsstore.CompressionCodec
	if provider, ok := m.store.Config.(appendlog.CompressionCodecProvider); ok {
		custom = provider.CompressionCodec()
	}
	verificationErr := fsstore.VerifyExistingCompressedFileMatchesSource(path, compressedPath, custom)
	if verificationErr != nil {
		return false, fmt.Errorf(
			"verify compressed peer %s for source %s: %w",
			compressedPath,
			path,
			verificationErr,
		)
	}
	m.compressedPeerCacheMu.Lock()
	m.verifiedCompressedPeers[verificationKey] = fingerprint
	m.compressedPeerCacheMu.Unlock()
	return true, nil
}

// removeInvalidCompressedPeerは、decode検証に失敗した通常fileの圧縮済みpeerを削除し、
// 同じpathについて保持している検証済みcacheも破棄します。判定と削除を混在させないため、
// shouldProtectFromLifecyclePurgeから呼ばれるcorrespondingCompressedFileExistsには
// この副作用を持たせません。
// 呼び出し元はsourceを保持してこの世代のerrorを通知し、次周期に再圧縮します。
func (m *FileSeriesManager) removeInvalidCompressedPeer(sourcePath, compressedPath string) error {
	if compressedPath == "" {
		return fmt.Errorf(
			"compression format %q has no valid file extension",
			m.store.GetCompressFormat(),
		)
	}
	if err := appendlog.RemoveInvalidCompressedFile(sourcePath, compressedPath); err != nil {
		return err
	}

	m.forgetVerifiedCompressedPeer(compressedPath)
	return nil
}

// shouldProtectFromLifecyclePurgeは、圧縮またはobject storage uploadが未完了で
// 通常purgeから保護すべきfileを判定します。
// 圧縮済みpeerと共存するsourceは完了済み圧縮の残骸として通常purgeへ任せます。
func (m *FileSeriesManager) shouldProtectFromLifecyclePurge(path string) bool {
	if m.isCompressionSource(path) {
		correspondingFileExists, err := m.correspondingCompressedFileExists(path)
		return err != nil || !correspondingFileExists
	}
	generation, generationValid := m.parseFileGeneration(path)
	requirePathUploadProof := !m.hasUncompressedExtension(path) &&
		m.objectStorageUpload.uploadEnabled()
	return m.objectStorageUpload.uploadPending(
		generation,
		generationValid,
		requirePathUploadProof,
	)
}
