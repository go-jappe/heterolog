package s3

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

const destinationIDPrefix = "s3-"

// ResolveDestinationIDsは全宛先を検証し、設定順にIDを返します。
// 認証情報だけ異なる同一保存先など、IDが重複する指定は拒否します。
func ResolveDestinationIDs(configs []objectstorage.S3Config) ([]objectstorage.DestinationID, error) {
	if len(configs) == 0 {
		return nil, fmt.Errorf("s3 destinations are required")
	}
	ids := make([]objectstorage.DestinationID, 0, len(configs))
	seen := make(map[objectstorage.DestinationID]struct{}, len(configs))
	for index, cfg := range configs {
		if err := ValidateConfig(cfg); err != nil {
			return nil, fmt.Errorf("s3 destination at index %d: %w", index, err)
		}
		id, err := ResolveDestinationID(cfg)
		if err != nil {
			return nil, fmt.Errorf("s3 destination at index %d: %w", index, err)
		}
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("s3 destination at index %d: %w", index, uploader.ErrDuplicateDestinationID)
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	return ids, nil
}

// ResolveDestinationIDは、S3の実保存先を決める値から安定したidentityを生成します。
// endpoint、region、bucket、ObjectKeyUploaderが実際に使うprefixを含めます。
// credential、timeout、force-path-styleは配送方法の設定であり、同じobject
// namespaceへのcheckpointを継続できるようidentityには含めません。
//
// identityはProvider、Endpoint、Region、Bucket、Prefixをこのfield順のJSON objectへ
// encodeし、そのbyte列のSHA-256をlowercase hexadecimalへ変換して生成します。値には
// 次の正規化を適用します。
//
//   - Provider: `s3`
//   - Endpoint: default endpointを補完し、schemeとhostをlowercase化する。
//     queryまたはfragmentを含むendpointはValidateConfigと同じく拒否する
//   - Region: trim後にlowercase化し、大文字小文字だけが異なるregionを同じ保存先とする。
//     Newはlowercase化せず、前後の空白だけを除いた入力値でSigV4署名する
//   - Bucket: 前後の空白を除去する
//   - Prefix: 前後のslashを除去する
//
// 戻り値は`s3-<64文字のSHA-256 hexadecimal>`です。このIDはcheckpoint file名にも
// 保存される永続形式なので、field順、JSON field名、正規化方法、hash algorithm、encodingを
// 変更する場合はcheckpoint migrationも必要です。
func ResolveDestinationID(cfg objectstorage.S3Config) (objectstorage.DestinationID, error) {
	region := normalizeRegion(cfg.Region)
	endpoint, err := normalizeEndpoint(cfg.Endpoint, region)
	if err != nil {
		return "", err
	}
	endpoint, err = canonicalDestinationEndpoint(endpoint)
	if err != nil {
		return "", err
	}

	identity := struct {
		Provider string `json:"provider"`
		Endpoint string `json:"endpoint"`
		Region   string `json:"region"`
		Bucket   string `json:"bucket"`
		Prefix   string `json:"prefix"`
	}{
		Provider: string(objectstorage.UploadTargetS3),
		Endpoint: endpoint,
		Region:   region,
		Bucket:   strings.TrimSpace(cfg.Bucket),
		Prefix:   strings.Trim(cfg.Prefix, "/"),
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return objectstorage.DestinationID(destinationIDPrefix + hex.EncodeToString(digest[:])), nil
}

// canonicalDestinationEndpointは、normalizeEndpointで検証済みのendpointについて
// 大文字小文字を区別しないschemeとhostをlowercaseへ揃えます。queryとfragmentは
// normalizeEndpointが拒否するため、identityには含まれません。
func canonicalDestinationEndpoint(endpoint string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	parsed.Host = strings.ToLower(parsed.Host)
	return parsed.String(), nil
}
