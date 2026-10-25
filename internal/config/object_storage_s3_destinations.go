package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/go-jappe/heterolog/internal/config/configerror"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// ErrInvalidS3Destinationsは、S3送信先一覧のJSONが、objectの空でない配列として解釈できないか、
// 未知のfieldや範囲外のtimeoutを含むことを表します。
var ErrInvalidS3Destinations = errors.New("invalid s3 destinations")

// parseS3Destinationsは読み込み済みのJSON一覧に、一件指定の設定を既定値として補います。
// 一覧を指定した場合は追加ではなく置換です。JSONにない項目だけを既定値から引き継ぎます。
// 認証情報を含むため、不正なJSONや値そのものはエラーメッセージへ含めません。
func parseS3Destinations(raw string, defaults objectstorage.S3Config) ([]objectstorage.S3Config, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var entries []json.RawMessage
	if err := json.Unmarshal([]byte(raw), &entries); err != nil || len(entries) == 0 {
		return nil, configerror.New(ErrInvalidS3Destinations, "s3 destinations must be a non-empty JSON array of objects")
	}
	configs := make([]objectstorage.S3Config, 0, len(entries))
	for index, entry := range entries {
		if len(entry) == 0 || entry[0] != '{' {
			return nil, configerror.New(ErrInvalidS3Destinations, "s3 destination at index %d must be an object", index)
		}
		value := struct {
			Region          string `json:"region"`
			Endpoint        string `json:"endpoint"`
			AccessKeyID     string `json:"access_key_id"`
			SecretAccessKey string `json:"secret_access_key"`
			SessionToken    string `json:"session_token"`
			Bucket          string `json:"bucket"`
			Prefix          string `json:"prefix"`
			ForcePathStyle  bool   `json:"force_path_style"`
			TimeoutSeconds  *int64 `json:"timeout_seconds"`
		}{
			Region: defaults.Region, Endpoint: defaults.Endpoint,
			AccessKeyID: defaults.AccessKeyID, SecretAccessKey: defaults.SecretAccessKey,
			SessionToken: defaults.SessionToken, Bucket: defaults.Bucket, Prefix: defaults.Prefix,
			ForcePathStyle: defaults.ForcePathStyle,
		}
		decoder := json.NewDecoder(strings.NewReader(string(entry)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&value); err != nil {
			return nil, configerror.New(
				ErrInvalidS3Destinations,
				"s3 destination at index %d has an unknown field or invalid field type",
				index,
			)
		}
		timeout := defaults.Timeout
		if value.TimeoutSeconds != nil {
			seconds := *value.TimeoutSeconds
			if seconds < 0 || seconds > math.MaxInt64/int64(time.Second) {
				return nil, configerror.New(
					ErrInvalidS3Destinations,
					"s3 destination at index %d: timeout_seconds must be nonnegative and fit time.Duration",
					index,
				)
			}
			timeout = time.Duration(seconds) * time.Second
		}
		configs = append(configs, objectstorage.S3Config{
			Region: value.Region, Endpoint: value.Endpoint,
			AccessKeyID: value.AccessKeyID, SecretAccessKey: value.SecretAccessKey,
			SessionToken: value.SessionToken, Bucket: value.Bucket, Prefix: value.Prefix,
			ForcePathStyle: value.ForcePathStyle, Timeout: timeout,
		})
	}
	return configs, nil
}

// configureLogFileS3Destinationsは各roleの一覧を設定します。error/debugの一覧が未指定なら
// defaultのJSONを使い、JSON内の省略項目には各roleの一件指定設定を補います。
func configureLogFileS3Destinations(cfg objectstorage.LogFileUploadConfig, defaultJSON, errorJSON, debugJSON string) (objectstorage.LogFileUploadConfig, error) {
	if !cfg.Enabled {
		return cfg, nil
	}
	var errs []error
	for _, role := range []struct {
		name   string
		raw    string
		target *objectstorage.UploadTargetConfig
	}{
		{"default", defaultJSON, &cfg.Default},
		{"error", errorJSON, &cfg.Error},
		{"debug", debugJSON, &cfg.Debug},
	} {
		if !role.target.Enabled || !role.target.Has(objectstorage.UploadTargetS3) {
			continue
		}
		raw := role.raw
		if strings.TrimSpace(raw) == "" {
			raw = defaultJSON
		}
		var err error
		role.target.S3Destinations, err = parseS3Destinations(raw, role.target.S3)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s log file upload: %w", role.name, err))
		}
	}
	return cfg, errors.Join(errs...)
}
