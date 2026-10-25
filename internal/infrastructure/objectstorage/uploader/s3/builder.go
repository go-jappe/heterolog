package s3

import (
	"context"

	infraobjectstorage "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage"
	infras3 "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/s3"
	"github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/uploader/compose"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	s3uploader "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
)

// Builders は S3 uploader 用の target builder 群を返します。
func Builders() map[objectstorage.UploadTarget]compose.Builder {
	return map[objectstorage.UploadTarget]compose.Builder{
		objectstorage.UploadTargetS3: New,
	}
}

// DestinationIDResolversは、S3 clientを構築せずに設定検証と宛先IDの生成を行います。
// IDの生成はNewと同じResolveDestinationIDsを使います。
func DestinationIDResolvers() map[objectstorage.UploadTarget]compose.DestinationIDResolver {
	return map[objectstorage.UploadTarget]compose.DestinationIDResolver{
		objectstorage.UploadTargetS3: func(cfg objectstorage.UploadTargetConfigProvider) ([]objectstorage.DestinationID, error) {
			return s3uploader.ResolveDestinationIDs(s3DestinationConfigs(cfg))
		},
	}
}

// Newは全S3宛先について、一回のPUTで保存するuploaderと安定した宛先IDを組み立てます。
// client内ではretryせず、失敗後の保持や再送は各呼び出し経路が担当します。
func New(cfg objectstorage.UploadTargetConfigProvider) ([]uploader.Destination, error) {
	configs := s3DestinationConfigs(cfg)
	ids, err := s3uploader.ResolveDestinationIDs(configs)
	if err != nil {
		return nil, err
	}
	destinations := make([]uploader.Destination, 0, len(configs))
	for index, config := range configs {
		client, err := s3uploader.New(s3uploader.Config{S3Config: config})
		if err != nil {
			return nil, err
		}
		destinations = append(destinations, uploader.Destination{
			ID:       ids[index],
			Service:  objectstorage.UploadTargetS3,
			Uploader: infraobjectstorage.NewObjectKeyUploader(client, config.Prefix),
			ClassifyFailure: func(ctx context.Context, err error) uploader.FailureCategory {
				return infras3.ClassifyDurableUploadFailure(ctx, err).FailureCategory
			},
		})
	}
	return destinations, nil
}

// s3DestinationConfigsは、adapterから取得した各宛先の値をS3 client用の入力へ変換します。
// 宛先の順序と、明示的な空一覧をそのまま引き継ぎます。
func s3DestinationConfigs(cfg objectstorage.UploadTargetConfigProvider) []objectstorage.S3Config {
	providers := cfg.S3DestinationConfigs()
	configs := make([]objectstorage.S3Config, len(providers))
	for index, provider := range providers {
		configs[index] = s3uploader.ConfigFromProvider(provider)
	}
	return configs
}
