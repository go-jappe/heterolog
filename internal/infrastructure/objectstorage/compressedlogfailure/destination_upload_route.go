package compressedlogfailure

import (
	"context"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// destinationUploadRouteは、複数宛先Uploaderからspoolへ登録する一宛先のIDを返します。
// WorkerがこのIDを指定して送信し、返った結果の宛先と件数を検証します。
// DestinationIDは設定との照合キーであり、IDから接続情報を復元しません。
type destinationUploadRoute struct {
	next        uploader.DestinationUploader
	destination objectstorage.DestinationID
}

func (r destinationUploadRoute) DestinationIDs() []objectstorage.DestinationID {
	return []objectstorage.DestinationID{r.destination}
}

func (r destinationUploadRoute) Upload(ctx context.Context, request uploader.UploadRequest) error {
	return r.UploadDestinations(ctx, request, r.DestinationIDs()).Err()
}

func (r destinationUploadRoute) UploadDestinations(ctx context.Context, request uploader.UploadRequest, ids []objectstorage.DestinationID) uploader.UploadReport {
	return r.next.UploadDestinations(ctx, request, ids)
}
