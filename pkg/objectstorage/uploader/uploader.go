package uploader

import (
	"context"
	"errors"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

var (
	// ErrDestinationIDRequiredは、宛先identityが空であることを表します。
	ErrDestinationIDRequired = errors.New("object storage destination id is required")
	// ErrDuplicateDestinationIDは、同じ宛先identityが複数指定されたことを表します。
	ErrDuplicateDestinationID = errors.New("duplicate object storage destination id")
	// ErrUnknownDestinationIDは、fan-out uploaderに登録されていない宛先を表します。
	ErrUnknownDestinationID = errors.New("unknown object storage destination id")
)

// Uploaderは送信元ファイルを受け付け、宛先ごとの保存名を決めて送信します。
type Uploader interface {
	Upload(ctx context.Context, request UploadRequest) error
}

// Destinationは、一つの物理送信先の安定したidentityと、その送信実装を対応付けます。
type Destination struct {
	ID       objectstorage.DestinationID
	Uploader Uploader
	// Serviceは、この宛先のbackendです。退避先と再送用adapterの構築に使います。
	// 宛先IDから推測せず、backendのbuilderが設定します。
	Service objectstorage.UploadTarget
	// ClassifyFailureは、この送信先の失敗をbackend固有の規則で分類します。
	// 未指定、または有効な分類を返さない場合はretryableとして扱います。
	ClassifyFailure func(context.Context, error) FailureCategory
}

// DestinationServiceProviderは、宛先ごとの退避先と再送用adapterを構築するための契約です。
// 未登録の宛先、またはbackendが未設定の場合はfalseを返します。
type DestinationServiceProvider interface {
	DestinationService(objectstorage.DestinationID) (objectstorage.UploadTarget, bool)
}

// DestinationResultは、一つの宛先に対するupload結果です。
// Errがnilなら成功、非nilなら失敗です。相反するstatusとerrorを保持しないことで、
// checkpoint側が結果を一意に判断できるようにします。
// upload失敗時の保存を組み込んだuploaderでは、全指定退避先への保存完了も成功として返します。
// その場合、Errがnilでもobject storageへの送信成功だけを意味するわけではありません。
type DestinationResult struct {
	DestinationID objectstorage.DestinationID
	Err           error
	// FailureCategoryはErrが非nilの場合のbackend固有の分類です。
	// 分類を持たない送信実装からの失敗もretryableとして扱います。
	FailureCategory FailureCategory
}

func (r DestinationResult) Succeeded() bool {
	return r.Err == nil
}

// UploadReportは、要求された宛先ごとの結果を要求順に保持します。
type UploadReport struct {
	Results []DestinationResult
}

// Errは失敗した宛先ごとの*DestinationUploadErrorを、要求順にerrors.Joinでまとめます。
// 失敗がなければnilです。
func (r UploadReport) Err() error {
	errs := make([]error, 0, len(r.Results))
	for _, result := range r.Results {
		if result.Err == nil {
			continue
		}
		errs = append(errs, &DestinationUploadError{Destination: result.DestinationID, Err: result.Err})
	}
	return errors.Join(errs...)
}

// DestinationUploaderは、複数宛先を個別に送信・分類できるfan-out uploaderです。
// Uploadは全宛先への互換入口で、UploadDestinationsは指定された宛先だけを処理します。
type DestinationUploader interface {
	Uploader
	DestinationIDs() []objectstorage.DestinationID
	UploadDestinations(
		ctx context.Context,
		request UploadRequest,
		destinationIDs []objectstorage.DestinationID,
	) UploadReport
}
