package opensearch

import (
	"errors"
	"fmt"

	infraopensearch "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

var ErrInvalidBufferFlushBytes = errors.New("invalid OpenSearch buffer flush bytes")

func validateBufferFlushBytes(bufferFlushBytes int) error {
	if bufferFlushBytes <= 0 {
		return fmt.Errorf(
			"%w: value must be positive: %d",
			ErrInvalidBufferFlushBytes,
			bufferFlushBytes,
		)
	}
	return nil
}

// measureEncodedLogEntryDocumentBodySizeは、entryをOpenSearch documentへ変換した
// JSON bodyのbyte数を返します。BufferFlushBytesは早期flushのsoft thresholdなので、
// flush時に生成するdocument IDやBulk action metadataはここでは加算しません。
func measureEncodedLogEntryDocumentBodySize(
	entry *logging.LogEntry,
	converter LogEntryToDocumentConverter,
) (int, error) {
	if entry == nil {
		return 0, fmt.Errorf("OpenSearch log entry is nil")
	}
	if converter == nil {
		return 0, fmt.Errorf("OpenSearch log entry converter is not initialized")
	}

	document := pkgopensearch.DocumentWriteRequest{
		Doc: converter.convertLogEntryToDocument(entry),
	}
	documentBytes, err := infraopensearch.DocumentBodySize(document)
	if err != nil {
		return 0, fmt.Errorf("measure OpenSearch buffered document body: %w", err)
	}
	return documentBytes, nil
}
