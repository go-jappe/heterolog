package cloudwatchlogs

const (
	// ErrorTypeResourceNotFoundException is the error type that CloudWatch Logs
	// returns for a resource that does not exist.
	ErrorTypeResourceNotFoundException = "ResourceNotFoundException"

	// ErrorTypeResourceAlreadyExistsException is the error type that CloudWatch
	// Logs returns for an attempt to create a resource that already exists.
	ErrorTypeResourceAlreadyExistsException = "ResourceAlreadyExistsException"

	// ErrorTypeOperationAbortedException is the error type that CloudWatch Logs
	// returns on a conflict, such as concurrent updates.
	ErrorTypeOperationAbortedException = "OperationAbortedException"

	// ErrorTypeRequestAbortedException is the error type, common to AWS
	// services, for a request whose processing was aborted before a response
	// was returned. It is used to classify retries when it is received as a
	// backend response, as distinct from the end of the caller's context.
	ErrorTypeRequestAbortedException = "RequestAbortedException"
)
