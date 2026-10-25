package shared

type RequestIDGenerator interface {
	NewID() string
}
