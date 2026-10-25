package uuid

import "github.com/google/uuid"

type UUIDGenerator struct{}

func (g *UUIDGenerator) NewID() string {
	return uuid.NewString()
}
