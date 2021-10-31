package repo

import (
	"github.com/ozonmp/cnm-serial-api/internal/model"
)

type EventRepo interface {
	Lock(n uint64) ([]model.SerialEvent, error)
	Unlock(eventIDs []uint64) error

	Add(event []model.SerialEvent) error
	Remove(eventIDs []uint64) error
}
