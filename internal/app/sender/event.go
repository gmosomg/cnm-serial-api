package sender

import (
	"github.com/ozonmp/cnm-serial-api/internal/model"
)

type EventSender interface {
	Send(subdomain *model.SerialEvent) error
}
