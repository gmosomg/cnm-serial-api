package model

type Serial struct {
	ID    uint64
	Title string
	Year  uint
}

type EventType uint8
type EventStatus uint8

const (
	Created EventType = iota
	Updated
	Removed

	Deffered EventStatus = iota
	Processed
)

type SerialEvent struct {
	ID     uint64
	Type   EventType
	Status EventStatus
	Entity *Serial
}
