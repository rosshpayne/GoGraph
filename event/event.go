package event

import (
	"fmt"
	"time"

	"github.com/GoGraph/event/internal/db"
	"github.com/GoGraph/util"
)

func newUID() (util.UID, error) {

	// eventlock = new(eventLock)
	// eventlock.Lock()

	// create event UID
	uid, err := util.MakeUID()
	if err != nil {
		return nil, fmt.Errorf("Failed to make event UID for Event New(): %w", err)
	}
	return uid, nil
}

type Event interface {
	event_()
	Tag() string
}

type EventMeta struct {
	EID    util.UID
	SEQ    int
	OP     string
	Status string
	Start  string
	Dur    string
	Err    string
}

func (e EventMeta) event_() {}
func (e EventMeta) Tag() string {
	return "Meta"
}

func New(eventData Event) ([]byte, error) {

	eID, err := newUID()
	if err != nil {
		return nil, err
	}

	m := EventMeta{EID: eID, SEQ: 1, Status: "I", Start: time.Now().String(), Dur: "_"}
	switch x := eventData.(type) {

	case AttachNode:
		m.OP = "AN"
		x.EventMeta = m
		db.LogEvent(x)

	case DetachNode:
		m.OP = "DN"
		x.EventMeta = m
		db.LogEvent(x)
	}

	return eID, nil

}

func LogEventSuccess(eID util.UID, duration string) error {
	//return nil
	return db.UpdateEvent(eID, "C", duration)
}

func LogEventFail(eID util.UID, duration string, err error) error {
	//return nil
	return db.UpdateEvent(eID, "F", duration, err)
}

type AttachNode struct {
	EventMeta
	CID []byte
	PID []byte
	SK  string
}

func (a AttachNode) Tag() string {
	return "Attach-Node"
}

type DetachNode struct {
	EventMeta
	CID []byte
	PID []byte
	SK  string
}

func (a DetachNode) Tag() string {
	return "Detach-Node"
}
