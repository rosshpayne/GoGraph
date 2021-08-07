package event

import (
	"fmt"
	"time"

	"github.com/GoGraph/event/internal/db"
	"github.com/GoGraph/tx"
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
	Tag() string
	LogEvent(string, ...error)
}

type event struct {
	tx     *tx.Handle
	tag    string
	eID    util.UID
	seq    int
	status string
	start  string
	dur    string
	err    string
}

func (e event) Tag() string {
	return "Meta"
}

func (e event) LogEvent(duration string, err ...error) error {
	//return nil
	if len(err) > 0 {
		return db.logEvent(e, "F", duration, err)
	} else {
		return db.logEvent(e, "C", duration)
	}
}

func newEvent(e Event) error {

	eID, err := newUID()
	if err != nil {
		return nil, err
	}
	// assign transaction handle
	txh := tx.New("LogEvent")
	m := event{eid: eID, seq: 1, status: "I", start: time.Now().String(), tx: txh}
	switch x := e.(type) {

	case attachNode:
		m.tag = "AN"
		x.Event = m
		//db.logEvent(x) - pointless as performed by defer in AttachNode()

	case detachNode:
		m.tag = "DN"
		x.Event = m
		//db.logEvent(x)
	}

	return nil

}

// func LogEventSuccess(eID util.UID, duration string) error {
// 	//return nil
// 	return db.UpdateEvent(eID, "C", duration)
// }

// func LogEventFail(eID util.UID, duration string, err error) error {
// 	//return nil
// 	return db.UpdateEvent(eID, "F", duration, err)
// }

type AttachNode struct {
	Event
	cuid  []byte
	puid  []byte
	sortk string
}

func NewAttachNode(puid, cuid util.UID, sortk string) (AttachNode, error) {
	an := AttachNode{cuid: cuid, puid: puid, sortk: sortK}
	err := newEvent(an)
	return an, err
}

func (a AttachNode) Tag() string {
	return "Attach-Node"
}

type DetachNode struct {
	Event
	cuid  []byte
	puid  []byte
	sortk string
}

func (a DetachNode) Tag() string {
	return "Detach-Node"
}

func NewDetachNode(puid, cuid util.UID, sortk string) (DetachNode, error) {
	an := detachNode{cuid: cuid, puid: puid, sortk: sortK}
	err := newEvent(an)
	return an, err
}
