package event

import (
	"fmt"
	"time"

	"github.com/GoGraph/event/internal/db"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/util"
)

type evStatus byte

const (
	Complete evStatus = 'C' // completed
	Running           = 'R' // running
	Failed            = 'F' // failed
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

// type Event interface {
// 	Tag() string
// 	LogEvent(string, ...error)
// }

type event struct {
	tx     *tx.Handle
	tag    string // short name
	name   string
	eID    util.UID
	seq    int
	status byte // "R" - Running, "C" - Completed, "F" - Failed
	start  string
	dur    string
	err    string
}

func (e event) Tag() string {
	return "Meta"
}

func (e event) LogEvent(duration string, err error) error {
	//
	mut := mut.NewMutation(EventTbl, nil, nil, mode)
	mut.AddMember("eID", x.eID)
	mut.AddMember("seq", x.seq)

	mut.AddMember("start", x.start)
	mut.AddMember("dur", duration)
	if err != nil {
		mut.AddMember("status", Failed)
		mut.AddMember("err", err.Error())
	} else {
		mut.AddMember("status", Completed)
	}
	e.tx.Add(mut)
}

func newEvent(name string, tag string) *event {

	eID := newUID()
	// assign transaction handle
	txh := tx.New("LogEvent")
	m := &event{eid: eID, seq: 1, status: Running, start: time.Now().String(), tx: txh}
	m.tag = tag
	m.name = name
	//db.LogEvent(x) - pointless as performed by defer in AttachNode() and mutations are run as single batch at end of event

	return m

}

type AttachNode struct {
	event
	cuid  []byte
	puid  []byte
	sortk string
}

func NewAttachNode(puid, cuid util.UID, sortk string) (*AttachNode, error) {
	an := &AttachNode{cuid: cuid, puid: puid, sortk: sortK}
	an.event = newEvent("Attach Node", "AN")
	return an, err
}

func (a AttachNode) Tag() string {
	return "Attach-Node"
}

func (e AttachNode) LogEvent(duration string, err error) error {

	e.event.LogEvent(duration, err)

	mut = mut.NewMutation(ANEventTbl, nil, nil, tx.Insert)
	mut.AddMember("eID", e.base.eID)
	mut.AddMember("cuid", e.cuid)
	mut.AddMember("puid", e.puid)
	mut.AddMember("sortk", e.sortk)
	e.tx.Add(mut)

	e.tx.Execute()
}

type DetachNode struct {
	event
	cuid  []byte
	puid  []byte
	sortk string
}

func (a DetachNode) Tag() string {
	return "Attach-Node"
}

func (e DetachNode) LogEvent(duration string, err error) error {

	e.event.LogEvent(duration, err)

	mut = mut.NewMutation(ANEventTbl, nil, nil, tx.Insert)
	mut.AddMember("eID", e.base.eID)
	mut.AddMember("cuid", e.cuid)
	mut.AddMember("puid", e.puid)
	mut.AddMember("sortk", e.sortk)
	e.tx.Add(mut)

	e.tx.Execute()
}
func NewDetachNode(puid, cuid util.UID, sortk string) (*DetachNode, error) {
	an := &DetachNode{cuid: cuid, puid: puid, sortk: sortK}
	an.event = newEvent("Detach Node","DN"
	return an, err
}
