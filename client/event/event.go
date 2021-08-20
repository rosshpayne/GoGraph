package event

import (
	"time"

	ev "github.com/GoGraph/event"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/util"
)

var evtbl tbl.Name = "NodeAttachDetachEvent"

func init() {
	tbl.Register(evtbl, "eid")
}

type AttachNode struct {
	ev.Event
	m *mut.Mutation // event data
	// cuid  []byte
	// puid  []byte
	// sortk string
}

func NewAttachNode(puid, cuid util.UID, sortk string, start ...time.Time) *AttachNode {
	an := &AttachNode{} //cuid: cuid, puid: puid, sortk: sortk}
	if len(start) > 0 {
		an.Event = ev.New("Attach", start[0])
	} else {
		an.Event = ev.New("Attach")
	}

	m := an.NewMutation(evtbl)
	m.AddMember("cuid", cuid).AddMember("puid", puid).AddMember("sortk", sortk)
	an.m = m

	return an
}

func (e *AttachNode) LogStart() (err error) {
	return e.Event.LogStart(e.m)
}

// LogEvent
func (e *AttachNode) LogEvent(err error, finish ...time.Time) error {

	// following code presumes event was not logged on start
	// em := e.event.LogEvent(finish, "AN", duration, err)
	// m:=NewMutation(start, param.AttachDetachEventTbl, em.eid, "", mut.Insert)
	// .AddMember("cuid", e.cuid)
	// .AddMember("puid", e.puid)
	// .AddMember("sortk", e.sortk)
	// e.Add(m)

	// e.Persist()

	// following presumes event was logged on start. Only update finish time and status, error if any
	if len(finish) > 0 {
		e.Event.LogEvent(err, finish[0])
	} else {
		e.Event.LogEvent(err, time.Now())
	}

	return e.Persist()
}

type DetachNode struct {
	ev.Event
	cuid  []byte
	puid  []byte
	sortk string
}

func NewDetachNode(puid, cuid util.UID, sortk string, start ...time.Time) (*DetachNode, error) {

	dn := &DetachNode{} //cuid: cuid, puid: puid, sortk: sortk}
	if len(start) > 0 {
		dn.Event = ev.New("Detach", start[0])
	} else {
		dn.Event = ev.New("Detach")
	}
	// add attach-node mutation to event
	m := dn.NewMutation(evtbl)
	m.AddMember("cuid", cuid).AddMember("puid", puid).AddMember("sortk", sortk)
	dn.Add(m)
	// optionally - log event start - useful for long running events. Short events < 1sec not much point
	dn.LogStart()
	// alternatively do not log at start but only when finished.

	return dn, nil
}
