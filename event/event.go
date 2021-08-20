package event

import (
	"fmt"
	"time"

	"github.com/GoGraph/tbl"
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

func newUID() util.UID {

	// eventlock = new(eventLock)
	// eventlock.Lock()

	// create event UID
	uid, err := util.MakeUID()
	if err != nil {
		panic(fmt.Errorf("Failed to make event UID for Event New(): %w", err))
	}
	return uid
}

type Event interface {
	LogStart(...*mut.Mutation) error
	LogEvent(error, ...time.Time) error
	// Tx and mutation related
	NewMutation(tbl tbl.Name) *mut.Mutation
	//AddMember(string, interface{} )
	Add(*mut.Mutation)
	Persist() error
	Reset()
}

// event represents a staging area before data written to storage. Not all event related data
// is necessarily held in the struct.
type event struct {
	*tx.TxHandle
	event         string   // event name e.g "AN" (attachnode), "DN" (detachnode)
	eid           util.UID //pk
	status        byte     // "R" - Running, "C" - Completed, "F" - Failed
	start         time.Time
	loggedAtStart bool
}

func New(name string, start ...time.Time) Event {

	eid := newUID()
	// assign transaction handle
	e := &event{eid: eid, event: name, status: Running, TxHandle: tx.New("LogEvent")}
	if len(start) > 0 {
		e.start = start[0]
	} else {
		e.start = time.Now()
	}
	//db.LogEvent(x) - pointless as performed by defer in AttachNode() and mutations are run as single batch at end of event

	return e

}

func (e *event) LogStart(m ...*mut.Mutation) (err error) {
	//
	m0 := e.TxHandle.NewMutation(tbl.Event, e.eid, "", mut.Insert)
	m0.AddMember("event", e.event).AddMember("start", e.start).AddMember("status", string(e.status))
	e.Add(m0)
	if len(m) > 0 {
		e.Add(m[0])
	}
	e.loggedAtStart = true

	return e.Persist()
}

func (e *event) LogEvent(err error, finish ...time.Time) error {
	//
	var m *mut.Mutation
	if e.loggedAtStart {
		m = e.TxHandle.NewMutation(tbl.Event, e.eid, "", mut.Update)
	} else {
		m = e.TxHandle.NewMutation(tbl.Event, e.eid, "", mut.Insert)
	}

	if err != nil {
		m.AddMember("status", string(Failed)).AddMember("err", err.Error())
	} else {
		m.AddMember("status", string(Complete))
	}
	var f time.Time
	if len(finish) > 0 {
		f = finish[0]
	} else {
		f = time.Now()
	}
	m.AddMember("finish", f)
	m.AddMember("dur", f.Sub(e.start).String())
	e.Add(m)

	return e.Persist()

}

func (e *event) GetStartTime() time.Time {
	return e.start
}

func (e *event) NewMutation(t tbl.Name) *mut.Mutation {
	return mut.NewMutation(t, e.eid, "", mut.Insert)
}
