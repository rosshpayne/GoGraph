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

// interface unecessary as event is only type.
//
// type Event interface {
// 	LogStart(...*mut.Mutation) error
// 	LogEvent(error, ...time.Time) error
// 	// Tx and mutation related
// 	NewMutation(tbl tbl.Name) *mut.Mutation
// 	//AddMember(string, interface{} )
// 	Add(*mut.Mutation)
// 	Persist() error
// 	Reset()
// }

// event represents a staging area before data written to storage. Not all event related data
// is necessarily held in the struct.
type Event struct {
	*tx.TxHandle
	event         string   // event name e.g "AN" (attachnode), "DN" (detachnode)
	eid           util.UID //pk
	status        byte     // "R" - Running, "C" - Completed, "F" - Failed
	start         time.Time
	loggedAtStart bool
}

func New(name string, start ...time.Time) *Event {

	eid := newUID()
	// assign transaction handle
	e := &Event{eid: eid, event: name, status: Running, TxHandle: tx.New("LogEvent")}
	if len(start) > 0 {
		e.start = start[0]
	} else {
		e.start = time.Now()
	}
	//db.LogEvent(x) - pointless as performed by defer in AttachNode() and mutations are run as single batch at end of event

	return e

}

func (e *Event) LogStart(m ...*mut.Mutation) (err error) {
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

// LogEvent inserts a new event if there no previously created log entry for this event
// or updates the event with the finish time if a log entry (start) exists
func (e *Event) LogEvent(err error, fs ...time.Time) error {
	//
	var (
		m  *mut.Mutation
		et time.Time = time.Now()
	)

	if e.loggedAtStart {

		m = e.TxHandle.NewMutation(tbl.Event, e.eid, "", mut.Update)
		// fs represents start time
		if len(fs) > 0 {
			m.AddMember("finish", fs[0])
			m.AddMember("dur", fs[0].Sub(e.start).String())
		} else {
			m.AddMember("finish", et)
			m.AddMember("dur", et.Sub(e.start).String())
		}

	} else {

		m = e.TxHandle.NewMutation(tbl.Event, e.eid, "", mut.Insert)
		if len(fs) > 0 {
			m.AddMember("start", fs[0])
			m.AddMember("dur", et.Sub(fs[0]).String())
		} else {
			m.AddMember("start", et)
		}

	}
	if err != nil {
		m.AddMember("status", string(Failed)).AddMember("err", err.Error())
	} else {
		m.AddMember("status", string(Complete))
	}
	e.Add(m)

	return e.Persist()

}

func (e *Event) GetStartTime() time.Time {
	return e.start
}

func (e *Event) NewMutation(t tbl.Name) *mut.Mutation {
	return mut.NewMutation(t, e.eid, "", mut.Insert)
}
