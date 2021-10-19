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

func New(name string) *Event {
	eid := newUID()
	// assign transaction handle
	e := &Event{eid: eid, event: name, status: Running, TxHandle: tx.New("LogEvent-" + name)}

	m0 := e.TxHandle.NewMutation(tbl.Event, e.eid, "", mut.Insert)
	m0.AddMember("event", e.event)
	e.Add(m0)

	return e

}

func (e *Event) Start(start ...time.Time) {
	//
	if len(start) > 0 {
		e.start = start[0]
	} else {
		e.start = time.Now()
	}
	m0 := e.GetMutation(0)
	m0.AddMember("start", e.start)

}

func (e *Event) LogStart(t ...time.Time) (err error) {
	//
	e.loggedAtStart = true
	if len(t) > 0 {
		e.start = t[0]
	} else {
		e.start = time.Now()
	}
	m0 := e.GetMutation(0)
	m0.AddMember("start", e.start)

	return e.Persist()
}

func (e *Event) LogComplete(err error, ef ...time.Time) error {
	//
	var (
		m  *mut.Mutation
		et time.Time = time.Now()
	)
	if e.loggedAtStart {

		m = e.TxHandle.NewMutation(tbl.Event, e.eid, "", mut.Update)
		// fs represents start time
		if len(ef) > 0 {
			m.AddMember("finish", ef[0])
			m.AddMember("dur", ef[0].Sub(e.start).String())
		} else {
			m.AddMember("finish", et)
			m.AddMember("dur", et.Sub(e.start).String())
		}

	} else {

		m = e.GetMutation(0)
		if len(ef) > 0 {
			m.AddMember("finish", ef[0])
			m.AddMember("dur", ef[0].Sub(e.start).String())
		} else {
			m.AddMember("finish", et)
			m.AddMember("dur", et.Sub(e.start).String())
		}

	}

	if err != nil {
		m.AddMember("status", string(Failed)).AddMember("err", err.Error())
	} else {
		m.AddMember("status", string(Complete))
	}

	return e.Persist()

}
func (e *Event) GetStartTime() time.Time {
	return e.start
}

func (e *Event) NewMutation(t tbl.Name) *mut.Mutation {
	return mut.NewMutation(t, e.eid, "", mut.Insert)
}
