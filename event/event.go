package event

import (
	"fmt"
	"strconv"
	"time"

	param "github.com/GoGraph/dygparam"
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

// type Event interface {
// 	Tag() string
// 	LogEvent(string, ...error)
// }

type event struct {
	tx     *tx.Handle
	tag    string // short name
	name   string
	eid    util.UID //pk
	seq    int      //sk
	status byte     // "R" - Running, "C" - Completed, "F" - Failed
	start  string
	dur    string
	err    string
}

func (e event) Tag() string {
	return "Meta"
}

func (e *event) LogEvent(duration string, err error) *mut.Mutation {
	//
	sk := strconv.Itoa(e.seq)
	mut := mut.NewMutation(param.EventTbl, e.eid, sk, mut.Insert)

	mut.AddMember("start", e.start)
	mut.AddMember("dur", duration)
	if err != nil {
		mut.AddMember("status", Failed)
		mut.AddMember("err", err.Error())
	} else {
		mut.AddMember("status", Complete)
	}
	e.tx.Add(mut)

	return mut
}

func newEvent(name string, tag string) *event {

	eid := newUID()
	// assign transaction handle
	txh := tx.New("LogEvent")
	m := &event{eid: eid, seq: 1, status: Running, start: time.Now().String(), tx: txh}
	m.tag = tag
	m.name = name
	//db.LogEvent(x) - pointless as performed by defer in AttachNode() and mutations are run as single batch at end of event

	return m

}

type AttachNode struct {
	*event
	cuid  []byte
	puid  []byte
	sortk string
}

func NewAttachNode(puid, cuid util.UID, sortk string) *AttachNode {
	an := &AttachNode{cuid: cuid, puid: puid, sortk: sortk}
	an.event = newEvent("Attach Node", "AN")
	return an
}

func (a AttachNode) Tag() string {
	return "Attach-Node"
}

func (e AttachNode) LogEvent(duration string, err error) {

	mut := e.event.LogEvent(duration, err)

	mut.AddMember("cuid", e.cuid)
	mut.AddMember("puid", e.puid)
	mut.AddMember("sortk", e.sortk)
	e.tx.Add(mut)

	e.tx.Execute()
}

type DetachNode struct {
	*event
	cuid  []byte
	puid  []byte
	sortk string
}

func (a DetachNode) Tag() string {
	return "Attach-Node"
}

func (e DetachNode) LogEvent(duration string, err error) {

	mut := e.event.LogEvent(duration, err)

	mut.AddMember("cuid", e.cuid)
	mut.AddMember("puid", e.puid)
	mut.AddMember("sortk", e.sortk)
	e.tx.Add(mut)

	e.tx.Execute()
}
func NewDetachNode(puid, cuid util.UID, sortk string) *DetachNode {
	an := &DetachNode{cuid: cuid, puid: puid, sortk: sortk}
	an.event = newEvent("Detach Node", "DN")
	return an
}
