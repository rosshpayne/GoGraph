package event

import (
	ev "github.com/GoGraph/event"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/util"
)

var evtbl tbl.Name = "NodeAttachDetachEvent"

func init() {
	tbl.Register(evtbl, "eid")
}

// Attach Node Event

type AttachNode struct {
	*ev.Event
}

func NewAttachNode(puid, cuid util.UID, sortk string) *AttachNode {

	an := &AttachNode{} //cuid: cuid, puid: puid, sortk: sortk}
	an.Event = ev.New("Attach")
	an.Add(an.NewMutation(evtbl).AddMember("cuid", cuid).AddMember("puid", puid).AddMember("sortk", sortk))

	return an
}

type DetachNode struct {
	*ev.Event
	cuid  []byte
	puid  []byte
	sortk string
}

func NewDetachNode(puid, cuid util.UID, sortk string) *DetachNode {

	dn := &DetachNode{} //cuid: cuid, puid: puid, sortk: sortk}
	dn.Event = ev.New("Detach")
	dn.Add(dn.NewMutation(evtbl).AddMember("cuid", cuid).AddMember("puid", puid).AddMember("sortk", sortk))

	return dn
}
