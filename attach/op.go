package main

import (
	"time"

	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/util"
)

type AttachOp struct {
	Puid, Cuid util.UID
	Sortk      string
}

var t0, t1 time.Time

// Status at Start of operation attach - for long running operations this function sets the status flag to "running" typically
// for shot operations thise is not much value.
// TODO: maybe some value in merging functionality of the event and op (operation) pacakages
func (a *AttachOp) Start() []*mut.Mutation {
	return nil
}

// StatusEnd - set the task status to completed or errored.
func (a *AttachOp) End(err error) []*mut.Mutation {

	if err != nil {

		m := make([]*mut.Mutation, 1, 1)
		e := "E"
		msg := err.Error()
		m[0] = mut.NewMutation(tbl.EdgeChild_, a.Puid, a.Sortk, mut.Update).AddMember("Status", e).AddMember("Cuid", a.Cuid, mut.IsKey).AddMember("ErrMsg", msg)
		return m

	} else {

		m := make([]*mut.Mutation, 2, 2)
		one := 1
		m[0] = mut.NewMutation(tbl.Edge_, a.Puid, "", mut.Update).AddMember("Cnt", one, mut.Subtract)
		c := "A"
		m[1] = mut.NewMutation(tbl.EdgeChild_, a.Puid, a.Sortk, mut.Update).AddMember("Status", c).AddMember("Cuid", a.Cuid, mut.IsKey)
		return m
	}

}
