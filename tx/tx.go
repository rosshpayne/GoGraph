package tx

import (
	"fmt"
	"time"

	"github.com/GoGraph/db"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/util"
)

func syslog(s string) {
	slog.Log("Tx:", s)
}

// Handle represents a transaction composed of 1 to many mutations.
type Handle = TxHandle

type TxHandle struct {
	Tag string
	*mut.Mutations
	//muts []*mut.Mutation
	// TODO: should we have a logger??
	TransactionStart time.Time
	TransactionEnd   time.Time
	//
	Err error
}

// new transaction
func New(tag string) *TxHandle {

	return &TxHandle{Tag: tag, Mutations: new(mut.Mutations)}

}

func (h *TxHandle) NewMutation(table tbl.Name, pk util.UID, sk string, opr mut.StdDML) *mut.Mutation {
	return mut.NewMutation(table, pk, sk, opr)
}

func (h *TxHandle) Persist() error {
	return h.Execute()
}

func (h *TxHandle) Execute() error {

	if len(*h.Mutations) != 0 {

		h.TransactionStart = time.Now()

		err := db.Execute(*h.Mutations, h.Tag)

		h.TransactionEnd = time.Now()

		if err == nil {
			h.Reset()
			//h.muts = nil
		}
		return err

	} else {

		syslog(fmt.Sprintf("No mutations in transaction %s ", h.Tag))
	}

	return nil

}
