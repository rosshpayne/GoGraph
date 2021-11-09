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
	i   int // batch
	*mut.Mutations
	b     int //batch id
	batch []*mut.Mutations
	//muts []*mut.Mutation
	// TODO: should we have a logger??
	TransactionStart time.Time
	TransactionEnd   time.Time
	//
	Err error
}

// new transaction
func New(tag string, m ...*mut.Mutation) *TxHandle {

	tx := TxHandle{Tag: tag, Mutations: new(mut.Mutations)}

	if len(m) > 0 {
		for _, v := range m {
			tx.Add(v)
		}
	}

	return &tx

}

func (h *TxHandle) MakeBatch() error {
	if len(*h.Mutations) == 0 {
		return fmt.Errorf("No Mutations in transaction %s add to batch", h.Tag)
	}
	h.b++
	h.batch = append(h.batch, h.Mutations)
	//fmt.Println("MakeBatch: len h.batch b ", len(h.batch), h.b, len(*h.batch[len(h.batch)-1]))
	h.Mutations = new(mut.Mutations)
	return nil
}

func (h *TxHandle) NewMutation(table tbl.Name, pk util.UID, sk string, opr mut.StdDML) *mut.Mutation {
	return mut.NewMutation(table, pk, sk, opr)
}

// func (h *TxHandle) NewInsert(table tbl.Name) *mut.Mutation {
// 	return mut.NewInsert(table)
// }

// func (h *TxHandle) NewBulkInsert(table tbl.Name) *mut.Mutation {
// 	return mut.NewBulkInsert(table)
// }

func (h *TxHandle) Persist() error {
	return h.Execute()
}

func (h *TxHandle) Execute(m ...*mut.Mutation) error {

	// fmt.Println("tx Execute()")
	// fmt.Println("h.b= ", h.b)
	// fmt.Println("len(h.batch): ", len(h.batch))
	// fmt.Println("len(*h.Mutations)= ", len(*h.Mutations))

	if len(m) > 0 {
		for _, v := range m {
			h.Add(v)
		}
	}
	if h.b == 0 && len(*h.Mutations) == 0 {
		syslog(fmt.Sprintf("No mutations in transaction %s to execute", h.Tag))
		return nil
	}
	if h.b == 0 || h.batch[len(h.batch)-1] != h.Mutations {
		if err := h.MakeBatch(); err != nil {
			return err
		}
	}

	h.TransactionStart = time.Now()

	err := db.Execute(h.batch, h.Tag)

	h.TransactionEnd = time.Now()

	if err == nil {
		h.Mutations.Reset()
		h.batch = nil
	}
	return err

}
