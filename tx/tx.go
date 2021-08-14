package tx

import (
	"time"

	"github.com/GoGraph/db"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/util"
)

const (
	LogLabel = "tx: "
)

// Handle represents a transaction composed of 1 to many mutations.
type Handle struct {
	Label string
	mut.Mutations
	// TODO: should we have a logger??
	TransactionStart time.Time
	TransactionEnd   time.Time
	//
	Err error
}

// new transaction
func New(label string) *Handle {

	return &Handle{Label: label}
	//return &Handle{respCh: make(chan struct{}), Label: label}
}

// // Add appends another mutation (SQL statement, Dynamodb: PutItem, UpdateItem) to the transaction.
// func (h *Handle) Add(m mut.Mutation) {
// 	h.ms=h.Add(m)
// }

func NewMutation(table string, pk util.UID, sk string, opr interface{}) *mut.Mutation {
	return mut.NewMutation(table, pk, sk, opr)
}

func (h *Handle) Execute() error {

	h.TransactionStart = time.Now()

	err := db.Execute(h.Mutations)

	h.TransactionEnd = time.Now()

	return err

}
