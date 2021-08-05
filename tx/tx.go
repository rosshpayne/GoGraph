package tx

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/GoGraph/db"
)

constcd  (
	LogLabel = "txmgr: "
)

// Handle represents a transaction composed of 1 to many mutations.
type Handle struct {
	Label string
	ms    []Mutation
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

// Add appends another mutation (SQL statement, Dynamodb: PutItem, UpdateItem) to the transaction.
func (h *Handle) Add(m Mutation) {

	h.ms = append(h.ms, m)
}

func (h *Handle) Execute() {

	h.executeStart = time.Now()

	db.Execute(h)

}

func (h *Handle) GetMutations() []Mutation {

	return h.ms
}
