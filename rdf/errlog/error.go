package errlog

import (
	"context"
	"sync"

	slog "github.com/GoGraph/syslog"
)

type Errors []*payload

type payload struct {
	Id  string
	Err error
}

var (
	addCh      chan *payload
	ListCh     chan error
	ClearCh    chan struct{}
	checkLimit chan chan bool
	GetErrCh   chan Errors
	ReqErrCh   chan struct{}
)

func CheckLimit(lc chan bool) {
	checkLimit <- lc
}

func Add(logid string, err error) {
	addCh <- &payload{logid, err}
}

func PowerOn(ctx context.Context, wp *sync.WaitGroup, wgEnd *sync.WaitGroup) {

	defer wgEnd.Done()

	slog.Log("errlog: ", "Powering on...")
	wp.Done()

	var (
		pld      *payload
		errors   Errors
		errLimit = 5
		lc       chan bool
	)

	addCh = make(chan *payload)
	ReqErrCh = make(chan struct{}, 1)
	//	Add = make(chan error)
	ClearCh = make(chan struct{})
	checkLimit = make(chan chan bool)
	GetErrCh = make(chan Errors)

	for {

		select {

		case pld = <-addCh:

			slog.Log(pld.Id, pld.Err.Error())
			errors = append(errors, pld)

		case lc = <-checkLimit:

			lc <- len(errors) > errLimit

		case <-ReqErrCh:

			// request can only be performed in zero concurrency otherwise
			// a copy of errors should be performed
			GetErrCh <- errors

		case <-ctx.Done():
			slog.Log("errlog: ", "Powering down...")
			return

		}
	}
}
