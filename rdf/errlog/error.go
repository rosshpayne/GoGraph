package errlog

import (
	"context"
	"fmt"
	"strings"
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

	var errmsg strings.Builder
	for {

		select {

		case pld = <-addCh:

			errmsg.WriteString("Error in ")
			errmsg.WriteString(pld.Id)
			errmsg.WriteString(".  Msg: [")
			errmsg.WriteString(pld.Err.Error())
			errmsg.WriteByte(']')
			slog.Log(pld.Id, errmsg.String())
			errmsg.Reset()
			errors = append(errors, pld)
			if len(errors) > errLimit {
				panic(fmt.Errorf("Number of errors exceeds limit of %d", errLimit))
			}

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
