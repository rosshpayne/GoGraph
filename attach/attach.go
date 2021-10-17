package main

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/GoGraph/attach/anmgr"
	"github.com/GoGraph/attach/db"
	"github.com/GoGraph/attach/ds"
	"github.com/GoGraph/attach/execute"
	"github.com/GoGraph/errlog"
	"github.com/GoGraph/grmgr"
	slog "github.com/GoGraph/syslog"
	//"github.com/GoGraph/tbl"
	//"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/util"
)

const (
	logid = "attach:"
)

var (
	ctx    context.Context
	wpEnd  sync.WaitGroup
	cancel context.CancelFunc
)

func syslog(s string) {
	slog.Log("dp: ", s)
}

//var attachers = flag.Int("a", 1, "Attachers: ")
var attachers = flag.Int("c", 6, "attacher goroutines: ")
var showsql = flag.Bool("sql", false, "ShowSQL: ")
var debug = flag.Bool("d", false, "Debug: ")
var runId int64

func GetRunId() int64 {
	return runId
}

func main() {

	flag.Parse()

	syslog(fmt.Sprintf("Argument: attachers: %d", *attachers))
	syslog(fmt.Sprintf("Argument: showsql: %v", *showsql))
	syslog(fmt.Sprintf("Argument: debug: %v", *debug))

	var (
		edgeCh         chan *ds.Edge
		wpEnd, wpStart sync.WaitGroup
		runNow         bool // whether to run attachNode on current edge
		err            error
	)

	// allocate a run id
	runId, err = db.MakeRunId(logid, "Attach")
	if err != nil {
		fmt.Println(fmt.Sprintf("Error in  MakeRunId() : %s", err))
		return
	}
	t0 := time.Now()

	ctx, cancel := context.WithCancel(context.Background())
	edgeCh = make(chan *ds.Edge, 2)

	wpEnd.Add(4)
	wpStart.Add(4)
	// supporting routine
	go sourceEdge(ctx, &wpStart, &wpEnd, edgeCh)
	// services
	//go stop.PowerOn(ctx, &wpStart, &wpEnd)    // detect a kill action (?) to terminate program alt just kill-9 it.
	go grmgr.PowerOn(ctx, &wpStart, &wpEnd, runId) // concurrent goroutine manager service
	go errlog.PowerOn(ctx, &wpStart, &wpEnd)       // error logging service
	go anmgr.PowerOn(ctx, &wpStart, &wpEnd)        // attach node service
	//go monitor.PowerOn(ctx, &wpStart, &wpEnd) // repository of system statistics service
	wpStart.Wait()

	limiterAttach := grmgr.New("nodeAttach", *attachers)

	respch := make(chan bool)

	var (
		edges = 0
		wg    sync.WaitGroup
	)

	for e := range edgeCh {

		e.RespCh = respch

		anmgr.AttachNowCh <- e

		runNow = <-e.RespCh

		if runNow {

			op := AttachOp{Puid: e.Puid, Cuid: e.Cuid, Sortk: e.Sortk}

			limiterAttach.Ask()
			<-limiterAttach.RespCh()
			edges++
			wg.Add(1)

			go execute.AttachNode(util.UID(e.Cuid), util.UID(e.Puid), e.Sortk, e, &wg, limiterAttach, &op)
		}

	}
	wg.Wait()
	t1 := time.Now()
	limiterAttach.Unregister()
	// send cancel to all registered goroutines
	cancel()
	wpEnd.Wait()
	db.FinishRun(runId)
	syslog(fmt.Sprintf("Attach operation finished. Edges: %d  Duration: %s ", edges, t1.Sub(t0)))
}

func sourceEdge(ctx context.Context, wp *sync.WaitGroup, wgEnd *sync.WaitGroup, edgeCh chan<- *ds.Edge) {

	defer wgEnd.Done()
	wp.Done()

	const logid = "SourceEdge:"

	slog.Log(logid, "Powering on...")

	for {

		var err error

		ns, eof := db.ScanForNodes()

		for _, n := range ns {

			edge, err := db.FetchEdge(n)

			if err != nil {
				err = err
				break
			}

			edgeCh <- edge
		}
		if err != nil {
			fmt.Println("Error in sourceEdge, %s", err.Error)
			break
		}
		if eof {
			break
		}
		// poll for cancel msg
		select {
		case <-ctx.Done():
			slog.Log(logid, "Premature shutdown")
			break
		default:
		}
	}
	close(edgeCh)
	slog.Log(logid, "Powering off....")
}
